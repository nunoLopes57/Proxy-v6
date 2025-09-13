
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

\"\"\"
Async, high-concurrency checker with proxy rotation (aiohttp)

USAGE (examples):
  python async_main.py --data items.txt --proxies proxies.txt \
    --out-valid valid.txt --out-invalid invalid.txt \
    --concurrency 1000 --retries 3 --timeout 20 --batch-size 100 \
    --check-url https://example.com/health --method GET

DATA & PROXIES:
  - items.txt: one item per line (e.g., email, id, url, ...).
  - proxies.txt: one per line in format ip:port:user:pass (or host:port:user:pass).
    We'll convert to proxy URL: http://user:pass@host:port

VALIDATION RULE (EDIT THIS TO YOUR NEEDS):
  - By default: HTTP 2xx => VALID; others => INVALID.
  - You can customize parse_result() to inspect JSON/text.

NOTES:
  - Requires: aiohttp (pip install aiohttp).
  - This is a template to adapt into your project; plug your business logic
    into build_request() and parse_result().
\"\"\"

import asyncio
import aiohttp
import argparse
import sys
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlencode

# ------------------------- Config & CLI -------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Async checker with proxy rotation")
    p.add_argument("--data", required=True, help="Path to items file (one per line)")
    p.add_argument("--proxies", required=False, help="Path to proxies file ip:port:user:pass")
    p.add_argument("--out-valid", default="valid.txt", help="Output file for VALID items")
    p.add_argument("--out-invalid", default="invalid.txt", help="Output file for INVALID items")
    p.add_argument("--concurrency", type=int, default=500, help="Max concurrent tasks")
    p.add_argument("--retries", type=int, default=2, help="Retries per request (not counting first try)")
    p.add_argument("--timeout", type=float, default=15.0, help="Per-request timeout (seconds)")
    p.add_argument("--batch-size", type=int, default=100, help="Batch write size")
    p.add_argument("--check-url", required=True, help="Base URL to hit for each item")
    p.add_argument("--method", choices=["GET", "POST"], default="GET", help="HTTP method")
    p.add_argument("--header", action="append", default=[], help='Extra headers, repeatable, format: Key: Value')
    p.add_argument("--param-key", default="q", help="Query/body key for the item value")
    p.add_argument("--json", action="store_true", help="Send POST body as JSON (else form-encoded)")
    p.add_argument("--verify-ssl", action="store_true", help="Verify SSL certs (default false)")
    return p.parse_args()


# ------------------------- IO Helpers ---------------------------

def read_lines(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        print(f"[!] File not found: {path}", file=sys.stderr)
        sys.exit(2)
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        return [line.strip() for line in f if line.strip()]


def parse_headers(header_list: List[str]) -> dict:
    headers = {}
    for h in header_list:
        if ":" in h:
            k, v = h.split(":", 1)
            headers[k.strip()] = v.strip()
    return headers


def proxy_line_to_url(line: str) -> Optional[str]:
    \"\"\"
    Convert 'host:port:user:pass' -> 'http://user:pass@host:port'
    Returns None if line malformed.
    \"\"\"
    parts = line.split(":")
    if len(parts) != 4:
        return None
    host, port, user, pw = parts
    return f"http://{user}:{pw}@{host}:{port}"


# ------------------------- Business Logic -----------------------

def build_request(args, item: str) -> Tuple[str, dict, Optional[dict], Optional[bytes]]:
    \"\"\"
    Build request for a given item.
    Returns (url, headers, json_body, data_bytes)
    - If method=GET: item sent as query param (?key=item)
    - If method=POST: item sent as JSON {key: item} or form-encoded (key=item)
    \"\"\"
    headers = parse_headers(args.header)

    if args.method == "GET":
        url = f\"{args.check_url}?{urlencode({args.param_key: item})}\"
        return url, headers, None, None
    else:
        url = args.check_url
        if args.json:
            return url, headers, {args.param_key: item}, None
        else:
            body = urlencode({args.param_key: item}).encode()
            headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
            return url, headers, None, body


def parse_result(status: int, text: str) -> bool:
    \"\"\"
    Decide VALID/INVALID from HTTP response.
    Default rule: 2xx -> VALID; others -> INVALID.
    Customize this for your API (e.g., JSON field checks).
    \"\"\"
    return 200 <= status < 300


# ------------------------- Async Engine -------------------------

class ProxyRotator:
    \"\"\"Simple round-robin over proxy URLs using an asyncio.Queue.\"\"\"
    def __init__(self, proxies: List[str]):
        self.q: asyncio.Queue[str] = asyncio.Queue()
        for p in proxies:
            self.q.put_nowait(p)

    async def get(self) -> Optional[str]:
        if self.q.empty():
            return None
        proxy = await self.q.get()
        # immediately put it back to the end for round-robin
        await self.q.put(proxy)
        return proxy


async def fetch_with_retries(session: aiohttp.ClientSession, method: str, url: str,
                             headers: dict, json_body: Optional[dict], data_bytes: Optional[bytes],
                             timeout: float, retries: int, proxy: Optional[str]) -> Tuple[int, str]:
    last_exc = None
    for attempt in range(retries + 1):  # include first try
        try:
            # aiohttp timeout
            to = aiohttp.ClientTimeout(total=timeout)
            async with session.request(method, url, headers=headers, json=json_body, data=data_bytes,
                                       timeout=to, proxy=proxy) as resp:
                text = await resp.text(errors="ignore")
                return resp.status, text
        except Exception as e:
            last_exc = e
            # Exponential backoff: 0.25, 0.5, 1.0, ...
            await asyncio.sleep(0.25 * (2 ** attempt))
    # If all failed, bubble up a "network error" as status 0
    return 0, f"ERROR: {type(last_exc).__name__}: {last_exc}"


async def worker(name: int, args, items_q: asyncio.Queue, results_q: asyncio.Queue,
                 session: aiohttp.ClientSession, proxies: Optional[ProxyRotator], sem: asyncio.Semaphore):
    while True:
        item = await items_q.get()
        if item is None:
            items_q.task_done()
            break

        url, headers, json_body, data_bytes = build_request(args, item)

        proxy = await proxies.get() if proxies else None

        async with sem:
            status, text = await fetch_with_retries(
                session, args.method, url, headers, json_body, data_bytes,
                timeout=args.timeout, retries=args.retries, proxy=proxy
            )

        ok = parse_result(status, text)
        await results_q.put((item, "VALID" if ok else "INVALID"))
        items_q.task_done()


async def writer_task(results_q: asyncio.Queue, out_valid: Path, out_invalid: Path, batch_size: int):
    batch_v, batch_i = [], []
    # Open files once; line-buffered writes
    with out_valid.open("a", encoding="utf-8") as fv, out_invalid.open("a", encoding="utf-8") as fi:
        while True:
            item = await results_q.get()
            if item is None:
                # Flush and exit
                if batch_v:
                    fv.writelines(x + "\n" for x in batch_v)
                    fv.flush()
                    batch_v.clear()
                if batch_i:
                    fi.writelines(x + "\n" for x in batch_i)
                    fi.flush()
                    batch_i.clear()
                results_q.task_done()
                break

            value, typ = item
            if typ == "VALID":
                batch_v.append(value)
                if len(batch_v) >= batch_size:
                    fv.writelines(x + "\n" for x in batch_v)
                    fv.flush()
                    batch_v.clear()
            else:
                batch_i.append(value)
                if len(batch_i) >= batch_size:
                    fi.writelines(x + "\n" for x in batch_i)
                    fi.flush()
                    batch_i.clear()

            results_q.task_done()


async def main_async(args):
    items = read_lines(args.data)
    proxies = None
    if args.proxies:
        raw = read_lines(args.proxies)
        urls = [proxy_line_to_url(x) for x in raw]
        urls = [u for u in urls if u]
        if urls:
            proxies = ProxyRotator(urls)

    # Queues
    items_q: asyncio.Queue = asyncio.Queue(maxsize=args.concurrency * 2)
    results_q: asyncio.Queue = asyncio.Queue(maxsize=args.concurrency * 4)

    # Prefill items
    for it in items:
        items_q.put_nowait(it)

    # Add sentinels for workers
    num_workers = args.concurrency
    for _ in range(num_workers):
        items_q.put_nowait(None)

    # HTTP session
    conn = aiohttp.TCPConnector(ssl=args.verify_ssl, limit_per_host=0, limit=args.concurrency*2)
    async with aiohttp.ClientSession(connector=conn) as session:
        # Semaphore to cap true concurrency
        sem = asyncio.Semaphore(args.concurrency)

        # Start writer
        writer = asyncio.create_task(writer_task(results_q, Path(args.out_valid), Path(args.out_invalid), args.batch_size))

        # Start workers
        workers = [
            asyncio.create_task(worker(i, args, items_q, results_q, session, proxies, sem))
            for i in range(num_workers)
        ]

        # Wait for workers to finish
        await asyncio.gather(*workers)

        # Signal writer to finish
        await results_q.put(None)
        await writer

    print(f"[DONE] Processed: {len(items)} items | out: {args.out_valid}, {args.out_invalid}")


def main():
    args = parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user", file=sys.stderr)


if __name__ == "__main__":
    main()
