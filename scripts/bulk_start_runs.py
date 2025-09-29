import argparse
import asyncio
import sys
import math
from typing import Any, Dict, List

import aiohttp


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bulk trigger /runs/start for teams with non-empty endpoint"
    )
    p.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="Base URL of the API, e.g. http://localhost:8000",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=50,
        help="Number of concurrent requests (default: 50)",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=40.0,
        help="HTTP timeout seconds for each request (default: 40.0)",
    )
    return p.parse_args()


class Counters:
    def __init__(self, total: int) -> None:
        self.total = total
        self.processed = 0
        self.ok = 0
        self.skipped = 0
        self.failed = 0
        self._lock = asyncio.Lock()

    async def incr(self, which: str) -> None:
        async with self._lock:
            self.processed += 1
            if which == "ok":
                self.ok += 1
            elif which == "skipped":
                self.skipped += 1
            else:
                self.failed += 1


def format_progress(cnt: Counters, width: int = 30) -> str:
    total = max(cnt.total, 1)
    done = min(cnt.processed, total)
    filled = int(width * done / total)
    bar = "#" * filled + "." * (width - filled)
    return f"[{bar}] {done}/{total} | ok:{cnt.ok} skipped:{cnt.skipped} failed:{cnt.failed}"


async def fetch_candidates(session: aiohttp.ClientSession, base_url: str) -> List[Dict[str, Any]]:
    url = base_url.rstrip("/") + "/teams/with_endpoint"
    async with session.get(url) as r:
        if r.status >= 400:
            text = await r.text()
            raise RuntimeError(f"GET {url} failed: {r.status} {text}")
        data = await r.json()
        if not isinstance(data, list):
            raise RuntimeError("Unexpected response format from /teams/with_endpoint")
        return data


async def start_for_team(
    session: aiohttp.ClientSession,
    base_url: str,
    item: Dict[str, Any],
    cnt: Counters,
) -> None:
    tg_chat_id = item.get("tg_chat_id")
    name = item.get("name")
    try:
        url = base_url.rstrip("/") + "/runs/start"
        async with session.post(url, json={"tg_chat_id": tg_chat_id}) as r:
            if r.status == 200:
                payload = await r.json()
                run_id = payload.get("run_id")
                status = payload.get("status")
                print(f"OK  | {name} (tg:{tg_chat_id}) -> run_id={run_id}, status={status}")
                await cnt.incr("ok")
            elif r.status == 409:
                if str(r.headers.get("Content-Type", "")).startswith("application/json"):
                    body = await r.json()
                    detail = body.get("detail")
                else:
                    detail = await r.text()
                print(f"SKIP| {name} (tg:{tg_chat_id}) -> 409 {detail}")
                await cnt.incr("skipped")
            else:
                if str(r.headers.get("Content-Type", "")).startswith("application/json"):
                    body = await r.json()
                    detail = body.get("detail")
                else:
                    detail = await r.text()
                print(f"FAIL| {name} (tg:{tg_chat_id}) -> {r.status} {detail}")
                await cnt.incr("fail")
    except Exception as e:
        print(f"FAIL| {name} (tg:{tg_chat_id}) -> exception: {e}")
        await cnt.incr("fail")


async def main_async() -> int:
    args = parse_args()
    timeout = aiohttp.ClientTimeout(total=args.timeout)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        candidates = await fetch_candidates(session, args.api_url)

    total = len(candidates)
    if total == 0:
        print("No teams with endpoint found.")
        return 0

    print(f"Found {total} teams with endpoints. Processing in groups of {args.concurrency}...")
    counters = Counters(total)
    groups_count = math.ceil(total / args.concurrency)

    for group_number in range(groups_count):
        print(f"Handling group {group_number + 1}/{groups_count}")
        start_index = group_number * args.concurrency
        end_index = start_index + args.concurrency
        current = candidates[start_index:end_index]

        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [
                asyncio.create_task(start_for_team(session, args.api_url, item, counters))
                for item in current
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

        print(format_progress(counters))

    print("Done.")
    return 0


def main() -> None:
    try:
        rc = asyncio.run(main_async())
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)


if __name__ == "__main__":
    main()
