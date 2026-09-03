"""Cross-platform HTTP load probe using only the Python standard library."""

from __future__ import annotations

import argparse
import json
import math
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1)]


def request(url: str) -> tuple[float, int]:
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(f"{url.rstrip('/')}/ready", timeout=10) as response:
            status = response.status
    except Exception:
        status = 0
    return (time.perf_counter() - started) * 1000, status


def default_gateway_url() -> str:
    if "LAB28_GATEWAY_URL" in os.environ:
        return os.environ["LAB28_GATEWAY_URL"]
    if "LAB28_GATEWAY_PORT" in os.environ:
        return f"http://localhost:{os.environ['LAB28_GATEWAY_PORT']}"
    for env_path in (Path(".env"), Path("/tmp/lab28-ports.local")):
        if env_path.is_file():
            try:
                for line in env_path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line.startswith("LAB28_GATEWAY_URL="):
                        return line.split("=", 1)[1].strip().strip("'\"")
                    if line.startswith("LAB28_GATEWAY_PORT="):
                        port = line.split("=", 1)[1].strip().strip("'\"")
                        return f"http://localhost:{port}"
            except Exception:
                pass
    for candidate in ("http://localhost:8080", "http://localhost:18080"):
        try:
            with urllib.request.urlopen(f"{candidate}/ready", timeout=1.0) as r:
                if r.status in (200, 429):
                    return candidate
        except Exception:
            pass
    return "http://localhost:8080"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=default_gateway_url())
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(lambda _: request(args.url), range(args.requests)))
    durations = [duration for duration, _ in results]
    statuses: dict[str, int] = {}
    for _, status in results:
        statuses[str(status)] = statuses.get(str(status), 0) + 1
    print(
        json.dumps(
            {
                "requests": args.requests,
                "workers": args.workers,
                "status_counts": statuses,
                "latency_ms": {
                    "p50": percentile(durations, 0.50),
                    "p95": percentile(durations, 0.95),
                    "p99": percentile(durations, 0.99),
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
