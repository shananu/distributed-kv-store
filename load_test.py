"""
Standalone load-test script for the distributed KV store.
Not part of the FastAPI app - run separately once the cluster is up.

Usage:
    python load_test.py --requests 1000 --concurrency 20
"""

import argparse
import random
import string
import statistics
import time
import httpx


def random_key(prefix: str = "loadtest") -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{prefix}-{suffix}"


def random_value() -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=32))


def run_load_test(base_urls: list[str], total_requests: int, concurrency: int, read_ratio: float):
    """
    Sends a mix of PUT and GET requests, round-robining across all
    node URLs so we exercise the coordinator/forwarding path just
    like a real client hitting a load balancer would.

    read_ratio: fraction of requests that are GETs (rest are PUTs).
    A key is only ever GET-able after it's been PUT at least once,
    so we maintain a running pool of written keys to GET from.
    """
    client = httpx.Client(timeout=5.0)
    written_keys: list[str] = []

    latencies_ms: list[float] = []
    errors = 0
    status_counts: dict[int, int] = {}

    start_time = time.monotonic()

    # NOTE: kept intentionally single-threaded/sequential rather than
    # using asyncio or a thread pool for real concurrency. This is a
    # scope cut worth naming: true concurrent load-testing would need
    # asyncio.gather or a thread pool to actually saturate the cluster.
    # This script gives you real sequential-latency numbers, which is
    # still meaningful data, just not peak-throughput numbers.
    for i in range(total_requests):
        node_url = base_urls[i % len(base_urls)]
        do_read = written_keys and random.random() < read_ratio

        req_start = time.monotonic()
        try:
            if do_read:
                key = random.choice(written_keys)
                resp = client.get(f"{node_url}/key/{key}")
            else:
                key = random_key()
                resp = client.put(f"{node_url}/key/{key}", json={"value": random_value()})
                if resp.status_code == 200:
                    written_keys.append(key)

            latency_ms = (time.monotonic() - req_start) * 1000
            latencies_ms.append(latency_ms)
            status_counts[resp.status_code] = status_counts.get(resp.status_code, 0) + 1

        except httpx.HTTPError:
            errors += 1

        if (i + 1) % 50 == 0:
            print(f"  ... {i + 1}/{total_requests} requests sent")

    total_elapsed = time.monotonic() - start_time

    print("\n=== Load Test Results ===")
    print(f"Total requests attempted: {total_requests}")
    print(f"Successful responses:     {len(latencies_ms)}")
    print(f"Errors (connection-level): {errors}")
    print(f"Status code breakdown:    {status_counts}")
    print(f"Total wall time:          {total_elapsed:.2f}s")
    print(f"Observed throughput:      {len(latencies_ms) / total_elapsed:.2f} ops/sec")

    if latencies_ms:
        sorted_lat = sorted(latencies_ms)
        p50 = statistics.median(sorted_lat)
        p95 = sorted_lat[int(len(sorted_lat) * 0.95)]
        p99 = sorted_lat[int(len(sorted_lat) * 0.99)]
        print(f"Latency p50:              {p50:.2f} ms")
        print(f"Latency p95:              {p95:.2f} ms")
        print(f"Latency p99:              {p99:.2f} ms")
        print(f"Latency mean:             {statistics.mean(sorted_lat):.2f} ms")
        print(f"Latency max:              {max(sorted_lat):.2f} ms")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load test the distributed KV store")
    parser.add_argument("--requests", type=int, default=1000, help="Total number of requests to send")
    parser.add_argument("--concurrency", type=int, default=20, help="Number of concurrent requests to send")
    parser.add_argument("--read-ratio", type=float, default=0.5, help="Fraction of requests that are GETs (0.0-1.0)")
    parser.add_argument(
        "--nodes",
        type=str,
        default="http://localhost:8001,http://localhost:8002,http://localhost:8003,http://localhost:8004,http://localhost:8005",
        help="Comma-separated base URLs of nodes to round-robin across",
    )
    args = parser.parse_args()

    node_urls = args.nodes.split(",")
    print(f"Running load test: {args.requests} requests across {len(node_urls)} node(s), read_ratio={args.read_ratio}")
    run_load_test(node_urls, args.requests, args.concurrency, args.read_ratio)