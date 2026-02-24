"""
OmniSQL Load Test — sustains ~500-1k QPS for 60 seconds.

Equivalent to a k6/Gatling script. Uses asyncio + aiohttp for high concurrency
with minimal overhead per request.

Usage:
    # Against prototype (port 8001)
    python prototype/tests/load_test.py

    # Against production gateway (port 8002)
    python prototype/tests/load_test.py --port 8002

    # Custom duration and concurrency
    python prototype/tests/load_test.py --duration 30 --concurrency 50
"""
import argparse
import asyncio
import sys
import time
from dataclasses import dataclass, field
from typing import List

import numpy as np

try:
    import aiohttp
except ImportError:
    print("aiohttp required: pip install aiohttp")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

QUERIES = [
    # Single-source (fast — should serve from cache after first hit)
    {
        "sql": "SELECT * FROM github.pull_requests WHERE status = 'merged' LIMIT 10",
        "metadata": {"max_staleness_ms": 5000},
    },
    # Cross-app join (heavier — exercises the full DAG pipeline)
    {
        "sql": (
            "SELECT gh.pr_id, ji.issue_key "
            "FROM github.pull_requests gh "
            "JOIN jira.issues ji ON gh.branch = ji.branch_name "
            "LIMIT 10"
        ),
        "metadata": {"max_staleness_ms": 5000},
    },
    # Projection pushdown (single source, small result)
    {
        "sql": "SELECT pr_id, author, status FROM github.pull_requests LIMIT 5",
        "metadata": {"max_staleness_ms": 5000},
    },
]


@dataclass
class Stats:
    """Collects per-request metrics during the load test."""
    latencies: List[float] = field(default_factory=list)
    status_codes: List[int] = field(default_factory=list)
    errors: int = 0
    start_time: float = 0.0
    end_time: float = 0.0


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

async def worker(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict,
    stats: Stats,
    stop_event: asyncio.Event,
    query_idx: int,
):
    """Single worker coroutine — sends requests in a loop until stop_event is set."""
    queries = QUERIES
    idx = query_idx
    while not stop_event.is_set():
        payload = queries[idx % len(queries)]
        idx += 1
        start = time.monotonic()
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                await resp.read()  # consume body
                elapsed = time.monotonic() - start
                stats.latencies.append(elapsed)
                stats.status_codes.append(resp.status)
        except asyncio.CancelledError:
            break
        except Exception:
            stats.errors += 1


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def run_load_test(
    port: int,
    duration_s: int,
    concurrency: int,
    ramp_up_s: int = 5,
):
    """
    Orchestrates a sustained load test with ramp-up phase.

    Phase 1 (ramp-up): linearly adds workers over `ramp_up_s` seconds.
    Phase 2 (sustained): all workers active until `duration_s` elapses.
    """
    # Detect which gateway we're hitting
    base_url = f"http://localhost:{port}"
    query_url = f"{base_url}/v1/query"

    # Prototype uses X-User-Token; production uses Authorization + X-Tenant-ID
    if port == 8001:
        headers = {
            "Content-Type": "application/json",
            "X-User-Token": "token_dev",
        }
    else:
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer token_dev",
            "X-Tenant-ID": "acme_corp",
        }

    # Verify server is reachable
    print(f"\n{'='*60}")
    print(f"  OmniSQL Load Test")
    print(f"  Target:      {base_url}")
    print(f"  Duration:    {duration_s}s (+ {ramp_up_s}s ramp-up)")
    print(f"  Concurrency: {concurrency} workers")
    print(f"{'='*60}\n")

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10),
        connector=aiohttp.TCPConnector(limit=concurrency * 2),
    ) as session:
        # Health check
        try:
            health_url = f"{base_url}/health" if port != 8001 else f"{base_url}/metrics"
            async with session.get(health_url) as resp:
                if resp.status != 200:
                    print(f"  [ERROR] Server returned {resp.status} on health check")
                    return
                print(f"  Server is up (health={resp.status})")
        except Exception as exc:
            print(f"  [ERROR] Cannot reach {base_url}: {exc}")
            print(f"  Make sure the server is running on port {port}")
            return

        stats = Stats()
        stop_event = asyncio.Event()
        tasks: List[asyncio.Task] = []

        stats.start_time = time.monotonic()

        # Ramp-up: add workers gradually
        workers_per_step = max(1, concurrency // ramp_up_s)
        launched = 0
        print(f"  Ramp-up: adding ~{workers_per_step} workers/sec ...")

        for step in range(ramp_up_s):
            batch = min(workers_per_step, concurrency - launched)
            for i in range(batch):
                t = asyncio.create_task(
                    worker(session, query_url, headers, stats, stop_event, launched + i)
                )
                tasks.append(t)
                launched += 1
            await asyncio.sleep(1.0)

        # Launch any remaining workers
        while launched < concurrency:
            t = asyncio.create_task(
                worker(session, query_url, headers, stats, stop_event, launched)
            )
            tasks.append(t)
            launched += 1

        print(f"  All {launched} workers active. Sustaining for {duration_s - ramp_up_s}s ...\n")

        # Sustained phase
        remaining = duration_s - ramp_up_s
        interval = 5  # progress report every 5s
        elapsed_report = 0
        while remaining > 0:
            wait = min(interval, remaining)
            await asyncio.sleep(wait)
            remaining -= wait
            elapsed_report += wait
            current_count = len(stats.latencies)
            elapsed_total = time.monotonic() - stats.start_time
            current_qps = current_count / elapsed_total if elapsed_total > 0 else 0
            print(f"  [{int(elapsed_total)}s] {current_count} requests | {current_qps:.0f} QPS")

        # Stop all workers
        stop_event.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        stats.end_time = time.monotonic()

    # ---------------------------------------------------------------------------
    # Report
    # ---------------------------------------------------------------------------

    total_duration = stats.end_time - stats.start_time
    total_requests = len(stats.latencies)

    if total_requests == 0:
        print("\n  [ERROR] No successful requests recorded.")
        return

    latencies_ms = np.array(stats.latencies) * 1000
    qps = total_requests / total_duration

    success_count = stats.status_codes.count(200)
    rate_limited = stats.status_codes.count(429)
    server_errors = sum(1 for c in stats.status_codes if c >= 500)

    print(f"\n{'='*60}")
    print(f"  RESULTS")
    print(f"{'='*60}")
    print(f"  Total Requests:   {total_requests}")
    print(f"  Total Duration:   {total_duration:.1f}s")
    print(f"  Throughput:       {qps:.0f} QPS")
    print(f"")
    print(f"  Latency:")
    print(f"    P50:  {np.percentile(latencies_ms, 50):.1f} ms")
    print(f"    P95:  {np.percentile(latencies_ms, 95):.1f} ms")
    print(f"    P99:  {np.percentile(latencies_ms, 99):.1f} ms")
    print(f"    Max:  {np.max(latencies_ms):.1f} ms")
    print(f"")
    print(f"  Status Codes:")
    print(f"    200 OK:           {success_count} ({success_count/total_requests*100:.1f}%)")
    if rate_limited:
        print(f"    429 Rate Limited: {rate_limited} ({rate_limited/total_requests*100:.1f}%)")
    if server_errors:
        print(f"    5xx Errors:       {server_errors} ({server_errors/total_requests*100:.1f}%)")
    if stats.errors:
        print(f"    Connection Errors:{stats.errors}")
    print(f"")

    # SLO check
    p50 = np.percentile(latencies_ms, 50)
    p95 = np.percentile(latencies_ms, 95)
    slo_p50 = p50 < 500
    slo_p95 = p95 < 1500
    slo_avail = (success_count / total_requests) > 0.999

    print(f"  SLO Compliance:")
    print(f"    P50 < 500ms:    {'PASS' if slo_p50 else 'FAIL'} ({p50:.1f}ms)")
    print(f"    P95 < 1.5s:     {'PASS' if slo_p95 else 'FAIL'} ({p95:.1f}ms)")
    print(f"    99.9%% success:  {'PASS' if slo_avail else 'FAIL'} ({success_count/total_requests*100:.2f}%%)")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="OmniSQL Load Test")
    parser.add_argument("--port", type=int, default=8001, help="Target port (8001=prototype, 8002=production)")
    parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds (default: 60)")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent workers (default: 50)")
    parser.add_argument("--ramp-up", type=int, default=5, help="Ramp-up seconds (default: 5)")
    args = parser.parse_args()

    asyncio.run(run_load_test(
        port=args.port,
        duration_s=args.duration,
        concurrency=args.concurrency,
        ramp_up_s=args.ramp_up,
    ))


if __name__ == "__main__":
    main()
