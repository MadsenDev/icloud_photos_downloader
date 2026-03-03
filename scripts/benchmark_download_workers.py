#!/usr/bin/env python3
"""Synthetic benchmark for download-worker limiter settings."""

from __future__ import annotations

import argparse
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor

from icloudpd.limiter import AdaptiveDownloadLimiter


def run_case(workers: int, tasks: int, latency_seconds: float, throttle_probability: float) -> dict:
    limiter = AdaptiveDownloadLimiter(
        max_workers=workers,
        min_workers=1,
        cooldown_seconds=max(0.0, latency_seconds * 2),
        increase_every=5,
    )
    errors = 0
    done = 0

    def unit_of_work() -> bool:
        nonlocal errors
        with limiter.slot(timeout=2.0):
            time.sleep(latency_seconds)
            throttled = random.random() < throttle_probability
            if throttled:
                limiter.on_throttle()
                errors += 1
                return False
            limiter.on_success()
            return True

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, workers * 2)) as pool:
        futures = [pool.submit(unit_of_work) for _ in range(tasks)]
        for future in futures:
            if future.result():
                done += 1
    elapsed = time.perf_counter() - start

    return {
        "workers": workers,
        "tasks": tasks,
        "successes": done,
        "errors": errors,
        "error_rate": (errors / tasks) if tasks else 0.0,
        "throughput_tasks_per_sec": (tasks / elapsed) if elapsed > 0 else 0.0,
        "elapsed_seconds": elapsed,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=200)
    parser.add_argument("--latency-seconds", type=float, default=0.01)
    parser.add_argument("--throttle-probability", type=float, default=0.03)
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--out", type=str, default=None)
    args = parser.parse_args()

    random.seed(args.seed)
    results = [
        run_case(
            workers=workers,
            tasks=args.tasks,
            latency_seconds=args.latency_seconds,
            throttle_probability=args.throttle_probability,
        )
        for workers in args.workers
    ]
    payload = {
        "timestamp_epoch": time.time(),
        "seed": args.seed,
        "tasks": args.tasks,
        "latency_seconds": args.latency_seconds,
        "throttle_probability": args.throttle_probability,
        "results": results,
    }
    rendered = json.dumps(payload, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(rendered)
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
