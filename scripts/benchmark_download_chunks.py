#!/usr/bin/env python3
"""Synthetic benchmark for download chunk size and verification tradeoffs."""

from __future__ import annotations

import argparse
import contextlib
import datetime
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from icloudpd import download


@dataclass(frozen=True)
class Case:
    chunk_bytes: int
    verify_size: bool
    verify_checksum: bool


class FakeResponse:
    def __init__(self, payload: bytes, total_bytes: int) -> None:
        self._payload = payload
        self._total_bytes = total_bytes
        self._payload_len = len(payload)

    def iter_content(self, chunk_size: int):
        sent = 0
        while sent < self._total_bytes:
            remaining = self._total_bytes - sent
            take = chunk_size if remaining >= chunk_size else remaining
            start = sent % self._payload_len
            if start + take <= self._payload_len:
                chunk = self._payload[start : start + take]
            else:
                part1 = self._payload[start:]
                left = take - len(part1)
                repeats = left // self._payload_len
                tail = left % self._payload_len
                chunk = part1 + (self._payload * repeats) + self._payload[:tail]
            sent += len(chunk)
            yield chunk


def _expected_content(payload: bytes, total_bytes: int) -> bytes:
    return (payload * ((total_bytes // len(payload)) + 1))[:total_bytes]


def run_single_download(case: Case, payload: bytes, total_bytes: int, root_dir: str) -> dict:
    expected = _expected_content(payload, total_bytes)
    expected_size = len(expected)
    checksum = hashlib.md5(expected).digest()

    thread_id = threading.get_ident()
    base = os.path.join(
        root_dir,
        f"case-{case.chunk_bytes}-{int(case.verify_size)}-{int(case.verify_checksum)}-{thread_id}-{time.time_ns()}",
    )
    temp_path = f"{base}.part"
    out_path = f"{base}.bin"

    if os.path.exists(temp_path):
        os.remove(temp_path)
    if os.path.exists(out_path):
        os.remove(out_path)

    response = FakeResponse(payload=payload, total_bytes=total_bytes)
    created = datetime.datetime(2026, 3, 3, tzinfo=datetime.timezone.utc)

    start_wall = time.perf_counter()
    start_cpu = time.process_time()
    download.set_download_chunk_bytes(case.chunk_bytes)
    write_ok = download.download_response_to_path(
        response,
        temp_path,
        append_mode=False,
        download_path=out_path,
        created_date=created,
    )
    verify_ok = download.verify_download_integrity(
        logger=logging.getLogger("benchmark"),
        download_path=out_path,
        expected_size=expected_size,
        expected_checksum=checksum,
        verify_size=case.verify_size,
        verify_checksum=case.verify_checksum,
    )
    elapsed = time.perf_counter() - start_wall
    cpu = time.process_time() - start_cpu

    with contextlib.suppress(OSError):
        os.remove(out_path)

    return {
        "chunk_bytes": case.chunk_bytes,
        "verify_size": case.verify_size,
        "verify_checksum": case.verify_checksum,
        "ok": bool(write_ok and verify_ok),
        "size_bytes": expected_size,
        "elapsed_seconds": elapsed,
        "cpu_seconds": cpu,
        "throughput_mib_per_sec": (expected_size / (1024 * 1024) / elapsed) if elapsed > 0 else 0.0,
    }


def run_case(case: Case, *, size_mib: int, workers: int, iterations: int) -> dict:
    payload = b"icloudpd-benchmark-payload-"
    total_bytes = size_mib * 1024 * 1024

    with tempfile.TemporaryDirectory(prefix="icloudpd-benchmark-chunks-") as temp_dir:
        tracemalloc.start()
        per_run: list[dict] = []
        for _ in range(iterations):
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [
                    pool.submit(run_single_download, case, payload, total_bytes, temp_dir)
                    for _ in range(workers)
                ]
                for future in futures:
                    per_run.append(future.result())
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    throughput = [run["throughput_mib_per_sec"] for run in per_run]
    cpu = [run["cpu_seconds"] for run in per_run]

    return {
        "chunk_bytes": case.chunk_bytes,
        "verify_size": case.verify_size,
        "verify_checksum": case.verify_checksum,
        "workers": workers,
        "iterations": iterations,
        "size_mib_per_worker": size_mib,
        "runs": len(per_run),
        "all_ok": all(run["ok"] for run in per_run),
        "throughput_mib_per_sec_avg": sum(throughput) / len(throughput),
        "cpu_seconds_avg": sum(cpu) / len(cpu),
        "tracemalloc_peak_bytes": peak,
        "theoretical_stream_buffer_bound_bytes": workers * case.chunk_bytes,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--size-mib", type=int, default=16)
    parser.add_argument("--iterations", type=int, default=2)
    parser.add_argument("--chunk-bytes", type=int, nargs="+", default=[65536, 262144, 1048576])
    parser.add_argument("--out", type=str, default=None)
    args = parser.parse_args()

    cases: list[Case] = []
    for chunk in args.chunk_bytes:
        cases.append(Case(chunk_bytes=chunk, verify_size=True, verify_checksum=False))
        cases.append(Case(chunk_bytes=chunk, verify_size=True, verify_checksum=True))

    results = [
        run_case(case, size_mib=args.size_mib, workers=args.workers, iterations=args.iterations)
        for case in cases
    ]

    payload = {
        "timestamp_epoch": time.time(),
        "workers": args.workers,
        "size_mib": args.size_mib,
        "iterations": args.iterations,
        "results": results,
    }

    rendered = json.dumps(payload, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(rendered)
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
