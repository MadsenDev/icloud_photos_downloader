"""Run metrics collection and export helpers."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass


@dataclass
class RunMetrics:
    username: str
    run_mode: str = "legacy_stateless"
    started_at_epoch: float = 0.0
    finished_at_epoch: float = 0.0
    assets_considered: int = 0
    downloads_attempted: int = 0
    downloads_succeeded: int = 0
    downloads_failed: int = 0
    bytes_downloaded: int = 0
    retries: int = 0
    throttle_events: int = 0
    low_disk_events: int = 0
    queue_depth_max: int = 0
    queue_depth_last: int = 0

    def start(self) -> None:
        self.started_at_epoch = time.time()

    def finish(self) -> None:
        self.finished_at_epoch = time.time()

    def on_asset_considered(self) -> None:
        self.assets_considered += 1

    def on_download_attempt(self) -> None:
        self.downloads_attempted += 1

    def on_download_success(self, bytes_written: int) -> None:
        self.downloads_succeeded += 1
        self.bytes_downloaded += max(0, bytes_written)

    def on_download_failed(self) -> None:
        self.downloads_failed += 1

    def on_retry(self) -> None:
        self.retries += 1

    def on_throttle(self) -> None:
        self.throttle_events += 1

    def on_low_disk(self) -> None:
        self.low_disk_events += 1

    def set_queue_depth(self, depth: int) -> None:
        normalized = max(0, depth)
        self.queue_depth_last = normalized
        if normalized > self.queue_depth_max:
            self.queue_depth_max = normalized

    def snapshot(self) -> dict[str, float | int | str]:
        elapsed = max(0.0, self.finished_at_epoch - self.started_at_epoch)
        return {
            "username": self.username,
            "run_mode": self.run_mode,
            "started_at_epoch": self.started_at_epoch,
            "finished_at_epoch": self.finished_at_epoch,
            "elapsed_seconds": elapsed,
            "assets_considered": self.assets_considered,
            "downloads_attempted": self.downloads_attempted,
            "downloads_succeeded": self.downloads_succeeded,
            "downloads_failed": self.downloads_failed,
            "success_gap": self.downloads_attempted - self.downloads_succeeded,
            "bytes_downloaded": self.bytes_downloaded,
            "throughput_downloads_per_sec": (
                self.downloads_succeeded / elapsed if elapsed > 0 else 0.0
            ),
            "retries": self.retries,
            "throttle_events": self.throttle_events,
            "queue_depth_max": self.queue_depth_max,
            "queue_depth_last": self.queue_depth_last,
            "low_disk_events": self.low_disk_events,
        }


def write_metrics_json(path: str, payload: dict[str, object]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)
