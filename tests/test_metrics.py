import json
import os
import shutil
import tempfile
from unittest import TestCase

from icloudpd.metrics import RunMetrics, write_metrics_json


class MetricsTestCase(TestCase):
    def test_run_metrics_snapshot_includes_expected_fields(self) -> None:
        metrics = RunMetrics(username="u1")
        metrics.start()
        metrics.on_asset_considered()
        metrics.on_download_attempt()
        metrics.on_download_success(123)
        metrics.on_retry()
        metrics.on_throttle()
        metrics.set_queue_depth(7)
        metrics.finish()

        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["username"], "u1")
        self.assertEqual(snapshot["run_mode"], "legacy_stateless")
        self.assertEqual(snapshot["downloads_attempted"], 1)
        self.assertEqual(snapshot["downloads_succeeded"], 1)
        self.assertEqual(snapshot["bytes_downloaded"], 123)
        self.assertIn("throughput_downloads_per_sec", snapshot)
        self.assertIn("success_gap", snapshot)

    def test_write_metrics_json(self) -> None:
        tmpdir = tempfile.mkdtemp(prefix="icloudpd-metrics-")
        self.addCleanup(lambda: shutil.rmtree(tmpdir, ignore_errors=True))
        out_path = os.path.join(tmpdir, "metrics.json")
        payload = {"users": [{"username": "u1"}]}
        write_metrics_json(out_path, payload)
        with open(out_path, encoding="utf-8") as f:
            parsed = json.load(f)
        self.assertEqual(parsed, payload)
