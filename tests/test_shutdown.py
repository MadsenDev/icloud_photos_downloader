import json
import logging
import os
import shutil
import tempfile
from unittest import TestCase

from icloudpd.base import (
    EXIT_CANCELLED,
    ShutdownController,
    _process_all_users_once,
)
from icloudpd.config import GlobalConfig
from icloudpd.log_level import LogLevel
from icloudpd.mfa_provider import MFAProvider
from icloudpd.password_provider import PasswordProvider
from icloudpd.status import StatusExchange


class ShutdownTestCase(TestCase):
    def test_shutdown_controller_requests_stop(self) -> None:
        status_exchange = StatusExchange()
        shutdown = ShutdownController(status_exchange)
        self.assertFalse(shutdown.requested())

        shutdown.request_stop("SIGINT")
        self.assertTrue(shutdown.requested())
        self.assertEqual(shutdown.signal_name(), "SIGINT")
        self.assertTrue(status_exchange.get_progress().cancel)
        self.assertFalse(shutdown.sleep_or_stop(0.01))

    def test_cancelled_run_writes_cancelled_summary(self) -> None:
        tmpdir = tempfile.mkdtemp(prefix="icloudpd-cancelled-summary-")
        self.addCleanup(lambda: shutil.rmtree(tmpdir, ignore_errors=True))
        metrics_path = os.path.join(tmpdir, "metrics.json")

        global_config = GlobalConfig(
            help=False,
            version=False,
            use_os_locale=False,
            only_print_filenames=False,
            log_level=LogLevel.INFO,
            log_format="text",
            no_progress_bar=True,
            threads_num=1,
            domain="com",
            watch_with_interval=None,
            password_providers=[PasswordProvider.PARAMETER],
            mfa_provider=MFAProvider.CONSOLE,
            max_retries=0,
            backoff_base_seconds=1.0,
            backoff_max_seconds=1.0,
            respect_retry_after=True,
            throttle_cooldown_seconds=0.0,
            metrics_json=metrics_path,
        )

        status_exchange = StatusExchange()
        shutdown = ShutdownController(status_exchange)
        shutdown.request_stop("SIGTERM")
        logger = logging.getLogger("icloudpd-test-shutdown")

        result = _process_all_users_once(global_config, [], logger, status_exchange, shutdown)
        self.assertEqual(result, EXIT_CANCELLED)
        with open(metrics_path, encoding="utf-8") as f:
            summary = json.load(f)
        self.assertEqual(summary["exit_code"], EXIT_CANCELLED)
        self.assertEqual(summary["status"], "cancelled")
        self.assertEqual(summary["users_total"], 0)
