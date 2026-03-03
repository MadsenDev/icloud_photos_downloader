import logging
from unittest import TestCase
from unittest.mock import patch

from icloudpd.base import emit_throttle_alert_if_needed
from icloudpd.metrics import RunMetrics


class AlertsTestCase(TestCase):
    def test_emit_throttle_alert_below_threshold(self) -> None:
        logger = logging.getLogger("icloudpd-test-alerts")
        metrics = RunMetrics(username="u1")
        metrics.throttle_events = 2
        with patch.object(logger, "warning") as warning_mock:
            emitted = emit_throttle_alert_if_needed(logger, metrics, threshold=3)
        self.assertFalse(emitted)
        warning_mock.assert_not_called()

    def test_emit_throttle_alert_at_threshold(self) -> None:
        logger = logging.getLogger("icloudpd-test-alerts")
        metrics = RunMetrics(username="u1")
        metrics.throttle_events = 3
        with patch.object(logger, "warning") as warning_mock:
            emitted = emit_throttle_alert_if_needed(logger, metrics, threshold=3)
        self.assertTrue(emitted)
        warning_mock.assert_called_once()
