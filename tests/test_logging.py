import io
import json
from unittest import TestCase
from unittest.mock import patch

from icloudpd.base import create_logger
from icloudpd.config import GlobalConfig
from icloudpd.log_level import LogLevel
from icloudpd.mfa_provider import MFAProvider
from icloudpd.password_provider import PasswordProvider


class LoggingTestCase(TestCase):
    def test_json_log_mode_emits_structured_fields(self) -> None:
        config = GlobalConfig(
            help=False,
            version=False,
            use_os_locale=False,
            only_print_filenames=False,
            log_level=LogLevel.INFO,
            log_format="json",
            no_progress_bar=True,
            threads_num=1,
            domain="com",
            watch_with_interval=None,
            password_providers=[PasswordProvider.PARAMETER],
            mfa_provider=MFAProvider.CONSOLE,
            max_retries=0,
            backoff_base_seconds=5.0,
            backoff_max_seconds=300.0,
            respect_retry_after=True,
            throttle_cooldown_seconds=60.0,
        )
        stream = io.StringIO()
        with patch("sys.stdout", stream):
            logger = create_logger(config)
            logger.info("hello structured logs")

        payload = json.loads(stream.getvalue().strip())
        self.assertEqual(payload["level"], "INFO")
        self.assertEqual(payload["message"], "hello structured logs")
        self.assertEqual(payload["logger"], "icloudpd")
        self.assertIsNotNone(payload["run_id"])
        self.assertIn("asset_id", payload)
        self.assertIn("attempt", payload)
        self.assertIn("http_status", payload)

    def test_json_log_mode_redacts_sensitive_values(self) -> None:
        config = GlobalConfig(
            help=False,
            version=False,
            use_os_locale=False,
            only_print_filenames=False,
            log_level=LogLevel.INFO,
            log_format="json",
            no_progress_bar=True,
            threads_num=1,
            domain="com",
            watch_with_interval=None,
            password_providers=[PasswordProvider.PARAMETER],
            mfa_provider=MFAProvider.CONSOLE,
            max_retries=0,
            backoff_base_seconds=5.0,
            backoff_max_seconds=300.0,
            respect_retry_after=True,
            throttle_cooldown_seconds=60.0,
        )
        stream = io.StringIO()
        with patch("sys.stdout", stream):
            logger = create_logger(config)
            logger.info(
                'payload={"password":"secret"} token=abc123 Authorization=Bearer mytoken cookie=rawcookie'
            )

        payload = json.loads(stream.getvalue().strip())
        message = payload["message"]
        self.assertNotIn("secret", message)
        self.assertNotIn("abc123", message)
        self.assertNotIn("mytoken", message)
        self.assertNotIn("rawcookie", message)
        self.assertIn("REDACTED", message)

    def test_text_log_mode_redacts_sensitive_values(self) -> None:
        config = GlobalConfig(
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
            backoff_base_seconds=5.0,
            backoff_max_seconds=300.0,
            respect_retry_after=True,
            throttle_cooldown_seconds=60.0,
        )
        stream = io.StringIO()
        with patch("sys.stdout", stream):
            logger = create_logger(config)
            logger.info("password=topsecret token=qwerty scnt=abcdef")

        output = stream.getvalue()
        self.assertNotIn("topsecret", output)
        self.assertNotIn("qwerty", output)
        self.assertNotIn("abcdef", output)
        self.assertIn("password=REDACTED", output)
