import datetime
from unittest import TestCase

from requests.exceptions import ChunkedEncodingError

from icloudpd.retry_utils import (
    RetryConfig,
    is_fatal_auth_config_error,
    is_session_invalid_error,
    is_throttle_error,
    is_transient_error,
    parse_retry_after_seconds,
)
from pyicloud_ipd.exceptions import (
    PyiCloud2SARequiredException,
    PyiCloudAPIResponseException,
    PyiCloudConnectionErrorException,
    PyiCloudFailedLoginException,
    PyiCloudFailedMFAException,
    PyiCloudNoStoredPasswordAvailableException,
    PyiCloudServiceNotActivatedException,
    PyiCloudServiceUnavailableException,
)


class RetryUtilsTestCase(TestCase):
    def test_parse_retry_after_numeric(self) -> None:
        self.assertEqual(parse_retry_after_seconds("120"), 120.0)

    def test_parse_retry_after_http_date(self) -> None:
        future = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=42)
        parsed = parse_retry_after_seconds(future.strftime("%a, %d %b %Y %H:%M:%S GMT"))
        assert parsed is not None
        self.assertTrue(0 < parsed <= 42)

    def test_transient_error_classification(self) -> None:
        self.assertTrue(is_transient_error(PyiCloudConnectionErrorException("test")))
        self.assertTrue(is_transient_error(PyiCloudServiceUnavailableException("test")))
        self.assertTrue(is_transient_error(PyiCloudAPIResponseException("throttle", "429")))
        self.assertTrue(is_transient_error(ChunkedEncodingError("chunk error")))
        self.assertFalse(is_transient_error(PyiCloudAPIResponseException("bad auth", "401")))

    def test_session_invalid_classification(self) -> None:
        self.assertTrue(
            is_session_invalid_error(PyiCloudAPIResponseException("Invalid global session", "500"))
        )
        self.assertFalse(is_session_invalid_error(PyiCloudAPIResponseException("other", "500")))

    def test_fatal_auth_config_classification(self) -> None:
        self.assertTrue(is_fatal_auth_config_error(PyiCloudFailedLoginException("bad credentials")))
        self.assertTrue(is_fatal_auth_config_error(PyiCloudFailedMFAException("mfa unavailable")))
        self.assertTrue(is_fatal_auth_config_error(PyiCloud2SARequiredException("user@example.com")))
        self.assertTrue(
            is_fatal_auth_config_error(PyiCloudNoStoredPasswordAvailableException("no password"))
        )
        self.assertTrue(
            is_fatal_auth_config_error(PyiCloudServiceNotActivatedException("web disabled", "X"))
        )
        self.assertFalse(is_fatal_auth_config_error(PyiCloudConnectionErrorException("test")))

    def test_throttle_classification(self) -> None:
        self.assertTrue(is_throttle_error(PyiCloudAPIResponseException("ACCESS_DENIED", "ACCESS_DENIED")))
        self.assertTrue(is_throttle_error(PyiCloudAPIResponseException("request throttled", "500")))
        self.assertFalse(is_throttle_error(PyiCloudAPIResponseException("other", "500")))

    def test_next_delay_respects_retry_after(self) -> None:
        config = RetryConfig(
            max_retries=2,
            backoff_base_seconds=1,
            backoff_max_seconds=300,
            respect_retry_after=True,
            throttle_cooldown_seconds=60,
            jitter_fraction=0,
        )
        delay = config.next_delay_seconds(1, retry_after="120", throttle_error=False)
        self.assertEqual(delay, 120)

    def test_next_delay_applies_throttle_cooldown(self) -> None:
        config = RetryConfig(
            max_retries=2,
            backoff_base_seconds=1,
            backoff_max_seconds=300,
            respect_retry_after=True,
            throttle_cooldown_seconds=60,
            jitter_fraction=0,
        )
        delay = config.next_delay_seconds(1, throttle_error=True)
        self.assertEqual(delay, 60)
