"""Shared retry policy and error classification utilities."""

from __future__ import annotations

import datetime
import random
from dataclasses import dataclass
from email.utils import parsedate_to_datetime

from requests.exceptions import (
    ChunkedEncodingError,
    ContentDecodingError,
    StreamConsumedError,
    UnrewindableBodyError,
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

_TRANSIENT_CODES = {"421", "429", "450", "500", "502", "503", "504", "ACCESS_DENIED"}
_THROTTLE_CODES = {"429", "ACCESS_DENIED"}
_SESSION_INVALID_MARKER = "invalid global session"


@dataclass(frozen=True)
class RetryConfig:
    max_retries: int
    backoff_base_seconds: float
    backoff_max_seconds: float
    respect_retry_after: bool
    throttle_cooldown_seconds: float
    jitter_fraction: float = 0.1

    def next_delay_seconds(
        self,
        retry_number: int,
        *,
        retry_after: str | None = None,
        throttle_error: bool = False,
    ) -> float:
        retry_after_seconds = (
            parse_retry_after_seconds(retry_after)
            if self.respect_retry_after and retry_after is not None
            else None
        )
        if retry_after_seconds is not None:
            delay = retry_after_seconds
        else:
            delay = self.backoff_base_seconds * (2 ** max(0, retry_number - 1))

        delay = min(delay, self.backoff_max_seconds)
        if throttle_error:
            delay = max(delay, self.throttle_cooldown_seconds)

        jitter_upper = max(0.0, delay * self.jitter_fraction)
        if jitter_upper > 0:
            delay += random.uniform(0.0, jitter_upper)
        return min(delay, self.backoff_max_seconds)


def parse_retry_after_seconds(retry_after: str | None) -> float | None:
    if retry_after is None:
        return None
    candidate = retry_after.strip()
    if not candidate:
        return None

    try:
        return max(0.0, float(candidate))
    except ValueError:
        pass

    try:
        parsed = parsedate_to_datetime(candidate)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        return max(0.0, (parsed - now).total_seconds())
    except (TypeError, ValueError):
        return None


def is_session_invalid_error(error: Exception) -> bool:
    if not isinstance(error, PyiCloudAPIResponseException):
        return False
    return _SESSION_INVALID_MARKER in str(error).lower()


def is_throttle_error(error: Exception) -> bool:
    if not isinstance(error, PyiCloudAPIResponseException):
        return False
    code = str(error.code or "").upper()
    message = str(error).lower()
    return code in _THROTTLE_CODES or "throttl" in message or "rate limit" in message


def is_fatal_auth_config_error(error: Exception) -> bool:
    return isinstance(
        error,
        (
            PyiCloudFailedLoginException,
            PyiCloudFailedMFAException,
            PyiCloud2SARequiredException,
            PyiCloudNoStoredPasswordAvailableException,
            PyiCloudServiceNotActivatedException,
        ),
    )


def is_transient_error(error: Exception) -> bool:
    if is_fatal_auth_config_error(error):
        return False
    if isinstance(
        error,
        (
            ChunkedEncodingError,
            ContentDecodingError,
            StreamConsumedError,
            UnrewindableBodyError,
        ),
    ):
        return True
    if isinstance(error, (PyiCloudServiceUnavailableException, PyiCloudConnectionErrorException)):
        return True
    if not isinstance(error, PyiCloudAPIResponseException):
        return False

    if is_session_invalid_error(error):
        return True
    code = str(error.code or "").upper()
    if code in _TRANSIENT_CODES:
        return True
    return "timed out" in str(error).lower()
