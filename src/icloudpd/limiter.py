"""Adaptive limiter for download request concurrency."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Callable, Iterator


class AdaptiveDownloadLimiter:
    def __init__(
        self,
        *,
        max_workers: int,
        cooldown_seconds: float,
        min_workers: int = 1,
        increase_every: int = 10,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        if min_workers < 1:
            raise ValueError("min_workers must be >= 1")
        if min_workers > max_workers:
            raise ValueError("min_workers must be <= max_workers")
        if increase_every < 1:
            raise ValueError("increase_every must be >= 1")
        if cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be >= 0")

        self._max_workers = max_workers
        self._min_workers = min_workers
        self._cooldown_seconds = cooldown_seconds
        self._increase_every = increase_every
        self._clock = clock

        self._condition = threading.Condition()
        self._in_flight = 0
        self._current_limit = max_workers
        self._success_streak = 0
        self._cooldown_until = 0.0
        self._stopped = False

    @property
    def max_workers(self) -> int:
        return self._max_workers

    @property
    def current_limit(self) -> int:
        with self._condition:
            return self._current_limit

    @property
    def cooldown_remaining_seconds(self) -> float:
        with self._condition:
            return max(0.0, self._cooldown_until - self._clock())

    def acquire(self, timeout: float | None = None) -> bool:
        deadline = None if timeout is None else (self._clock() + timeout)
        with self._condition:
            while True:
                if self._stopped:
                    return False
                now = self._clock()
                cooldown_remaining = max(0.0, self._cooldown_until - now)
                can_enter = cooldown_remaining <= 0 and self._in_flight < self._current_limit
                if can_enter:
                    self._in_flight += 1
                    return True

                wait_for = cooldown_remaining if cooldown_remaining > 0 else None
                if deadline is not None:
                    remaining = deadline - now
                    if remaining <= 0:
                        return False
                    wait_for = remaining if wait_for is None else min(wait_for, remaining)
                self._condition.wait(wait_for)

    def release(self) -> None:
        with self._condition:
            if self._in_flight > 0:
                self._in_flight -= 1
            self._condition.notify_all()

    def stop(self, wait: bool = True, timeout: float | None = None) -> bool:
        deadline = None if timeout is None else (self._clock() + timeout)
        with self._condition:
            self._stopped = True
            self._condition.notify_all()
            if not wait:
                return True
            while self._in_flight > 0:
                if deadline is not None:
                    remaining = deadline - self._clock()
                    if remaining <= 0:
                        return False
                    self._condition.wait(remaining)
                else:
                    self._condition.wait()
            return True

    def start(self) -> None:
        with self._condition:
            self._stopped = False
            self._condition.notify_all()

    def on_success(self) -> None:
        with self._condition:
            if self._clock() < self._cooldown_until:
                return
            if self._current_limit >= self._max_workers:
                self._success_streak = 0
                return
            self._success_streak += 1
            if self._success_streak >= self._increase_every:
                self._current_limit += 1
                self._success_streak = 0
                self._condition.notify_all()

    def on_throttle(self) -> None:
        with self._condition:
            self._success_streak = 0
            self._current_limit = max(self._min_workers, self._current_limit // 2)
            self._cooldown_until = max(
                self._cooldown_until, self._clock() + self._cooldown_seconds
            )
            self._condition.notify_all()

    @contextmanager
    def slot(self, timeout: float | None = None) -> Iterator[None]:
        if not self.acquire(timeout=timeout):
            raise TimeoutError("Could not acquire download slot within timeout")
        try:
            yield
        finally:
            self.release()
