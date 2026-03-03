"""Thread-safe counter."""

from threading import Lock


class Counter:
    def __init__(self, value: int = 0):
        self.initial_value = value
        self.val = value
        self.lock = Lock()

    def increment(self) -> None:
        with self.lock:
            self.val += 1

    def reset(self) -> None:
        with self.lock:
            self.val = self.initial_value

    def value(self) -> int:
        with self.lock:
            return int(self.val)
