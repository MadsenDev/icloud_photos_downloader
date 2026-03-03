import threading
import time
from unittest import TestCase

from icloudpd.limiter import AdaptiveDownloadLimiter


class AdaptiveDownloadLimiterTestCase(TestCase):
    def test_additive_increase(self) -> None:
        limiter = AdaptiveDownloadLimiter(
            max_workers=4,
            min_workers=1,
            cooldown_seconds=0.0,
            increase_every=2,
        )

        limiter.on_throttle()  # 4 -> 2
        self.assertEqual(limiter.current_limit, 2)
        limiter.on_success()
        self.assertEqual(limiter.current_limit, 2)
        limiter.on_success()
        self.assertEqual(limiter.current_limit, 3)

    def test_multiplicative_decrease_and_cooldown(self) -> None:
        limiter = AdaptiveDownloadLimiter(
            max_workers=8,
            min_workers=1,
            cooldown_seconds=0.2,
            increase_every=1,
        )
        limiter.on_throttle()  # 8 -> 4
        self.assertEqual(limiter.current_limit, 4)
        self.assertGreater(limiter.cooldown_remaining_seconds, 0.0)

    def test_slot_respects_limit(self) -> None:
        limiter = AdaptiveDownloadLimiter(
            max_workers=2,
            min_workers=1,
            cooldown_seconds=0.0,
            increase_every=10,
        )
        entered = threading.Event()

        def hold_slot() -> None:
            with limiter.slot():
                entered.set()
                time.sleep(0.15)

        with limiter.slot():
            t = threading.Thread(target=hold_slot)
            t.start()
            self.assertTrue(entered.wait(0.5))
            self.assertFalse(limiter.acquire(timeout=0.05))
        t.join()

        self.assertTrue(limiter.acquire(timeout=0.2))
        limiter.release()

    def test_stop_drain_and_restart(self) -> None:
        limiter = AdaptiveDownloadLimiter(
            max_workers=2,
            min_workers=1,
            cooldown_seconds=0.0,
            increase_every=10,
        )
        entered = threading.Event()
        release_holder = threading.Event()

        def holder() -> None:
            with limiter.slot():
                entered.set()
                release_holder.wait(1.0)

        thread = threading.Thread(target=holder)
        thread.start()
        self.assertTrue(entered.wait(0.5))

        stopped = []

        def stop_limiter() -> None:
            stopped.append(limiter.stop(wait=True, timeout=1.0))

        stopper = threading.Thread(target=stop_limiter)
        stopper.start()
        time.sleep(0.05)
        self.assertFalse(limiter.acquire(timeout=0.05))

        release_holder.set()
        stopper.join()
        thread.join()
        self.assertEqual(stopped, [True])

        self.assertFalse(limiter.acquire(timeout=0.05))
        limiter.start()
        self.assertTrue(limiter.acquire(timeout=0.2))
        limiter.release()
