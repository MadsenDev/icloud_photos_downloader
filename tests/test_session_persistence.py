import json
import os
import shutil
import tempfile
import threading
import time
from unittest import TestCase

from pyicloud_ipd.session import persist_session_and_cookies


class _FakeCookieJar:
    def save(self, filename=None, ignore_discard=True, ignore_expires=True):  # type: ignore[no-untyped-def]
        assert filename is not None
        # Delay to increase overlap pressure in concurrent writers.
        time.sleep(0.01)
        with open(filename, "w", encoding="utf-8") as f:
            f.write("cookiejar-ok\n")


class SessionPersistenceTestCase(TestCase):
    def test_concurrent_persistence_does_not_corrupt_session_or_cookies(self) -> None:
        tmpdir = tempfile.mkdtemp(prefix="icloudpd-session-persist-")
        self.addCleanup(lambda: shutil.rmtree(tmpdir, ignore_errors=True))
        session_path = os.path.join(tmpdir, "session.json")
        cookiejar_path = os.path.join(tmpdir, "cookies")

        def writer(index: int) -> None:
            payload = {"client_id": f"client-{index}", "session_id": f"session-{index}"}
            persist_session_and_cookies(
                session_path=session_path,
                cookiejar_path=cookiejar_path,
                session_data=payload,
                cookies=_FakeCookieJar(),
            )

        threads = [threading.Thread(target=writer, args=(idx,)) for idx in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        with open(session_path, encoding="utf-8") as session_file:
            parsed = json.load(session_file)
        self.assertIn("client_id", parsed)
        self.assertIn("session_id", parsed)
        self.assertTrue(parsed["client_id"].startswith("client-"))
        self.assertTrue(parsed["session_id"].startswith("session-"))

        with open(cookiejar_path, encoding="utf-8") as cookie_file:
            cookie_contents = cookie_file.read()
        self.assertEqual(cookie_contents, "cookiejar-ok\n")
