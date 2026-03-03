import os
import sqlite3
from datetime import datetime, timezone
from unittest import TestCase

from icloudpd.state_db import (
    checkpoint_wal,
    clear_asset_tasks_need_url_refresh,
    enqueue_task,
    initialize_state_db,
    lease_next_task,
    load_checkpoint,
    mark_asset_tasks_need_url_refresh,
    mark_task_done,
    mark_task_failed,
    prune_completed_tasks,
    record_asset_checksum_result,
    requeue_in_progress_tasks,
    requeue_stale_leases,
    resolve_state_db_path,
    save_checkpoint,
    upsert_asset_tasks,
    vacuum_state_db,
)


class StateDbTestCase(TestCase):
    def test_resolve_state_db_path_disabled(self) -> None:
        self.assertIsNone(resolve_state_db_path(None, "~/.pyicloud"))

    def test_resolve_state_db_path_auto(self) -> None:
        resolved = resolve_state_db_path("auto", "~/.pyicloud")
        assert resolved is not None
        self.assertTrue(resolved.endswith("/.pyicloud/icloudpd.sqlite"))

    def test_initialize_state_db_creates_schema(self) -> None:
        db_path = "/tmp/icloudpd-test-state/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        initialize_state_db(db_path)

        with sqlite3.connect(db_path) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('assets','tasks','checkpoints')"
                )
            }
            indexes = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_tasks_%'"
                )
            }
            task_columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        self.assertEqual(tables, {"assets", "tasks", "checkpoints"})
        self.assertIn("idx_tasks_status_updated", indexes)
        self.assertIn("idx_tasks_lease_expires", indexes)
        self.assertIn("idx_tasks_library_album_status", indexes)
        self.assertIn("checksum_result", task_columns)
        self.assertIn("needs_url_refresh", task_columns)

    def test_initialize_state_db_is_idempotent(self) -> None:
        db_path = "/tmp/icloudpd-test-state-idempotent/icloudpd.sqlite"
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)
        initialize_state_db(db_path)

        with sqlite3.connect(db_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()
        assert row is not None
        self.assertGreaterEqual(row[0], 3)

    def test_lease_and_requeue_flow(self) -> None:
        db_path = "/tmp/icloudpd-test-state-lease/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)
        enqueue_task(
            db_path,
            asset_id="a1",
            library="PrimarySync",
            album="all",
            version="original",
            expected_size=123,
            checksum="abc",
            url="https://example.com/a1",
            local_path="/tmp/a1.jpg",
        )

        leased = lease_next_task(
            db_path,
            lease_owner="worker-1",
            lease_seconds=60,
            now_iso="2026-03-02T12:00:00+00:00",
        )
        self.assertEqual(leased, ("a1", "PrimarySync", "all", "original"))

        # No second pending task.
        self.assertIsNone(
            lease_next_task(
                db_path,
                lease_owner="worker-2",
                lease_seconds=60,
                now_iso="2026-03-02T12:00:01+00:00",
            )
        )

        # Requeue after lease expiry.
        requeued = requeue_stale_leases(db_path, now_iso="2026-03-02T12:02:00+00:00")
        self.assertEqual(requeued, 1)

        leased_again = lease_next_task(
            db_path,
            lease_owner="worker-2",
            lease_seconds=60,
            now_iso="2026-03-02T12:03:00+00:00",
        )
        self.assertEqual(leased_again, ("a1", "PrimarySync", "all", "original"))

    def test_mark_task_status(self) -> None:
        db_path = "/tmp/icloudpd-test-state-status/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)
        enqueue_task(
            db_path,
            asset_id="a2",
            library="PrimarySync",
            album="all",
            version="original",
            expected_size=456,
            checksum=None,
            url=None,
            local_path=None,
        )
        lease_next_task(
            db_path,
            lease_owner="worker-1",
            lease_seconds=60,
            now_iso="2026-03-02T12:00:00+00:00",
        )

        mark_task_failed(
            db_path,
            asset_id="a2",
            library="PrimarySync",
            album="all",
            version="original",
            error="network error",
        )
        with sqlite3.connect(db_path) as conn:
            status, last_error = conn.execute(
                "SELECT status, last_error FROM tasks WHERE asset_id='a2'"
            ).fetchone()
        self.assertEqual(status, "failed")
        self.assertEqual(last_error, "network error")

        mark_task_done(
            db_path,
            asset_id="a2",
            library="PrimarySync",
            album="all",
            version="original",
        )
        with sqlite3.connect(db_path) as conn:
            status, last_error = conn.execute(
                "SELECT status, last_error FROM tasks WHERE asset_id='a2'"
            ).fetchone()
        self.assertEqual(status, "done")
        self.assertIsNone(last_error)

    def test_requeue_in_progress_tasks(self) -> None:
        db_path = "/tmp/icloudpd-test-state-requeue-in-progress/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)
        enqueue_task(
            db_path,
            asset_id="a4",
            library="PrimarySync",
            album="all",
            version="original",
            expected_size=100,
            checksum=None,
            url=None,
            local_path=None,
        )
        leased = lease_next_task(
            db_path,
            lease_owner="worker-x",
            lease_seconds=300,
            now_iso="2026-03-03T00:00:00+00:00",
        )
        self.assertEqual(leased, ("a4", "PrimarySync", "all", "original"))

        requeued = requeue_in_progress_tasks(db_path, lease_owner="worker-x")
        self.assertEqual(requeued, 1)
        with sqlite3.connect(db_path) as conn:
            status, owner = conn.execute(
                "SELECT status, lease_owner FROM tasks WHERE asset_id='a4'"
            ).fetchone()
        self.assertEqual(status, "pending")
        self.assertIsNone(owner)

    def test_prune_completed_tasks(self) -> None:
        db_path = "/tmp/icloudpd-test-state-prune-completed/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)
        enqueue_task(
            db_path,
            asset_id="a5",
            library="PrimarySync",
            album="all",
            version="original",
            expected_size=10,
            checksum=None,
            url=None,
            local_path=None,
        )
        mark_task_done(
            db_path,
            asset_id="a5",
            library="PrimarySync",
            album="all",
            version="original",
        )
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "UPDATE tasks SET updated_at=datetime('now', '-5 days') WHERE asset_id='a5'"
            )

        pruned = prune_completed_tasks(db_path, older_than_days=1)
        self.assertEqual(pruned, 1)
        with sqlite3.connect(db_path) as conn:
            remaining = conn.execute("SELECT COUNT(*) FROM tasks WHERE asset_id='a5'").fetchone()[0]
        self.assertEqual(remaining, 0)

    def test_checkpoint_wal_and_vacuum(self) -> None:
        db_path = "/tmp/icloudpd-test-state-maintenance/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)

        result = checkpoint_wal(db_path)
        self.assertEqual(len(result), 3)
        vacuum_state_db(db_path)

    def test_mark_and_clear_url_refresh_marker(self) -> None:
        db_path = "/tmp/icloudpd-test-state-url-refresh-marker/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)
        enqueue_task(
            db_path,
            asset_id="a6",
            library="PrimarySync",
            album="all",
            version="original",
            expected_size=1,
            checksum=None,
            url="https://example.test/a6",
            local_path=None,
        )
        marked = mark_asset_tasks_need_url_refresh(
            db_path,
            asset_id="a6",
            library="PrimarySync",
            album="all",
        )
        self.assertEqual(marked, 1)
        with sqlite3.connect(db_path) as conn:
            value = conn.execute(
                "SELECT needs_url_refresh FROM tasks WHERE asset_id='a6'"
            ).fetchone()[0]
        self.assertEqual(value, 1)

        cleared = clear_asset_tasks_need_url_refresh(
            db_path,
            asset_id="a6",
            library="PrimarySync",
            album="all",
        )
        self.assertEqual(cleared, 1)
        with sqlite3.connect(db_path) as conn:
            value = conn.execute(
                "SELECT needs_url_refresh FROM tasks WHERE asset_id='a6'"
            ).fetchone()[0]
        self.assertEqual(value, 0)

    def test_record_asset_checksum_result(self) -> None:
        db_path = "/tmp/icloudpd-test-state-checksum-result/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)
        enqueue_task(
            db_path,
            asset_id="a3",
            library="PrimarySync",
            album="all",
            version="original",
            expected_size=789,
            checksum="abc",
            url="https://example.com/a3",
            local_path="/tmp/a3.jpg",
        )
        enqueue_task(
            db_path,
            asset_id="a3",
            library="PrimarySync",
            album="all",
            version="medium",
            expected_size=456,
            checksum="def",
            url="https://example.com/a3m",
            local_path="/tmp/a3m.jpg",
        )

        record_asset_checksum_result(
            db_path,
            asset_id="a3",
            library="PrimarySync",
            album="all",
            checksum_result="verified",
        )

        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT version, checksum_result FROM tasks WHERE asset_id='a3' ORDER BY version"
            ).fetchall()
        self.assertEqual(rows, [("medium", "verified"), ("original", "verified")])

    def test_checkpoint_roundtrip(self) -> None:
        db_path = "/tmp/icloudpd-test-state-checkpoint/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)

        self.assertIsNone(load_checkpoint(db_path, library="PrimarySync", album="all"))
        save_checkpoint(db_path, library="PrimarySync", album="all", start_rank=42)
        self.assertEqual(load_checkpoint(db_path, library="PrimarySync", album="all"), 42)
        save_checkpoint(db_path, library="PrimarySync", album="all", start_rank=100)
        self.assertEqual(load_checkpoint(db_path, library="PrimarySync", album="all"), 100)

    def test_upsert_asset_tasks_persists_asset_and_versions(self) -> None:
        db_path = "/tmp/icloudpd-test-state-asset-tasks/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)

        class _ItemType:
            value = "image"

        class _Version:
            def __init__(self, size: int, checksum: str, url: str):
                self.size = size
                self.checksum = checksum
                self.url = url

        class _VersionKey:
            def __init__(self, value: str):
                self.value = value

        class _FakePhoto:
            id = "asset-1"
            item_type = _ItemType()
            added_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
            asset_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
            versions = {
                _VersionKey("original"): _Version(100, "abc", "https://example.com/a.jpg"),
                _VersionKey("medium"): _Version(50, "def", "https://example.com/m.jpg"),
            }

        upsert_asset_tasks(db_path, photo=_FakePhoto(), library="PrimarySync", album="all")

        with sqlite3.connect(db_path) as conn:
            assets = conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
            tasks = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        self.assertEqual(assets, 1)
        self.assertEqual(tasks, 2)

    def test_crash_resume_does_not_redo_completed_tasks(self) -> None:
        db_path = "/tmp/icloudpd-test-state-crash-resume/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)

        enqueue_task(
            db_path,
            asset_id="done-task",
            library="PrimarySync",
            album="all",
            version="original",
            expected_size=1,
            checksum=None,
            url=None,
            local_path=None,
        )
        enqueue_task(
            db_path,
            asset_id="stale-task",
            library="PrimarySync",
            album="all",
            version="original",
            expected_size=1,
            checksum=None,
            url=None,
            local_path=None,
        )

        first = lease_next_task(
            db_path,
            lease_owner="worker-1",
            lease_seconds=60,
            now_iso="2026-03-02T12:00:00+00:00",
        )
        self.assertEqual(first, ("done-task", "PrimarySync", "all", "original"))
        mark_task_done(
            db_path,
            asset_id="done-task",
            library="PrimarySync",
            album="all",
            version="original",
        )

        second = lease_next_task(
            db_path,
            lease_owner="worker-1",
            lease_seconds=60,
            now_iso="2026-03-02T12:00:10+00:00",
        )
        self.assertEqual(second, ("stale-task", "PrimarySync", "all", "original"))

        # Simulate crash, worker disappears. Stale lease should be requeued.
        self.assertEqual(requeue_stale_leases(db_path, now_iso="2026-03-02T12:05:00+00:00"), 1)
        resumed = lease_next_task(
            db_path,
            lease_owner="worker-2",
            lease_seconds=60,
            now_iso="2026-03-02T12:06:00+00:00",
        )
        self.assertEqual(resumed, ("stale-task", "PrimarySync", "all", "original"))

        # Completed task is not leased again.
        mark_task_done(
            db_path,
            asset_id="stale-task",
            library="PrimarySync",
            album="all",
            version="original",
        )
        self.assertIsNone(
            lease_next_task(
                db_path,
                lease_owner="worker-3",
                lease_seconds=60,
                now_iso="2026-03-02T12:07:00+00:00",
            )
        )

    def test_checkpoint_resume_after_partial_enumeration(self) -> None:
        db_path = "/tmp/icloudpd-test-state-partial-enumeration/icloudpd.sqlite"
        if os.path.exists(db_path):
            os.remove(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        initialize_state_db(db_path)

        save_checkpoint(db_path, library="PrimarySync", album="all", start_rank=100)
        # Simulate restart from checkpoint and advancing.
        start_rank = load_checkpoint(db_path, library="PrimarySync", album="all")
        self.assertEqual(start_rank, 100)
        save_checkpoint(db_path, library="PrimarySync", album="all", start_rank=150)
        self.assertEqual(load_checkpoint(db_path, library="PrimarySync", album="all"), 150)
