"""State DB support for resumable processing."""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyicloud_ipd.services.photos import PhotoAsset


def resolve_state_db_path(state_db_option: str | None, cookie_directory: str) -> str | None:
    if state_db_option is None:
        return None
    if state_db_option == "auto":
        return os.path.join(os.path.expanduser(cookie_directory), "icloudpd.sqlite")
    return os.path.expanduser(state_db_option)


def initialize_state_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assets (
                asset_id TEXT NOT NULL,
                library TEXT NOT NULL,
                album TEXT NOT NULL,
                added_date TEXT,
                asset_date TEXT,
                item_type TEXT,
                metadata_json TEXT,
                PRIMARY KEY (asset_id, library, album)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                asset_id TEXT NOT NULL,
                library TEXT NOT NULL,
                album TEXT NOT NULL,
                version TEXT NOT NULL,
                expected_size INTEGER,
                checksum TEXT,
                url TEXT,
                local_path TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                lease_owner TEXT,
                lease_expires_at TEXT,
                last_error TEXT,
                checksum_result TEXT,
                needs_url_refresh INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (asset_id, library, album, version)
            )
            """
        )
        _ensure_column(conn, "tasks", "checksum_result", "TEXT")
        _ensure_column(conn, "tasks", "needs_url_refresh", "INTEGER NOT NULL DEFAULT 0")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                library TEXT NOT NULL,
                album TEXT NOT NULL,
                start_rank INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (library, album)
            )
            """
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_status_updated ON tasks(status, updated_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_lease_expires ON tasks(lease_expires_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_library_album_status ON tasks(library, album, status)"
        )


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def enqueue_task(
    db_path: str,
    *,
    asset_id: str,
    library: str,
    album: str,
    version: str,
    expected_size: int | None,
    checksum: str | None,
    url: str | None,
    local_path: str | None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                asset_id, library, album, version, expected_size, checksum, url, local_path,
                status, attempts, needs_url_refresh, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, CURRENT_TIMESTAMP)
            ON CONFLICT(asset_id, library, album, version) DO UPDATE SET
                expected_size=excluded.expected_size,
                checksum=excluded.checksum,
                url=excluded.url,
                local_path=excluded.local_path,
                needs_url_refresh=0,
                updated_at=CURRENT_TIMESTAMP
            """,
            (asset_id, library, album, version, expected_size, checksum, url, local_path),
        )


def requeue_stale_leases(db_path: str, now_iso: str | None = None) -> int:
    now = now_iso or utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE tasks
            SET status='pending',
                lease_owner=NULL,
                lease_expires_at=NULL,
                updated_at=CURRENT_TIMESTAMP
            WHERE status='in_progress' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
            """,
            (now,),
        )
    return cursor.rowcount


def requeue_in_progress_tasks(db_path: str, lease_owner: str | None = None) -> int:
    with sqlite3.connect(db_path) as conn:
        if lease_owner is None:
            cursor = conn.execute(
                """
                UPDATE tasks
                SET status='pending',
                    lease_owner=NULL,
                    lease_expires_at=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE status='in_progress'
                """
            )
        else:
            cursor = conn.execute(
                """
                UPDATE tasks
                SET status='pending',
                    lease_owner=NULL,
                    lease_expires_at=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE status='in_progress' AND lease_owner=?
                """,
                (lease_owner,),
            )
    return cursor.rowcount


def prune_completed_tasks(db_path: str, *, older_than_days: int) -> int:
    if older_than_days <= 0:
        raise ValueError("older_than_days must be greater than 0")
    cutoff = f"-{older_than_days} days"
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            DELETE FROM tasks
            WHERE status IN ('done', 'failed')
              AND updated_at < datetime('now', ?)
            """,
            (cutoff,),
        )
    return cursor.rowcount


def checkpoint_wal(db_path: str, *, mode: str = "PASSIVE") -> tuple[int, int, int]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
    if row is None:
        return (0, 0, 0)
    return (int(row[0]), int(row[1]), int(row[2]))


def vacuum_state_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("VACUUM")


def lease_next_task(
    db_path: str, *, lease_owner: str, lease_seconds: int = 300, now_iso: str | None = None
) -> tuple[str, str, str, str] | None:
    now_dt = datetime.fromisoformat(now_iso) if now_iso else datetime.now(tz=timezone.utc)
    lease_expires = (now_dt + timedelta(seconds=lease_seconds)).isoformat()
    now = now_dt.isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            UPDATE tasks
            SET status='pending',
                lease_owner=NULL,
                lease_expires_at=NULL,
                updated_at=CURRENT_TIMESTAMP
            WHERE status='in_progress' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
            """,
            (now,),
        )
        row = conn.execute(
            """
            SELECT asset_id, library, album, version
            FROM tasks
            WHERE status='pending'
            ORDER BY updated_at ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        conn.execute(
            """
            UPDATE tasks
            SET status='in_progress',
                lease_owner=?,
                lease_expires_at=?,
                attempts=attempts+1,
                updated_at=CURRENT_TIMESTAMP
            WHERE asset_id=? AND library=? AND album=? AND version=?
            """,
            (lease_owner, lease_expires, row[0], row[1], row[2], row[3]),
        )
        conn.commit()
    return (row[0], row[1], row[2], row[3])


def mark_task_done(db_path: str, *, asset_id: str, library: str, album: str, version: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE tasks
            SET status='done',
                lease_owner=NULL,
                lease_expires_at=NULL,
                last_error=NULL,
                updated_at=CURRENT_TIMESTAMP
            WHERE asset_id=? AND library=? AND album=? AND version=?
            """,
            (asset_id, library, album, version),
        )


def record_asset_checksum_result(
    db_path: str,
    *,
    asset_id: str,
    library: str,
    album: str,
    checksum_result: str,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE tasks
            SET checksum_result=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE asset_id=? AND library=? AND album=?
            """,
            (checksum_result, asset_id, library, album),
        )


def mark_task_failed(
    db_path: str, *, asset_id: str, library: str, album: str, version: str, error: str
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE tasks
            SET status='failed',
                lease_owner=NULL,
                lease_expires_at=NULL,
                last_error=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE asset_id=? AND library=? AND album=? AND version=?
            """,
            (error, asset_id, library, album, version),
        )


def mark_asset_tasks_need_url_refresh(
    db_path: str,
    *,
    asset_id: str,
    library: str,
    album: str,
    error: str = "expired_download_url",
) -> int:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE tasks
            SET needs_url_refresh=1,
                last_error=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE asset_id=? AND library=? AND album=?
            """,
            (error, asset_id, library, album),
        )
    return cursor.rowcount


def clear_asset_tasks_need_url_refresh(
    db_path: str,
    *,
    asset_id: str,
    library: str,
    album: str,
) -> int:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE tasks
            SET needs_url_refresh=0,
                updated_at=CURRENT_TIMESTAMP
            WHERE asset_id=? AND library=? AND album=?
            """,
            (asset_id, library, album),
        )
    return cursor.rowcount


def save_checkpoint(db_path: str, *, library: str, album: str, start_rank: int) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO checkpoints (library, album, start_rank, created_at, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(library, album) DO UPDATE SET
                start_rank=excluded.start_rank,
                updated_at=CURRENT_TIMESTAMP
            """,
            (library, album, start_rank),
        )


def load_checkpoint(db_path: str, *, library: str, album: str) -> int | None:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT start_rank FROM checkpoints WHERE library=? AND album=?",
            (library, album),
        ).fetchone()
    if row is None:
        return None
    return int(row[0])


def upsert_asset(
    db_path: str,
    *,
    asset_id: str,
    library: str,
    album: str,
    added_date: str | None,
    asset_date: str | None,
    item_type: str | None,
    metadata_json: str | None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO assets (
                asset_id, library, album, added_date, asset_date, item_type, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(asset_id, library, album) DO UPDATE SET
                added_date=excluded.added_date,
                asset_date=excluded.asset_date,
                item_type=excluded.item_type,
                metadata_json=excluded.metadata_json
            """,
            (asset_id, library, album, added_date, asset_date, item_type, metadata_json),
        )


def upsert_asset_tasks(db_path: str, *, photo: PhotoAsset, library: str, album: str) -> None:
    item_type = photo.item_type.value if photo.item_type is not None else None
    try:
        added_date = photo.added_date.isoformat()
    except Exception:
        added_date = None
    try:
        asset_date = photo.asset_date.isoformat()
    except Exception:
        asset_date = None
    upsert_asset(
        db_path,
        asset_id=photo.id,
        library=library,
        album=album,
        added_date=added_date,
        asset_date=asset_date,
        item_type=item_type,
        metadata_json=None,
    )
    for version, version_value in photo.versions.items():
        enqueue_task(
            db_path,
            asset_id=photo.id,
            library=library,
            album=album,
            version=str(version.value),
            expected_size=version_value.size,
            checksum=version_value.checksum,
            url=version_value.url,
            local_path=None,
        )
