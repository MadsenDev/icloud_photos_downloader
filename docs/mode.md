# Operation Modes

```{versionchanged} 1.8.0
Added `--delete-after-download` parameter
```

`icloudpd` works in one of three modes of operation:

Copy
:   Download assets from iCloud that are not in the local storage

    This is the default mode

Sync
:   Download assets from iCloud that are not in the local storage (same as Copy). In addition, delete local files that were removed in iCloud (moved into the "Recently Deleted" album)

    This mode is selected with [`--auto-delete`](auto-delete-parameter) parameter

Move
:   Download assets from iCloud that are not in the local storage (same as Copy). Then delete assets in iCloud that are in local storage, optionally leaving recent ones in iCloud

    This mode is selected with [`--keep-icloud-recent-days`](keep-icloud-recent-days-parameter) parameter

## Engine Modes (Execution Contract)

`icloudpd` also has two execution-engine modes that control resume/task behavior:

Legacy / Stateless Engine (`run_mode=legacy_stateless`)
:   Default when `--state-db` is not used.

:   Contract:
    - No SQLite state DB is required.
    - Existing local files are skipped using filesystem checks (`already exists` semantics).
    - Restart behavior is best-effort and stateless (no task/checkpoint persistence).

Stateful Engine (`run_mode=stateful_engine`)
:   Enabled when `--state-db` is used.

:   Contract:
    - Uses persistent SQLite task/checkpoint state.
    - Supports deterministic resume via checkpoint/task state.
    - In-progress/stale leases are safely requeued on restart/cancellation.
