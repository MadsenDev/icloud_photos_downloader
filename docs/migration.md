# Migration Guide

This guide describes migration from the legacy stateless behavior to the stateful engine behavior.

## Stateless vs Stateful

Stateless mode (default)
:   No `--state-db` configured.

:   Behavior:
    - No SQLite DB is required.
    - Existing local files are skipped using filesystem checks.
    - Restarts do not persist task/checkpoint state.

Stateful mode
:   Enabled by `--state-db` (or `--state-db PATH`).

:   Behavior:
    - Persists task/checkpoint state in SQLite.
    - Supports deterministic resume semantics for long runs.
    - Requeues stale in-progress leases on restart.

## Compatibility and Defaults

- Default behavior remains stateless unless `--state-db` is explicitly set.
- Existing naming, folder structure, and dedup behavior remain unchanged.
- `--threads-num` remains accepted for compatibility, but download concurrency is controlled by `--download-workers`.
- Watch mode behavior is unchanged unless engine options are explicitly enabled.

## Recommended Migration Steps

1. Start with your current command and add `--state-db` only.
2. Verify first run output and resulting local files match expected parity.
3. Optionally add:
   - `--state-db-prune-completed-days` for retention
   - `--state-db-vacuum` for periodic space reclamation
4. If throttling appears, lower `--download-workers` before other changes.
