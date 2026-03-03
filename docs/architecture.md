# Engine Architecture

This note describes the current downloader execution pipeline and the target resilient pipeline now implemented.

## Modes

- `legacy_stateless`:
  - No state DB required.
  - Filesystem existence checks drive skip/retry behavior.
  - Preserves legacy CLI expectations.
- `stateful_engine`:
  - Uses SQLite state DB (`--state-db`) for assets, tasks, and checkpoints.
  - Supports deterministic resume with leased task recovery.

## Pipeline Stages

1. Authenticate and initialize per-user run context (`run_id`, retry/limiter/metrics).
2. Enumerate remote assets (single-threaded) and persist checkpoints/tasks in stateful mode.
3. Download via bounded worker pool with adaptive limiter.
4. Apply unified retry/backoff policy for metadata and downloads (with jitter and `Retry-After`).
5. Verify integrity (size and optional checksum).
6. Persist task outcomes and emit end-of-run summary (machine-readable + human logs).

## Resilience Guarantees

- Shared retry classifier for transient vs fatal errors.
- Re-auth on session-invalid failures.
- URL freshness path for expired download URLs (`401`/`403`/`410`) with one metadata refresh retry.
- Graceful cancellation (`SIGINT`/`SIGTERM`) with safe requeue semantics.
- Restart safety for stale leases and pagination checkpoints.

## Throughput and Safety Controls

- Configurable chunked streaming (`--download-chunk-bytes`) with bounded memory behavior.
- Adaptive concurrency (`--download-workers`) with throttle backoff/cooldown.
- Optional remote-count skip and page-size tuning for lower API pressure.

## Operability

- Structured JSON logs (`--log-format json`).
- JSON metrics snapshot (`--metrics-json`) for wrappers/GUI integration.
- State DB maintenance options (`--state-db-prune-completed-days`, `--state-db-vacuum`).
