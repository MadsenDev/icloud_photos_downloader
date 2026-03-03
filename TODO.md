# iCloud Photos Downloader Improvement Checklist

Last updated: 2026-03-03

Use this as the source of truth for implementation progress. Every code change should update this file.

## 0. Project hygiene and tracking
- [x] Add/update architecture note describing current pipeline and target pipeline.
- [x] Keep this checklist aligned with actual implemented code and tests.
- [ ] For each completed task, reference the related PR/commit in this file.
- [x] Keep changelog/release notes in sync when user-facing flags/behavior change.
- [x] Ensure local development/testing uses Python 3.13 in `.venv` to match project constraints.

## 1. Unified retry and backoff (metadata + downloads)
### 1.1 Policy and configuration
- [x] Define one retry policy module shared by metadata calls and file downloads.
- [x] Add CLI option: `--max-retries` (default target: 6).
- [x] Add CLI option: `--backoff-base-seconds`.
- [x] Add CLI option: `--backoff-max-seconds`.
- [x] Add CLI option: `--respect-retry-after/--no-respect-retry-after`.
- [x] Add CLI option: `--throttle-cooldown-seconds`.
- [x] Ensure defaults preserve safe behavior for existing users.

### 1.2 Error classification
- [x] Classify fatal auth/config errors as no-retry (invalid creds, MFA unavailable, ADP/web-disabled).
- [x] Classify session-invalid errors as re-auth-then-retry.
- [x] Classify transient errors as retryable (429, 503, timeouts, connection resets, throttling-like denials).
- [x] Centralize retry decision logging (attempt, reason, next delay).

### 1.3 Integration points
- [x] Apply shared retry policy to album/asset enumeration calls.
- [x] Apply shared retry policy to download calls.
- [x] Remove/replace duplicated ad-hoc retry loops in existing code paths.
- [x] Add jitter to exponential backoff.
- [x] Honor `Retry-After` when present on retryable responses.

### 1.4 Verification
- [x] Unit tests for retry classifier.
- [x] Unit tests for backoff math and jitter bounds.
- [x] Unit tests for `Retry-After` handling.
- [x] Integration tests: metadata retry behavior under simulated 429/503.
- [x] Integration tests: download retry behavior under simulated 429/503/reset.

## 2. Persistent state DB and resumable task queue
### 2.1 Data model
- [x] Add `--state-db` option (or equivalent path option) with sensible default.
- [x] Create DB initialization/migration path.
- [x] Create `assets` table.
- [x] Create `tasks` table with status/attempt/error fields.
- [x] Create `checkpoints` table for pagination progress.
- [x] Add indexes for task leasing and status filtering.

### 2.2 Enumeration persistence
- [x] Persist enumerated assets in batches.
- [x] Persist tasks per asset version.
- [x] Save checkpoint every page (or configurable page interval).
- [x] Resume enumeration from checkpoint after restart.

### 2.3 Worker/task lifecycle
- [x] Add task states: `pending`, `in_progress`, `done`, `failed`.
- [x] Add lease timestamp/owner for `in_progress`.
- [x] Requeue stale leased tasks on startup.
- [x] Track per-task attempts and last error.

### 2.4 Verification
- [x] Unit tests for DB schema creation and migrations.
- [x] Unit tests for lease/requeue behavior.
- [x] Integration test: crash mid-run and resume without redoing completed tasks.
- [x] Integration test: checkpoint resume after partial enumeration.

### 2.5 URL freshness
- [x] Detect expired/invalid persisted download URLs and refresh asset version metadata.
- [x] Add task/state marker for URL refresh path (e.g., `needs_url_refresh`) and retry flow.

## 3. Bounded adaptive concurrency
### 3.1 CLI and defaults
- [x] Add `--download-workers` option (default target: 4).
- [x] Keep metadata enumeration single-threaded by default.
- [x] Document deprecation relationship with `--threads-num`.

### 3.2 Limiting and adaptation
- [x] Implement shared account-level limiter for download workers.
- [x] Separate metadata and download request budgets (if needed by code design).
- [x] Implement AIMD or equivalent adaptive reduction on throttling events.
- [x] Add global cool-down behavior when repeated throttle signals occur.

### 3.3 Session/cookie safety
- [x] Audit all session/cookie writes under concurrent access.
- [x] Add locking or redesign to avoid concurrent write races.
- [x] Ensure no cookie/session corruption under multithreaded runs.

### 3.4 Verification
- [x] Unit tests for limiter/token bucket behavior.
- [x] Concurrency tests for session persistence safety.
- [x] Integration tests for worker pool drain/stop/restart behavior.
- [x] Benchmark runs at workers = 1, 2, 4, 8 and record throughput + error rate.

## 4. Download efficiency and integrity
### 4.1 Throughput improvements
- [x] Add `--download-chunk-bytes` option (default target: 262144).
- [x] Replace fixed 1 KiB streaming chunk with configurable larger chunk.
- [x] Verify memory usage remains bounded by worker count and chunk size.
- [x] Benchmark chunk-size/verification combinations for throughput vs CPU tradeoff.

### 4.2 Integrity checks
- [x] Add `--verify-size/--no-verify-size` option.
- [x] Add `--verify-checksum/--no-verify-checksum` option.
- [x] Validate downloaded file size against expected metadata.
- [x] Implement optional checksum validation strategy.
- [x] Store local checksum/result in state DB when enabled.

### 4.3 Range resume hardening
- [x] Keep `.part` resume behavior with `Range` requests.
- [x] Detect non-`206` response when resuming and safely restart partial file.
- [x] Add corruption-safe handling for mismatched range behavior.

### 4.4 Verification
- [x] Unit tests for chunk-size configuration and defaults.
- [x] Unit tests for size verification success/failure.
- [x] Unit tests for checksum verification success/failure.
- [x] Integration tests for resume with partial files and range edge cases.

## 5. Request volume and enumeration efficiency
- [x] Add `--album-page-size` option (target range: 50-500).
- [x] Add `--no-remote-count` option to skip expensive album count calls.
- [x] Reduce redundant metadata queries where possible.
- [x] Add/align chunked date-based run options (`since/until added date` behavior).
- [x] Document clear behavior differences between added-date and created-date usage.
- [x] Add tests for new pagination and remote-count toggles.

## 6. Observability and operations
### 6.1 Logging
- [x] Add structured JSON log mode.
- [x] Include stable fields (`run_id`, `asset_id`, `attempt`, `http_status`, etc.).
- [x] Ensure sensitive data redaction remains enforced.

### 6.2 Metrics and health
- [x] Add metrics endpoint or export path (if compatible with current stack).
- [x] Track throughput, retries, throttle events, queue depth, success gap.
- [x] Add low-disk-space warning/error classification.
- [x] Provide JSON stats snapshot output suitable for GUI wrappers (`--metrics-json`).

### 6.3 Alerts and notifications
- [x] Add alert condition for repeated throttling.
- [x] Keep MFA expiry notification path working with new engine.
- [x] Add docs for recommended operational thresholds.

## 7. Documentation and migration
- [x] Update CLI reference docs for all new options.
- [x] Add migration guide: stateless mode vs stateful mode.
- [x] Document compatibility and unchanged default behavior.
- [x] Document concurrency limitations and safe defaults.
- [x] Add troubleshooting guide for throttling/session issues.

## 9. Runtime Semantics and Operability Hardening
### 9.1 Mode contract
- [x] Define explicit legacy/stateless mode contract (no DB required, filesystem skip semantics).
- [x] Define explicit stateful engine mode contract (resume guarantees, task-state semantics).
- [x] Add integration tests asserting mode-specific behavior and parity expectations.

### 9.2 Exit and summary semantics
- [x] Define process exit code contract (success, partial success, fatal auth/config, cancelled, stalled).
- [x] Emit machine-readable end-of-run summary with totals/failures/error location hints.

### 9.3 Cancellation and shutdown
- [x] Handle SIGINT/SIGTERM with graceful stop (drain or safe requeue of in-flight work).
- [x] Ensure clean shutdown is distinguishable from crash and restart behavior is deterministic.

### 9.4 State DB growth and retention
- [x] Add DB retention/pruning policy (completed task cleanup / capped error history).
- [x] Document and/or automate WAL checkpointing and vacuum guidance.

## 8. Final validation before release
- [ ] Full test suite passes.
- [x] New tests added for each new subsystem.
- [x] Lint/type checks pass.
- [ ] Manual end-to-end dry run on small sample library.
- [ ] Manual end-to-end run with injected transient failures.
- [x] Confirm no regressions in naming/dedup/folder behavior.
- [x] Confirm watch mode behavior is unchanged unless explicitly modified.
