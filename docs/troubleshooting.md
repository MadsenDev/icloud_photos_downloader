# Troubleshooting

## Throttling (429/503, slow progress, repeated retries)

Symptoms:
- frequent retry logs
- repeated throttle warnings
- reduced throughput over time

Actions:
1. Lower `--download-workers` (for example, `4` to `2`).
2. Increase `--watch-with-interval` for watch mode runs.
3. Use `--no-remote-count` to reduce metadata request load.
4. Keep retry defaults unless you have a measured reason to tune them.

## Session/Cookie Issues

Symptoms:
- repeated re-authentication prompts
- intermittent authentication failures across runs

Actions:
1. Keep one active process per account/cookie directory.
2. Use separate `--cookie-directory` values for different accounts.
3. If using stateful mode, keep `--state-db` in the same account-scoped directory.

## Resume Expectations

- Stateless mode resumes only via filesystem skip checks.
- Stateful mode resumes via persisted task/checkpoint state.
- On clean cancellation (`SIGINT`/`SIGTERM`), in-progress state is safely requeued.
- Expired URL failures in stateful mode are marked with `needs_url_refresh=1` for affected tasks.

## State DB Size Growth

If the state DB grows large:
1. Add `--state-db-prune-completed-days` (for example, `30`).
2. Run periodic `--state-db-vacuum` (not on every run).
