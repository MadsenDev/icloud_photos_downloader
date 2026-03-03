# Concurrency and Safe Defaults

## Current Concurrency Model

- Metadata enumeration is single-threaded.
- Downloads use bounded worker concurrency via `--download-workers`.
- Account-level adaptive limiting reduces effective concurrency under throttling.

## Safe Defaults

- Default `--download-workers` is `4`.
- Start at `2` for constrained networks/NAS devices.
- Increase gradually only after verifying stable error rates.

## Practical Limits

- High worker counts can trigger more throttling and retries.
- Very short watch intervals plus high worker counts increase request pressure.
- One process per account/cookie directory remains the safest operational pattern.

## Tuning Order

1. Set `--download-workers`.
2. Validate throughput vs retries/throttle events.
3. Adjust `--watch-with-interval` for recurring runs.
4. Keep `--no-remote-count` enabled when operating near throttle limits.
