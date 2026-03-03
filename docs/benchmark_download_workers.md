# Download Worker Benchmark (Synthetic)

Date: 2026-03-03

Command used:

```bash
.venv/bin/python scripts/benchmark_download_workers.py \
  --workers 1 2 4 8 \
  --tasks 400 \
  --latency-seconds 0.01 \
  --throttle-probability 0.03 \
  --seed 1337
```

Results:

| Workers | Throughput (tasks/s) | Error Rate |
|---|---:|---:|
| 1 | 92.92 | 0.02 |
| 2 | 157.01 | 0.03 |
| 4 | 289.67 | 0.02 |
| 8 | 364.21 | 0.03 |

Raw JSON:

```json
{
  "timestamp_epoch": 1772548722.9070098,
  "seed": 1337,
  "tasks": 400,
  "latency_seconds": 0.01,
  "throttle_probability": 0.03,
  "results": [
    {
      "workers": 1,
      "tasks": 400,
      "successes": 392,
      "errors": 8,
      "error_rate": 0.02,
      "throughput_tasks_per_sec": 92.91940860752013,
      "elapsed_seconds": 4.304805701998703
    },
    {
      "workers": 2,
      "tasks": 400,
      "successes": 388,
      "errors": 12,
      "error_rate": 0.03,
      "throughput_tasks_per_sec": 157.00543983061695,
      "elapsed_seconds": 2.5476824270008365
    },
    {
      "workers": 4,
      "tasks": 400,
      "successes": 392,
      "errors": 8,
      "error_rate": 0.02,
      "throughput_tasks_per_sec": 289.6725751729342,
      "elapsed_seconds": 1.3808694170002127
    },
    {
      "workers": 8,
      "tasks": 400,
      "successes": 388,
      "errors": 12,
      "error_rate": 0.03,
      "throughput_tasks_per_sec": 364.2110641947837,
      "elapsed_seconds": 1.0982642739982111
    }
  ]
}
```
