# Download Chunk Benchmark (Synthetic)

Date: 2026-03-03

Command used:

```bash
.venv/bin/python scripts/benchmark_download_chunks.py \
  --workers 4 \
  --size-mib 8 \
  --iterations 2 \
  --chunk-bytes 65536 262144 1048576
```

## Throughput vs CPU

| Chunk bytes | Verify checksum | Avg Throughput (MiB/s) | Avg CPU seconds |
|---|---|---:|---:|
| 65536 | no | 277.90 | 0.0505 |
| 65536 | yes | 137.00 | 0.1496 |
| 262144 | no | 663.89 | 0.0297 |
| 262144 | yes | 207.23 | 0.1291 |
| 1048576 | no | 704.66 | 0.0293 |
| 1048576 | yes | 181.65 | 0.1437 |

Notes:
- Larger chunks improve throughput significantly vs 64 KiB in this synthetic stream test.
- Enabling checksum verification increases CPU cost and reduces throughput, as expected.
- `262144` and `1048576` are close on CPU cost when checksum is disabled; `262144` remains a good default.

## Memory boundedness verification

A dedicated integration test verifies streaming memory remains bounded during large transfers:

```bash
.venv/bin/python -m pytest \
  tests/test_download_config.py::DownloadConfigTestCase::test_download_response_streaming_memory_is_bounded -q
```

Test behavior:
- Streams a 64 MiB response to disk using `--download-chunk-bytes=65536`.
- Asserts peak traced memory stays below 8 MiB (well below transferred bytes), confirming bounded streaming behavior.
