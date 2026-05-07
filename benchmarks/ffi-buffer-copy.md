# FFI buffer-copy bench gate

**Scope.** Isolate the cost of moving a msgpack payload across the
Rust↔Node and Rust↔Python FFI seams as a regression-trackable number
in nanoseconds per byte. Closes the "FFI buffer-copy bench gate"
[`CLAUDE.md`](../CLAUDE.md) flagged post-Phase 4.

The harness is a Criterion microbench at
[`chasquimq-bench/benches/ffi_buffer_copy.rs`](../chasquimq-bench/benches/ffi_buffer_copy.rs).
It exercises the exact convert primitives both bindings hit:

* **Inbound** (host → engine): the binding wrapper takes a `&[u8]` view
  out of the host VM (`napi::bindgen_prelude::Buffer::as_ref()` on the
  Node side, `pyo3::types::PyBytes::as_bytes()` on the Python side)
  and lands it in a refcounted `bytes::Bytes` via
  `Bytes::copy_from_slice`. This is the only buffer copy the inbound
  path does — the engine then carries `RawBytes(Bytes)` end-to-end
  through `XADD` / `ZADD` without re-copying.
* **Outbound** (engine → host): the DLQ peek path does
  `Buffer::from(payload.to_vec())` (Node) and the equivalent
  `PyBytes::new(py, slice)` (Python). Both end in a single `memcpy`
  out of the shared `Bytes` into a freshly-allocated host-VM buffer;
  refcount sharing ends at the FFI boundary.

The bench measures the Rust side of the seam in isolation. The
`benchmarks/scripts/python_handler_bench.py` script measures the
end-to-end Python FFI cross including PyO3 overhead.

## Run

```bash
cargo bench -p chasquimq-bench --bench ffi_buffer_copy
```

Add `-- --save-baseline post-1.0` to checkpoint a baseline that future
runs can `diff` against:

```bash
cargo bench -p chasquimq-bench --bench ffi_buffer_copy -- \
    --save-baseline post-1.0
# ... change something ...
cargo bench -p chasquimq-bench --bench ffi_buffer_copy -- \
    --baseline post-1.0
```

## Baseline

**Run date:** 2026-05-07
**Host:** Apple M3, 8 logical cores, macOS 15.
**Toolchain:** Rust 1.85, `cargo bench --release` (LTO thin, codegen-units 1).
**Tool:** Criterion 0.5, 100 samples × ≥40M iterations per group.

### Inbound: `Bytes::copy_from_slice(host_view)`

Mirrors `pybytes_to_bytes` (Python) and `buffer_to_bytes` (Node).

| Payload | Mean   | Throughput  |
|--------:|-------:|------------:|
|     64B | 14.2ns |   4.19 GiB/s |
|    256B | 14.8ns |  16.13 GiB/s |
|   1024B | 23.9ns |  39.92 GiB/s |
|   4096B | 60.0ns |  63.53 GiB/s |

### Outbound: `Bytes::to_vec()` (Node `Buffer::from(Vec)`)

Mirrors `Buffer::from(e.payload.to_vec())` in `chasquimq-node/src/producer.rs`.

| Payload | Mean    | Throughput   |
|--------:|--------:|-------------:|
|     64B |  13.7ns |   4.34 GiB/s |
|    256B |  30.0ns |   7.96 GiB/s |
|   1024B |  83.2ns |  11.47 GiB/s |
|   4096B | 313.4ns |  12.17 GiB/s |

### Outbound: PyBytes-equivalent `slice.to_vec()`

Approximates `PyBytes::new(py, slice)` — same memcpy, plus a fixed
~32-byte CPython object-header alloc (not measured here; small-object
arena keeps it sub-ns amortized).

| Payload | Mean   | Throughput   |
|--------:|-------:|-------------:|
|     64B | 14.0ns |   4.26 GiB/s |
|    256B | 13.8ns |  17.24 GiB/s |
|   1024B | 23.5ns |  40.55 GiB/s |
|   4096B | 56.8ns |  67.17 GiB/s |

## Interpretation

* **The 256-byte case is the one to watch.** It approximates a
  realistic msgpack job payload (10×10 dict + name + framing). At
  ~15ns inbound + ~30ns outbound = **~45ns per round trip** at the
  byte-copy layer. Multiply by `worker-concurrent`'s ~420k jobs/s
  ceiling on this host: 18.9ms/sec of CPU spent on FFI copies, ~1.9%
  of one core. Not the bottleneck.
* **The Python dispatch seam is ~3 orders of magnitude more
  expensive** than the byte copy itself. The
  [`python-handler.md`](python-handler.md) bench measures ~46.7k
  jobs/s — an aggregate ~21µs per dispatch. Of that, the byte copy is
  ~0.1%. PyO3 GIL acquire + `into_future_with_locals` setup +
  asyncio task creation + awaitable resolution dominate.
* **The two outbound paths diverge at large payloads.** `Bytes::to_vec()`
  hits ~12 GiB/s, `slice.to_vec()` hits ~67 GiB/s. The discrepancy is
  attributable to allocator behavior: `Bytes::to_vec()` exits via
  `Vec::from_iter`-like paths in some code shapes, while a direct
  `slice.to_vec()` with a known length is the libc-`memcpy` fast
  path. Both are dominated by the host-VM allocator, not by the
  binding.

## Regression criteria

A change to the binding crates that drops the inbound 256B figure
below 16 GiB/s on this host (under similar load) is a regression
worth investigating. Re-run with `--save-baseline post-1.0` and
diff future runs against it.

The numbers above are stable run-to-run (per-row p-value > 0.05 in 3
of 4 inbound rows on the second-pass measurement). The 4096B inbound
row drifts ~5% across runs because `memcpy` at that size is
cache-line-bound and sensitive to host noise; treat it as a soft gate.
