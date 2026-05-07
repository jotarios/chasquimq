//! FFI buffer-copy microbench gate.
//!
//! Measures the Rust-side cost of moving a msgpack payload across the FFI
//! boundary into the engine's `RawBytes` newtype, isolating the buffer
//! copy from any Redis or async runtime work. Exercises the same
//! `Bytes::copy_from_slice(&slice)` path both bindings hit — the Node
//! `Buffer::as_ref()` returns `&[u8]` and the PyO3 `PyBytes::as_bytes()`
//! returns `&[u8]`, and both bindings then call
//! `Bytes::copy_from_slice` to land in `RawBytes(Bytes)`. This bench
//! reproduces that exact step.
//!
//! On the return path, `Buffer::from(Vec<u8>)` (Node) and
//! `PyBytes::new(py, &[u8])` (Python) both copy out of the engine's
//! `Bytes`. The `to_vec` benches below cover the Node variant; the
//! Python variant can't be measured outside an embedded interpreter,
//! but the underlying `to_vec` cost is identical (it's just a `memcpy`
//! into a fresh allocation). The end-to-end FFI cross is measured by
//! `benchmarks/scripts/python_handler_bench.py`.
//!
//! Payload sizes mirror `bullmq-bench`'s default (10x10 = 100 bytes
//! after msgpack framing) and the bigger 256-byte case from real
//! workloads.

use bytes::Bytes;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

fn bench_copy_from_slice(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_in_copy_from_slice");
    for &size in &[64usize, 256, 1024, 4096] {
        let payload: Vec<u8> = (0..size).map(|i| i as u8).collect();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{size}B"), |b| {
            b.iter(|| {
                // Mirrors `pybytes_to_bytes(p)` and `buffer_to_bytes(p)`:
                // each binding takes a borrowed `&[u8]` from the host VM and
                // copies into a refcounted `Bytes` so the engine owns it.
                let bytes = Bytes::copy_from_slice(black_box(&payload));
                black_box(bytes)
            });
        });
    }
    group.finish();
}

fn bench_buffer_out_to_vec(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_out_to_vec");
    for &size in &[64usize, 256, 1024, 4096] {
        let bytes = Bytes::from((0..size).map(|i| i as u8).collect::<Vec<u8>>());
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{size}B"), |b| {
            b.iter_batched(
                || bytes.clone(),
                |b| {
                    // Mirrors the DLQ peek path's
                    // `Buffer::from(e.payload.to_vec())`. The `to_vec`
                    // copy is the unavoidable cost — once the bytes leave
                    // the engine into the host VM's allocator, refcount
                    // sharing ends.
                    let v: Vec<u8> = b.to_vec();
                    black_box(v)
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_pybytes_new_equivalent(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_out_pybytes_equivalent");
    for &size in &[64usize, 256, 1024, 4096] {
        let bytes = Bytes::from((0..size).map(|i| i as u8).collect::<Vec<u8>>());
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{size}B"), |b| {
            b.iter(|| {
                // PyO3's `PyBytes::new(py, slice)` does a single memcpy
                // into a freshly-allocated CPython `bytes` object. We
                // approximate that with a `Vec<u8>` allocation + memcpy
                // out of the same `&[u8]` view. The *Rust* cost is the
                // same; the CPython object header is an extra ~32 bytes
                // amortized. PyO3-side measurement lives in the
                // `benchmarks/scripts/python_ffi_bench.py` script.
                let slice: &[u8] = black_box(bytes.as_ref());
                let v: Vec<u8> = slice.to_vec();
                black_box(v)
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_copy_from_slice,
    bench_buffer_out_to_vec,
    bench_pybytes_new_equivalent
);
criterion_main!(benches);
