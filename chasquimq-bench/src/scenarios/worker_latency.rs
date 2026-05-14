// Do not be helpful and add latency to other scenarios — see
// .plans/latency-bench.md section 6 for why this would lie. The short
// version: preloaded scenarios' `created_at_ms` predates consumer start,
// so end_to_end_us would carry a constant per-job offset unrelated to
// engine behavior. Saturated `worker-concurrent` measures queue depth,
// not dispatch overhead. This scenario exists precisely because those
// numbers would be fiction.

use super::{LatencyDistribution, LatencyReport, ScenarioReport, Stopwatch, scaled_params};
use crate::sample::{Payload, generate_sample};
use chasquimq::config::{ConsumerConfig, ProducerConfig};
use chasquimq::metrics::{JobOutcome, MetricsSink};
use chasquimq::{Consumer, Job, Producer};
use hdrhistogram::Histogram;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

const HIST_LO: u64 = 1;
const HIST_HI: u64 = 600_000_000;
const HIST_SIGFIG: u8 = 3;
const INTER_ARRIVAL: Duration = Duration::from_millis(1);

struct LatencyState {
    handler: StdMutex<Histogram<u64>>,
    end_to_end: StdMutex<Histogram<u64>>,
    overflow: AtomicU64,
    warned: AtomicBool,
    /// Flipped to `true` when the warmup boundary is crossed. Per-job
    /// recorders consult this so the 500 warmup jobs don't pollute the
    /// distribution.
    warm: AtomicBool,
}

impl LatencyState {
    fn new() -> Self {
        Self {
            handler: StdMutex::new(make_hist()),
            end_to_end: StdMutex::new(make_hist()),
            overflow: AtomicU64::new(0),
            warned: AtomicBool::new(false),
            warm: AtomicBool::new(false),
        }
    }

    fn is_warm(&self) -> bool {
        self.warm.load(Ordering::Relaxed)
    }

    fn mark_warm(&self) {
        self.warm.store(true, Ordering::Relaxed);
    }

    fn record_handler(&self, value_us: u64) {
        if !self.is_warm() {
            return;
        }
        record_into(
            &self.handler,
            value_us,
            &self.overflow,
            &self.warned,
            "handler_us",
        );
    }

    fn record_end_to_end(&self, value_us: u64) {
        if !self.is_warm() {
            return;
        }
        record_into(
            &self.end_to_end,
            value_us,
            &self.overflow,
            &self.warned,
            "end_to_end_us",
        );
    }
}

fn make_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(HIST_LO, HIST_HI, HIST_SIGFIG)
        .expect("histogram bounds are valid")
}

fn record_into(
    hist: &StdMutex<Histogram<u64>>,
    value_us: u64,
    overflow: &AtomicU64,
    warned: &AtomicBool,
    which: &'static str,
) {
    let clamped = value_us.clamp(HIST_LO, HIST_HI);
    if value_us > HIST_HI {
        overflow.fetch_add(1, Ordering::Relaxed);
        if !warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                metric = which,
                value_us,
                ceiling_us = HIST_HI,
                "worker-latency: value exceeds histogram ceiling, clamping"
            );
        }
    }
    let mut guard = hist.lock().unwrap_or_else(|e| e.into_inner());
    let _ = guard.record(clamped);
}

struct LatencySink {
    state: Arc<LatencyState>,
}

impl MetricsSink for LatencySink {
    fn job_outcome(&self, outcome: JobOutcome) {
        self.state.record_handler(outcome.handler_duration_us);
    }
}

pub async fn run(redis_url: &str, queue: &str, scale: u32) -> ScenarioReport {
    let params = scaled_params(500, 10_000, scale);
    let total = params.warmup + params.bench;
    let payload: Payload = generate_sample(1, 1);

    let state = Arc::new(LatencyState::new());
    let sink_state = state.clone();

    let consumer_cfg = ConsumerConfig {
        queue_name: queue.to_string(),
        group: "bench".to_string(),
        consumer_id: "w1".to_string(),
        batch: 64,
        block_ms: 100,
        claim_min_idle_ms: 30_000,
        concurrency: 100,
        max_attempts: 3,
        ack_batch: 64,
        ack_idle_ms: 2,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 64,
        delayed_enabled: false,
        run_scheduler: false,
        metrics: Arc::new(LatencySink { state: sink_state }),
        ..Default::default()
    };

    let sw = Arc::new(TokioMutex::new(Stopwatch::new(params.warmup, params.bench)));
    let (done_tx, done_rx) = oneshot::channel::<super::ScenarioOutcome>();
    let done_tx = Arc::new(TokioMutex::new(Some(done_tx)));
    let consumer_shutdown = CancellationToken::new();
    let producer_shutdown = CancellationToken::new();

    let producer_cfg = ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 1,
        max_stream_len: 1_000_000,
        ..Default::default()
    };
    let producer: Producer<Payload> = Producer::connect(redis_url, producer_cfg)
        .await
        .expect("connect producer");

    let producer_task = {
        let producer = producer;
        let payload = payload.clone();
        let cancel = producer_shutdown.clone();
        tokio::spawn(async move {
            let mut emitted: u64 = 0;
            let mut next = Instant::now();
            while emitted < total {
                if cancel.is_cancelled() {
                    break;
                }
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep_until(next.into()) => {}
                }
                if producer.add(payload.clone()).await.is_err() {
                    break;
                }
                emitted += 1;
                next += INTER_ARRIVAL;
            }
        })
    };

    let consumer: Consumer<Payload> = Consumer::new(redis_url, consumer_cfg);
    let handler_state = state.clone();
    let consumer_shutdown_clone = consumer_shutdown.clone();
    let consumer_task = tokio::spawn(async move {
        consumer
            .run(
                {
                    let sw = sw.clone();
                    let done_tx = done_tx.clone();
                    let shutdown = consumer_shutdown_clone.clone();
                    let state = handler_state.clone();
                    move |job: Job<Payload>| {
                        let sw = sw.clone();
                        let done_tx = done_tx.clone();
                        let shutdown = shutdown.clone();
                        let state = state.clone();
                        async move {
                            let finish_us = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|d| d.as_micros())
                                .unwrap_or(0);
                            let created_us = (job.created_at_ms as u128) * 1_000;
                            let end_to_end_us =
                                finish_us.saturating_sub(created_us).min(u64::MAX as u128) as u64;

                            // Tick the stopwatch first so we can read its
                            // post-tick warm state in the same critical section.
                            // This flips `state.warm` exactly at the warmup
                            // boundary, so warmup jobs are excluded from both
                            // the end-to-end histogram (recorded here) and the
                            // handler histogram (recorded by `LatencySink`).
                            let outcome = {
                                let mut guard = sw.lock().await;
                                let outcome = guard.tick();
                                if guard.is_warm() {
                                    state.mark_warm();
                                }
                                outcome
                            };
                            state.record_end_to_end(end_to_end_us);

                            if let Some(outcome) = outcome
                                && let Some(tx) = done_tx.lock().await.take()
                            {
                                let _ = tx.send(outcome);
                                shutdown.cancel();
                            }
                            Ok(chasquimq::Bytes::new())
                        }
                    }
                },
                consumer_shutdown_clone,
            )
            .await
    });

    // BUG 3: avoid `expect()` panic if the consumer dies before reporting
    // (Redis disconnect, all attempts exhaust to DLQ, etc.). Fall back to a
    // generous timeout and surface scenario state on failure.
    const SCENARIO_TIMEOUT: Duration = Duration::from_secs(300);
    let outcome = match tokio::time::timeout(SCENARIO_TIMEOUT, done_rx).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(_)) => panic!(
            "worker-latency: done_tx dropped without firing (consumer task likely panicked); \
             warm={}, samples_handler={}, samples_end_to_end={}",
            state.warm.load(Ordering::Relaxed),
            state.handler.lock().map(|g| g.len()).unwrap_or(0),
            state.end_to_end.lock().map(|g| g.len()).unwrap_or(0),
        ),
        Err(_) => panic!(
            "worker-latency: timed out after {:?} waiting for {} bench jobs \
             (warm={}, samples_handler={}, samples_end_to_end={})",
            SCENARIO_TIMEOUT,
            params.warmup + params.bench,
            state.warm.load(Ordering::Relaxed),
            state.handler.lock().map(|g| g.len()).unwrap_or(0),
            state.end_to_end.lock().map(|g| g.len()).unwrap_or(0),
        ),
    };
    producer_shutdown.cancel();
    consumer_shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), producer_task).await;

    let mut report = outcome.into_report("worker-latency");
    let handler_dist = summarize(&state.handler);
    let end_to_end_dist = summarize(&state.end_to_end);
    report.latency = Some(LatencyReport {
        handler_us: handler_dist,
        end_to_end_us: end_to_end_dist,
        overflow_count: state.overflow.load(Ordering::Relaxed),
    });
    report
}

fn summarize(hist: &StdMutex<Histogram<u64>>) -> LatencyDistribution {
    let guard = hist.lock().unwrap_or_else(|e| e.into_inner());
    LatencyDistribution {
        p50_us: guard.value_at_quantile(0.50),
        p90_us: guard.value_at_quantile(0.90),
        p99_us: guard.value_at_quantile(0.99),
        p999_us: guard.value_at_quantile(0.999),
        max_us: guard.max(),
        samples: guard.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps_overflow_and_warns_once() {
        let state = LatencyState::new();
        state.mark_warm();
        state.record_handler(HIST_HI + 1);
        assert_eq!(state.overflow.load(Ordering::Relaxed), 1);
        assert!(state.warned.load(Ordering::Relaxed));
        state.record_handler(HIST_HI + 2);
        assert_eq!(state.overflow.load(Ordering::Relaxed), 2);
        let guard = state.handler.lock().unwrap();
        assert_eq!(guard.len(), 2);
    }

    #[test]
    fn underflow_clamps_to_one() {
        let state = LatencyState::new();
        state.mark_warm();
        state.record_handler(0);
        let guard = state.handler.lock().unwrap();
        assert_eq!(guard.len(), 1);
        assert!(guard.value_at_quantile(0.5) >= HIST_LO);
    }

    #[test]
    fn metrics_sink_forwards_handler_duration() {
        let state = Arc::new(LatencyState::new());
        state.mark_warm();
        let sink = LatencySink {
            state: state.clone(),
        };
        sink.job_outcome(JobOutcome {
            kind: chasquimq::metrics::JobOutcomeKind::Ok,
            attempt: 1,
            handler_duration_us: 42,
            name: String::new(),
        });
        let guard = state.handler.lock().unwrap();
        assert_eq!(guard.len(), 1);
        assert!(guard.value_at_quantile(1.0) >= 42);
    }

    #[test]
    fn record_pre_warmup_is_dropped() {
        let state = LatencyState::new();
        // Before warmup boundary: recordings must be discarded.
        state.record_handler(100);
        state.record_end_to_end(200);
        assert_eq!(state.handler.lock().unwrap().len(), 0);
        assert_eq!(state.end_to_end.lock().unwrap().len(), 0);
        // After mark_warm: recordings land.
        state.mark_warm();
        state.record_handler(100);
        state.record_end_to_end(200);
        assert_eq!(state.handler.lock().unwrap().len(), 1);
        assert_eq!(state.end_to_end.lock().unwrap().len(), 1);
    }
}
