pub struct Stats {
    pub mean: f64,
    pub stddev: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

pub fn compute_stats(samples: &[f64], discard_slowest: u32) -> Stats {
    let mut sorted: Vec<f64> = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let total = sorted.len();
    let discard = (discard_slowest as usize).min(total.saturating_sub(1));
    let kept: &[f64] = &sorted[discard..];
    let n = kept.len();
    let mean = kept.iter().copied().sum::<f64>() / n as f64;
    let var = kept.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
    let stddev = var.sqrt();
    Stats {
        mean,
        stddev,
        p50: percentile(kept, 50.0),
        p95: percentile(kept, 95.0),
        p99: percentile(kept, 99.0),
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p / 100.0) * (sorted.len() as f64 - 1.0);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = rank - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
}
