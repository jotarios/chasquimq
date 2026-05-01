use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "chasquimq-bench", about = "Benchmark harness for ChasquiMQ")]
pub struct Args {
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    pub redis_url: String,

    #[arg(long, value_delimiter = ',', default_values_t = vec![
        "queue-add".to_string(),
        "queue-add-bulk".to_string(),
        "worker-generic".to_string(),
        "worker-concurrent".to_string(),
    ])]
    pub scenario: Vec<String>,

    #[arg(long, default_value_t = 3)]
    pub repeats: u32,

    /// Multiply warmup+bench job counts by this factor.
    /// Default 1 keeps parity with bullmq-bench.
    /// Use --scale=10 for tighter numbers (bench window grows ~10×).
    #[arg(long, default_value_t = 1)]
    pub scale: u32,

    /// Drop the N slowest repeats per scenario from the mean (cold-start outliers).
    /// Effective only when repeats > N + 1.
    #[arg(long, default_value_t = 1)]
    pub discard_slowest: u32,

    #[arg(long, value_enum, default_value_t = Format::Markdown)]
    pub format: Format,

    /// Log level inside the bench process. Defaults to ERROR so retry warnings
    /// don't pollute timing.
    #[arg(long, default_value = "error")]
    pub log_level: String,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum Format {
    Markdown,
    Jsonl,
}
