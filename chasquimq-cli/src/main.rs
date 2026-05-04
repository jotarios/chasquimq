mod inspect;

use clap::{Parser, Subcommand};

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";
const DEFAULT_GROUP: &str = "default";

#[derive(Parser, Debug)]
#[command(
    name = "chasqui",
    about = "ChasquiMQ command-line dashboard",
    version,
    subcommand_required = true,
    arg_required_else_help = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// One-shot snapshot of a queue: stream depth, pending, DLQ depth,
    /// delayed depth, oldest delayed lag, and repeatable count.
    Inspect {
        /// Queue name (without the `{chasqui:...}` hash-tag wrapping).
        queue: String,
        #[arg(long, default_value = DEFAULT_REDIS_URL)]
        redis_url: String,
        /// Consumer group used to compute pending count. Matches the
        /// engine default `ConsumerConfig::group`.
        #[arg(long, default_value = DEFAULT_GROUP)]
        group: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Inspect {
            queue,
            redis_url,
            group,
        } => inspect::run(&redis_url, &queue, &group).await,
    }
}
