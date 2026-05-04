mod dlq;
mod inspect;
mod repeatable;

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
    /// Inspect or replay dead-letter queue entries.
    #[command(subcommand)]
    Dlq(DlqCommand),
    /// List or remove repeatable job specs.
    #[command(subcommand)]
    Repeatable(RepeatableCommand),
}

#[derive(Subcommand, Debug)]
enum DlqCommand {
    /// Render the next N DLQ entries with a histogram by reason.
    Peek {
        queue: String,
        #[arg(long, default_value_t = 20)]
        limit: u32,
        #[arg(long, default_value = DEFAULT_REDIS_URL)]
        redis_url: String,
    },
    /// Atomically replay up to N DLQ entries back into the main stream.
    /// The replayed jobs get a fresh retry budget (attempt counter reset).
    Replay {
        queue: String,
        #[arg(long, default_value_t = 100)]
        limit: u32,
        /// Skip the interactive confirmation prompt.
        #[arg(long)]
        yes: bool,
        #[arg(long, default_value = DEFAULT_REDIS_URL)]
        redis_url: String,
    },
}

#[derive(Subcommand, Debug)]
enum RepeatableCommand {
    /// List repeatable specs ordered by next fire time, ascending.
    List {
        queue: String,
        #[arg(long, default_value_t = 100)]
        limit: u32,
        #[arg(long, default_value = DEFAULT_REDIS_URL)]
        redis_url: String,
    },
    /// Remove a repeatable spec by its resolved key.
    Remove {
        queue: String,
        key: String,
        #[arg(long, default_value = DEFAULT_REDIS_URL)]
        redis_url: String,
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
        Command::Dlq(DlqCommand::Peek {
            queue,
            limit,
            redis_url,
        }) => dlq::peek(&redis_url, &queue, limit).await,
        Command::Dlq(DlqCommand::Replay {
            queue,
            limit,
            yes,
            redis_url,
        }) => dlq::replay(&redis_url, &queue, limit, yes).await,
        Command::Repeatable(RepeatableCommand::List {
            queue,
            limit,
            redis_url,
        }) => repeatable::list(&redis_url, &queue, limit).await,
        Command::Repeatable(RepeatableCommand::Remove {
            queue,
            key,
            redis_url,
        }) => repeatable::remove(&redis_url, &queue, &key).await,
    }
}
