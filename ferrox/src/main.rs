//! Ferrox command-line entrypoint.
//!
//! This binary is intentionally thin: it parses arguments and dispatches to
//! the workspace crates that own real behavior. Subcommands are wired in as
//! their backing crates land across the phased delivery plan.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "ferrox",
    version,
    about = "Rust-native Airflow scheduler replacement"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the scheduler against the configured metadata database.
    Start {
        /// Path to ferrox.toml. Defaults to ./ferrox.toml.
        #[arg(long, value_name = "FILE")]
        config: Option<PathBuf>,
    },
    /// Validate an Airflow metadata database for Ferrox compatibility.
    Validate {
        /// Connection URL for the Airflow metadata database.
        #[arg(long, value_name = "URL")]
        db: String,
    },
    /// Translate an existing Airflow scheduler environment into ferrox.toml.
    Migrate {
        /// Connection URL for the Airflow metadata database.
        #[arg(long, value_name = "URL")]
        db: String,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Command::Start { .. } | Command::Validate { .. } | Command::Migrate { .. } => {
            // The scheduler, validator, and migrator crates are still being
            // built out; fail loudly rather than pretend to do the work.
            eprintln!("ferrox: this command is not available in the current build yet");
            ExitCode::from(64)
        }
    }
}
