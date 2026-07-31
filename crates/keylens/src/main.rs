//! keylens -- a TUI for Redis and Valkey that understands your keys.
//!
//! v0.1 is read-only by construction. That is a feature: it means you can point it at
//! production on day one.

use clap::{Parser, Subcommand};
use color_eyre::eyre::{eyre, Result};

use keylens::config::Config;
use keylens::{browse, probe};

const DEFAULT_URL: &str = "redis://127.0.0.1:6379";

#[derive(Parser, Debug)]
#[command(
    name = "keylens",
    version,
    about = "A TUI for Redis and Valkey that understands your keys"
)]
struct Cli {
    /// Connection URL. Supports redis://, rediss://, redis-sentinel://, redis-cluster://
    #[arg(short, long, env = "KEYLENS_URL", global = true)]
    url: Option<String>,

    /// Name of a connection from ~/.config/keylens/config.toml
    #[arg(short, long, global = true)]
    name: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Connect, detect the server vendor, probe capabilities and list detected lenses.
    ///
    /// Run this first against an unfamiliar server -- it reports exactly which panes will
    /// work and which are blocked by the host.
    Probe {
        /// Also list detected BullMQ queues with per-state counts.
        #[arg(long)]
        queues: bool,
    },

    /// Open the interactive key browser. This is also what a bare `keylens` does.
    Browse,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("KEYLENS_LOG")
                .unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let url = resolve_url(&cli)?;

    match cli.command {
        Some(Command::Probe { queues }) => probe::run(&url, queues).await,
        // Bare `keylens` opens the browser -- the common case should need no subcommand.
        Some(Command::Browse) | None => browse::run(&url).await,
    }
}

/// Precedence: `--url` (or `KEYLENS_URL`) > `--name` from config > default.
fn resolve_url(cli: &Cli) -> Result<String> {
    if let Some(url) = &cli.url {
        return Ok(url.clone());
    }

    if let Some(name) = &cli.name {
        let cfg = Config::load_default();
        return cfg.get(name).map(|c| c.url.clone()).ok_or_else(|| {
            let known: Vec<_> = cfg.connections.iter().map(|c| c.name.clone()).collect();
            if known.is_empty() {
                eyre!(
                    "no connection named `{name}`; no config file found at {}",
                    Config::default_path()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "<unknown>".into())
                )
            } else {
                eyre!("no connection named `{name}`; known: {}", known.join(", "))
            }
        });
    }

    Ok(DEFAULT_URL.to_string())
}
