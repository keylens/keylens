//! keylens -- a TUI for Redis, Valkey and Recached that understands your keys.
//!
//! v0.1 is read-only by construction. That is a feature: it means you can point it at
//! production on day one.

use clap::{Parser, Subcommand};
use color_eyre::eyre::{Result, eyre};

use keylens::config::Config;
use keylens::{browse, probe};

const DEFAULT_URL: &str = "redis://127.0.0.1:6379";

#[derive(Parser, Debug)]
#[command(
    name = "keylens",
    version,
    about = "A TUI for Redis, Valkey and Recached that understands your keys"
)]
struct Cli {
    /// Connection URL. Supports redis://, rediss://, redis-sentinel://, redis-cluster://
    #[arg(short, long, env = "KEYLENS_URL", global = true)]
    url: Option<String>,

    /// Name of a connection from your config file. Run `keylens connections` to see them
    /// and the exact path on this platform.
    #[arg(short, long, global = true)]
    name: Option<String>,

    /// Disable colour. Also honoured via the NO_COLOR environment variable.
    #[arg(long, global = true)]
    no_color: bool,

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

    /// List the named connections in your config file.
    Connections,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse before installing the error hook: colour has to be decided first, because
    // color-eyre does its own colouring and would otherwise emit escape codes into a
    // NO_COLOR terminal regardless of what the theme does.
    let cli = Cli::parse();
    let no_color = cli.no_color || std::env::var_os("NO_COLOR").is_some();

    if no_color {
        color_eyre::config::HookBuilder::default()
            .theme(color_eyre::config::Theme::new())
            .install()?;
    } else {
        color_eyre::install()?;
    }
    keylens_ui::theme::set_color_enabled(!no_color);

    init_logging(&cli)?;

    // Listing connections must not require a reachable server.
    if matches!(cli.command, Some(Command::Connections)) {
        return list_connections();
    }

    let url = resolve_url(&cli)?;

    match cli.command {
        Some(Command::Probe { queues }) => probe::run(&url, queues).await,
        // Bare `keylens` opens the browser -- the common case should need no subcommand.
        Some(Command::Browse) | None => browse::run(&url).await,
        Some(Command::Connections) => unreachable!("handled above"),
    }
}

/// Set up logging without letting it corrupt the TUI.
///
/// The browser owns the terminal, and anything written to stderr lands on top of the
/// rendered frame — the client library logging one warning is enough to garble the status
/// bar. So the interactive path logs to a file, or nowhere; only the non-interactive
/// commands write to stderr.
fn init_logging(cli: &Cli) -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_env("KEYLENS_LOG")
        .unwrap_or_else(|_| "warn".into());

    let interactive = matches!(cli.command, None | Some(Command::Browse));

    if !interactive {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
        return Ok(());
    }

    match std::env::var_os("KEYLENS_LOG_FILE") {
        Some(path) => {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|e| eyre!("could not open {}: {e}", path.to_string_lossy()))?;
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_ansi(false)
                .with_writer(file)
                .init();
        }
        // No log file configured: discard rather than paint over the UI.
        None => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(std::io::sink)
                .init();
        }
    }
    Ok(())
}

fn list_connections() -> Result<()> {
    let path = Config::default_path();
    let cfg = Config::load_default();

    match &path {
        Some(p) => println!("config: {}", p.display()),
        None => println!("config: <could not resolve a config directory>"),
    }

    if cfg.connections.is_empty() {
        println!("\nno named connections configured.\n");
        println!("create the file above with entries like:\n");
        println!("  [[connections]]");
        println!("  name = \"prod\"");
        println!("  url = \"rediss://user:pass@prod.example.com:6379\"");
        println!("  readonly = true");
        println!("\nthen:  keylens --name prod");
        return Ok(());
    }

    println!();
    let width = cfg
        .connections
        .iter()
        .map(|c| c.name.len())
        .max()
        .unwrap_or(4);
    for c in &cfg.connections {
        // The URL can carry a password, so it is masked rather than printed.
        println!(
            "  {:<width$}  {}{}",
            c.name,
            mask_url(&c.url),
            if c.readonly { "  [readonly]" } else { "" }
        );
    }
    println!("\nuse:  keylens --name <name>");
    Ok(())
}

/// Replace any password in a connection URL with `***`.
///
/// `keylens connections` is exactly the command someone runs while screen-sharing.
fn mask_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let Some((creds, host)) = rest.split_once('@') else {
        return url.to_string();
    };
    match creds.split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:***@{host}"),
        None => format!("{scheme}://{creds}@{host}"),
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

#[cfg(test)]
mod tests {
    use super::mask_url;

    #[test]
    fn masks_passwords_but_keeps_the_rest_readable() {
        assert_eq!(
            mask_url("rediss://admin:hunter2@prod.example.com:6379"),
            "rediss://admin:***@prod.example.com:6379"
        );
        // No password, nothing to hide.
        assert_eq!(mask_url("redis://127.0.0.1:6379"), "redis://127.0.0.1:6379");
        assert_eq!(mask_url("redis://user@host:6379"), "redis://user@host:6379");
        assert_eq!(mask_url("not-a-url"), "not-a-url");
    }
}
