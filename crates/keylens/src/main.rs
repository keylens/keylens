//! keylens -- a TUI for Redis, Valkey and Recached that understands your keys.
//!
//! v0.1 is read-only by construction. That is a feature: it means you can point it at
//! production on day one.
// `unwrap` in a test is a deliberate assertion, not a reachable panic: the lint that
// guards the production paths would otherwise force `?` into every fixture.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

use clap::{Parser, Subcommand};
use color_eyre::eyre::{Result, eyre};

use keylens::config::{Config, Target, permission_warning};
use keylens::{browse, probe};
use keylens_conn::redact_url;

const DEFAULT_URL: &str = "redis://127.0.0.1:6379";

// Not `Debug`: it holds the url, password and all.
#[derive(Parser)]
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

    warn_if_password_in_argv(&cli);
    let target = resolve_target(&cli)?;

    match cli.command {
        Some(Command::Probe { queues }) => probe::run(&target, queues).await,
        // Bare `keylens` opens the browser -- the common case should need no subcommand.
        Some(Command::Browse) | None => browse::run(&target).await,
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
            let file = open_log_file(std::path::Path::new(&path))?;
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

/// Open the debug log: owner-only, appended, never through a symlink.
///
/// Debug logs carry key names, command shapes and server details. 0.1.7 created the file
/// with the process umask (typically 0644, world-readable) and followed a symlink at the
/// path, so a pre-planted link in a shared directory redirected the log anywhere the user
/// could write.
fn open_log_file(path: &std::path::Path) -> Result<std::fs::File> {
    if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(eyre!(
            "refusing to log to {}: it is a symbolic link",
            path.display()
        ));
    }
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Applies when the file is created; an existing file keeps its mode, checked below.
        options.mode(0o600);
    }
    let file = options
        .open(path)
        .map_err(|e| eyre!("could not open {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = file.metadata()
            && meta.permissions().mode() & 0o077 != 0
        {
            eprintln!(
                "warning: log file {} is readable by other users; it can contain key names \
                 and values. Run: chmod 600 '{}'",
                path.display(),
                path.display()
            );
        }
    }
    Ok(file)
}

/// Say so, once, when a password arrived on the command line.
///
/// Arguments are visible to every local user through `ps` and land in shell history;
/// the environment and the config file are not visible that way.
fn warn_if_password_in_argv(cli: &Cli) {
    let Some(url) = &cli.url else { return };
    let from_env = std::env::var("KEYLENS_URL").is_ok_and(|env| env == *url);
    if !from_env && redact_url(url) != *url {
        eprintln!(
            "note: the password in --url is visible to other local users via `ps` and is \
             saved in shell history. Prefer KEYLENS_URL or a named connection in the config \
             file (`keylens connections` shows where)."
        );
    }
}

fn warn_if_config_is_readable(path: Option<&std::path::Path>, cfg: &Config) {
    if let Some(warning) = path.and_then(|p| permission_warning(p, cfg)) {
        eprintln!("{warning}");
    }
}

fn list_connections() -> Result<()> {
    let path = Config::default_path();
    let cfg = Config::load_default()?;
    warn_if_config_is_readable(path.as_deref(), &cfg);

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
            keylens_ui::format::terminal_safe(&c.name),
            keylens_ui::format::terminal_safe(&redact_url(&c.url)),
            if c.readonly { "  [readonly]" } else { "" }
        );
    }
    println!("\nuse:  keylens --name <name>");
    Ok(())
}

/// Replace any password in a connection URL with `***`.
///
/// `keylens connections` is exactly the command someone runs while screen-sharing.
/// Precedence: `--url` (or `KEYLENS_URL`) > `--name` from config > default.
///
/// Only the `--name` path can carry a lens prefix, because that is the only path with a
/// config entry behind it to read one from.
fn resolve_target(cli: &Cli) -> Result<Target> {
    if let Some(url) = &cli.url {
        return Ok(Target::new(url.clone()));
    }

    if let Some(name) = &cli.name {
        // A parse failure propagates rather than reading as an empty config. The two are
        // very different problems and only one of them is the user's to fix by adding an
        // entry they have, in fact, already added.
        let cfg = Config::load_default()?;
        warn_if_config_is_readable(Config::default_path().as_deref(), &cfg);
        return cfg
            .get(name)
            .map(|c| Target {
                url: c.url.clone(),
                prefix: c.prefix.clone(),
            })
            .ok_or_else(|| {
                let known: Vec<_> = cfg.connections.iter().map(|c| c.name.clone()).collect();
                if !known.is_empty() {
                    return eyre!("no connection named `{name}`; known: {}", known.join(", "));
                }
                // Say which empty this is. "No config file" and "a config file listing no
                // connections" send the user to different places.
                match Config::default_path() {
                    Some(p) if p.exists() => eyre!(
                        "no connection named `{name}`; {} has no [[connections]] entries",
                        p.display()
                    ),
                    Some(p) => eyre!(
                        "no connection named `{name}`; no config file at {}",
                        p.display()
                    ),
                    None => {
                        eyre!("no connection named `{name}`; could not resolve a config directory")
                    }
                }
            });
    }

    Ok(Target::new(DEFAULT_URL))
}

#[cfg(test)]
mod tests {
    use keylens_conn::redact_url;

    #[test]
    fn masks_passwords_but_keeps_the_rest_readable() {
        assert_eq!(
            redact_url("rediss://admin:hunter2@prod.example.com:6379"),
            "rediss://admin:***@prod.example.com:6379"
        );
        // No password, nothing to hide.
        assert_eq!(
            redact_url("redis://127.0.0.1:6379"),
            "redis://127.0.0.1:6379"
        );
        assert_eq!(
            redact_url("redis://user@host:6379"),
            "redis://user@host:6379"
        );
        assert_eq!(redact_url("not-a-url"), "not-a-url");
    }
}
