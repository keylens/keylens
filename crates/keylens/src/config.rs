//! Named connections in `~/.config/keylens/config.toml`.
//!
//! Typing `--url` every time is friction, and friction is the thing that decides whether
//! a tool gets used daily or once. A named connection is also where `readonly = true`
//! lives, so a production entry can be marked un-mutable before v0.2 ships mutations.

use std::path::{Path, PathBuf};

use keylens_conn::redact_url;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub connections: Vec<Connection>,
}

/// Unknown keys are rejected rather than ignored.
///
/// The failure mode this prevents is specific: a misspelled `prefix` leaves detection
/// scanning `bull:*:meta`, which finds nothing, and "no queues here" is a perfectly
/// normal answer — so the typo produces a working tool that is quietly looking in the
/// wrong place. There is no symptom to notice.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connection {
    pub name: String,
    pub url: String,
    /// Reserved for v0.2 mutations, which do not exist yet -- v0.1 is read-only
    /// throughout, so today this only labels the entry in `keylens connections`. Parsed
    /// now so a production entry written today keeps meaning what it says later.
    #[serde(default)]
    pub readonly: bool,
    /// Key prefix for the BullMQ lens, when the keyspace does not use the default `bull`.
    ///
    /// Detection scans `<prefix>:*:meta`, so on a custom prefix it finds nothing and the
    /// queues tab never appears -- with no error, because "no queues here" is a perfectly
    /// normal answer. This is how you tell it where to look instead.
    #[serde(default)]
    pub prefix: Option<String>,
}

/// Debug output redacts the url. A derived `Debug` printed the password into any log
/// line, panic message or `{:?}` that ever touched a connection.
impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("name", &self.name)
            .field("url", &redact_url(&self.url))
            .field("readonly", &self.readonly)
            .field("prefix", &self.prefix)
            .finish()
    }
}

/// A connection resolved from flags, environment and config, with everything downstream
/// needs to open it.
#[derive(Clone, PartialEq)]
pub struct Target {
    pub url: String,
    /// Lens prefix override, if the config named one.
    pub prefix: Option<String>,
}

impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Target")
            .field("url", &redact_url(&self.url))
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl Target {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            prefix: None,
        }
    }
}

impl Config {
    /// `~/.config/keylens/config.toml` (XDG on Linux, Application Support on macOS).
    pub fn default_path() -> Option<PathBuf> {
        directories::ProjectDirs::from("", "", "keylens")
            .map(|d| d.config_dir().join("config.toml"))
    }

    pub fn load(path: &Path) -> color_eyre::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&raw)?)
    }

    /// Load from the default path, tolerating absence. A missing config is the normal
    /// first-run state, not an error.
    ///
    /// # Errors
    ///
    /// A config file that exists but does not parse. **Absence and malformed are not the
    /// same answer** — swallowing the parse error left the caller holding an empty config
    /// and reporting "no config file found", naming a path where the user's file was
    /// sitting the whole time, with their connection in it.
    pub fn load_default() -> color_eyre::Result<Self> {
        match Self::default_path() {
            Some(path) => Self::load(&path),
            None => Ok(Self::default()),
        }
    }

    pub fn get(&self, name: &str) -> Option<&Connection> {
        self.connections.iter().find(|c| c.name == name)
    }

    /// Whether any connection url carries a credential.
    pub fn has_credentials(&self) -> bool {
        self.connections.iter().any(|c| redact_url(&c.url) != c.url)
    }
}

/// A warning when a config file holding credentials can be read by other local users.
///
/// The file is where people are told to put production urls, passwords included. Like
/// `ssh`, keylens does not refuse -- the user may have reasons -- but it says so, once,
/// before the TUI takes the terminal. `None` when the file holds no credentials, is
/// private, or the platform has no unix permission bits.
pub fn permission_warning(path: &Path, cfg: &Config) -> Option<String> {
    if !cfg.has_credentials() {
        return None;
    }
    let mode = file_mode(path)?;
    (mode & 0o077 != 0).then(|| {
        format!(
            "warning: {} contains passwords and is readable by other users (mode {:o}). \
             Run: chmod 600 '{}'",
            path.display(),
            mode & 0o777,
            path.display()
        )
    })
}

#[cfg(unix)]
fn file_mode(path: &Path) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).ok().map(|m| m.permissions().mode())
}

#[cfg(not(unix))]
fn file_mode(_path: &Path) -> Option<u32> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_connections() {
        let raw = r#"
            [[connections]]
            name = "prod"
            url = "rediss://user:pass@prod.example.com:6379"
            readonly = true

            [[connections]]
            name = "local"
            url = "redis://127.0.0.1:6379"
        "#;
        let cfg: Config = toml::from_str(raw).unwrap();
        assert_eq!(cfg.connections.len(), 2);
        assert!(cfg.get("prod").unwrap().readonly);
        // readonly defaults to false so local entries need no boilerplate
        assert!(!cfg.get("local").unwrap().readonly);
        assert!(cfg.get("staging").is_none());
    }

    #[test]
    fn a_custom_lens_prefix_survives_parsing() {
        // The README documents this and it has to actually reach the lens: detection
        // scans `<prefix>:*:meta`, so a dropped prefix means no queues tab at all, with
        // no error to explain why.
        let cfg: Config = toml::from_str(
            r#"
            [[connections]]
            name = "queues"
            url = "redis://jobs.internal:6379"
            prefix = "myapp"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.get("queues").unwrap().prefix.as_deref(), Some("myapp"));
    }

    #[test]
    fn a_connection_without_a_prefix_leaves_the_lens_default_alone() {
        let cfg: Config = toml::from_str(
            r#"
            [[connections]]
            name = "local"
            url = "redis://127.0.0.1:6379"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.get("local").unwrap().prefix, None);
        assert_eq!(Target::new("redis://x").prefix, None);
    }

    #[test]
    fn debug_output_never_carries_a_password() {
        let cfg: Config = toml::from_str(
            r#"
            [[connections]]
            name = "prod"
            url = "rediss://admin:hunter2@prod.example.com:6379"
            "#,
        )
        .unwrap();
        let c = cfg.get("prod").unwrap();
        let target = Target::new(c.url.clone());
        for rendered in [format!("{c:?}"), format!("{target:?}"), format!("{cfg:?}")] {
            assert!(!rendered.contains("hunter2"), "{rendered}");
            assert!(rendered.contains("prod.example.com"), "{rendered}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_readable_config_with_passwords_is_flagged_and_a_private_one_is_not() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("keylens-perm-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        let secret = "[[connections]]\nname = \"p\"\nurl = \"redis://u:pw@h:6379\"\n";
        std::fs::write(&path, secret).unwrap();
        let cfg = Config::load(&path).unwrap();

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let warning = permission_warning(&path, &cfg).expect("0644 with a password");
        assert!(warning.contains("chmod 600"), "{warning}");
        assert!(
            !warning.contains(":pw@"),
            "the warning must not echo the secret"
        );

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(permission_warning(&path, &cfg), None);

        // No credentials, no warning, whatever the mode.
        std::fs::write(
            &path,
            "[[connections]]\nname = \"l\"\nurl = \"redis://h:6379\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let cfg = Config::load(&path).unwrap();
        assert_eq!(permission_warning(&path, &cfg), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_config_is_valid() {
        let cfg: Config = toml::from_str("").unwrap();
        assert!(cfg.connections.is_empty());
    }

    #[test]
    fn a_misspelled_key_is_an_error_rather_than_a_silent_default() {
        // `prefx` used to parse cleanly and do nothing. Detection then scanned the
        // default `bull:*:meta`, found nothing, and reported no queues -- which is also
        // what a server with no queues looks like, so there was nothing to notice.
        let err = toml::from_str::<Config>(
            r#"
            [[connections]]
            name = "queues"
            url = "redis://jobs.internal:6379"
            prefx = "myapp"
            "#,
        )
        .expect_err("an unknown key must not be accepted");
        assert!(err.to_string().contains("prefx"), "{err}");
    }
}
