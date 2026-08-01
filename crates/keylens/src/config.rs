//! Named connections in `~/.config/keylens/config.toml`.
//!
//! Typing `--url` every time is friction, and friction is the thing that decides whether
//! a tool gets used daily or once. A named connection is also where `readonly = true`
//! lives, so a production entry can be marked un-mutable before v0.2 ships mutations.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub connections: Vec<Connection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// A connection resolved from flags, environment and config, with everything downstream
/// needs to open it.
#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    pub url: String,
    /// Lens prefix override, if the config named one.
    pub prefix: Option<String>,
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
    pub fn load_default() -> Self {
        Self::default_path()
            .and_then(|p| Self::load(&p).ok())
            .unwrap_or_default()
    }

    pub fn get(&self, name: &str) -> Option<&Connection> {
        self.connections.iter().find(|c| c.name == name)
    }
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
    fn empty_config_is_valid() {
        let cfg: Config = toml::from_str("").unwrap();
        assert!(cfg.connections.is_empty());
    }
}
