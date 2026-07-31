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
    /// Hard-disable mutations for this connection regardless of build or flags.
    #[serde(default)]
    pub readonly: bool,
    /// Key prefix hint for lenses, e.g. BullMQ's `bull`.
    #[serde(default)]
    pub prefix: Option<String>,
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
    fn empty_config_is_valid() {
        let cfg: Config = toml::from_str("").unwrap();
        assert!(cfg.connections.is_empty());
    }
}
