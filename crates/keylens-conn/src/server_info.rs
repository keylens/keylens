//! Parsing of `INFO` into a vendor-neutral server description.
//!
//! Since the 2024 Redis/Valkey license fork, vendor is a *detected* property, never an
//! assumption. Valkey sets `server_name:valkey` in `INFO server`; Dragonfly, KeyDB and
//! Garnet each announce themselves differently. Feature gating downstream keys off
//! [`Capabilities`](crate::Capabilities), not off a version string.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Vendor {
    Redis,
    Valkey,
    Dragonfly,
    KeyDb,
    Garnet,
    Recached,
    Unknown(String),
}

impl Vendor {
    pub fn label(&self) -> &str {
        match self {
            Vendor::Redis => "Redis",
            Vendor::Valkey => "Valkey",
            Vendor::Dragonfly => "Dragonfly",
            Vendor::KeyDb => "KeyDB",
            Vendor::Garnet => "Garnet",
            Vendor::Recached => "Recached",
            Vendor::Unknown(s) => s,
        }
    }
}

/// A parsed `INFO` payload plus the vendor/version conclusions drawn from it.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub vendor: Vendor,
    pub version: String,
    /// `standalone` | `sentinel` | `cluster`
    pub mode: String,
    pub fields: BTreeMap<String, String>,
}

impl ServerInfo {
    /// What we know about a server that did not answer `INFO`: nothing.
    ///
    /// Deliberately not `Vendor::Redis` -- guessing here is how a tool ends up claiming a
    /// Dragonfly instance is Redis 7.
    pub fn unknown() -> Self {
        Self {
            vendor: Vendor::Unknown("unknown".into()),
            version: "unknown".into(),
            mode: "standalone".into(),
            fields: BTreeMap::new(),
        }
    }

    pub fn parse(raw: &str) -> Self {
        let mut fields = BTreeMap::new();
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once(':') {
                fields.insert(k.to_string(), v.to_string());
            }
        }

        let vendor = detect_vendor(&fields);
        let version = fields
            .get("valkey_version")
            .or_else(|| fields.get("redis_version"))
            .cloned()
            .unwrap_or_else(|| "unknown".into());
        let mode = fields
            .get("redis_mode")
            .or_else(|| fields.get("server_mode"))
            .cloned()
            .unwrap_or_else(|| "standalone".into());

        Self {
            vendor,
            version,
            mode,
            fields,
        }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(|s| s.as_str())
    }

    pub fn get_u64(&self, key: &str) -> Option<u64> {
        self.get(key).and_then(|v| v.parse().ok())
    }

    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.get(key).and_then(|v| v.parse().ok())
    }

    /// Keyspace hit rate in `0.0..=1.0`, or `None` when the server has served no reads.
    pub fn hit_rate(&self) -> Option<f64> {
        let hits = self.get_u64("keyspace_hits")?;
        let misses = self.get_u64("keyspace_misses")?;
        let total = hits + misses;
        (total > 0).then(|| hits as f64 / total as f64)
    }
}

fn detect_vendor(fields: &BTreeMap<String, String>) -> Vendor {
    // `server_name` is the explicit, forward-compatible signal. Everything else is a
    // fallback for servers that predate it or emulate Redis without setting it.
    if let Some(name) = fields.get("server_name") {
        return match name.to_ascii_lowercase().as_str() {
            "valkey" => Vendor::Valkey,
            "redis" => Vendor::Redis,
            "dragonfly" => Vendor::Dragonfly,
            "keydb" => Vendor::KeyDb,
            "garnet" => Vendor::Garnet,
            "recached" => Vendor::Recached,
            other => Vendor::Unknown(other.to_string()),
        };
    }
    if fields.contains_key("valkey_version") {
        return Vendor::Valkey;
    }
    if fields.contains_key("dragonfly_version") {
        return Vendor::Dragonfly;
    }
    if fields.contains_key("keydb_version") || fields.contains_key("mvcc_depth") {
        return Vendor::KeyDb;
    }
    if fields.contains_key("garnet_version") {
        return Vendor::Garnet;
    }
    if fields.contains_key("recached_version") {
        return Vendor::Recached;
    }
    if fields.contains_key("redis_version") {
        return Vendor::Redis;
    }
    Vendor::Unknown("unknown".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_redis_info() {
        let raw = "# Server\r\nredis_version:8.0.1\r\nredis_mode:standalone\r\n\r\n# Stats\r\nkeyspace_hits:90\r\nkeyspace_misses:10\r\n";
        let info = ServerInfo::parse(raw);
        assert_eq!(info.vendor, Vendor::Redis);
        assert_eq!(info.version, "8.0.1");
        assert_eq!(info.mode, "standalone");
        assert_eq!(info.hit_rate(), Some(0.9));
    }

    #[test]
    fn server_name_wins_over_version_keys() {
        // Valkey still emits `redis_version` for client compatibility; `server_name` is
        // what disambiguates. Getting this backwards mislabels every Valkey server.
        let raw = "server_name:valkey\r\nvalkey_version:8.1.0\r\nredis_version:7.2.4\r\n";
        let info = ServerInfo::parse(raw);
        assert_eq!(info.vendor, Vendor::Valkey);
        assert_eq!(info.version, "8.1.0");
    }

    #[test]
    fn recognises_recached() {
        // Recached does not implement INFO today; this is ready for when it does, and
        // covers both the explicit `server_name` and a version-key fallback.
        assert_eq!(
            ServerInfo::parse("server_name:recached\r\nredis_version:7.2.0\r\n").vendor,
            Vendor::Recached
        );
        assert_eq!(
            ServerInfo::parse("recached_version:0.2.2\r\n").vendor,
            Vendor::Recached
        );
    }

    #[test]
    fn a_server_that_never_answered_info_is_not_guessed_as_redis() {
        let info = ServerInfo::unknown();
        assert!(matches!(info.vendor, Vendor::Unknown(_)));
        assert!(
            info.fields.is_empty(),
            "empty fields is how panes detect this"
        );
    }

    #[test]
    fn hit_rate_is_none_on_idle_server() {
        let info = ServerInfo::parse("keyspace_hits:0\r\nkeyspace_misses:0\r\n");
        assert_eq!(info.hit_rate(), None);
    }
}
