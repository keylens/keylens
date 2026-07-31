//! Runtime capability probing.
//!
//! Managed hosts (Upstash, ElastiCache, MemoryDB) block subsets of `CONFIG`, `CLIENT`,
//! `MEMORY`, `MONITOR` and `DEBUG`. A pane that assumes those commands exist will either
//! panic or spray error toasts on exactly the servers most people point a tool at.
//!
//! So: probe once at connect time, then render an explicit "unavailable on this server"
//! state. Doing this also buys Dragonfly/KeyDB/Garnet support essentially for free.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Availability {
    /// The command ran (or failed for a reason unrelated to permissions).
    Available,
    /// The server knows the command but refuses it for this connection.
    Denied(String),
    /// The server does not implement the command at all.
    Unsupported,
}

impl Availability {
    pub fn is_available(&self) -> bool {
        matches!(self, Availability::Available)
    }

    /// Short reason to show in a disabled pane.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Availability::Available => None,
            Availability::Denied(why) => Some(why),
            Availability::Unsupported => Some("not implemented by this server"),
        }
    }
}

/// Names are the *feature* keylens needs, not the raw command, because one pane can be
/// backed by different commands across vendors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Feature {
    Config,
    ClientList,
    Slowlog,
    MemoryStats,
    Cluster,
    Modules,
    /// `SCAN ... TYPE <t>` — Redis 6+. Without it, type filtering happens client-side.
    ScanTypeFilter,
    Streams,
    PubSub,
}

impl Feature {
    pub const ALL: [Feature; 9] = [
        Feature::Config,
        Feature::ClientList,
        Feature::Slowlog,
        Feature::MemoryStats,
        Feature::Cluster,
        Feature::Modules,
        Feature::ScanTypeFilter,
        Feature::Streams,
        Feature::PubSub,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            Feature::Config => "CONFIG",
            Feature::ClientList => "CLIENT LIST",
            Feature::Slowlog => "SLOWLOG",
            Feature::MemoryStats => "MEMORY",
            Feature::Cluster => "CLUSTER",
            Feature::Modules => "MODULE LIST",
            Feature::ScanTypeFilter => "SCAN TYPE",
            Feature::Streams => "STREAMS",
            Feature::PubSub => "PUBSUB",
        }
    }

    /// Which pane degrades when this is missing — used for the probe report.
    pub fn affects(&self) -> &'static str {
        match self {
            Feature::Config => "config pane",
            Feature::ClientList => "clients pane",
            Feature::Slowlog => "slowlog pane",
            Feature::MemoryStats => "memory breakdown",
            Feature::Cluster => "cluster topology",
            Feature::Modules => "module-backed viewers",
            Feature::ScanTypeFilter => "type filter (falls back to client-side)",
            Feature::Streams => "stream + consumer group viewer",
            Feature::PubSub => "pub/sub pane",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Capabilities {
    map: BTreeMap<Feature, Availability>,
    pub modules: Vec<String>,
}

impl Capabilities {
    pub fn set(&mut self, f: Feature, a: Availability) {
        self.map.insert(f, a);
    }

    pub fn get(&self, f: Feature) -> Availability {
        self.map.get(&f).cloned().unwrap_or(Availability::Unsupported)
    }

    pub fn has(&self, f: Feature) -> bool {
        self.get(f).is_available()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Feature, &Availability)> {
        self.map.iter()
    }

    pub fn has_module(&self, name: &str) -> bool {
        self.modules.iter().any(|m| m.eq_ignore_ascii_case(name))
    }
}

/// Classify a command failure into an [`Availability`].
///
/// The important subtlety: an error is *not* the same as unavailability. `XLEN` against a
/// missing key, or `MEMORY USAGE` on a nonexistent key, can error while proving the
/// command exists and is permitted. Only permission and unknown-command errors count.
pub fn classify(err: &fred::error::Error) -> Availability {
    let msg = err.details().to_string();
    let lower = msg.to_ascii_lowercase();

    if lower.contains("unknown command")
        || lower.contains("unknown subcommand")
        || lower.contains("not supported")
    {
        return Availability::Unsupported;
    }

    // `disabled` is matched loosely on purpose. Redis 8 answers `CLUSTER INFO` on a
    // standalone instance with "ERR This instance has cluster support disabled" -- which
    // matched none of the narrower phrasings and was silently classified as *available*,
    // so the probe reported CLUSTER as working on every standalone server.
    if lower.contains("noperm")
        || lower.contains("no permissions")
        || lower.contains("not allowed")
        || lower.contains("disabled")
        || lower.contains("unauthorized")
        || lower.contains("restricted")
        || lower.contains("forbidden")
    {
        // Trim to something that fits in a status line.
        let short = msg.lines().next().unwrap_or(&msg).trim().to_string();
        return Availability::Denied(short);
    }

    // Command exists and ran; it just didn't like our arguments or the keyspace state.
    Availability::Available
}

#[cfg(test)]
mod tests {
    use super::*;
    use fred::error::{Error, ErrorKind};

    fn err(msg: &str) -> Error {
        Error::new(ErrorKind::Unknown, msg.to_string())
    }

    #[test]
    fn unknown_command_is_unsupported() {
        assert_eq!(
            classify(&err("ERR unknown command 'SLOWLOG'")),
            Availability::Unsupported
        );
    }

    #[test]
    fn noperm_is_denied() {
        assert!(matches!(
            classify(&err("NOPERM this user has no permissions to run the 'config' command")),
            Availability::Denied(_)
        ));
    }

    #[test]
    fn standalone_cluster_rejection_is_not_available() {
        // Redis 8's exact wording. Classifying this as available made the probe claim
        // CLUSTER worked on every standalone server.
        assert!(matches!(
            classify(&err("ERR This instance has cluster support disabled")),
            Availability::Denied(_)
        ));
    }

    #[test]
    fn benign_errors_still_count_as_available() {
        // `XLEN` on a missing key errors, but proves the command is permitted. Treating
        // this as unavailable would black out the stream viewer on healthy servers.
        assert_eq!(
            classify(&err("WRONGTYPE Operation against a key holding the wrong kind of value")),
            Availability::Available
        );
        assert_eq!(classify(&err("ERR no such key")), Availability::Available);
    }
}
