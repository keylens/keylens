//! The single chokepoint for all Redis access.
//!
//! Every pane and every lens goes through [`Conn`]. That keeps the client swappable
//! (`fred` today, `redis-rs` is a closer call than it used to be now that it's 1.x) and
//! gives one place to enforce the invariants that matter:
//!
//! * `KEYS` is never issued. Ever. Only cursor-paged `SCAN` with a bounded `COUNT`.
//! * Every call is capability-aware, so managed hosts degrade instead of erroring.

use fred::prelude::*;
use fred::types::{ClusterHash, CustomCommand};
use tracing::debug;

use crate::capability::{Availability, Capabilities, Feature, classify};
use crate::error::{ConnError, Result};
use crate::server_info::ServerInfo;

/// A page of `SCAN` results plus the cursor to resume from.
#[derive(Debug, Clone)]
pub struct ScanPage {
    pub cursor: String,
    pub keys: Vec<String>,
}

impl ScanPage {
    /// Redis signals "iteration complete" with a zero cursor, not an empty page. A page
    /// can legitimately be empty while iteration continues.
    pub fn is_complete(&self) -> bool {
        self.cursor == "0"
    }
}

pub struct Conn {
    client: Client,
    server: ServerInfo,
    caps: Capabilities,
    label: String,
}

impl Conn {
    /// Connect, read `INFO`, and probe capabilities. Everything downstream can then
    /// assume vendor and capabilities are known.
    pub async fn connect(url: &str, label: impl Into<String>) -> Result<Self> {
        let config = Config::from_url(url).map_err(|e| ConnError::Url(e.to_string()))?;
        let client = Builder::from_config(config)
            .with_connection_config(|c| {
                c.connection_timeout = std::time::Duration::from_secs(5);
                c.internal_command_timeout = std::time::Duration::from_secs(5);
            })
            .build()
            .map_err(ConnError::Connect)?;

        client.init().await.map_err(ConnError::Connect)?;

        // A missing `INFO` must not stop us connecting. Some Redis-compatible servers
        // don't implement it at all, and locked-down managed hosts block it -- in both
        // cases the key browser still works perfectly, so failing the whole connection
        // over a stats pane would be absurd.
        let (server, mut caps) = match raw_info(&client).await {
            Ok(raw) => (ServerInfo::parse(&raw), probe(&client).await),
            Err(e) => {
                debug!(error = %e, "INFO unavailable; continuing without server metadata");
                let mut caps = probe(&client).await;
                caps.set(Feature::ServerInfo, classify_from(&e));
                (ServerInfo::unknown(), caps)
            }
        };
        if caps.get(Feature::ServerInfo) == Availability::Unsupported && !server.fields.is_empty() {
            caps.set(Feature::ServerInfo, Availability::Available);
        }

        Ok(Self {
            client,
            server,
            caps,
            label: label.into(),
        })
    }

    /// Whether this server answered `INFO` at all.
    pub fn has_server_info(&self) -> bool {
        self.caps.has(Feature::ServerInfo)
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn server(&self) -> &ServerInfo {
        &self.server
    }

    pub fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Re-read `INFO`. Cheap enough for the stats pane's refresh tick.
    pub async fn refresh_info(&self) -> Result<ServerInfo> {
        Ok(ServerInfo::parse(&raw_info(&self.client).await?))
    }

    /// One page of a `SCAN`. Callers drive the cursor so the UI can expand a subtree
    /// lazily instead of walking the whole keyspace up front.
    ///
    /// `count` is a hint, not a limit -- Redis may return more or fewer.
    pub async fn scan_page(
        &self,
        cursor: &str,
        pattern: Option<&str>,
        count: u32,
        type_filter: Option<&str>,
    ) -> Result<ScanPage> {
        let mut args: Vec<Value> = vec![cursor.into()];
        if let Some(p) = pattern {
            args.push("MATCH".into());
            args.push(p.into());
        }
        args.push("COUNT".into());
        args.push(count.into());
        // Servers older than Redis 6 lack the TYPE option; callers filter client-side then.
        if let Some(t) = type_filter
            && self.caps.has(Feature::ScanTypeFilter)
        {
            args.push("TYPE".into());
            args.push(t.into());
        }

        let reply: Value = self
            .client
            .custom(
                CustomCommand::new_static("SCAN", ClusterHash::FirstKey, false),
                args,
            )
            .await
            .map_err(|source| ConnError::Command {
                cmd: "SCAN",
                source,
            })?;

        parse_scan_reply(reply)
    }

    /// Run an arbitrary command. Used by lenses and the console; capability checks are
    /// the caller's job.
    pub async fn cmd(&self, name: &'static str, args: Vec<Value>) -> Result<Value> {
        self.client
            .custom(
                CustomCommand::new_static(name, ClusterHash::FirstKey, false),
                args,
            )
            .await
            .map_err(|source| ConnError::Command { cmd: name, source })
    }

    /// Run many commands in one round trip.
    ///
    /// This is not an optimisation, it's a usability floor: typing 500 listed keys with
    /// individual `TYPE` calls is 500 round trips, which is imperceptible on localhost and
    /// half a minute against a server 60ms away.
    ///
    /// Cluster caveat: a pipeline spanning multiple hash slots can be rejected. Callers
    /// that may span slots should be prepared to fall back to sequential calls.
    pub async fn pipeline(&self, cmds: &[(&'static str, Vec<Value>)]) -> Result<Vec<Value>> {
        let pipe = self.client.pipeline();
        for (name, args) in cmds {
            // In a pipeline this `await` only buffers -- it does not hit the server.
            let _: () = pipe
                .custom(
                    CustomCommand::new(name.to_string(), ClusterHash::FirstKey, false),
                    args.clone(),
                )
                .await
                .map_err(|source| ConnError::Command {
                    cmd: "PIPELINE",
                    source,
                })?;
        }
        pipe.all::<Vec<Value>>()
            .await
            .map_err(|source| ConnError::Command {
                cmd: "PIPELINE",
                source,
            })
    }
}

fn classify_from(e: &ConnError) -> Availability {
    match e {
        ConnError::Command { source, .. } => classify(source),
        _ => Availability::Unsupported,
    }
}

async fn raw_info(client: &Client) -> Result<String> {
    client
        .custom::<String, Value>(
            CustomCommand::new_static("INFO", ClusterHash::FirstKey, false),
            vec![],
        )
        .await
        .map_err(|source| ConnError::Command {
            cmd: "INFO",
            source,
        })
}

fn parse_scan_reply(reply: Value) -> Result<ScanPage> {
    let parts = match reply {
        Value::Array(a) if a.len() == 2 => a,
        other => {
            return Err(ConnError::Reply {
                cmd: "SCAN",
                detail: format!("expected 2-element array, got {other:?}"),
            });
        }
    };

    let cursor = parts[0].as_string().ok_or_else(|| ConnError::Reply {
        cmd: "SCAN",
        detail: "cursor not a string".into(),
    })?;

    let keys = match &parts[1] {
        Value::Array(items) => items.iter().filter_map(|v| v.as_string()).collect(),
        other => {
            return Err(ConnError::Reply {
                cmd: "SCAN",
                detail: format!("keys not an array, got {other:?}"),
            });
        }
    };

    Ok(ScanPage { cursor, keys })
}

/// Probe each restricted command once, with harmless arguments.
async fn probe(client: &Client) -> Capabilities {
    let mut caps = Capabilities::default();

    // (feature, command, args) -- every probe is a no-op read.
    let probes: &[(Feature, &'static str, &[&str])] = &[
        (Feature::Config, "CONFIG", &["GET", "maxmemory"]),
        (Feature::ClientList, "CLIENT", &["INFO"]),
        (Feature::Slowlog, "SLOWLOG", &["LEN"]),
        (
            Feature::MemoryStats,
            "MEMORY",
            &["USAGE", "keylens:__probe__"],
        ),
        (Feature::Cluster, "CLUSTER", &["INFO"]),
        (Feature::Modules, "MODULE", &["LIST"]),
        (Feature::Streams, "XLEN", &["keylens:__probe__"]),
        (
            Feature::PubSub,
            "PUBSUB",
            &["CHANNELS", "keylens:__probe__"],
        ),
    ];

    for (feature, cmd, args) in probes {
        let argv: Vec<Value> = args.iter().map(|a| Value::from(*a)).collect();
        let result: std::result::Result<Value, Error> = client
            .custom(
                CustomCommand::new(cmd.to_string(), ClusterHash::FirstKey, false),
                argv,
            )
            .await;

        let availability = match result {
            Ok(_) => Availability::Available,
            Err(e) => {
                debug!(feature = feature.label(), error = %e, "probe failed");
                classify(&e)
            }
        };
        caps.set(*feature, availability);
    }

    // `SCAN ... TYPE` is Redis 6+; probing it needs its own shape.
    let scan_type_args: Vec<Value> = vec![
        "0".into(),
        "COUNT".into(),
        1.into(),
        "TYPE".into(),
        "string".into(),
    ];
    let scan_type: std::result::Result<Value, Error> = client
        .custom(
            CustomCommand::new_static("SCAN", ClusterHash::FirstKey, false),
            scan_type_args,
        )
        .await;
    caps.set(
        Feature::ScanTypeFilter,
        match scan_type {
            Ok(_) => Availability::Available,
            Err(e) => {
                // A syntax error here means an older server that lacks the TYPE option,
                // not a permissions problem.
                if e.details().to_ascii_lowercase().contains("syntax") {
                    Availability::Unsupported
                } else {
                    classify(&e)
                }
            }
        },
    );

    if caps.has(Feature::Modules) {
        caps.modules = module_names(client).await;
    }

    caps
}

async fn module_names(client: &Client) -> Vec<String> {
    let reply: std::result::Result<Value, Error> = client
        .custom(
            CustomCommand::new_static("MODULE", ClusterHash::FirstKey, false),
            vec![Value::from("LIST")],
        )
        .await;

    let Ok(Value::Array(mods)) = reply else {
        return Vec::new();
    };

    // Each entry is a flat map: name <n> ver <v> ...
    mods.iter()
        .filter_map(|m| match m {
            Value::Array(kv) => kv
                .windows(2)
                .find(|w| w[0].as_string().as_deref() == Some("name"))
                .and_then(|w| w[1].as_string()),
            Value::Map(map) => map
                .iter()
                .find(|(k, _)| k.as_str() == Some("name"))
                .and_then(|(_, v)| v.as_string()),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_page_completion_is_cursor_driven_not_emptiness() {
        // An empty page mid-iteration is normal -- SCAN can return zero keys while the
        // cursor is still non-zero. Stopping on emptiness silently truncates the tree.
        let mid = ScanPage {
            cursor: "1234".into(),
            keys: vec![],
        };
        assert!(!mid.is_complete());

        let done = ScanPage {
            cursor: "0".into(),
            keys: vec!["a".into()],
        };
        assert!(done.is_complete());
    }

    #[test]
    fn parses_scan_reply() {
        let reply = Value::Array(vec![
            Value::from("42"),
            Value::Array(vec![Value::from("bull:emails:meta"), Value::from("k2")]),
        ]);
        let page = parse_scan_reply(reply).unwrap();
        assert_eq!(page.cursor, "42");
        assert_eq!(page.keys, vec!["bull:emails:meta", "k2"]);
        assert!(!page.is_complete());
    }

    #[test]
    fn rejects_malformed_scan_reply() {
        assert!(parse_scan_reply(Value::from("nope")).is_err());
    }
}
