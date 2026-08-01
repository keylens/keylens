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
use crate::error::{ConnError, Result, classify_connect, connect_timeout};
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

/// Key used by the read-only capability probes. It is never written.
const PROBE_KEY: &str = "keylens:__probe__";

/// Backstop for a connect with nothing on screen -- the non-interactive commands.
///
/// A deadline is a poor substitute for showing progress: too short breaks a slow link,
/// too long looks frozen. The browser passes its own, much longer, because it draws a
/// status and lets the user cancel; this value only has to stop `probe` hanging silently.
pub const DEFAULT_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

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
        Self::connect_with_timeout(url, label, DEFAULT_CONNECT_TIMEOUT).await
    }

    /// Connect with an explicit deadline.
    ///
    /// The browser uses a long one because it shows what it is doing and can be
    /// cancelled; a deadline there is a backstop against a black-holed connection, not a
    /// guess at how far away the server is.
    pub async fn connect_with_timeout(
        url: &str,
        label: impl Into<String>,
        timeout: std::time::Duration,
    ) -> Result<Self> {
        let config = Config::from_url(url).map_err(|e| ConnError::Url(e.to_string()))?;
        let client = Builder::from_config(config)
            .with_connection_config(|c| {
                // Generous, because these are *per-step* limits and the client's own
                // handshake is already four round trips (PING, CLIENT ID, INFO, ROLE).
                // At 1.4s round trip — an ordinary managed database on another continent
                // — a 5s limit here kills a perfectly healthy connection before the
                // caller's deadline is ever consulted. The overall bound is the caller's
                // job; these only need to stop a wedged socket waiting forever.
                c.connection_timeout = std::time::Duration::from_secs(30);
                c.internal_command_timeout = std::time::Duration::from_secs(30);
            })
            .build()
            .map_err(ConnError::Connect)?;

        // The whole handshake gets one deadline, not just `init`.
        //
        // `init` can report success while the connection is already dead -- a TLS-only
        // port accepts the TCP connection, then closes it -- after which fred *queues*
        // commands and retries in the background rather than failing them. So `INFO`
        // never returns and the tool hangs with no error at all, which is worse than any
        // message. Bounding only `init` was not enough; this covers everything that must
        // succeed before the UI can open.
        let handshake = async {
            // A server answering with non-RESP bytes (a TLS alert, say) fails here with
            // `Protocol Error: Expected string`, which tells the user nothing on its own.
            client.init().await.map_err(|e| classify_connect(url, e))?;

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
            if caps.get(Feature::ServerInfo) == Availability::Unsupported
                && !server.fields.is_empty()
            {
                caps.set(Feature::ServerInfo, Availability::Available);
            }
            Ok::<_, ConnError>((server, caps))
        };

        let (server, caps) = match tokio::time::timeout(timeout, handshake).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                // Stop the background reconnect loop, or it keeps the process alive after
                // we return and the error is never seen.
                let _ = client.quit().await;
                return Err(e);
            }
            Err(_) => {
                let _ = client.quit().await;
                return Err(connect_timeout(url, timeout));
            }
        };

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
    let probes = probe_commands();

    // Every probe is a harmless read, and they are all independent -- so they go in one
    // pipeline rather than one round trip each.
    //
    // This is not micro-optimisation. Against a managed host ~1.4s away, eleven serial
    // probes is fifteen seconds of staring at nothing before the UI opens, which is long
    // enough to look like a hang. One round trip makes it one.
    let replies = run_probe_pipeline(client, &probes).await;
    apply_probe_results(&mut caps, &probes, &replies);
    caps
}

/// Every capability probe. One harmless read each.
fn probe_commands() -> Vec<(Feature, &'static str, Vec<Value>)> {
    vec![
        (
            Feature::Config,
            "CONFIG",
            vec!["GET".into(), "maxmemory".into()],
        ),
        (Feature::ClientList, "CLIENT", vec!["INFO".into()]),
        (Feature::Slowlog, "SLOWLOG", vec!["LEN".into()]),
        (
            Feature::MemoryStats,
            "MEMORY",
            vec!["USAGE".into(), PROBE_KEY.into()],
        ),
        (Feature::Cluster, "CLUSTER", vec!["INFO".into()]),
        (Feature::Modules, "MODULE", vec!["LIST".into()]),
        (Feature::Streams, "XLEN", vec![PROBE_KEY.into()]),
        (
            Feature::PubSub,
            "PUBSUB",
            vec!["CHANNELS".into(), PROBE_KEY.into()],
        ),
        // Not every Redis-compatible server has the bounded read variants.
        (
            Feature::GetRange,
            "GETRANGE",
            vec![PROBE_KEY.into(), 0.into(), 0.into()],
        ),
        (
            Feature::CursorCollectionScan,
            "HSCAN",
            vec![PROBE_KEY.into(), "0".into(), "COUNT".into(), 1.into()],
        ),
        // `SCAN ... TYPE` is Redis 6+ and needs its own shape.
        (
            Feature::ScanTypeFilter,
            "SCAN",
            vec![
                "0".into(),
                "COUNT".into(),
                1.into(),
                "TYPE".into(),
                "string".into(),
            ],
        ),
    ]
}

fn apply_probe_results(
    caps: &mut Capabilities,
    probes: &[(Feature, &'static str, Vec<Value>)],
    replies: &[std::result::Result<Value, Error>],
) {
    for ((feature, _, _), reply) in probes.iter().zip(replies.iter()) {
        let availability = match reply {
            Ok(_) => Availability::Available,
            Err(e) => {
                debug!(feature = feature.label(), error = %e, "probe failed");
                // A syntax error on SCAN means an older server without the TYPE option,
                // not a permissions problem.
                if *feature == Feature::ScanTypeFilter
                    && e.details().to_ascii_lowercase().contains("syntax")
                {
                    Availability::Unsupported
                } else {
                    classify(e)
                }
            }
        };
        caps.set(*feature, availability);
    }

    // The module list came back in the same pipeline; no second round trip needed.
    if caps.has(Feature::Modules)
        && let Some(Ok(reply)) = probes
            .iter()
            .position(|(f, _, _)| *f == Feature::Modules)
            .and_then(|i| replies.get(i))
    {
        caps.modules = parse_module_names(reply);
    }
}

/// Run every probe in one round trip, falling back to serial calls if the pipeline
/// cannot be used -- a cluster can reject one that spans hash slots.
async fn run_probe_pipeline(
    client: &Client,
    probes: &[(Feature, &'static str, Vec<Value>)],
) -> Vec<std::result::Result<Value, Error>> {
    let pipe = client.pipeline();
    for (_, cmd, args) in probes {
        // In a pipeline this only buffers; it does not reach the server.
        let buffered: std::result::Result<(), Error> = pipe
            .custom(
                CustomCommand::new(cmd.to_string(), ClusterHash::FirstKey, false),
                args.clone(),
            )
            .await;
        if let Err(e) = buffered {
            debug!(error = %e, "could not buffer probe; falling back to serial");
            return probe_serially(client, probes).await;
        }
    }

    let replies = pipe.try_all::<Value>().await;
    if replies.len() == probes.len() {
        return replies;
    }

    debug!(
        expected = probes.len(),
        got = replies.len(),
        "probe pipeline returned the wrong number of replies; falling back to serial"
    );
    probe_serially(client, probes).await
}

async fn probe_serially(
    client: &Client,
    probes: &[(Feature, &'static str, Vec<Value>)],
) -> Vec<std::result::Result<Value, Error>> {
    let mut out = Vec::with_capacity(probes.len());
    for (_, cmd, args) in probes {
        out.push(
            client
                .custom(
                    CustomCommand::new(cmd.to_string(), ClusterHash::FirstKey, false),
                    args.clone(),
                )
                .await,
        );
    }
    out
}

/// Extract module names from a `MODULE LIST` reply.
///
/// The reply arrives with the rest of the capability probes, so this is pure parsing --
/// no extra round trip just to name the modules.
fn parse_module_names(reply: &Value) -> Vec<String> {
    let Value::Array(mods) = reply else {
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
    fn every_capability_is_actually_probed() {
        // `GetRange` and `CursorCollectionScan` were added to the enum but never given a
        // probe, so they silently defaulted to unsupported and keylens took the
        // whole-collection fallback path on servers that support HSCAN perfectly well.
        // A capability nobody asks about is worse than no capability at all.
        let probed: Vec<Feature> = probe_commands().into_iter().map(|(f, _, _)| f).collect();

        for feature in Feature::ALL {
            // ServerInfo is answered by the INFO call itself, not by a probe.
            if feature == Feature::ServerInfo {
                continue;
            }
            assert!(
                probed.contains(&feature),
                "{:?} is in Feature::ALL but has no probe, so it will always read as \
                 unsupported",
                feature
            );
        }
    }

    #[test]
    fn probes_are_read_only_and_use_the_reserved_key() {
        // A probe that wrote anything would make `keylens` unsafe to point at production,
        // which is the whole promise.
        const MUTATING: [&str; 8] = [
            "SET", "DEL", "HSET", "LPUSH", "SADD", "ZADD", "EXPIRE", "RENAME",
        ];
        for (feature, cmd, args) in probe_commands() {
            assert!(
                !MUTATING.contains(&cmd),
                "{feature:?} probes with the mutating command {cmd}"
            );
            for arg in &args {
                if let Some(s) = arg.as_string() {
                    assert!(
                        !s.starts_with("keylens:") || s == PROBE_KEY,
                        "{feature:?} touches {s}, not the reserved probe key"
                    );
                }
            }
        }
    }

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
