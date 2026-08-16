//! The single chokepoint for all Redis access.
//!
//! Every pane and every lens goes through [`Conn`]. That keeps the client swappable
//! (`fred` today, `redis-rs` is a closer call than it used to be now that it's 1.x) and
//! gives one place to enforce the invariants that matter:
//!
//! * `KEYS` is never issued. Ever. Only cursor-paged `SCAN` with a bounded `COUNT`.
//! * Every call is capability-aware, so managed hosts degrade instead of erroring.

use fred::prelude::*;
use fred::socket2::TcpKeepalive;
use fred::types::config::UnresponsiveConfig;
use fred::types::scan::{ScanResult, ScanType, Scanner};
use fred::types::{ClusterHash, CustomCommand};
use futures::{Stream, StreamExt, future::join_all};
use std::pin::Pin;
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

/// A throttled keyspace scan that also walks every primary in Redis Cluster.
///
/// A Redis cursor belongs to one server, so a cluster scan cannot be represented by the
/// single cursor string used by [`ScanPage`]. This wrapper owns fred's per-primary scanner
/// state and exposes one page at a time without buffering the whole keyspace.
pub struct KeyScanner {
    pages: Pin<Box<dyn Stream<Item = std::result::Result<ScanResult, fred::error::Error>> + Send>>,
}

impl KeyScanner {
    /// Return the next page, or `None` once every relevant server has returned cursor zero.
    pub async fn next_page(&mut self) -> Result<Option<Vec<String>>> {
        match self.pages.next().await {
            Some(Ok(mut page)) => {
                let keys = page
                    .take_results()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|key| key.into_string())
                    .collect();
                // Explicitly continue this node. Dropping also continues it, but spelling
                // this out makes the throttling contract independent of Drop behaviour.
                page.next();
                Ok(Some(keys))
            }
            Some(Err(source)) => Err(ConnError::Command {
                cmd: "SCAN",
                source,
            }),
            None => Ok(None),
        }
    }
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

/// Ceiling on any single command once the connection is up.
///
/// fred defaults this to zero, which means *no* timeout: a reply that never arrives is
/// awaited forever. That is not hypothetical on a lossy link — a DigitalOcean managed
/// endpoint measured at 390ms best case answered 9.7s at worst, and a connection silently
/// dropped by the load balancer in front of it never answers at all. One unbounded await
/// is enough to wedge a whole task.
///
/// Generous, so it does not fire on a link that is slow but alive. Finite, so a dead one
/// surfaces as an error the user can see instead of a pane that says `loading…` forever.
pub const COMMAND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// How long a connection may go without answering anything before it is torn down.
///
/// Above [`COMMAND_TIMEOUT`] on purpose: an individual command should time out and report
/// itself first. This is the backstop for a socket that is half-open — one that accepted
/// our bytes and will never reply — which fred otherwise never notices.
const UNRESPONSIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// TCP keepalive idle time, so a connection dropped by a NAT or a load balancer is
/// discovered by the kernel rather than by the user's next keypress.
const KEEPALIVE_IDLE: std::time::Duration = std::time::Duration::from_secs(30);

pub struct Conn {
    client: Client,
    server: ServerInfo,
    caps: Capabilities,
    label: String,
    rtt: std::time::Duration,
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
            .with_performance_config(|c| {
                // See COMMAND_TIMEOUT: fred's default here is "wait forever".
                c.default_command_timeout = COMMAND_TIMEOUT;
            })
            .with_connection_config(|c| {
                // Generous, because these are *per-step* limits and the client's own
                // handshake is already four round trips (PING, CLIENT ID, INFO, ROLE).
                // At 1.4s round trip — an ordinary managed database on another continent
                // — a 5s limit here kills a perfectly healthy connection before the
                // caller's deadline is ever consulted. The overall bound is the caller's
                // job; these only need to stop a wedged socket waiting forever.
                c.connection_timeout = std::time::Duration::from_secs(30);
                c.internal_command_timeout = std::time::Duration::from_secs(30);

                // Nagle holds a small write back hoping for more to coalesce; the peer's
                // delayed ACK holds the reply that would release it. A request/response
                // protocol never has anything to coalesce, so the pair buys nothing and
                // costs tens of milliseconds on every command. Every serious Redis client
                // disables it; fred leaves it to the OS unless asked.
                c.tcp.nodelay = Some(true);
                c.tcp.keepalive = Some(TcpKeepalive::new().with_time(KEEPALIVE_IDLE));

                // Without this fred has no way to notice a half-open socket, so a command
                // sent into one is awaited until COMMAND_TIMEOUT and every reconnect that
                // would have fixed it never happens.
                c.unresponsive = UnresponsiveConfig {
                    max_timeout: Some(UNRESPONSIVE_TIMEOUT),
                    ..Default::default()
                };
            })
            // `Builder::from_config` leaves the reconnect policy unset, so a dropped
            // connection stayed dropped: every later command failed against a client that
            // would never try to come back. Unlimited attempts, backing off to 10s, is the
            // right shape for a TUI someone leaves open — but only because
            // COMMAND_TIMEOUT now bounds the wait, so retrying cannot mean hanging.
            .set_policy(ReconnectPolicy::new_exponential(0, 100, 10_000, 2))
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

            // One timed `PING`, so everything downstream can size itself against how far
            // away this server actually is rather than against a constant tuned on
            // localhost. Costs one round trip here and saves many later: the stats refresh
            // and the selection debounce are both derived from it.
            let rtt = measure_rtt(&client).await;

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
            Ok::<_, ConnError>((server, caps, rtt))
        };

        let (server, caps, rtt) = match tokio::time::timeout(timeout, handshake).await {
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
            rtt,
        })
    }

    /// Whether this server answered `INFO` at all.
    pub fn has_server_info(&self) -> bool {
        self.caps.has(Feature::ServerInfo)
    }

    /// Measured round trip to this server, from a single `PING` at connect time.
    ///
    /// Zero when the server did not answer it, which callers must read as "no measurement"
    /// and fall back to their own floor — never as "this server is infinitely close".
    pub fn rtt(&self) -> std::time::Duration {
        self.rtt
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

    /// Whether this connection addresses a sharded Redis Cluster deployment.
    pub fn is_clustered(&self) -> bool {
        self.client.is_clustered()
    }

    /// Start a throttled scan. Cluster connections scan every primary; standalone and
    /// sentinel connections scan their selected server.
    pub fn key_scanner(
        &self,
        pattern: Option<&str>,
        count: u32,
        type_filter: Option<&str>,
    ) -> KeyScanner {
        let pattern = pattern.unwrap_or("*").to_string();
        let scan_type = type_filter.and_then(parse_scan_type);
        let pages: Pin<
            Box<dyn Stream<Item = std::result::Result<ScanResult, fred::error::Error>> + Send>,
        > = if self.client.is_clustered() {
            Box::pin(self.client.scan_cluster(pattern, Some(count), scan_type))
        } else {
            Box::pin(self.client.scan(pattern, Some(count), scan_type))
        };
        KeyScanner { pages }
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
        if self.client.is_clustered() {
            return Err(ConnError::Reply {
                cmd: "SCAN",
                detail: "a single cursor cannot scan Redis Cluster; use Conn::key_scanner".into(),
            });
        }
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

    /// Run a command through the read-only allowlist used by built-in and third-party
    /// lenses. Capability checks remain the caller's job.
    pub async fn cmd(&self, name: &'static str, args: Vec<Value>) -> Result<Value> {
        if !read_only_command(name, &args) {
            return Err(ConnError::UnsafeCommand(name));
        }
        self.execute(name, args).await
    }

    async fn execute(&self, name: &'static str, args: Vec<Value>) -> Result<Value> {
        self.client
            .custom(
                CustomCommand::new_static(name, ClusterHash::FirstKey, false),
                args,
            )
            .await
            .map_err(|source| ConnError::Command { cmd: name, source })
    }

    /// Raw command access for live-test fixture setup only.
    #[cfg(feature = "dangerous-test-commands")]
    #[doc(hidden)]
    pub async fn test_cmd(&self, name: &'static str, args: Vec<Value>) -> Result<Value> {
        self.execute(name, args).await
    }

    /// Blocking stream read with the client library's correct routing and connection flags.
    pub async fn xread(
        &self,
        keys: &[String],
        ids: &[String],
        count: u64,
        block_ms: u64,
    ) -> Result<Value> {
        self.client
            .xread(Some(count), Some(block_ms), keys.to_vec(), ids.to_vec())
            .await
            .map_err(|source| ConnError::Command {
                cmd: "XREAD",
                source,
            })
    }

    /// Run many commands in one round trip, reporting each command's outcome separately.
    ///
    /// This is not an optimisation, it's a usability floor: typing 500 listed keys with
    /// individual `TYPE` calls is 500 round trips, which is imperceptible on localhost and
    /// half a minute against a server 60ms away.
    ///
    /// **Per-command results, not a single all-or-nothing reply.** One command failing is
    /// not the pipeline failing: a `WRONGTYPE` on a single queue's `wait` key used to blank
    /// the entire queue table, and one bad key used to drop the types of all 500 keys in a
    /// batch. Callers read the slot they care about and degrade only that slot.
    ///
    /// The outer `Err` is reserved for a failure that invalidates the mapping itself --
    /// the pipeline could not be buffered, or came back with a reply count that no longer
    /// lines up with `cmds`, at which point slot `i` is not necessarily command `i`.
    ///
    /// On Redis Cluster, commands are routed independently in ordered, bounded-concurrency
    /// chunks so an arbitrary-key batch cannot fail merely because it spans hash slots.
    pub async fn pipeline(
        &self,
        cmds: &[(&'static str, Vec<Value>)],
    ) -> Result<Vec<Result<Value>>> {
        if let Some((name, _)) = cmds
            .iter()
            .find(|(name, args)| !read_only_command(name, args))
        {
            return Err(ConnError::UnsafeCommand(name));
        }
        self.pipeline_unchecked(cmds).await
    }

    #[cfg(feature = "dangerous-test-commands")]
    #[doc(hidden)]
    pub async fn test_pipeline(
        &self,
        cmds: &[(&'static str, Vec<Value>)],
    ) -> Result<Vec<Result<Value>>> {
        self.pipeline_unchecked(cmds).await
    }

    async fn pipeline_unchecked(
        &self,
        cmds: &[(&'static str, Vec<Value>)],
    ) -> Result<Vec<Result<Value>>> {
        if self.client.is_clustered() {
            // A regular Redis pipeline is tied to one connection, while arbitrary keys in
            // a cluster can live on different primaries. Preserve correctness by letting
            // fred route each command independently, with bounded concurrency. The result
            // order remains the input order because `buffered` is ordered.
            let owned: Vec<(&'static str, Vec<Value>)> = cmds
                .iter()
                .map(|(name, args)| (*name, args.clone()))
                .collect();
            let mut replies = Vec::with_capacity(owned.len());
            for chunk in owned.chunks(64) {
                let pending: Vec<_> = chunk
                    .iter()
                    .map(|(name, args)| self.execute(name, args.clone()))
                    .collect();
                replies.extend(join_all(pending).await);
            }
            return Ok(replies);
        }

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

        let replies = pipe.try_all::<Value>().await;
        if replies.len() != cmds.len() {
            return Err(ConnError::Reply {
                cmd: "PIPELINE",
                detail: format!("expected {} replies, got {}", cmds.len(), replies.len()),
            });
        }

        Ok(replies
            .into_iter()
            .zip(cmds.iter())
            .map(|(reply, (cmd, _))| reply.map_err(|source| ConnError::Command { cmd, source }))
            .collect())
    }
}

/// The public connection surface is intentionally an allowlist. A lens is loaded into a
/// process that users are encouraged to point at production; letting it spell arbitrary
/// Redis commands would make the read-only promise a convention rather than a boundary.
fn read_only_command(name: &str, args: &[Value]) -> bool {
    let subcommand = || {
        args.first()
            .and_then(Value::as_string)
            .unwrap_or_default()
            .to_ascii_uppercase()
    };

    match name {
        "CLIENT" => matches!(subcommand().as_str(), "INFO" | "LIST"),
        "CLUSTER" => matches!(subcommand().as_str(), "INFO" | "NODES" | "SHARDS" | "SLOTS"),
        "CONFIG" => subcommand() == "GET",
        "MEMORY" => subcommand() == "USAGE",
        "MODULE" => subcommand() == "LIST",
        "PUBSUB" => matches!(subcommand().as_str(), "CHANNELS" | "NUMPAT" | "NUMSUB"),
        "SLOWLOG" => matches!(subcommand().as_str(), "GET" | "LEN"),
        "XINFO" => matches!(subcommand().as_str(), "STREAM" | "GROUPS" | "CONSUMERS"),
        "GETRANGE" | "HGET" | "HMGET" | "HSCAN" | "HLEN" | "LLEN" | "LRANGE" | "PTTL" | "SCAN"
        | "SCARD" | "SSCAN" | "STRLEN" | "TYPE" | "XLEN" | "XPENDING" | "XRANGE" | "ZCARD"
        | "ZRANGE" | "ZREVRANGE" => true,
        _ => false,
    }
}

fn parse_scan_type(raw: &str) -> Option<ScanType> {
    match raw {
        "string" => Some(ScanType::String),
        "hash" => Some(ScanType::Hash),
        "list" => Some(ScanType::List),
        "set" => Some(ScanType::Set),
        "zset" => Some(ScanType::ZSet),
        "stream" => Some(ScanType::Stream),
        _ => None,
    }
}

fn classify_from(e: &ConnError) -> Availability {
    match e {
        ConnError::Command { source, .. } => classify(source),
        _ => Availability::Unsupported,
    }
}

/// Time one `PING`.
///
/// A failure is reported as zero rather than an error: a server that will not answer
/// `PING` is not one to size timings against, and refusing to connect over a missing
/// *measurement* would be absurd.
async fn measure_rtt(client: &Client) -> std::time::Duration {
    let started = std::time::Instant::now();
    match client
        .custom::<Value, Value>(
            CustomCommand::new_static("PING", ClusterHash::FirstKey, false),
            vec![],
        )
        .await
    {
        Ok(_) => started.elapsed(),
        Err(e) => {
            debug!(error = %e, "PING failed; continuing without a latency measurement");
            std::time::Duration::ZERO
        }
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

/// Run every probe in one round trip on standalone servers.
///
/// Cluster pipelines are tied to one node and these probes include commands with different
/// routing shapes, so let the client route them independently there.
async fn run_probe_pipeline(
    client: &Client,
    probes: &[(Feature, &'static str, Vec<Value>)],
) -> Vec<std::result::Result<Value, Error>> {
    if client.is_clustered() {
        return probe_serially(client, probes).await;
    }

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

    #[test]
    fn public_command_surface_rejects_writes_and_unsafe_subcommands() {
        assert!(read_only_command("TYPE", &[Value::from("k")]));
        assert!(read_only_command(
            "CONFIG",
            &[Value::from("GET"), Value::from("*")]
        ));
        assert!(!read_only_command(
            "CONFIG",
            &[
                Value::from("SET"),
                Value::from("maxmemory"),
                Value::from("1")
            ]
        ));
        assert!(!read_only_command(
            "CLIENT",
            &[Value::from("KILL"), Value::from("ID"), Value::from("1")]
        ));
        assert!(!read_only_command(
            "SET",
            &[Value::from("k"), Value::from("v")]
        ));
    }
}
