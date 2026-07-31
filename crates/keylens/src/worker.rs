//! The Redis side of the app, on its own task.
//!
//! The UI thread must never block on I/O. A `SCAN` against a cold, remote keyspace can
//! take seconds; if the render loop awaits it, keystrokes queue up and the tool feels
//! broken. So: the worker owns the [`Conn`] and the scan cursor, the UI owns the tree and
//! the selection, and they exchange messages over bounded channels.

use keylens_conn::{
    ClientInfo, ClusterTopology, Conn, Feature, KeyMeta, KeyValue, Kind, PubSubChannel,
    ServerInfo, SlowEntry, Value,
};
use keylens_ui::PaneState;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::warn;

/// Keys to gather before returning a batch to the UI.
const BATCH_TARGET: usize = 500;
/// Hard cap on pages per batch. A restrictive `MATCH` can return empty page after empty
/// page; without this the worker would walk the entire keyspace before rendering anything.
const MAX_PAGES_PER_BATCH: usize = 40;
const SCAN_COUNT: u32 = 500;
/// Cap on how many keys get typed per batch, to bound the pipeline size.
const MAX_TYPED: usize = 1_000;
const SLOWLOG_ENTRIES: u32 = 128;
const PUBSUB_CHANNELS: usize = 200;

#[derive(Debug)]
pub enum Request {
    /// Start over with a new filter.
    Rescan { pattern: Option<String>, kind: Option<Kind> },
    /// Continue the current scan.
    More,
    /// Load metadata and a value page for one key.
    Select { key: String, offset: usize },
    RefreshInfo,

    LoadSlowlog,
    LoadClients,
    LoadCluster,
    LoadPubSub,
}

#[derive(Debug)]
pub enum Update {
    Batch {
        keys: Vec<(String, Option<Kind>)>,
        /// True when this batch starts a new scan, so the UI clears first.
        reset: bool,
        /// True when the cursor came back to 0 -- the keyspace is fully walked.
        complete: bool,
        scanned_pages: usize,
    },
    Detail {
        meta: Box<KeyMeta>,
        value: Box<KeyValue>,
    },
    Info(Box<ServerInfo>),
    Error(String),

    Slowlog(PaneState<Vec<SlowEntry>>),
    Clients(PaneState<Vec<ClientInfo>>),
    Cluster(PaneState<Box<ClusterTopology>>),
    PubSub(PaneState<Vec<PubSubChannel>>),
}

pub struct Worker {
    conn: Conn,
    cursor: String,
    pattern: Option<String>,
    kind: Option<Kind>,
    complete: bool,
}

impl Worker {
    pub fn new(conn: Conn) -> Self {
        Self { conn, cursor: "0".into(), pattern: None, kind: None, complete: false }
    }

    pub async fn run(mut self, mut rx: Receiver<Request>, tx: Sender<Update>) {
        while let Some(req) = rx.recv().await {
            let update = match req {
                Request::Rescan { pattern, kind } => {
                    self.cursor = "0".into();
                    self.pattern = pattern;
                    self.kind = kind;
                    self.complete = false;
                    self.scan_batch(true).await
                }
                Request::More => {
                    if self.complete {
                        continue;
                    }
                    self.scan_batch(false).await
                }
                Request::Select { key, offset } => self.detail(&key, offset).await,
                Request::RefreshInfo => match self.conn.refresh_info().await {
                    Ok(info) => Update::Info(Box::new(info)),
                    Err(e) => Update::Error(e.to_string()),
                },

                Request::LoadSlowlog => Update::Slowlog(
                    self.pane(Feature::Slowlog, self.conn.slowlog(SLOWLOG_ENTRIES)).await,
                ),
                Request::LoadClients => {
                    Update::Clients(self.pane(Feature::ClientList, self.conn.client_list()).await)
                }
                Request::LoadCluster => Update::Cluster(self.cluster().await),
                Request::LoadPubSub => Update::PubSub(
                    self.pane(Feature::PubSub, self.conn.pubsub_channels(PUBSUB_CHANNELS)).await,
                ),
            };

            // A closed channel means the UI is gone; stop rather than spin.
            if tx.send(update).await.is_err() {
                break;
            }
        }
    }

    async fn scan_batch(&mut self, reset: bool) -> Update {
        let mut keys: Vec<String> = Vec::new();
        let mut pages = 0usize;

        while keys.len() < BATCH_TARGET && pages < MAX_PAGES_PER_BATCH && !self.complete {
            let type_filter = self.kind.map(|k| k.label());
            let page = match self
                .conn
                .scan_page(&self.cursor, self.pattern.as_deref(), SCAN_COUNT, type_filter)
                .await
            {
                Ok(p) => p,
                Err(e) => return Update::Error(e.to_string()),
            };

            pages += 1;
            keys.extend(page.keys.iter().cloned());
            self.cursor = page.cursor.clone();
            if page.is_complete() {
                self.complete = true;
            }
        }

        let typed = self.type_keys(keys).await;
        Update::Batch { keys: typed, reset, complete: self.complete, scanned_pages: pages }
    }

    /// Resolve each key's type in one round trip.
    ///
    /// When a `TYPE` filter is already active the answer is known, so this is skipped
    /// entirely -- no reason to ask the server what it just filtered on.
    async fn type_keys(&self, keys: Vec<String>) -> Vec<(String, Option<Kind>)> {
        if let Some(k) = self.kind {
            return keys.into_iter().map(|key| (key, Some(k))).collect();
        }
        if keys.is_empty() {
            return Vec::new();
        }

        let head = keys.len().min(MAX_TYPED);
        let cmds: Vec<(&'static str, Vec<Value>)> =
            keys[..head].iter().map(|k| ("TYPE", vec![Value::from(k.as_str())])).collect();

        let kinds = match self.conn.pipeline(&cmds).await {
            Ok(v) => v,
            Err(e) => {
                // Types are a nicety; the tree is still usable without them.
                warn!(error = %e, "typing keys failed; continuing untyped");
                Vec::new()
            }
        };

        keys.into_iter()
            .enumerate()
            .map(|(i, key)| {
                let kind = kinds
                    .get(i)
                    .map(|v| Kind::parse(&keylens_conn::value::display_string(v)));
                (key, kind)
            })
            .collect()
    }

    /// Turn a capability + a fallible load into a [`PaneState`].
    ///
    /// The capability check comes first so a blocked command reports *why* it's blocked,
    /// using the reason captured at connect time, rather than whatever generic error the
    /// server happens to return for it.
    async fn pane<T, F>(&self, feature: Feature, load: F) -> PaneState<T>
    where
        F: Future<Output = keylens_conn::Result<T>>,
    {
        let availability = self.conn.capabilities().get(feature);
        if !availability.is_available() {
            return PaneState::Unavailable(
                availability.reason().unwrap_or("blocked by this server").to_string(),
            );
        }
        match load.await {
            Ok(v) => PaneState::Ready(v),
            Err(e) => PaneState::Failed(e.to_string()),
        }
    }

    /// Cluster gets its own path rather than the generic capability gate.
    ///
    /// A standalone Redis 8 *rejects* `CLUSTER INFO` ("this instance has cluster support
    /// disabled"), which the probe correctly records as unavailable. But for this pane the
    /// honest answer is "you're on a standalone server", not "your host blocked this" --
    /// those are very different messages to show someone.
    async fn cluster(&self) -> PaneState<Box<ClusterTopology>> {
        let availability = self.conn.capabilities().get(Feature::Cluster);
        if let keylens_conn::Availability::Denied(why) = &availability
            && why.to_ascii_lowercase().contains("cluster support disabled")
        {
            return PaneState::Ready(Box::default());
        }
        self.pane(Feature::Cluster, async {
            self.conn.cluster_topology().await.map(Box::new)
        })
        .await
    }

    async fn detail(&self, key: &str, offset: usize) -> Update {
        let meta = match self.conn.key_meta(key).await {
            Ok(m) => m,
            Err(e) => return Update::Error(e.to_string()),
        };
        let value = match self.conn.read_value(key, meta.kind, offset).await {
            Ok(v) => v,
            Err(e) => return Update::Error(e.to_string()),
        };
        Update::Detail { meta: Box::new(meta), value: Box::new(value) }
    }
}
