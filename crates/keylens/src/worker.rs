//! The Redis side of the app, on its own task.
//!
//! The UI thread must never block on I/O. A `SCAN` against a cold, remote keyspace can
//! take seconds; if the render loop awaits it, keystrokes queue up and the tool feels
//! broken. So: the worker owns the [`Conn`] and the scan cursor, the UI owns the tree and
//! the selection, and they exchange messages over bounded channels.

use std::sync::Arc;

use keylens_bullmq::{BullMqLens, EventsStatus, Job, JobRef, QueueSummary, State};
use keylens_conn::{
    ClientInfo, ClusterTopology, Conn, Feature, KeyMeta, KeyValue, Kind, PubSubChannel, ServerInfo,
    SlowEntry, StreamInfo, Value,
};
use keylens_lens::{Detection, Lens};
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
/// Job ids fetched per state page.
const JOB_PAGE: usize = 200;
const JOB_LOG_LINES: usize = 200;

#[derive(Debug)]
pub enum Request {
    /// Start over with a new filter.
    Rescan {
        pattern: Option<String>,
        kind: Option<Kind>,
    },
    /// Continue the current scan.
    More,
    /// Load metadata and the first value page for one key.
    Select {
        key: String,
    },
    RefreshInfo,

    LoadSlowlog,
    LoadClients,
    LoadCluster,
    LoadPubSub,

    /// Run every lens detector. Cheap, and it decides whether the queues tab exists.
    Detect,
    LoadQueues,
    LoadJobs {
        queue: String,
        state: State,
    },
    LoadJob {
        queue: String,
        id: String,
    },
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
        /// Groups, consumers and pending state — only for stream keys.
        stream: Option<Box<StreamInfo>>,
    },
    Info(Box<ServerInfo>),
    Error(String),

    Slowlog(PaneState<Vec<SlowEntry>>),
    Clients(PaneState<Vec<ClientInfo>>),
    Cluster(PaneState<Box<ClusterTopology>>),
    PubSub(PaneState<Vec<PubSubChannel>>),

    Detected(Vec<Detection>),
    /// How the live events reader is getting on -- attaching, live, or never going to
    /// work here. The graph reads very differently depending on which.
    EventsStatus(EventsStatus),
    Events(Vec<crate::events::StreamEvent>),
    Queues(PaneState<Vec<QueueSummary>>),
    Jobs {
        state: State,
        data: PaneState<Vec<JobRef>>,
    },
    /// `None` inside `Ready` means the job was removed between listing and reading, which
    /// happens constantly on a live queue with retention configured.
    Job(PaneState<Option<Box<JobDetail>>>),
}

/// A job plus its logs, which are a separate key.
#[derive(Debug, Clone)]
pub struct JobDetail {
    pub job: Job,
    pub logs: Vec<String>,
}

pub struct Worker {
    conn: Arc<Conn>,
    cursor: String,
    pattern: Option<String>,
    kind: Option<Kind>,
    complete: bool,
    /// Re-created with the detected prefix once detection runs, so a keyspace using a
    /// custom BullMQ prefix works without configuration.
    bullmq: BullMqLens,
    /// The key read currently in flight, if any. Held so a newer selection can cancel it.
    detail_task: Option<tokio::task::JoinHandle<()>>,
    /// The stats refresh currently in flight, so a slow `INFO` is never asked for twice
    /// over.
    info_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        // `run` consumes the worker, so this fires when the UI aborts it. The spawned
        // tasks hold their own `Arc<Conn>` and would otherwise outlive the terminal.
        for task in [self.detail_task.take(), self.info_task.take()]
            .into_iter()
            .flatten()
        {
            task.abort();
        }
    }
}

impl Worker {
    pub fn new(conn: Conn) -> Self {
        Self::with_prefix(conn, None)
    }

    /// `prefix` overrides the lens's default `bull`, for a keyspace that uses another one.
    /// Detection scans `<prefix>:*:meta`, so without this a custom prefix simply looks
    /// like a server with no queues on it.
    pub fn with_prefix(conn: Conn, prefix: Option<String>) -> Self {
        Self {
            conn: Arc::new(conn),
            cursor: "0".into(),
            pattern: None,
            kind: None,
            complete: false,
            bullmq: match prefix {
                Some(p) => BullMqLens::new(p),
                None => BullMqLens::default(),
            },
            detail_task: None,
            info_task: None,
        }
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
                // A request always gets a reply, including this one. The UI sets `loading`
                // when it asks for more; answering an already-finished scan with silence
                // would leave the status bar spinning with nothing on the way.
                Request::More => self.scan_batch(false).await,

                // Reading a key runs on its own task, sharing the connection -- fred
                // multiplexes, so this costs no second handshake.
                //
                // What it buys is that `Select` no longer queues behind `Rescan` or
                // `Detect`, each of which walks up to 40 sequential `SCAN` pages. Against
                // a server measured at 390ms that queue was sixteen seconds long, and
                // every keypress made during it looked like a hang.
                Request::Select { key } => {
                    self.spawn_detail(key, &tx);
                    continue;
                }

                Request::RefreshInfo => {
                    // Servers that don't implement `INFO` would otherwise fail this on
                    // every tick and paint the error over whatever pane is open.
                    if !self.conn.has_server_info() {
                        continue;
                    }
                    self.spawn_info(&tx);
                    continue;
                }

                Request::LoadSlowlog => Update::Slowlog(
                    self.pane(Feature::Slowlog, self.conn.slowlog(SLOWLOG_ENTRIES))
                        .await,
                ),
                Request::LoadClients => Update::Clients(
                    self.pane(Feature::ClientList, self.conn.client_list())
                        .await,
                ),
                Request::LoadCluster => Update::Cluster(self.cluster().await),
                Request::LoadPubSub => Update::PubSub(
                    self.pane(Feature::PubSub, self.conn.pubsub_channels(PUBSUB_CHANNELS))
                        .await,
                ),

                Request::Detect => {
                    let detections = match self.bullmq.detect(&self.conn).await {
                        Ok(Some(d)) => {
                            // Adopt the detected prefix for every later query.
                            self.bullmq = BullMqLens::new(d.prefix.clone());
                            vec![d]
                        }
                        Ok(None) => Vec::new(),
                        Err(e) => {
                            warn!(error = %e, "bullmq detection failed");
                            Vec::new()
                        }
                    };
                    Update::Detected(detections)
                }

                Request::LoadQueues => {
                    Update::Queues(match self.bullmq.all_queues(&self.conn).await {
                        Ok(q) => PaneState::Ready(q),
                        Err(e) => PaneState::Failed(e.to_string()),
                    })
                }

                Request::LoadJobs { queue, state } => Update::Jobs {
                    state,
                    data: match self
                        .bullmq
                        .jobs(&self.conn, &queue, state, 0, JOB_PAGE)
                        .await
                    {
                        Ok(j) => PaneState::Ready(j),
                        Err(e) => PaneState::Failed(e.to_string()),
                    },
                },

                Request::LoadJob { queue, id } => Update::Job(self.job(&queue, &id).await),
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
                .scan_page(
                    &self.cursor,
                    self.pattern.as_deref(),
                    SCAN_COUNT,
                    type_filter,
                )
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
        Update::Batch {
            keys: typed,
            reset,
            complete: self.complete,
            scanned_pages: pages,
        }
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
        let cmds: Vec<(&'static str, Vec<Value>)> = keys[..head]
            .iter()
            .map(|k| ("TYPE", vec![Value::from(k.as_str())]))
            .collect();

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
                // One key failing to type -- deleted mid-batch, say -- costs that key its
                // tag and nothing else. It used to cost the whole batch its types.
                let kind = kinds
                    .get(i)
                    .and_then(|r| r.as_ref().ok())
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
                availability
                    .reason()
                    .unwrap_or("blocked by this server")
                    .to_string(),
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
            && why
                .to_ascii_lowercase()
                .contains("cluster support disabled")
        {
            return PaneState::Ready(Box::default());
        }
        self.pane(Feature::Cluster, async {
            self.conn.cluster_topology().await.map(Box::new)
        })
        .await
    }

    async fn job(&self, queue: &str, id: &str) -> PaneState<Option<Box<JobDetail>>> {
        let job = match self.bullmq.job(&self.conn, queue, id).await {
            Ok(Some(j)) => j,
            Ok(None) => return PaneState::Ready(None),
            Err(e) => return PaneState::Failed(e.to_string()),
        };

        // Logs live in a separate key and are usually absent; their absence is not a
        // reason to fail the whole job view.
        let logs = self
            .bullmq
            .job_logs(&self.conn, queue, id, JOB_LOG_LINES)
            .await
            .unwrap_or_default();

        PaneState::Ready(Some(Box::new(JobDetail { job, logs })))
    }

    /// Read one key on its own task, cancelling whatever read was already running.
    ///
    /// Cancelling is not just tidiness. The UI already discards a reply whose key is no
    /// longer selected, so a superseded read was pure waste -- and on the link this exists
    /// for, waste is measured in whole seconds of the only round trips available.
    fn spawn_detail(&mut self, key: String, tx: &Sender<Update>) {
        if let Some(task) = self.detail_task.take() {
            task.abort();
        }

        let conn = self.conn.clone();
        let tx = tx.clone();
        self.detail_task = Some(tokio::spawn(async move {
            let update = detail(&conn, &key).await;
            tx.send(update).await.ok();
        }));
    }

    /// Refresh server stats on their own task, unless the last refresh is still out.
    ///
    /// Skipping rather than queueing matters once `INFO` takes longer than the tick: the
    /// requests would otherwise stack up, each one spending a round trip the key browser
    /// wanted.
    fn spawn_info(&mut self, tx: &Sender<Update>) {
        if self.info_task.as_ref().is_some_and(|t| !t.is_finished()) {
            return;
        }

        let conn = self.conn.clone();
        let tx = tx.clone();
        self.info_task = Some(tokio::spawn(async move {
            let update = match conn.refresh_info().await {
                Ok(info) => Update::Info(Box::new(info)),
                Err(e) => Update::Error(e.to_string()),
            };
            tx.send(update).await.ok();
        }));
    }
}

/// One round trip, not five.
///
/// Type, TTL, memory, size and the first page of the value all arrive together -- see
/// [`Conn::read_key`] for how, and why it is worth speculating over six types to get it.
/// Sequentially this was five round trips per keypress, over a second each against a
/// managed host on another continent, which is what made scrolling the tree feel broken.
async fn detail(conn: &Conn, key: &str) -> Update {
    let (meta, value) = match conn.read_key(key).await {
        Ok(pair) => pair,
        Err(e) => return Update::Error(e.to_string()),
    };

    // Consumer-group state is the reason to open a stream at all, but it's several extra
    // round trips, so it's fetched only for streams. A failure here degrades to "entries
    // only" rather than failing the whole key.
    let stream = if meta.kind == Kind::Stream {
        conn.stream_info(key).await.ok().map(Box::new)
    } else {
        None
    };

    Update::Detail {
        meta: Box::new(meta),
        value: Box::new(value),
        stream,
    }
}
