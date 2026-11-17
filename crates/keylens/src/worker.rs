//! The Redis side of the app, on its own task.
//!
//! The UI thread must never block on I/O. A `SCAN` against a cold, remote keyspace can
//! take seconds; if the render loop awaits it, keystrokes queue up and the tool feels
//! broken. So: the worker owns the [`Conn`] and the scan cursor, the UI owns the tree and
//! the selection, and they exchange messages over bounded channels.

use std::sync::Arc;

use keylens_bullmq::{BullMqLens, EventsStatus, Job, JobRef, QueueList, State};
use keylens_conn::{
    Capped, ClientInfo, ClusterTopology, Conn, Coverage, Feature, KeyMeta, KeyScanner, KeyValue,
    Kind, PubSubChannel, ServerInfo, SlowEntry, StreamInfo, Value,
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
/// Cap on each typing pipeline. A SCAN page may exceed its COUNT hint, so large replies
/// are split rather than silently left untyped or incorrectly dropped by a type filter.
const MAX_TYPED: usize = 1_000;
const SLOWLOG_ENTRIES: u32 = 128;
const PUBSUB_CHANNELS: usize = 200;
/// Job ids fetched per state page.
pub const JOB_PAGE: usize = 200;
const JOB_LOG_LINES: usize = 200;

/// Names one request, so its reply can be matched against what the UI still wants.
///
/// Replies arrive out of order and after the user has moved on: a key read is cancelled
/// but may already have sent, a job opened a moment ago answers after the next one was
/// opened, and a refresh started before a reconnect answers after it. Matching a reply by
/// its *content* — the key name, the job state — cannot tell those apart, because the
/// stale reply and the wanted one can name the same thing. An id issued per request can.
///
/// Ids are issued by [`crate::app::App`] and are strictly increasing within one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct RequestId(u64);

impl RequestId {
    /// Wrap a raw id. Public so tests and embedders can stage a reply.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The id after this one. Saturates rather than wrapping: `u64::MAX` requests is
    /// unreachable, and a wrapped id would match a reply from the very first request.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Debug)]
pub enum Request {
    /// Start over with a new filter. `scan` names the new scan generation; every batch
    /// produced by it, including the ones `More` asks for, carries it back.
    Rescan {
        scan: RequestId,
        pattern: Option<String>,
        kind: Option<Kind>,
    },
    /// Continue the scan generation `scan`.
    More {
        scan: RequestId,
    },
    /// Load metadata and the first value page for one key.
    Select {
        id: RequestId,
        key: String,
    },
    RefreshInfo {
        id: RequestId,
    },

    LoadSlowlog {
        id: RequestId,
    },
    LoadClients {
        id: RequestId,
    },
    LoadCluster {
        id: RequestId,
    },
    LoadPubSub {
        id: RequestId,
    },

    /// Run every lens detector. Cheap, and it decides whether the queues tab exists.
    Detect,
    LoadQueues {
        id: RequestId,
    },
    LoadJobs {
        id: RequestId,
        queue: String,
        state: State,
    },
    LoadJob {
        id: RequestId,
        queue: String,
        job: String,
    },
}

/// Where a scan stands after a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanProgress {
    /// More of the keyspace is left; `More` continues it.
    Partial,
    /// The cursor came back to 0: every key was visited.
    Complete,
    /// The scan failed and cannot continue. The batch still carries every key gathered
    /// before the failure -- they are real keys -- but the tree is not the whole keyspace.
    Failed(String),
}

/// A worker reply. Every variant that answers a request carries that request's id, and
/// that includes the failures: an error is a reply like any other and is just as capable
/// of being stale.
#[derive(Debug)]
pub enum Update {
    Batch {
        /// The scan generation this batch belongs to (the id of its `Rescan`).
        scan: RequestId,
        keys: Vec<(String, Option<Kind>)>,
        /// True when this batch starts a new scan, so the UI clears first.
        reset: bool,
        progress: ScanProgress,
        scanned_pages: usize,
    },
    Detail {
        id: RequestId,
        meta: Box<KeyMeta>,
        value: Box<KeyValue>,
        /// Whether `value` is all of it, or the first bounded page.
        coverage: Coverage,
        /// Groups, consumers and pending state — only for stream keys.
        stream: Option<Box<StreamInfo>>,
    },
    /// Reading the key requested by `id` failed.
    DetailFailed {
        id: RequestId,
        key: String,
        error: String,
    },
    Info {
        id: RequestId,
        info: Box<ServerInfo>,
    },
    /// The stats refresh `id` failed. Reported against the stats pane, never the value
    /// pane: a failed `INFO` says nothing about the key someone is looking at.
    InfoFailed {
        id: RequestId,
        error: String,
    },

    Slowlog {
        id: RequestId,
        data: PaneState<Vec<SlowEntry>>,
    },
    Clients {
        id: RequestId,
        data: PaneState<Capped<ClientInfo>>,
    },
    Cluster {
        id: RequestId,
        data: PaneState<Box<ClusterTopology>>,
    },
    PubSub {
        id: RequestId,
        data: PaneState<Capped<PubSubChannel>>,
    },

    Detected(Vec<Detection>),
    /// How the live events reader is getting on -- attaching, live, or never going to
    /// work here. The graph reads very differently depending on which.
    EventsStatus(EventsStatus),
    Events(Vec<crate::events::StreamEvent>),
    Queues {
        id: RequestId,
        data: PaneState<QueueList>,
    },
    Jobs {
        id: RequestId,
        data: PaneState<Vec<JobRef>>,
    },
    /// `None` inside `Ready` means the job was removed between listing and reading, which
    /// happens constantly on a live queue with retention configured.
    Job {
        id: RequestId,
        data: PaneState<Option<Box<JobDetail>>>,
    },
}

/// A job plus its logs, which are a separate key.
#[derive(Debug, Clone)]
pub struct JobDetail {
    pub job: Job,
    pub logs: Vec<String>,
}

pub struct Worker {
    conn: Arc<Conn>,
    scanner: Option<tokio::sync::Mutex<KeyScanner>>,
    /// Generation of `scanner`: the id of the `Rescan` that created it.
    scan: RequestId,
    /// Why the current generation's scanner could not be created, if it could not.
    scan_error: Option<String>,
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
            scanner: None,
            scan: RequestId::default(),
            scan_error: None,
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
                Request::Rescan {
                    scan,
                    pattern,
                    kind,
                } => {
                    self.scan = scan;
                    self.pattern = pattern;
                    self.kind = kind;
                    self.complete = false;
                    self.scan_error = None;
                    match self.conn.key_scanner(
                        self.pattern.as_deref(),
                        SCAN_COUNT,
                        self.server_type_filter(),
                    ) {
                        Ok(scanner) => {
                            self.scanner = Some(tokio::sync::Mutex::new(scanner));
                            self.scan_batch(true).await
                        }
                        Err(e) => {
                            // Not `None` alone: a missing scanner reads as "nothing left",
                            // and a `More` would then report this scan complete.
                            self.scanner = None;
                            self.scan_error = Some(e.to_string());
                            Update::Batch {
                                scan,
                                keys: Vec::new(),
                                reset: true,
                                progress: ScanProgress::Failed(e.to_string()),
                                scanned_pages: 0,
                            }
                        }
                    }
                }
                // A request always gets a reply, including this one. The UI sets `loading`
                // when it asks for more; answering an already-finished scan with silence
                // would leave the status bar spinning with nothing on the way.
                //
                // A `More` for a generation this worker has already replaced cannot
                // continue the scan it meant. Answering it from the current scanner would
                // splice the new filter's keys into a tree the UI is about to discard, so
                // it gets an empty batch under its own generation, which the UI drops.
                Request::More { scan } if scan != self.scan => Update::Batch {
                    scan,
                    keys: Vec::new(),
                    reset: false,
                    progress: ScanProgress::Partial,
                    scanned_pages: 0,
                },
                Request::More { .. } => self.scan_batch(false).await,

                // Reading a key runs on its own task, sharing the connection -- fred
                // multiplexes, so this costs no second handshake.
                //
                // What it buys is that `Select` no longer queues behind `Rescan` or
                // `Detect`, each of which walks up to 40 sequential `SCAN` pages. Against
                // a server measured at 390ms that queue was sixteen seconds long, and
                // every keypress made during it looked like a hang.
                Request::Select { id, key } => {
                    self.spawn_detail(id, key, &tx);
                    continue;
                }

                Request::RefreshInfo { id } => {
                    // Servers that don't implement `INFO` would otherwise fail this on
                    // every tick and paint the error over whatever pane is open.
                    if !self.conn.has_server_info() {
                        continue;
                    }
                    self.spawn_info(id, &tx);
                    continue;
                }

                Request::LoadSlowlog { id } => Update::Slowlog {
                    id,
                    data: Self::pane(
                        self.conn.capabilities().get(Feature::Slowlog),
                        self.conn.slowlog(SLOWLOG_ENTRIES),
                    )
                    .await,
                },
                Request::LoadClients { id } => Update::Clients {
                    id,
                    data: Self::pane(
                        self.conn.capabilities().get(Feature::ClientList),
                        self.conn.client_list(),
                    )
                    .await,
                },
                Request::LoadCluster { id } => Update::Cluster {
                    id,
                    data: Self::cluster(self.conn.clone()).await,
                },
                Request::LoadPubSub { id } => Update::PubSub {
                    id,
                    data: Self::pane(
                        self.conn.capabilities().get(Feature::PubSub),
                        self.conn.pubsub_channels(PUBSUB_CHANNELS),
                    )
                    .await,
                },

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

                Request::LoadQueues { id } => Update::Queues {
                    id,
                    data: match self.bullmq.all_queues(&self.conn).await {
                        Ok(q) => PaneState::Ready(q),
                        Err(e) => PaneState::Failed(e.to_string()),
                    },
                },

                Request::LoadJobs { id, queue, state } => Update::Jobs {
                    id,
                    data: match self
                        .bullmq
                        .jobs(&self.conn, &queue, state, 0, JOB_PAGE)
                        .await
                    {
                        Ok(j) => PaneState::Ready(j),
                        Err(e) => PaneState::Failed(e.to_string()),
                    },
                },

                Request::LoadJob { id, queue, job } => Update::Job {
                    id,
                    data: self.job(&queue, &job).await,
                },
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
        let mut failure: Option<String> = self.scan_error.clone();

        while failure.is_none()
            && keys.len() < BATCH_TARGET
            && pages < MAX_PAGES_PER_BATCH
            && !self.complete
        {
            // No scanner means no `Rescan` has been handled yet. In the app that cannot
            // happen — `browse` queues `Rescan` before the event loop starts, and the
            // worker drains the channel in order — but `Worker` is public API, and a
            // `More` that arrives first should end the batch rather than panic the task
            // that owns the only connection.
            let Some(scanner) = self.scanner.as_mut() else {
                self.complete = true;
                break;
            };

            match scanner.get_mut().next_page().await {
                Ok(Some(page)) => {
                    pages += 1;
                    keys.extend(page);
                }
                Ok(None) => {
                    self.complete = true;
                    break;
                }
                // Keep what was gathered: those keys are real. 0.1.7 returned the error
                // alone, dropping up to 40 pages of keys, and the *next* `More` then saw
                // the end of fred's stream and reported the keyspace fully walked.
                Err(e) => {
                    failure = Some(e.to_string());
                    break;
                }
            }
        }

        let typed = self.type_keys(keys).await;
        Update::Batch {
            scan: self.scan,
            keys: typed,
            reset,
            progress: match failure {
                Some(why) => ScanProgress::Failed(why),
                None if self.complete => ScanProgress::Complete,
                None => ScanProgress::Partial,
            },
            scanned_pages: pages,
        }
    }

    fn server_type_filter(&self) -> Option<&str> {
        self.kind
            .filter(|_| self.conn.capabilities().has(Feature::ScanTypeFilter))
            .map(|kind| kind.label())
    }

    /// Resolve each key's type in one round trip.
    ///
    /// When a `TYPE` filter is already active the answer is known, so this is skipped
    /// entirely -- no reason to ask the server what it just filtered on.
    async fn type_keys(&self, keys: Vec<String>) -> Vec<(String, Option<Kind>)> {
        if let Some(k) = self.kind
            && self.conn.capabilities().has(Feature::ScanTypeFilter)
        {
            return keys.into_iter().map(|key| (key, Some(k))).collect();
        }
        if keys.is_empty() {
            return Vec::new();
        }

        let mut kinds = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(MAX_TYPED) {
            let cmds: Vec<(&'static str, Vec<Value>)> = chunk
                .iter()
                .map(|k| ("TYPE", vec![Value::from(k.as_str())]))
                .collect();
            match self.conn.pipeline(&cmds).await {
                Ok(mut replies) => kinds.append(&mut replies),
                Err(e) => {
                    // Preserve one positional slot per key. Without an active filter an
                    // unknown type is only a missing tag; with one it must be excluded.
                    warn!(error = %e, "typing keys failed; continuing untyped");
                    kinds.extend((0..chunk.len()).map(|_| {
                        Err(keylens_conn::ConnError::Reply {
                            cmd: "TYPE",
                            detail: "typing batch failed".into(),
                        })
                    }));
                }
            }
        }

        keep_typed(keys, &kinds, self.kind)
    }

    /// Turn a capability + a fallible load into a [`PaneState`].
    ///
    /// The capability check comes first so a blocked command reports *why* it's blocked,
    /// using the reason captured at connect time, rather than whatever generic error the
    /// server happens to return for it.
    async fn pane<T, F>(availability: keylens_conn::Availability, load: F) -> PaneState<T>
    where
        F: Future<Output = keylens_conn::Result<T>>,
    {
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
    async fn cluster(conn: Arc<Conn>) -> PaneState<Box<ClusterTopology>> {
        let availability = conn.capabilities().get(Feature::Cluster);
        if let keylens_conn::Availability::Denied(why) = &availability
            && why
                .to_ascii_lowercase()
                .contains("cluster support disabled")
        {
            return PaneState::Ready(Box::default());
        }
        Self::pane(availability, async {
            conn.cluster_topology().await.map(Box::new)
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
    ///
    /// Aborting is best effort: the task may already have sent its reply, which is why the
    /// reply carries `id` and the UI, not this function, decides whether it is current.
    fn spawn_detail(&mut self, id: RequestId, key: String, tx: &Sender<Update>) {
        if let Some(task) = self.detail_task.take() {
            task.abort();
        }

        let conn = self.conn.clone();
        let tx = tx.clone();
        self.detail_task = Some(tokio::spawn(async move {
            let update = detail(&conn, id, &key).await;
            tx.send(update).await.ok();
        }));
    }

    /// Refresh server stats on their own task, unless the last refresh is still out.
    ///
    /// Skipping rather than queueing matters once `INFO` takes longer than the tick: the
    /// requests would otherwise stack up, each one spending a round trip the key browser
    /// wanted.
    fn spawn_info(&mut self, id: RequestId, tx: &Sender<Update>) {
        if self.info_task.as_ref().is_some_and(|t| !t.is_finished()) {
            return;
        }

        let conn = self.conn.clone();
        let tx = tx.clone();
        self.info_task = Some(tokio::spawn(async move {
            let update = match conn.refresh_info().await {
                Ok(info) => Update::Info {
                    id,
                    info: Box::new(info),
                },
                Err(e) => Update::InfoFailed {
                    id,
                    error: e.to_string(),
                },
            };
            tx.send(update).await.ok();
        }));
    }
}

/// Pair each key with its `TYPE` reply, dropping the keys that should not be listed.
///
/// One key failing to type -- an ACL on `TYPE`, say -- costs that key its tag and nothing
/// else. A key whose type came back `none` was deleted between the `SCAN` that listed it
/// and the `TYPE`, and is dropped: listing it puts a key in the tree the server no longer
/// holds. With a type filter active, anything not provably of that type is excluded.
fn keep_typed(
    keys: Vec<String>,
    kinds: &[keylens_conn::Result<Value>],
    requested: Option<Kind>,
) -> Vec<(String, Option<Kind>)> {
    keys.into_iter()
        .enumerate()
        .filter_map(|(i, key)| {
            let kind = kinds
                .get(i)
                .and_then(|r| r.as_ref().ok())
                .map(|v| Kind::parse(&keylens_conn::value::display_string(v)));
            if kind == Some(Kind::None) {
                return None;
            }
            if requested.is_some_and(|wanted| kind != Some(wanted)) {
                return None;
            }
            Some((key, kind))
        })
        .collect()
}

/// One round trip, not five.
///
/// Type, TTL, memory, size and the first page of the value all arrive together -- see
/// [`Conn::read_key`] for how, and why it is worth speculating over six types to get it.
/// Sequentially this was five round trips per keypress, over a second each against a
/// managed host on another continent, which is what made scrolling the tree feel broken.
async fn detail(conn: &Conn, id: RequestId, key: &str) -> Update {
    let read = match conn.read_key(key).await {
        Ok(read) => read,
        Err(e) => {
            return Update::DetailFailed {
                id,
                key: key.to_string(),
                error: e.to_string(),
            };
        }
    };

    // Consumer-group state is the reason to open a stream at all, but it's several extra
    // round trips, so it's fetched only for streams. A failure here degrades to "entries
    // only" rather than failing the whole key.
    let keylens_conn::KeyRead {
        meta,
        value,
        coverage,
    } = read;
    let stream = if meta.kind == Kind::Stream {
        conn.stream_info(key).await.ok().map(Box::new)
    } else {
        None
    };

    Update::Detail {
        id,
        meta: Box::new(meta),
        value: Box::new(value),
        coverage,
        stream,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err() -> keylens_conn::Result<Value> {
        Err(keylens_conn::ConnError::Reply {
            cmd: "TYPE",
            detail: "denied".into(),
        })
    }

    #[test]
    fn keys_deleted_between_scan_and_type_are_not_listed() {
        // 0.1.7 inserted them with a `-` tag: a key in the tree the server no longer had.
        let keys = vec![
            "live".to_string(),
            "gone".to_string(),
            "untyped".to_string(),
        ];
        let kinds = vec![Ok(Value::from("hash")), Ok(Value::from("none")), err()];
        let kept = keep_typed(keys, &kinds, None);
        assert_eq!(
            kept,
            vec![
                ("live".to_string(), Some(Kind::Hash)),
                ("untyped".to_string(), None),
            ],
            "a failed TYPE keeps the key untagged; a `none` drops it"
        );
    }

    #[test]
    fn a_type_filter_excludes_what_it_cannot_prove() {
        let keys = vec!["h".to_string(), "s".to_string(), "unknown".to_string()];
        let kinds = vec![Ok(Value::from("hash")), Ok(Value::from("string")), err()];
        let kept = keep_typed(keys, &kinds, Some(Kind::Hash));
        assert_eq!(kept, vec![("h".to_string(), Some(Kind::Hash))]);
    }
}
