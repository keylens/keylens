//! Application state and input handling.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use keylens_bullmq::{JobRef, QueueList, QueueSummary, State, Throughput};
use keylens_conn::{
    Capped, ClientInfo, ClusterTopology, Coverage, KeyMeta, KeyValue, Kind, PubSubChannel,
    ServerInfo, SlowEntry, StreamInfo, redact_url,
};
use keylens_lens::Detection;
use keylens_ui::KeyTree;
use keylens_ui::PaneState;
use keylens_ui::tree::Row;
use ratatui::widgets::ListState;
use tokio::sync::mpsc::Sender;

use crate::worker::{JobDetail, Request, RequestId, ScanProgress, Update};

/// The top-level tabs.
///
/// [`View::Queues`] only appears once a lens has detected a queue system. That is the
/// lens idea showing up in the chrome: keylens grows a tab because of what's *in* your
/// keyspace, not because someone turned a feature flag on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    Keys,
    Queues,
    Stats,
    Slowlog,
    Clients,
    Cluster,
    PubSub,
}

impl View {
    pub const ALL: [View; 7] = [
        View::Keys,
        View::Queues,
        View::Stats,
        View::Slowlog,
        View::Clients,
        View::Cluster,
        View::PubSub,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            View::Keys => "keys",
            View::Queues => "queues",
            View::Stats => "stats",
            View::Slowlog => "slowlog",
            View::Clients => "clients",
            View::Cluster => "cluster",
            View::PubSub => "pubsub",
        }
    }
}

/// How deep into the queue view we are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueLevel {
    Queues,
    Jobs,
    Job,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Tree,
    Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    Normal,
    /// Editing the key pattern. The filter is applied server-side via `SCAN MATCH`.
    Search,
    Help,
}

/// Order the `t` key cycles through.
const KIND_CYCLE: [Option<Kind>; 7] = [
    None,
    Some(Kind::String),
    Some(Kind::Hash),
    Some(Kind::List),
    Some(Kind::Set),
    Some(Kind::ZSet),
    Some(Kind::Stream),
];

/// Most keys the tree will hold.
///
/// Every `m` adds a batch, and nothing else bounds the tree. Past this the browser stops
/// asking for more and says why: a filter is the way to look at a keyspace this size, and
/// a tree of millions of rows is neither navigable nor cheap to rebuild on every batch.
pub const MAX_TREE_KEYS: usize = 500_000;

fn worker_busy<T>() -> PaneState<T> {
    PaneState::Failed("worker busy — press r to retry".into())
}

/// The newest request issued for each kind of reply.
///
/// A reply is applied only when it answers the request recorded here. Anything else
/// answers a question the user has stopped asking — a key they scrolled past, a job they
/// backed out of, a scan whose filter they changed — and painting it would show one
/// thing's data under another thing's heading.
#[derive(Debug, Default)]
struct Pending {
    /// The current scan generation. Batches from any other generation are dropped.
    scan: RequestId,
    /// The key read the value pane is waiting on, and which key it was.
    detail: Option<(RequestId, String)>,
    /// The newest stats refresh *applied*. Stats are accepted newest-wins rather than
    /// exact-match because the worker coalesces refreshes: it skips a tick while one is
    /// still out, so the newest *issued* id may never be answered at all.
    info_applied: RequestId,
    slowlog: Option<RequestId>,
    clients: Option<RequestId>,
    cluster: Option<RequestId>,
    pubsub: Option<RequestId>,
    queues: Option<RequestId>,
    jobs: Option<RequestId>,
    job: Option<RequestId>,
}

/// Whether a reply for `id` answers the request recorded in `slot`.
fn answers(slot: Option<RequestId>, id: RequestId) -> bool {
    slot == Some(id)
}

pub struct App {
    pub tree: KeyTree,
    pub rows: Vec<Row>,
    pub selected: usize,
    /// Owned by the app rather than rebuilt per frame, so the viewport keeps its scroll
    /// offset instead of snapping the selection to an edge on every redraw.
    pub list_state: ListState,

    pub detail: Option<(KeyMeta, KeyValue)>,
    /// Whether `detail`'s value is the whole value or its first bounded page.
    pub coverage: Coverage,
    /// Consumer-group state for the selected key, when it's a stream.
    pub stream: Option<Box<StreamInfo>>,
    pub detail_scroll: u16,
    /// The newest request of each kind; see [`Pending`].
    pending: Pending,
    /// Last id handed out. Ids are issued here, not by the worker, so that a request
    /// and the check on its reply cannot disagree about what was asked.
    last_id: RequestId,
    /// The cursor moved and the key under it has not been fetched yet.
    ///
    /// Cursor movement marks; the event loop fetches once the keys stop coming. Firing a
    /// request per keypress meant holding `j` queued one five-round-trip fetch per row --
    /// against a remote server that is a backlog measured in tens of seconds, of which
    /// every reply but the last is discarded as stale on arrival.
    selection_dirty: bool,

    pub focus: Focus,
    pub mode: Mode,
    pub search_input: String,
    pub pattern: Option<String>,
    pub kind_filter: Option<Kind>,

    pub view: View,
    /// Scroll offset for whichever server pane is showing.
    pub pane_scroll: u16,
    pub slowlog: PaneState<Vec<SlowEntry>>,
    pub clients: PaneState<Capped<ClientInfo>>,
    pub cluster: PaneState<Box<ClusterTopology>>,
    pub pubsub: PaneState<Capped<PubSubChannel>>,

    /// Lenses that matched this keyspace. Non-empty is what makes the queues tab appear.
    pub detections: Vec<Detection>,
    pub level: QueueLevel,
    pub queues: PaneState<QueueList>,
    pub queue_selected: usize,
    pub job_state: State,
    pub jobs: PaneState<Vec<JobRef>>,
    pub job_selected: usize,
    pub job: PaneState<Option<Box<JobDetail>>>,
    /// Live per-queue throughput, fed by the events-stream reader.
    pub throughput: Throughput,

    pub server: ServerInfo,
    /// Why the last stats refresh failed, if it did. Shown on the stats pane — a failed
    /// `INFO` is not a reason to hide the value of the key being inspected.
    pub info_error: Option<String>,
    pub url: String,
    pub status: String,
    /// A failure of the current scan or of the selected key's read. Rendered in place of
    /// the value, so it must only ever describe the current scan or the selected key.
    pub error: Option<String>,
    pub loading: bool,
    pub scan_complete: bool,
    /// The tree reached [`MAX_TREE_KEYS`]; `m` no longer asks for more.
    pub tree_full: bool,
    /// Covers the gap between "process started" and "first keys arrived", which on a cold
    /// remote keyspace is a real second or two of otherwise-blank screen.
    pub splash: bool,
    pub quit: bool,

    tx: Sender<Request>,
}

impl App {
    pub fn new(server: ServerInfo, url: String, tx: Sender<Request>) -> Self {
        Self {
            tree: KeyTree::new(),
            rows: Vec::new(),
            selected: 0,
            list_state: ListState::default(),
            detail: None,
            coverage: Coverage::Complete,
            stream: None,
            detail_scroll: 0,
            pending: Pending::default(),
            last_id: RequestId::default(),
            selection_dirty: false,
            focus: Focus::Tree,
            mode: Mode::Normal,
            search_input: String::new(),
            pattern: None,
            kind_filter: None,
            view: View::Keys,
            pane_scroll: 0,
            slowlog: PaneState::Idle,
            clients: PaneState::Idle,
            cluster: PaneState::Idle,
            pubsub: PaneState::Idle,
            detections: Vec::new(),
            level: QueueLevel::Queues,
            queues: PaneState::Idle,
            queue_selected: 0,
            // Failed is the state people open a queue dashboard to look at.
            job_state: State::Failed,
            jobs: PaneState::Idle,
            job_selected: 0,
            job: PaneState::Idle,
            throughput: Throughput::default(),
            server,
            info_error: None,
            // App is public within the library, so enforce the display boundary here too;
            // callers should not have to remember that Redis URLs can contain passwords.
            url: redact_url(&url),
            // The connection is already established by the time the TUI starts, so the
            // splash is covering the first scan, not the connect.
            status: "scanning keyspace…".into(),
            error: None,
            loading: true,
            scan_complete: false,
            tree_full: false,
            splash: true,
            quit: false,
            tx,
        }
    }

    pub fn selected_row(&self) -> Option<&Row> {
        self.rows.get(self.selected)
    }

    /// Hand out the next request id.
    fn issue(&mut self) -> RequestId {
        self.last_id = self.last_id.next();
        self.last_id
    }

    /// Record that the value pane is waiting on `key`, returning the id its reply must
    /// carry.
    ///
    /// Exposed so tests can stage a selection without driving the worker; the staleness
    /// rule in [`App::apply`] depends on it.
    pub fn expect_detail(&mut self, key: impl Into<String>) -> RequestId {
        let id = self.issue();
        self.pending.detail = Some((id, key.into()));
        id
    }

    /// The key the value pane is waiting on, if any.
    pub fn pending_key(&self) -> Option<&str> {
        self.pending.detail.as_ref().map(|(_, k)| k.as_str())
    }

    /// Stage the pending request for a queue-view reply, for tests. Returns the id the
    /// reply must carry.
    pub fn expect_queues(&mut self) -> RequestId {
        let id = self.issue();
        self.pending.queues = Some(id);
        id
    }

    /// See [`expect_queues`](Self::expect_queues).
    pub fn expect_jobs(&mut self) -> RequestId {
        let id = self.issue();
        self.pending.jobs = Some(id);
        id
    }

    /// See [`expect_queues`](Self::expect_queues).
    pub fn expect_job(&mut self) -> RequestId {
        let id = self.issue();
        self.pending.job = Some(id);
        id
    }

    /// The current scan generation, for tests that stage a batch.
    pub fn scan_generation(&self) -> RequestId {
        self.pending.scan
    }

    /// Rebuild the visible rows, keeping the cursor on the same path where possible.
    ///
    /// Index-based restoration would silently jump the user somewhere else whenever a
    /// batch inserts rows above the cursor -- which is most batches.
    fn rebuild(&mut self) {
        let anchor = self.selected_row().map(|r| r.path.clone());
        self.rows = self.tree.rows();
        self.selected = anchor
            .and_then(|p| self.rows.iter().position(|r| r.path == p))
            .unwrap_or(self.selected)
            .min(self.rows.len().saturating_sub(1));
    }

    pub fn apply(&mut self, update: Update) {
        match update {
            Update::Batch {
                scan,
                keys,
                reset,
                progress,
                scanned_pages,
            } => {
                // A batch from a superseded scan describes a filter that is no longer
                // applied. Dropping it also leaves `loading` alone: the scan the status
                // bar is waiting on has not answered yet.
                if scan != self.pending.scan {
                    return;
                }
                if reset {
                    self.tree_full = false;
                    self.tree.clear_keys();
                    self.selected = 0;
                    self.detail = None;
                    // The stream panel belongs to the detail it was read with. Clearing
                    // one without the other left a previous key's consumer groups on
                    // screen above an empty value pane.
                    self.stream = None;
                    self.pending.detail = None;
                }
                let room = MAX_TREE_KEYS.saturating_sub(self.tree.len());
                let over_budget = keys.len() > room;
                for (key, kind) in keys.into_iter().take(room) {
                    self.tree.insert_with_kind(&key, kind);
                }
                self.scan_complete = progress == ScanProgress::Complete;
                self.loading = false;
                // The first batch is the signal that there's something worth showing.
                self.splash = false;
                let n = keylens_ui::format::count(self.tree.len() as u64);
                self.status = match progress {
                    ScanProgress::Complete => format!("{n} keys"),
                    ScanProgress::Partial => format!(
                        "{n} keys so far ({scanned_pages} page{}) - `m` for more",
                        if scanned_pages == 1 { "" } else { "s" }
                    ),
                    // The status bar, not the value pane: the keys listed are real, the
                    // selected key's value is unaffected, and the honest summary is "this
                    // is not the whole keyspace".
                    ScanProgress::Failed(why) => {
                        format!("scan failed after {n} keys (incomplete) - r to rescan: {why}")
                    }
                };
                if over_budget || self.tree.len() >= MAX_TREE_KEYS {
                    self.tree_full = true;
                    self.scan_complete = false;
                    self.status = format!(
                        "{n} keys -- tree limit reached; the scan stopped here. / to filter"
                    );
                }
                self.rebuild();
            }

            Update::Detail {
                id,
                meta,
                value,
                coverage,
                stream,
            } => {
                // Drop replies for a key the user has already scrolled past -- and for an
                // older read of the *same* key, which the key name alone cannot tell
                // apart from the current one.
                if self.pending.detail.as_ref().map(|(want, _)| *want) == Some(id) {
                    self.detail_scroll = 0;
                    self.stream = stream;
                    self.detail = Some((*meta, *value));
                    self.coverage = coverage;
                    // A key that loaded is proof the last failure is over. The value pane
                    // renders `error` in place of the value, so leaving a stale one set
                    // meant one bad key hid every key selected after it until a rescan.
                    self.error = None;
                }
            }

            Update::DetailFailed { id, key, error } => {
                // A failure is a reply like any other. Applied unconditionally, an old
                // key's error replaced the value of whichever key was selected when it
                // happened to arrive.
                if self.pending.detail.as_ref().map(|(want, _)| *want) == Some(id) {
                    self.detail = None;
                    self.stream = None;
                    self.error = Some(format!("{key}: {error}"));
                }
            }

            Update::Info { id, info } => {
                if id > self.pending.info_applied {
                    self.pending.info_applied = id;
                    self.server = *info;
                    self.info_error = None;
                }
            }
            Update::InfoFailed { id, error } => {
                if id > self.pending.info_applied {
                    self.pending.info_applied = id;
                    self.info_error = Some(error);
                }
            }

            Update::Slowlog { id, data } => {
                if answers(self.pending.slowlog, id) {
                    self.slowlog = data;
                }
            }
            Update::Clients { id, data } => {
                if answers(self.pending.clients, id) {
                    self.clients = data;
                }
            }
            Update::Cluster { id, data } => {
                if answers(self.pending.cluster, id) {
                    self.cluster = data;
                }
            }
            Update::PubSub { id, data } => {
                if answers(self.pending.pubsub, id) {
                    self.pubsub = data;
                }
            }

            Update::Detected(d) => self.detections = d,
            Update::EventsStatus(s) => self.throughput.status = s,
            Update::Events(events) => {
                for e in events {
                    self.throughput.record(&e.queue, e.kind, e.at_ms);
                }
            }
            Update::Queues { id, data } => {
                if !answers(self.pending.queues, id) {
                    return;
                }
                if let Some(list) = data.ready() {
                    self.queue_selected = self.queue_selected.min(list.len().saturating_sub(1));
                }
                self.queues = data;
            }
            Update::Jobs { id, data } => {
                // Only the page for the newest (queue, state) asked for. Matching on the
                // state alone let one queue's `failed` page render under another's.
                if !answers(self.pending.jobs, id) {
                    return;
                }
                if let Some(list) = data.ready() {
                    self.job_selected = self.job_selected.min(list.len().saturating_sub(1));
                }
                self.jobs = data;
            }
            Update::Job { id, data } => {
                // The job detail had no identity at all: open job A, back out, open job B,
                // and A's reply rendered under B's id.
                if answers(self.pending.job, id) {
                    self.job = data;
                }
            }
        }
    }

    /// The tabs currently on offer.
    ///
    /// `queues` is present only when a lens matched, so a plain cache server never shows a
    /// queue tab it can't fill.
    pub fn views(&self) -> Vec<View> {
        View::ALL
            .into_iter()
            .filter(|v| *v != View::Queues || !self.detections.is_empty())
            .collect()
    }

    /// Digits are assigned by position, so they stay contiguous whether or not the queues
    /// tab exists.
    pub fn digit_for(&self, view: View) -> Option<char> {
        let idx = self.views().iter().position(|v| *v == view)?;
        char::from_digit(idx as u32 + 1, 10)
    }

    fn view_from_digit(&self, c: char) -> Option<View> {
        let idx = c.to_digit(10)?.checked_sub(1)? as usize;
        self.views().get(idx).copied()
    }

    /// The queue the cursor is on.
    pub fn selected_queue(&self) -> Option<&QueueSummary> {
        self.queues.ready()?.get(self.queue_selected)
    }

    pub fn selected_job(&self) -> Option<&JobRef> {
        self.jobs.ready()?.get(self.job_selected)
    }

    /// Switch tabs, loading the target pane the first time it's opened.
    ///
    /// Panes load lazily rather than at connect: on a managed host several of these are
    /// blocked, and firing them all up front means a burst of failing commands before the
    /// user has asked for anything.
    async fn goto(&mut self, view: View) {
        if self.view == view {
            return;
        }
        self.view = view;
        self.pane_scroll = 0;
        self.load_view(false).await;
    }

    /// Load the current view's data. `force` reloads even when it's already populated.
    async fn load_view(&mut self, force: bool) {
        match self.view {
            View::Keys => {
                if force {
                    self.rescan().await;
                }
            }
            View::Stats => self.refresh_info().await,
            View::Slowlog => {
                if force || self.slowlog.is_idle() {
                    let id = self.issue();
                    self.slowlog = if self.send(Request::LoadSlowlog { id }).await {
                        self.pending.slowlog = Some(id);
                        PaneState::Loading
                    } else {
                        worker_busy()
                    };
                }
            }
            View::Clients => {
                if force || self.clients.is_idle() {
                    let id = self.issue();
                    self.clients = if self.send(Request::LoadClients { id }).await {
                        self.pending.clients = Some(id);
                        PaneState::Loading
                    } else {
                        worker_busy()
                    };
                }
            }
            View::Cluster => {
                if force || self.cluster.is_idle() {
                    let id = self.issue();
                    self.cluster = if self.send(Request::LoadCluster { id }).await {
                        self.pending.cluster = Some(id);
                        PaneState::Loading
                    } else {
                        worker_busy()
                    };
                }
            }
            View::PubSub => {
                if force || self.pubsub.is_idle() {
                    let id = self.issue();
                    self.pubsub = if self.send(Request::LoadPubSub { id }).await {
                        self.pending.pubsub = Some(id);
                        PaneState::Loading
                    } else {
                        worker_busy()
                    };
                }
            }
            View::Queues => match self.level {
                QueueLevel::Queues => {
                    if force || self.queues.is_idle() {
                        let id = self.issue();
                        self.queues = if self.send(Request::LoadQueues { id }).await {
                            self.pending.queues = Some(id);
                            PaneState::Loading
                        } else {
                            worker_busy()
                        };
                    }
                }
                QueueLevel::Jobs => self.load_jobs().await,
                QueueLevel::Job => self.load_job().await,
            },
        }
    }

    async fn load_jobs(&mut self) {
        let Some(queue) = self.selected_queue().map(|q| q.name.clone()) else {
            return;
        };
        let state = self.job_state;
        let id = self.issue();
        self.jobs = if self.send(Request::LoadJobs { id, queue, state }).await {
            self.pending.jobs = Some(id);
            PaneState::Loading
        } else {
            worker_busy()
        };
    }

    async fn load_job(&mut self) {
        let Some(queue) = self.selected_queue().map(|q| q.name.clone()) else {
            return;
        };
        let Some(job) = self.selected_job().map(|j| j.id.clone()) else {
            return;
        };
        let id = self.issue();
        self.job = if self.send(Request::LoadJob { id, queue, job }).await {
            self.pending.job = Some(id);
            PaneState::Loading
        } else {
            worker_busy()
        };
    }

    async fn handle_queues_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => self.move_queue_cursor(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_queue_cursor(-1),
            KeyCode::PageDown => self.move_queue_cursor(10),
            KeyCode::PageUp => self.move_queue_cursor(-10),
            KeyCode::Home | KeyCode::Char('g') => self.move_queue_cursor(isize::MIN / 2),
            KeyCode::End | KeyCode::Char('G') => self.move_queue_cursor(isize::MAX / 2),

            // Cycle which state's jobs are listed. Bracket keys because h/l are already
            // in/out navigation and arrows would be ambiguous here.
            KeyCode::Char('[') | KeyCode::Char(']') => {
                if self.level != QueueLevel::Queues {
                    let delta: isize = if key.code == KeyCode::Char(']') {
                        1
                    } else {
                        -1
                    };
                    let i = State::ALL
                        .iter()
                        .position(|s| *s == self.job_state)
                        .unwrap_or(0);
                    let next = (i as isize + delta).rem_euclid(State::ALL.len() as isize) as usize;
                    self.job_state = State::ALL[next];
                    self.job_selected = 0;
                    self.level = QueueLevel::Jobs;
                    self.load_jobs().await;
                }
            }

            KeyCode::Enter | KeyCode::Char(' ') | KeyCode::Right | KeyCode::Char('l') => {
                match self.level {
                    QueueLevel::Queues => {
                        if self.selected_queue().is_some() {
                            self.level = QueueLevel::Jobs;
                            self.job_selected = 0;
                            self.load_jobs().await;
                        }
                    }
                    QueueLevel::Jobs => {
                        if self.selected_job().is_some() {
                            self.level = QueueLevel::Job;
                            self.pane_scroll = 0;
                            self.load_job().await;
                        }
                    }
                    QueueLevel::Job => {}
                }
            }

            KeyCode::Esc | KeyCode::Left | KeyCode::Char('h') => match self.level {
                QueueLevel::Job => {
                    self.level = QueueLevel::Jobs;
                    self.pane_scroll = 0;
                }
                QueueLevel::Jobs => {
                    self.level = QueueLevel::Queues;
                    self.pane_scroll = 0;
                }
                QueueLevel::Queues => {}
            },

            _ => {}
        }
    }

    fn move_queue_cursor(&mut self, delta: isize) {
        let (len, cursor) = match self.level {
            QueueLevel::Queues => (
                self.queues.ready().map(|q| q.len()).unwrap_or(0),
                &mut self.queue_selected,
            ),
            QueueLevel::Jobs => (
                self.jobs.ready().map(|j| j.len()).unwrap_or(0),
                &mut self.job_selected,
            ),
            // The job detail is scrolled, not stepped through.
            QueueLevel::Job => {
                self.pane_scroll = if delta > 0 {
                    self.pane_scroll.saturating_add(delta.min(20) as u16)
                } else {
                    self.pane_scroll.saturating_sub((-delta).min(20) as u16)
                };
                return;
            }
        };

        if len == 0 {
            return;
        }
        let last = (len - 1) as isize;
        *cursor = (*cursor as isize).saturating_add(delta).clamp(0, last) as usize;
    }

    /// Queue a request for the worker. Returns whether it was accepted.
    ///
    /// Deliberately non-blocking. `send().await` on a bounded channel parks the *UI task*
    /// until the worker drains one — so a saturated worker stopped keystrokes and redraws
    /// entirely, which looks exactly like a hang. Backpressure belongs on the request, not
    /// on the render loop.
    async fn send(&mut self, req: Request) -> bool {
        use tokio::sync::mpsc::error::TrySendError;
        match self.tx.try_send(req) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => false,
            Err(TrySendError::Closed(_)) => {
                self.error = Some("worker stopped".into());
                false
            }
        }
    }

    /// Ask for a stats refresh. Called on the refresh tick and when the stats tab opens.
    pub async fn refresh_info(&mut self) {
        let id = self.issue();
        // A full channel just skips this tick; the next one asks again.
        let _ = self.send(Request::RefreshInfo { id }).await;
    }

    /// Note that the cursor moved, without fetching yet.
    ///
    /// The fetch happens in [`flush_selection`](Self::flush_selection) once input goes
    /// quiet, so a held-down `j` costs one request instead of one per row.
    fn touch_selection(&mut self) {
        self.selection_dirty = true;
        // Retire the previous key's failure immediately, though -- not on the next fetch.
        // Otherwise the old error sits under the new selection and reads as though *that*
        // key failed too.
        self.error = None;
    }

    /// Whether a selection is waiting to be fetched.
    pub fn selection_pending(&self) -> bool {
        self.selection_dirty
    }

    /// Fetch the key under the cursor. Called by the event loop after a short idle gap,
    /// and directly on `enter`, where the user has said exactly which key they mean.
    pub async fn flush_selection(&mut self) {
        self.selection_dirty = false;

        let Some((path, is_key)) = self.selected_row().map(|r| (r.path.clone(), r.is_key)) else {
            return;
        };

        if !is_key {
            self.detail = None;
            self.stream = None;
            self.pending.detail = None;
            return;
        }

        // Already showing it, or already asked for it: a repeat keypress on the same row
        // must not re-fetch.
        if self.pending_key() == Some(path.as_str()) {
            return;
        }

        let id = self.expect_detail(path.clone());
        if !self.send(Request::Select { id, key: path }).await {
            // The worker is saturated. Leave it dirty so the next idle tick retries
            // rather than the pane sitting on `loading…` for a request that never went.
            self.pending.detail = None;
            self.selection_dirty = true;
        }
    }

    /// Start a new scan generation with the current filter.
    pub async fn rescan(&mut self) {
        let (pattern, kind) = (self.pattern.clone(), self.kind_filter);
        let scan = self.issue();
        if self
            .send(Request::Rescan {
                scan,
                pattern,
                kind,
            })
            .await
        {
            // Only once it is actually queued: a rejected rescan leaves the previous
            // generation's batches as the ones worth applying.
            self.pending.scan = scan;
            self.loading = true;
            self.status = "scanning keyspace…".into();
            self.scan_complete = false;
        } else {
            self.loading = false;
            self.status = "worker busy — press r to retry".into();
        }
    }

    pub async fn handle_key(&mut self, key: KeyEvent) {
        // Ctrl-C always quits, in every mode.
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.quit = true;
            return;
        }

        // Any keypress dismisses the splash, but still does whatever it normally does --
        // making the first keystroke a no-op would feel like a dropped input.
        self.splash = false;

        match self.mode {
            Mode::Help => {
                self.mode = Mode::Normal;
            }
            Mode::Search => self.handle_search_key(key).await,
            Mode::Normal => self.handle_normal_key(key).await,
        }
    }

    async fn handle_search_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.mode = Mode::Normal;
                self.search_input = self.pattern.clone().unwrap_or_default();
            }
            KeyCode::Enter => {
                self.mode = Mode::Normal;
                let raw = self.search_input.trim();
                // A bare term is far more useful as a substring match than an exact one,
                // so wrap it in globs unless the user wrote their own.
                self.pattern = if raw.is_empty() {
                    None
                } else if raw.contains(['*', '?', '[']) {
                    Some(raw.to_string())
                } else {
                    Some(format!("*{raw}*"))
                };
                self.rescan().await;
            }
            KeyCode::Backspace => {
                self.search_input.pop();
            }
            KeyCode::Char(c) => self.search_input.push(c),
            _ => {}
        }
    }

    async fn handle_normal_key(&mut self, key: KeyEvent) {
        // Keys that mean the same thing in every view.
        match key.code {
            KeyCode::Char('q') => {
                self.quit = true;
                return;
            }
            KeyCode::Char('?') => {
                self.mode = Mode::Help;
                return;
            }
            KeyCode::Char(c) if c.is_ascii_digit() => {
                if let Some(view) = self.view_from_digit(c) {
                    self.goto(view).await;
                }
                return;
            }
            KeyCode::Char('r') => {
                self.load_view(true).await;
                return;
            }
            _ => {}
        }

        match self.view {
            View::Keys => self.handle_keys_view_key(key).await,
            View::Queues => self.handle_queues_key(key).await,
            _ => self.handle_pane_key(key),
        }
    }

    /// Scrolling for the server panes, which are plain scrollable text.
    fn handle_pane_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => {
                self.pane_scroll = self.pane_scroll.saturating_add(1)
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.pane_scroll = self.pane_scroll.saturating_sub(1)
            }
            KeyCode::PageDown => self.pane_scroll = self.pane_scroll.saturating_add(20),
            KeyCode::PageUp => self.pane_scroll = self.pane_scroll.saturating_sub(20),
            KeyCode::Home | KeyCode::Char('g') => self.pane_scroll = 0,
            _ => {}
        }
    }

    async fn handle_keys_view_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Tab => {
                self.focus = match self.focus {
                    Focus::Tree => Focus::Value,
                    Focus::Value => Focus::Tree,
                };
            }

            KeyCode::Char('/') => {
                self.mode = Mode::Search;
                self.search_input = self.pattern.clone().unwrap_or_default();
            }

            KeyCode::Esc => {
                if self.pattern.is_some() {
                    self.pattern = None;
                    self.search_input.clear();
                    self.rescan().await;
                }
            }

            KeyCode::Char('t') => {
                let i = KIND_CYCLE
                    .iter()
                    .position(|k| *k == self.kind_filter)
                    .unwrap_or(0);
                self.kind_filter = KIND_CYCLE[(i + 1) % KIND_CYCLE.len()];
                self.rescan().await;
            }

            KeyCode::Char('m') => {
                if self.tree_full {
                    self.status = format!(
                        "tree limit of {} keys reached -- / to filter",
                        keylens_ui::format::count(MAX_TREE_KEYS as u64)
                    );
                } else if !self.scan_complete {
                    let scan = self.pending.scan;
                    if self.send(Request::More { scan }).await {
                        self.loading = true;
                    } else {
                        self.loading = false;
                        self.status = "worker busy — press m to retry".into();
                    }
                }
            }

            KeyCode::Char('E') => {
                // Guarded: expanding a 200k-key tree produces a row list nothing can
                // usefully scroll.
                if self.tree.len() <= 5_000 {
                    self.tree.expand_all();
                    self.rebuild();
                } else {
                    self.status = "too many keys to expand all - filter first".into();
                }
            }
            KeyCode::Char('C') => {
                self.tree.collapse_all();
                self.rebuild();
            }

            _ if self.focus == Focus::Value => self.handle_value_key(key),

            KeyCode::Down | KeyCode::Char('j') => self.move_cursor(1).await,
            KeyCode::Up | KeyCode::Char('k') => self.move_cursor(-1).await,
            KeyCode::PageDown => self.move_cursor(10).await,
            KeyCode::PageUp => self.move_cursor(-10).await,
            KeyCode::Home | KeyCode::Char('g') => {
                self.selected = 0;
                self.touch_selection();
                self.flush_selection().await;
            }
            KeyCode::End | KeyCode::Char('G') => {
                self.selected = self.rows.len().saturating_sub(1);
                self.touch_selection();
                self.flush_selection().await;
            }

            KeyCode::Enter | KeyCode::Char(' ') | KeyCode::Right | KeyCode::Char('l') => {
                if let Some(row) = self.selected_row() {
                    let (path, is_branch) = (row.path.clone(), row.is_branch);
                    if is_branch {
                        self.tree.toggle(&path);
                        self.rebuild();
                    } else {
                        self.touch_selection();
                        self.flush_selection().await;
                    }
                }
            }

            KeyCode::Left | KeyCode::Char('h') => self.collapse_or_parent(),

            _ => {}
        }
    }

    fn handle_value_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => {
                self.detail_scroll = self.detail_scroll.saturating_add(1)
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.detail_scroll = self.detail_scroll.saturating_sub(1)
            }
            KeyCode::PageDown => self.detail_scroll = self.detail_scroll.saturating_add(20),
            KeyCode::PageUp => self.detail_scroll = self.detail_scroll.saturating_sub(20),
            KeyCode::Home => self.detail_scroll = 0,
            _ => {}
        }
    }

    /// Collapse an open branch, otherwise jump to the parent -- the behaviour every file
    /// browser has, so it needs no explanation.
    fn collapse_or_parent(&mut self) {
        let Some(row) = self.selected_row() else {
            return;
        };
        let (path, is_branch, expanded) = (row.path.clone(), row.is_branch, row.expanded);

        if is_branch && expanded {
            self.tree.toggle(&path);
            self.rebuild();
            return;
        }

        if let Some((parent, _)) = path.rsplit_once(':')
            && let Some(i) = self.rows.iter().position(|r| r.path == parent)
        {
            self.selected = i;
        }
    }

    async fn move_cursor(&mut self, delta: isize) {
        if self.rows.is_empty() {
            return;
        }
        let last = self.rows.len() - 1;
        let next = (self.selected as isize + delta).clamp(0, last as isize) as usize;
        if next != self.selected {
            self.selected = next;
            self.touch_selection();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keylens_conn::ServerInfo;
    use tokio::sync::mpsc;

    fn app_with_rx() -> (App, mpsc::Receiver<Request>) {
        let (tx, rx) = mpsc::channel(8);
        (
            App::new(
                ServerInfo::parse("redis_version:8.0.0\r\n"),
                "redis://x".into(),
                tx,
            ),
            rx,
        )
    }

    fn app() -> App {
        app_with_rx().0
    }

    fn batch(keys: &[&str]) -> Update {
        Update::Batch {
            scan: RequestId::default(),
            keys: keys.iter().map(|k| (k.to_string(), None)).collect(),
            reset: false,
            progress: ScanProgress::Complete,
            scanned_pages: 1,
        }
    }

    #[test]
    fn batch_populates_rows() {
        let mut a = app();
        a.apply(batch(&["bull:emails:1", "cache:x"]));
        assert_eq!(a.rows.len(), 2);
        assert!(!a.loading);
        assert!(a.scan_complete);
    }

    #[test]
    fn app_never_stores_a_displayable_password() {
        let (tx, _rx) = mpsc::channel(1);
        let a = App::new(
            ServerInfo::unknown(),
            "rediss://admin:hunter2@redis.example:6379".into(),
            tx,
        );
        assert_eq!(a.url, "rediss://admin:***@redis.example:6379");
        assert!(!a.url.contains("hunter2"));
    }

    #[tokio::test]
    async fn a_rejected_request_never_leaves_loading_state_behind() {
        // app() drops the receiver, which exercises the same immediate rejection as a
        // saturated channel without needing a worker task.
        let mut a = app();
        a.rescan().await;
        assert!(!a.loading);
        assert!(a.status.contains("worker busy"));

        a.goto(View::Slowlog).await;
        assert!(matches!(a.slowlog, PaneState::Failed(ref why) if why.contains("worker busy")));
    }

    #[test]
    fn selection_sticks_to_its_path_across_batches() {
        let mut a = app();
        a.apply(batch(&["m:1"]));
        a.selected = 0;
        let before = a.selected_row().unwrap().path.clone();

        // A key sorting *above* the selection arrives; index-based restore would drift.
        a.apply(batch(&["a:1"]));
        assert_eq!(a.selected_row().unwrap().path, before);
    }

    fn meta(key: &str) -> Box<KeyMeta> {
        Box::new(KeyMeta {
            key: key.into(),
            kind: Kind::String,
            ttl_ms: None,
            size: Some(1),
            memory: None,
        })
    }

    fn detail(id: RequestId, key: &str, text: &str) -> Update {
        Update::Detail {
            id,
            meta: meta(key),
            value: Box::new(KeyValue::String(text.into())),
            coverage: Coverage::Complete,
            stream: None,
        }
    }

    #[test]
    fn stale_detail_replies_are_dropped() {
        let mut a = app();
        a.apply(batch(&["k1", "k2"]));
        let k1 = a.expect_detail("k1");
        let k2 = a.expect_detail("k2");

        a.apply(detail(k1, "k1", "stale"));
        assert!(
            a.detail.is_none(),
            "reply for k1 must not render while k2 is selected"
        );

        a.apply(detail(k2, "k2", "fresh"));
        assert!(a.detail.is_some());
    }

    #[test]
    fn an_older_read_of_the_same_key_is_dropped() {
        // Select k, move away, come back: two reads of `k` are in flight. Matching on the
        // key name accepted both, so whichever arrived last won -- including the older
        // one, which can carry a value the key no longer has.
        let mut a = app();
        let first = a.expect_detail("k");
        let _away = a.expect_detail("other");
        let second = a.expect_detail("k");

        a.apply(detail(second, "k", "new"));
        a.apply(detail(first, "k", "old"));
        assert!(
            matches!(&a.detail, Some((_, KeyValue::String(v))) if v == "new"),
            "the older reply overwrote the newer one: {:?}",
            a.detail
        );
    }

    #[test]
    fn a_stale_key_failure_does_not_replace_the_selected_value() {
        let mut a = app();
        let old = a.expect_detail("old");
        let cur = a.expect_detail("cur");
        a.apply(detail(cur, "cur", "value"));

        a.apply(Update::DetailFailed {
            id: old,
            key: "old".into(),
            error: "WRONGTYPE".into(),
        });
        assert!(a.error.is_none(), "old key's error painted over `cur`");
        assert!(a.detail.is_some());

        // The current key's own failure is still shown, and says which key it was.
        a.apply(Update::DetailFailed {
            id: cur,
            key: "cur".into(),
            error: "timeout".into(),
        });
        assert_eq!(a.error.as_deref(), Some("cur: timeout"));
    }

    #[test]
    fn a_stats_refresh_failure_never_reaches_the_value_pane() {
        let mut a = app();
        let id = a.expect_detail("k");
        a.apply(detail(id, "k", "value"));

        a.apply(Update::InfoFailed {
            id: RequestId::new(1_000),
            error: "INFO timed out".into(),
        });
        assert!(a.error.is_none(), "an INFO failure hid the selected key");
        assert_eq!(a.info_error.as_deref(), Some("INFO timed out"));

        // A later successful refresh clears it; an older one does not resurrect it.
        a.apply(Update::Info {
            id: RequestId::new(1_001),
            info: Box::new(ServerInfo::parse("redis_version:8.0.1\r\n")),
        });
        assert!(a.info_error.is_none());
        a.apply(Update::InfoFailed {
            id: RequestId::new(999),
            error: "late".into(),
        });
        assert!(
            a.info_error.is_none(),
            "an older refresh's failure was applied"
        );
        assert_eq!(a.server.version, "8.0.1");
    }

    #[tokio::test]
    async fn batches_from_a_superseded_scan_are_dropped() {
        let (mut a, mut rx) = app_with_rx();
        a.rescan().await;
        let old = a.scan_generation();
        a.apply(Update::Batch {
            scan: old,
            keys: vec![("old:1".into(), None)],
            reset: true,
            progress: ScanProgress::Partial,
            scanned_pages: 1,
        });

        // Change the filter. The `More` answered for the old filter arrives after.
        a.pattern = Some("new:*".into());
        a.rescan().await;
        assert_ne!(a.scan_generation(), old);
        a.apply(Update::Batch {
            scan: old,
            keys: vec![("old:2".into(), None)],
            reset: false,
            progress: ScanProgress::Complete,
            scanned_pages: 1,
        });
        assert!(
            a.loading,
            "a stale batch must not end the new scan's loading state"
        );
        assert!(
            !a.scan_complete,
            "a stale batch claimed the new scan was complete"
        );
        assert!(a.rows.iter().all(|r| r.path != "old:2"));

        a.apply(Update::Batch {
            scan: old,
            keys: vec![],
            reset: false,
            progress: ScanProgress::Failed("stale".into()),
            scanned_pages: 0,
        });
        assert!(
            !a.status.contains("stale"),
            "an old scan's failure was shown"
        );

        // Both rescans went out with the generation the app recorded.
        let gens: Vec<RequestId> = std::iter::from_fn(|| rx.try_recv().ok())
            .filter_map(|r| match r {
                Request::Rescan { scan, .. } => Some(scan),
                _ => None,
            })
            .collect();
        assert_eq!(gens, vec![old, a.scan_generation()]);
    }

    #[test]
    fn a_reset_batch_clears_the_previous_stream_panel() {
        let mut a = app();
        let id = a.expect_detail("s");
        a.apply(Update::Detail {
            id,
            meta: meta("s"),
            value: Box::new(KeyValue::Stream(vec![])),
            coverage: Coverage::Complete,
            stream: Some(Box::default()),
        });
        assert!(a.stream.is_some());
        a.apply(Update::Batch {
            scan: RequestId::default(),
            keys: vec![],
            reset: true,
            progress: ScanProgress::Complete,
            scanned_pages: 1,
        });
        assert!(a.stream.is_none(), "consumer groups outlived their key");
        assert!(a.pending_key().is_none());
    }

    #[test]
    fn reset_batch_clears_previous_keys() {
        let mut a = app();
        a.apply(batch(&["old:1"]));
        a.apply(Update::Batch {
            scan: RequestId::default(),
            keys: vec![("new:1".into(), None)],
            reset: true,
            progress: ScanProgress::Complete,
            scanned_pages: 1,
        });
        assert_eq!(a.rows.len(), 1);
        // A lone key folds to a single row for its whole path.
        assert_eq!(a.rows[0].path, "new:1");
    }

    #[test]
    fn a_failed_scan_keeps_its_keys_and_says_it_is_incomplete() {
        // 0.1.7 replaced the batch with a bare error -- dropping every key gathered before
        // the failure -- and the next `m` then reported the keyspace complete.
        let mut a = app();
        a.apply(Update::Batch {
            scan: RequestId::default(),
            keys: vec![("k:1".into(), None), ("k:2".into(), None)],
            reset: true,
            progress: ScanProgress::Failed("Request timed out.".into()),
            scanned_pages: 3,
        });
        assert!(!a.loading);
        assert!(!a.scan_complete, "a failed scan claimed to be complete");
        assert_eq!(
            a.tree.len(),
            2,
            "keys gathered before the failure were dropped"
        );
        assert!(a.status.contains("incomplete"), "{}", a.status);
        assert!(a.status.contains("Request timed out."), "{}", a.status);
        assert!(
            a.error.is_none(),
            "a scan failure must not blank the value pane"
        );
    }

    #[tokio::test]
    async fn the_tree_stops_growing_at_its_budget_and_says_so() {
        let (mut a, mut rx) = app_with_rx();
        let over = MAX_TREE_KEYS + 10;
        a.apply(Update::Batch {
            scan: RequestId::default(),
            keys: (0..over).map(|i| (format!("k:{i}"), None)).collect(),
            reset: true,
            progress: ScanProgress::Partial,
            scanned_pages: 1,
        });
        assert_eq!(a.tree.len(), MAX_TREE_KEYS);
        assert!(a.tree_full);
        assert!(!a.scan_complete, "a capped tree is not the whole keyspace");
        assert!(a.status.contains("limit"), "{}", a.status);

        a.handle_normal_key(KeyEvent::from(KeyCode::Char('m')))
            .await;
        assert!(
            rx.try_recv().is_err(),
            "`m` must not ask for more past the budget"
        );
    }

    #[tokio::test]
    async fn bare_search_terms_become_substring_globs() {
        let mut a = app();
        a.mode = Mode::Search;
        a.search_input = "emails".into();
        a.handle_search_key(KeyEvent::from(KeyCode::Enter)).await;
        assert_eq!(a.pattern.as_deref(), Some("*emails*"));

        // An explicit glob is respected as written.
        a.mode = Mode::Search;
        a.search_input = "bull:*:meta".into();
        a.handle_search_key(KeyEvent::from(KeyCode::Enter)).await;
        assert_eq!(a.pattern.as_deref(), Some("bull:*:meta"));
    }

    #[tokio::test]
    async fn empty_search_clears_the_filter() {
        let mut a = app();
        a.pattern = Some("*x*".into());
        a.mode = Mode::Search;
        a.search_input = "  ".into();
        a.handle_search_key(KeyEvent::from(KeyCode::Enter)).await;
        assert_eq!(a.pattern, None);
    }

    #[tokio::test]
    async fn type_filter_cycles_and_wraps() {
        let mut a = app();
        for expected in [Some(Kind::String), Some(Kind::Hash), Some(Kind::List)] {
            a.handle_normal_key(KeyEvent::from(KeyCode::Char('t')))
                .await;
            assert_eq!(a.kind_filter, expected);
        }
        for _ in 0..4 {
            a.handle_normal_key(KeyEvent::from(KeyCode::Char('t')))
                .await;
        }
        assert_eq!(a.kind_filter, None, "cycle wraps back to unfiltered");
    }

    #[tokio::test]
    async fn digits_switch_views() {
        let mut a = app();
        assert_eq!(a.view, View::Keys);

        a.handle_normal_key(KeyEvent::from(KeyCode::Char('3')))
            .await;
        assert_eq!(a.view, View::Slowlog);
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('1')))
            .await;
        assert_eq!(a.view, View::Keys);

        // Out of range digits are ignored rather than panicking on an index.
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('9')))
            .await;
        assert_eq!(a.view, View::Keys);
    }

    #[tokio::test]
    async fn opening_a_pane_loads_it_once() {
        let (mut a, mut rx) = app_with_rx();
        assert!(a.slowlog.is_idle(), "panes must not load until opened");

        a.handle_normal_key(KeyEvent::from(KeyCode::Char('3')))
            .await;
        assert!(matches!(a.slowlog, PaneState::Loading));
        assert!(matches!(rx.try_recv(), Ok(Request::LoadSlowlog { .. })));

        // Coming back to an already-loaded pane must not refire the request.
        a.slowlog = PaneState::Ready(vec![]);
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('1')))
            .await;
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('3')))
            .await;
        assert!(matches!(a.slowlog, PaneState::Ready(_)));
        assert!(rx.try_recv().is_err(), "a ready pane must not refire");
    }

    #[tokio::test]
    async fn r_reloads_the_pane_that_is_showing() {
        let (mut a, mut rx) = app_with_rx();
        a.view = View::Clients;
        a.clients = PaneState::Ready(Capped::default());

        a.handle_normal_key(KeyEvent::from(KeyCode::Char('r')))
            .await;
        assert!(
            matches!(a.clients, PaneState::Loading),
            "r should force a reload"
        );
        assert!(matches!(rx.try_recv(), Ok(Request::LoadClients { .. })));
    }

    #[tokio::test]
    async fn tree_keys_do_not_leak_into_server_panes() {
        // `t` cycles the type filter in the keys view. In the slowlog view it must do
        // nothing rather than silently re-scanning the keyspace.
        let mut a = app();
        a.view = View::Slowlog;
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('t')))
            .await;
        assert_eq!(a.kind_filter, None);
    }

    #[tokio::test]
    async fn switching_views_resets_pane_scroll() {
        let mut a = app();
        a.view = View::Clients;
        a.pane_scroll = 42;
        a.goto(View::Cluster).await;
        assert_eq!(a.pane_scroll, 0);
    }

    fn detection() -> Detection {
        Detection {
            lens_id: "bullmq",
            confidence: keylens_lens::Confidence::Certain,
            version: Some("6.0.2".into()),
            prefix: "bull".into(),
            summary: "bullmq 6.0.2 - 2 queues".into(),
            targets: vec!["emails".into(), "reports".into()],
        }
    }

    fn apply_queues(a: &mut App, list: Vec<QueueSummary>) {
        let id = a.expect_queues();
        a.apply(Update::Queues {
            id,
            data: PaneState::Ready(list.into()),
        });
    }

    fn summary(name: &str, paused: bool) -> QueueSummary {
        QueueSummary {
            name: name.into(),
            paused: Some(paused),
            counts: State::ALL.iter().map(|s| (*s, Some(3))).collect(),
        }
    }

    #[test]
    fn queues_tab_appears_only_once_a_lens_matches() {
        let mut a = app();
        assert!(!a.views().contains(&View::Queues), "no lens, no queue tab");
        assert_eq!(a.digit_for(View::Stats), Some('2'));

        a.apply(Update::Detected(vec![detection()]));
        assert!(a.views().contains(&View::Queues));
        // Digits stay contiguous: stats shifts along rather than leaving a gap.
        assert_eq!(a.digit_for(View::Queues), Some('2'));
        assert_eq!(a.digit_for(View::Stats), Some('3'));
    }

    #[tokio::test]
    async fn digits_follow_the_visible_tabs_not_the_enum() {
        let mut a = app();
        // Without a lens, `2` is stats.
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('2')))
            .await;
        assert_eq!(a.view, View::Stats);

        a.apply(Update::Detected(vec![detection()]));
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('2')))
            .await;
        assert_eq!(a.view, View::Queues, "the same digit now means queues");
    }

    #[tokio::test]
    async fn drilling_in_and_back_out_walks_the_levels() {
        let mut a = app();
        a.apply(Update::Detected(vec![detection()]));
        a.view = View::Queues;
        apply_queues(&mut a, vec![summary("emails", false)]);
        assert_eq!(a.level, QueueLevel::Queues);

        a.handle_queues_key(KeyEvent::from(KeyCode::Enter)).await;
        assert_eq!(a.level, QueueLevel::Jobs);

        let id = a.expect_jobs();
        a.apply(Update::Jobs {
            id,
            data: PaneState::Ready(vec![JobRef {
                id: "42".into(),
                score: Some(1.0),
            }]),
        });
        a.handle_queues_key(KeyEvent::from(KeyCode::Enter)).await;
        assert_eq!(a.level, QueueLevel::Job);

        a.handle_queues_key(KeyEvent::from(KeyCode::Esc)).await;
        assert_eq!(a.level, QueueLevel::Jobs);
        a.handle_queues_key(KeyEvent::from(KeyCode::Esc)).await;
        assert_eq!(a.level, QueueLevel::Queues);
        // Already at the top: further backing out is a no-op, not an underflow.
        a.handle_queues_key(KeyEvent::from(KeyCode::Esc)).await;
        assert_eq!(a.level, QueueLevel::Queues);
    }

    #[tokio::test]
    async fn cannot_drill_into_an_empty_queue_list() {
        let mut a = app();
        a.view = View::Queues;
        apply_queues(&mut a, vec![]);
        a.handle_queues_key(KeyEvent::from(KeyCode::Enter)).await;
        assert_eq!(
            a.level,
            QueueLevel::Queues,
            "nothing selected, nothing to open"
        );
    }

    #[tokio::test]
    async fn bracket_keys_cycle_job_state_and_wrap() {
        let mut a = app();
        a.view = View::Queues;
        apply_queues(&mut a, vec![summary("emails", false)]);
        a.level = QueueLevel::Jobs;
        assert_eq!(a.job_state, State::Failed);

        // Failed is last in the ordering, so forward wraps to the first.
        a.handle_queues_key(KeyEvent::from(KeyCode::Char(']')))
            .await;
        assert_eq!(a.job_state, State::Waiting);
        a.handle_queues_key(KeyEvent::from(KeyCode::Char('[')))
            .await;
        assert_eq!(a.job_state, State::Failed);
    }

    #[tokio::test]
    async fn state_cycling_does_nothing_at_the_queue_list_level() {
        let mut a = app();
        a.view = View::Queues;
        apply_queues(&mut a, vec![summary("emails", false)]);
        a.handle_queues_key(KeyEvent::from(KeyCode::Char(']')))
            .await;
        assert_eq!(a.job_state, State::Failed, "no job list open to re-filter");
        assert_eq!(a.level, QueueLevel::Queues);
    }

    fn jobs_page(id: RequestId, ids: &[&str]) -> Update {
        Update::Jobs {
            id,
            data: PaneState::Ready(
                ids.iter()
                    .map(|j| JobRef {
                        id: (*j).into(),
                        score: None,
                    })
                    .collect(),
            ),
        }
    }

    #[test]
    fn job_pages_for_a_stale_state_are_dropped() {
        // Cycling states faster than the server answers must not paint the old page under
        // the new heading.
        let mut a = app();
        let failed = a.expect_jobs();
        a.job_state = State::Waiting;
        let waiting = a.expect_jobs();
        a.apply(jobs_page(failed, &["stale"]));
        assert!(
            a.jobs.ready().is_none(),
            "reply for `failed` must not render under `waiting`"
        );
        a.apply(jobs_page(waiting, &["fresh"]));
        assert_eq!(a.selected_job().map(|j| j.id.as_str()), Some("fresh"));
    }

    #[tokio::test]
    async fn a_job_page_for_another_queue_in_the_same_state_is_dropped() {
        // The page used to be matched on its state alone. Open `emails` -> failed, back
        // out, open `reports` -> failed: `emails`' page arrived late and rendered under
        // `reports`, with nothing on screen to say so.
        let (mut a, mut rx) = app_with_rx();
        a.view = View::Queues;
        apply_queues(
            &mut a,
            vec![summary("emails", false), summary("reports", false)],
        );
        while rx.try_recv().is_ok() {}

        a.handle_queues_key(KeyEvent::from(KeyCode::Enter)).await;
        let Ok(Request::LoadJobs {
            id: emails, queue, ..
        }) = rx.try_recv()
        else {
            panic!("entering a queue must request its jobs")
        };
        assert_eq!(queue, "emails");
        a.handle_queues_key(KeyEvent::from(KeyCode::Esc)).await;
        a.handle_queues_key(KeyEvent::from(KeyCode::Down)).await;
        a.handle_queues_key(KeyEvent::from(KeyCode::Enter)).await;
        let Ok(Request::LoadJobs {
            id: reports,
            queue,
            state,
        }) = rx.try_recv()
        else {
            panic!("entering a queue must request its jobs")
        };
        assert_eq!((queue.as_str(), state), ("reports", State::Failed));

        a.apply(jobs_page(emails, &["emails-job"]));
        assert!(
            a.jobs.ready().is_none(),
            "emails' failed page rendered under reports"
        );
        a.apply(jobs_page(reports, &["reports-job"]));
        assert_eq!(a.selected_job().map(|j| j.id.as_str()), Some("reports-job"));
    }

    fn job_detail(id: RequestId, job: &str) -> Update {
        Update::Job {
            id,
            data: PaneState::Ready(Some(Box::new(JobDetail {
                job: keylens_bullmq::Job {
                    id: job.into(),
                    ..Default::default()
                },
                logs: vec![],
            }))),
        }
    }

    #[test]
    fn a_job_detail_for_a_job_no_longer_open_is_dropped() {
        // Job detail replies had no identity at all: whichever arrived last rendered,
        // under whichever job the user had open by then.
        let mut a = app();
        let first = a.expect_job();
        let second = a.expect_job();
        a.apply(job_detail(second, "B"));
        a.apply(job_detail(first, "A"));
        let shown = a
            .job
            .ready()
            .and_then(|j| j.as_ref())
            .map(|d| d.job.id.clone());
        assert_eq!(
            shown.as_deref(),
            Some("B"),
            "job A's detail rendered under job B"
        );
    }

    #[test]
    fn a_superseded_pane_reload_does_not_end_the_newer_load() {
        let mut a = app();
        let first = a.issue();
        let second = a.issue();
        a.pending.slowlog = Some(second);
        a.slowlog = PaneState::Loading;
        a.apply(Update::Slowlog {
            id: first,
            data: PaneState::Ready(vec![]),
        });
        assert!(matches!(a.slowlog, PaneState::Loading));
        a.apply(Update::Slowlog {
            id: second,
            data: PaneState::Ready(vec![]),
        });
        assert!(matches!(a.slowlog, PaneState::Ready(_)));
    }

    #[test]
    fn a_shrinking_queue_list_clamps_the_cursor() {
        let mut a = app();
        apply_queues(
            &mut a,
            vec![
                summary("a", false),
                summary("b", false),
                summary("c", false),
            ],
        );
        a.queue_selected = 2;
        apply_queues(&mut a, vec![summary("a", false)]);
        assert_eq!(a.queue_selected, 0, "cursor must not point past the end");
    }

    #[tokio::test]
    async fn ctrl_c_quits_from_help_mode() {
        let mut a = app();
        a.mode = Mode::Help;
        a.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL))
            .await;
        assert!(a.quit);
    }

    #[tokio::test]
    async fn enter_expands_a_branch() {
        let mut a = app();
        a.apply(batch(&["bull:emails:1", "bull:emails:2"]));
        // The single-child chain folds, so the one row is `bull:emails`.
        assert_eq!(a.rows.len(), 1);
        assert_eq!(a.rows[0].path, "bull:emails");

        a.handle_normal_key(KeyEvent::from(KeyCode::Enter)).await;
        assert_eq!(a.rows.len(), 3, "expanding reveals both children");
    }

    #[tokio::test]
    async fn enter_on_a_leaf_requests_its_value_instead_of_expanding() {
        let mut a = app();
        a.apply(batch(&["solo:key"]));
        assert!(!a.rows[0].is_branch);

        a.handle_normal_key(KeyEvent::from(KeyCode::Enter)).await;
        assert_eq!(a.rows.len(), 1, "a leaf has nothing to expand");
    }
}
