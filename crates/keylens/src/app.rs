//! Application state and input handling.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use keylens_conn::{
    ClientInfo, ClusterTopology, KeyMeta, KeyValue, Kind, PubSubChannel, ServerInfo, SlowEntry,
};
use keylens_ui::KeyTree;
use keylens_ui::tree::Row;
use keylens_ui::PaneState;
use ratatui::widgets::ListState;
use tokio::sync::mpsc::Sender;

use crate::worker::{Request, Update};

/// The top-level tabs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    Keys,
    Stats,
    Slowlog,
    Clients,
    Cluster,
    PubSub,
}

impl View {
    pub const ALL: [View; 6] =
        [View::Keys, View::Stats, View::Slowlog, View::Clients, View::Cluster, View::PubSub];

    pub fn label(&self) -> &'static str {
        match self {
            View::Keys => "keys",
            View::Stats => "stats",
            View::Slowlog => "slowlog",
            View::Clients => "clients",
            View::Cluster => "cluster",
            View::PubSub => "pubsub",
        }
    }

    /// Digit that selects this view.
    pub fn digit(&self) -> char {
        match self {
            View::Keys => '1',
            View::Stats => '2',
            View::Slowlog => '3',
            View::Clients => '4',
            View::Cluster => '5',
            View::PubSub => '6',
        }
    }

    pub fn from_digit(c: char) -> Option<View> {
        View::ALL.into_iter().find(|v| v.digit() == c)
    }
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

pub struct App {
    pub tree: KeyTree,
    pub rows: Vec<Row>,
    pub selected: usize,
    /// Owned by the app rather than rebuilt per frame, so the viewport keeps its scroll
    /// offset instead of snapping the selection to an edge on every redraw.
    pub list_state: ListState,

    pub detail: Option<(KeyMeta, KeyValue)>,
    pub detail_scroll: u16,
    /// Key whose detail we asked for most recently. Replies for anything else are stale
    /// -- they arrive when the user scrolls faster than the server answers.
    pending_key: Option<String>,

    pub focus: Focus,
    pub mode: Mode,
    pub search_input: String,
    pub pattern: Option<String>,
    pub kind_filter: Option<Kind>,

    pub view: View,
    /// Scroll offset for whichever server pane is showing.
    pub pane_scroll: u16,
    pub slowlog: PaneState<Vec<SlowEntry>>,
    pub clients: PaneState<Vec<ClientInfo>>,
    pub cluster: PaneState<Box<ClusterTopology>>,
    pub pubsub: PaneState<Vec<PubSubChannel>>,

    pub server: ServerInfo,
    pub url: String,
    pub status: String,
    pub error: Option<String>,
    pub loading: bool,
    pub scan_complete: bool,
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
            detail_scroll: 0,
            pending_key: None,
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
            server,
            url,
            // The connection is already established by the time the TUI starts, so the
            // splash is covering the first scan, not the connect.
            status: "scanning keyspace…".into(),
            error: None,
            loading: true,
            scan_complete: false,
            splash: true,
            quit: false,
            tx,
        }
    }

    pub fn selected_row(&self) -> Option<&Row> {
        self.rows.get(self.selected)
    }

    /// The key whose detail reply the app will currently accept.
    ///
    /// Exposed so tests can stage a selection without driving the worker; the staleness
    /// rule in [`App::apply`] depends on it.
    pub fn set_pending_key(&mut self, key: Option<String>) {
        self.pending_key = key;
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
            Update::Batch { keys, reset, complete, scanned_pages } => {
                if reset {
                    self.tree.clear_keys();
                    self.selected = 0;
                    self.detail = None;
                }
                for (key, kind) in keys {
                    self.tree.insert_with_kind(&key, kind);
                }
                self.scan_complete = complete;
                self.loading = false;
                self.error = None;
                // The first batch is the signal that there's something worth showing.
                self.splash = false;
                self.status = if complete {
                    format!("{} keys", keylens_ui::format::count(self.tree.len() as u64))
                } else {
                    format!(
                        "{} keys so far ({scanned_pages} page{}) - `m` for more",
                        keylens_ui::format::count(self.tree.len() as u64),
                        if scanned_pages == 1 { "" } else { "s" }
                    )
                };
                self.rebuild();
            }

            Update::Detail { meta, value } => {
                // Drop replies for a key the user has already scrolled past.
                if self.pending_key.as_deref() == Some(meta.key.as_str()) {
                    self.detail_scroll = 0;
                    self.detail = Some((*meta, *value));
                }
            }

            Update::Info(info) => self.server = *info,

            Update::Error(e) => {
                self.loading = false;
                self.error = Some(e);
            }

            Update::Slowlog(s) => self.slowlog = s,
            Update::Clients(s) => self.clients = s,
            Update::Cluster(s) => self.cluster = s,
            Update::PubSub(s) => self.pubsub = s,
        }
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
            View::Stats => {
                self.send(Request::RefreshInfo).await;
            }
            View::Slowlog => {
                if force || self.slowlog.is_idle() {
                    self.slowlog = PaneState::Loading;
                    self.send(Request::LoadSlowlog).await;
                }
            }
            View::Clients => {
                if force || self.clients.is_idle() {
                    self.clients = PaneState::Loading;
                    self.send(Request::LoadClients).await;
                }
            }
            View::Cluster => {
                if force || self.cluster.is_idle() {
                    self.cluster = PaneState::Loading;
                    self.send(Request::LoadCluster).await;
                }
            }
            View::PubSub => {
                if force || self.pubsub.is_idle() {
                    self.pubsub = PaneState::Loading;
                    self.send(Request::LoadPubSub).await;
                }
            }
        }
    }

    async fn send(&mut self, req: Request) {
        if self.tx.send(req).await.is_err() {
            self.error = Some("worker stopped".into());
        }
    }

    async fn load_selected(&mut self) {
        let Some(row) = self.selected_row() else { return };
        if !row.is_key {
            self.detail = None;
            self.pending_key = None;
            return;
        }
        let key = row.path.clone();
        self.pending_key = Some(key.clone());
        self.send(Request::Select { key, offset: 0 }).await;
    }

    async fn rescan(&mut self) {
        self.loading = true;
        self.status = "scanning…".into();
        self.scan_complete = false;
        let (pattern, kind) = (self.pattern.clone(), self.kind_filter);
        self.send(Request::Rescan { pattern, kind }).await;
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
                if let Some(view) = View::from_digit(c) {
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

        if self.view != View::Keys {
            self.handle_pane_key(key);
            return;
        }

        self.handle_keys_view_key(key).await;
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
                let i = KIND_CYCLE.iter().position(|k| *k == self.kind_filter).unwrap_or(0);
                self.kind_filter = KIND_CYCLE[(i + 1) % KIND_CYCLE.len()];
                self.rescan().await;
            }

            KeyCode::Char('m') => {
                if !self.scan_complete {
                    self.loading = true;
                    self.send(Request::More).await;
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
                self.load_selected().await;
            }
            KeyCode::End | KeyCode::Char('G') => {
                self.selected = self.rows.len().saturating_sub(1);
                self.load_selected().await;
            }

            KeyCode::Enter | KeyCode::Char(' ') | KeyCode::Right | KeyCode::Char('l') => {
                if let Some(row) = self.selected_row() {
                    let (path, is_branch) = (row.path.clone(), row.is_branch);
                    if is_branch {
                        self.tree.toggle(&path);
                        self.rebuild();
                    } else {
                        self.load_selected().await;
                    }
                }
            }

            KeyCode::Left | KeyCode::Char('h') => self.collapse_or_parent(),

            _ => {}
        }
    }

    fn handle_value_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => self.detail_scroll = self.detail_scroll.saturating_add(1),
            KeyCode::Up | KeyCode::Char('k') => self.detail_scroll = self.detail_scroll.saturating_sub(1),
            KeyCode::PageDown => self.detail_scroll = self.detail_scroll.saturating_add(20),
            KeyCode::PageUp => self.detail_scroll = self.detail_scroll.saturating_sub(20),
            KeyCode::Home => self.detail_scroll = 0,
            _ => {}
        }
    }

    /// Collapse an open branch, otherwise jump to the parent -- the behaviour every file
    /// browser has, so it needs no explanation.
    fn collapse_or_parent(&mut self) {
        let Some(row) = self.selected_row() else { return };
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
            self.load_selected().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keylens_conn::ServerInfo;
    use tokio::sync::mpsc;

    fn app() -> App {
        let (tx, _rx) = mpsc::channel(8);
        App::new(ServerInfo::parse("redis_version:8.0.0\r\n"), "redis://x".into(), tx)
    }

    fn batch(keys: &[&str]) -> Update {
        Update::Batch {
            keys: keys.iter().map(|k| (k.to_string(), None)).collect(),
            reset: false,
            complete: true,
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
    fn selection_sticks_to_its_path_across_batches() {
        let mut a = app();
        a.apply(batch(&["m:1"]));
        a.selected = 0;
        let before = a.selected_row().unwrap().path.clone();

        // A key sorting *above* the selection arrives; index-based restore would drift.
        a.apply(batch(&["a:1"]));
        assert_eq!(a.selected_row().unwrap().path, before);
    }

    #[test]
    fn stale_detail_replies_are_dropped() {
        let mut a = app();
        a.apply(batch(&["k1", "k2"]));
        a.pending_key = Some("k2".into());

        a.apply(Update::Detail {
            meta: Box::new(KeyMeta {
                key: "k1".into(),
                kind: Kind::String,
                ttl_ms: None,
                size: 1,
                memory: None,
            }),
            value: Box::new(KeyValue::String("stale".into())),
        });
        assert!(a.detail.is_none(), "reply for k1 must not render while k2 is selected");

        a.apply(Update::Detail {
            meta: Box::new(KeyMeta {
                key: "k2".into(),
                kind: Kind::String,
                ttl_ms: None,
                size: 1,
                memory: None,
            }),
            value: Box::new(KeyValue::String("fresh".into())),
        });
        assert!(a.detail.is_some());
    }

    #[test]
    fn reset_batch_clears_previous_keys() {
        let mut a = app();
        a.apply(batch(&["old:1"]));
        a.apply(Update::Batch {
            keys: vec![("new:1".into(), None)],
            reset: true,
            complete: true,
            scanned_pages: 1,
        });
        assert_eq!(a.rows.len(), 1);
        // A lone key folds to a single row for its whole path.
        assert_eq!(a.rows[0].path, "new:1");
    }

    #[test]
    fn error_clears_loading_state() {
        let mut a = app();
        a.apply(Update::Error("boom".into()));
        assert!(!a.loading);
        assert_eq!(a.error.as_deref(), Some("boom"));
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
            a.handle_normal_key(KeyEvent::from(KeyCode::Char('t'))).await;
            assert_eq!(a.kind_filter, expected);
        }
        for _ in 0..4 {
            a.handle_normal_key(KeyEvent::from(KeyCode::Char('t'))).await;
        }
        assert_eq!(a.kind_filter, None, "cycle wraps back to unfiltered");
    }

    #[tokio::test]
    async fn digits_switch_views() {
        let mut a = app();
        assert_eq!(a.view, View::Keys);

        a.handle_normal_key(KeyEvent::from(KeyCode::Char('3'))).await;
        assert_eq!(a.view, View::Slowlog);
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('1'))).await;
        assert_eq!(a.view, View::Keys);

        // Out of range digits are ignored rather than panicking on an index.
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('9'))).await;
        assert_eq!(a.view, View::Keys);
    }

    #[tokio::test]
    async fn opening_a_pane_loads_it_once() {
        let mut a = app();
        assert!(a.slowlog.is_idle(), "panes must not load until opened");

        a.handle_normal_key(KeyEvent::from(KeyCode::Char('3'))).await;
        assert!(matches!(a.slowlog, PaneState::Loading));

        // Coming back to an already-loaded pane must not refire the request.
        a.slowlog = PaneState::Ready(vec![]);
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('1'))).await;
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('3'))).await;
        assert!(matches!(a.slowlog, PaneState::Ready(_)));
    }

    #[tokio::test]
    async fn r_reloads_the_pane_that_is_showing() {
        let mut a = app();
        a.view = View::Clients;
        a.clients = PaneState::Ready(vec![]);

        a.handle_normal_key(KeyEvent::from(KeyCode::Char('r'))).await;
        assert!(matches!(a.clients, PaneState::Loading), "r should force a reload");
    }

    #[tokio::test]
    async fn tree_keys_do_not_leak_into_server_panes() {
        // `t` cycles the type filter in the keys view. In the slowlog view it must do
        // nothing rather than silently re-scanning the keyspace.
        let mut a = app();
        a.view = View::Slowlog;
        a.handle_normal_key(KeyEvent::from(KeyCode::Char('t'))).await;
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

    #[tokio::test]
    async fn ctrl_c_quits_from_help_mode() {
        let mut a = app();
        a.mode = Mode::Help;
        a.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)).await;
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
