//! Headless render tests.
//!
//! A TUI that compiles proves nothing -- panics live in layout arithmetic and in the
//! assumption that a pane is wide enough for its content. `TestBackend` renders into an
//! in-memory buffer, so these run in CI with no terminal and no Redis.

use keylens::app::{App, Mode, View};
use keylens::ui;
use keylens::worker::{Request, Update};
use keylens_conn::{KeyMeta, KeyValue, Kind, ServerInfo, StreamEntry};
use keylens_ui::PaneState;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use tokio::sync::mpsc::{self, Receiver};

/// Returns the receiver too -- dropping it would make every send report a dead worker.
fn app_with(keys: &[&str]) -> (App, Receiver<Request>) {
    let (tx, rx) = mpsc::channel(8);

    let mut app = App::new(
        ServerInfo::parse("server_name:valkey\r\nvalkey_version:8.1.0\r\n"),
        "redis://127.0.0.1:6379".into(),
        tx,
    );
    app.apply(Update::Batch {
        keys: keys.iter().map(|k| (k.to_string(), Some(Kind::Hash))).collect(),
        reset: true,
        complete: true,
        scanned_pages: 1,
    });
    (app, rx)
}

fn meta(key: &str, kind: Kind) -> Box<KeyMeta> {
    Box::new(KeyMeta { key: key.into(), kind, ttl_ms: Some(90_000), size: 2, memory: Some(1024) })
}

fn render(app: &mut App, width: u16, height: u16) -> String {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal.draw(|f| ui::draw(f, app)).unwrap();
    let buffer = terminal.backend().buffer().clone();
    (0..buffer.area.height)
        .map(|y| {
            (0..buffer.area.width)
                .map(|x| buffer[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn renders_tree_and_status_bar() {
    let (mut app, _rx) = app_with(&["bull:emails:1", "bull:emails:2", "cache:user:7"]);
    let out = render(&mut app, 100, 24);

    assert!(out.contains("KEYLENS"), "status bar wordmark missing");
    assert!(out.contains("Valkey"), "vendor should come from INFO, not be assumed");

    // Single-child chains fold, so `bull` shows as `bull:emails` and the lone cache key
    // collapses to its whole path -- one row each instead of three levels of expanding.
    assert!(out.contains("bull:emails"), "chain should be folded:\n{out}");
    assert!(out.contains("cache:user:7"), "{out}");

    // Still collapsed below the fold: the two job ids must not be visible yet.
    let tree_pane: String = out.lines().map(|l| l.split("││").next().unwrap_or(l)).collect();
    assert!(!tree_pane.contains("emails:1"), "tree should start collapsed:\n{out}");
}

#[test]
fn renders_every_value_type_without_panicking() {
    let values = [
        KeyValue::String(r#"{"to":"a@b.c"}"#.into()),
        KeyValue::Hash(vec![("version".into(), "bullmq:6.0.2".into())]),
        KeyValue::List(vec!["job-1".into(), "job-2".into()]),
        KeyValue::Set(vec!["member".into()]),
        KeyValue::ZSet(vec![("job-9".into(), 1712.5)]),
        KeyValue::Stream(vec![StreamEntry {
            id: "1712-0".into(),
            fields: vec![("event".into(), "failed".into())],
        }]),
        KeyValue::Missing,
        KeyValue::Unsupported("module type".into()),
    ];

    for value in values {
        let (mut app, _rx) = app_with(&["k"]);
        app.set_pending_key(Some("k".into()));
        app.apply(Update::Detail { meta: meta("k", Kind::String), value: Box::new(value) });
        assert!(app.detail.is_some(), "detail should have been accepted");
        let _ = render(&mut app, 100, 24);
    }
}

#[test]
fn json_payloads_render_pretty_printed() {
    let (mut app, _rx) = app_with(&["job"]);
    app.set_pending_key(Some("job".into()));
    app.apply(Update::Detail {
        meta: meta("job", Kind::String),
        value: Box::new(KeyValue::String(r#"{"to":"a@b.c","attempts":3}"#.into())),
    });

    let out = render(&mut app, 100, 24);
    // Minified JSON has no space after the colon; pretty-printed does.
    assert!(out.contains("\"to\": \"a@b.c\""), "payload should be pretty-printed:\n{out}");
}

#[test]
fn ttl_and_memory_appear_in_the_detail_header() {
    let (mut app, _rx) = app_with(&["k"]);
    app.set_pending_key(Some("k".into()));
    app.apply(Update::Detail {
        meta: meta("k", Kind::Hash),
        value: Box::new(KeyValue::Hash(vec![("f".into(), "v".into())])),
    });

    let out = render(&mut app, 100, 24);
    assert!(out.contains("1m30s"), "ttl should be humanised:\n{out}");
    assert!(out.contains("hash"));
}

#[test]
fn survives_a_terminal_too_narrow_to_be_reasonable() {
    // Layout arithmetic that subtracts a border width panics on tiny terminals, and users
    // really do drag windows to absurd sizes.
    let (mut app, _rx) = app_with(&["bull:emails:1"]);
    app.set_pending_key(Some("bull:emails:1".into()));
    app.apply(Update::Detail {
        meta: meta("bull:emails:1", Kind::Hash),
        value: Box::new(KeyValue::Hash(vec![("field".into(), "value".into())])),
    });

    for (w, h) in [(20, 5), (10, 3), (4, 2), (1, 1)] {
        let _ = render(&mut app, w, h);
    }
}

#[test]
fn help_overlay_draws_over_the_tree() {
    let (mut app, _rx) = app_with(&["bull:emails:1"]);
    app.mode = Mode::Help;
    let out = render(&mut app, 100, 24);
    assert!(out.contains("help"));
    assert!(out.contains("expand branch or open key"));
}

#[test]
fn empty_keyspace_explains_itself() {
    let (mut app, _rx) = app_with(&[]);
    let out = render(&mut app, 100, 24);
    assert!(out.contains("no keys"), "an empty pane should say what to do next:\n{out}");
}

#[test]
fn every_view_renders() {
    for view in View::ALL {
        let (mut app, _rx) = app_with(&["bull:emails:1"]);
        app.view = view;
        let out = render(&mut app, 110, 30);
        assert!(out.contains(view.label()), "tab bar missing {}:\n{out}", view.label());
    }
}

#[test]
fn a_blocked_command_reads_as_unavailable_not_as_an_error() {
    // This is the Upstash/ElastiCache path, and it must not look like something broke.
    let (mut app, _rx) = app_with(&[]);
    app.view = View::Slowlog;
    app.slowlog = PaneState::Unavailable("NOPERM this user has no permissions".into());

    let out = render(&mut app, 110, 30);
    assert!(out.contains("unavailable on this server"), "{out}");
    assert!(out.contains("NOPERM"), "the reason should be shown:\n{out}");
    assert!(!out.to_lowercase().contains("failed"), "must not read as a failure:\n{out}");
}

#[test]
fn stats_pane_reports_what_info_actually_returned() {
    let (mut app, _rx) = app_with(&[]);
    app.view = View::Stats;
    app.server = ServerInfo::parse(
        "server_name:valkey\r\nvalkey_version:8.1.0\r\nredis_mode:standalone\r\n\
         used_memory_human:5.17M\r\nconnected_clients:17\r\nkeyspace_hits:90\r\n\
         keyspace_misses:10\r\nmaxmemory_policy:noeviction\r\ndb0:keys=501,expires=0\r\n",
    );

    let out = render(&mut app, 110, 52);
    assert!(out.contains("5.17M"));
    assert!(out.contains("90.0%"), "hit rate should be computed:\n{out}");
    assert!(out.contains("noeviction"));
    assert!(out.contains("keys=501"), "per-db keyspace should be listed:\n{out}");

    // The dashboard is taller than most terminals, so scrolling has to work: the keyspace
    // section sits below the fold at a realistic height.
    let short = render(&mut app, 110, 30);
    assert!(!short.contains("keys=501"));
    app.pane_scroll = 24;
    let scrolled = render(&mut app, 110, 30);
    assert!(scrolled.contains("keys=501"), "scrolling should reach the keyspace:\n{scrolled}");
}

#[test]
fn standalone_cluster_pane_explains_itself_rather_than_showing_an_empty_table() {
    let (mut app, _rx) = app_with(&[]);
    app.view = View::Cluster;
    app.cluster = PaneState::Ready(Box::default());

    let out = render(&mut app, 110, 30);
    assert!(out.contains("cluster mode is not enabled"), "{out}");
}

#[test]
fn empty_slowlog_says_so_and_says_why() {
    let (mut app, _rx) = app_with(&[]);
    app.view = View::Slowlog;
    app.slowlog = PaneState::Ready(vec![]);

    let out = render(&mut app, 110, 30);
    assert!(out.contains("no slow commands logged"), "{out}");
    assert!(out.contains("slowlog-log-slower-than"), "should hint at the threshold:\n{out}");
}

#[test]
fn active_filters_are_visible_in_the_status_bar() {
    // A filter you can't see is a filter you forget you set, then report as a bug.
    let (mut app, _rx) = app_with(&["bull:emails:1"]);
    app.pattern = Some("*emails*".into());
    app.kind_filter = Some(Kind::Hash);

    let out = render(&mut app, 120, 24);
    assert!(out.contains("match *emails*"), "{out}");
    assert!(out.contains("type hash"), "{out}");
}

#[test]
fn splash_shows_the_wordmark_until_the_first_batch_lands() {
    let (tx, _rx) = mpsc::channel(8);
    let mut app = App::new(
        ServerInfo::parse("server_name:valkey\r\nvalkey_version:8.1.0\r\n"),
        "redis://127.0.0.1:6379".into(),
        tx,
    );
    assert!(app.splash);

    let out = render(&mut app, 100, 30);
    assert!(out.contains("█"), "block wordmark should render:\n{out}");
    assert!(out.contains("Valkey"));
    assert!(out.contains("read-only"), "the safety promise belongs on the splash:\n{out}");

    // The first batch is the signal there's something worth showing.
    app.apply(Update::Batch {
        keys: vec![("k".into(), None)],
        reset: true,
        complete: true,
        scanned_pages: 1,
    });
    assert!(!app.splash);
    let out = render(&mut app, 100, 30);
    assert!(!out.contains("█"), "splash should be gone:\n{out}");
}

#[test]
fn splash_degrades_on_a_narrow_terminal() {
    // Block glyphs sliced mid-letter look broken, not minimal.
    let (tx, _rx) = mpsc::channel(8);
    let mut app =
        App::new(ServerInfo::parse("redis_version:8.0.0\r\n"), "redis://x".into(), tx);

    let out = render(&mut app, 40, 20);
    assert!(out.contains("KEYLENS"));
    assert!(!out.contains("█"), "should fall back to plain text:\n{out}");
}
