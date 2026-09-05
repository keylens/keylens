//! Headless render tests.
//!
//! A TUI that compiles proves nothing -- panics live in layout arithmetic and in the
//! assumption that a pane is wide enough for its content. `TestBackend` renders into an
//! in-memory buffer, so these run in CI with no terminal and no Redis.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use crossterm::event::{KeyCode, KeyEvent};
use keylens::app::{App, Mode, View};
use keylens::events::StreamEvent;
use keylens::ui;
use keylens::worker::{JobDetail, Request, Update};
use keylens_bullmq::{EventKind, EventsStatus, JobRef, QueueSummary, State};
use keylens_conn::{KeyMeta, KeyValue, Kind, ServerInfo, StreamEntry};
use keylens_lens::{Confidence, Detection};
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
        keys: keys
            .iter()
            .map(|k| (k.to_string(), Some(Kind::Hash)))
            .collect(),
        reset: true,
        complete: true,
        scanned_pages: 1,
    });
    (app, rx)
}

fn meta(key: &str, kind: Kind) -> Box<KeyMeta> {
    Box::new(KeyMeta {
        key: key.into(),
        kind,
        ttl_ms: Some(90_000),
        size: 2,
        memory: Some(1024),
    })
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
    assert!(
        out.contains("Valkey"),
        "vendor should come from INFO, not be assumed"
    );

    // Single-child chains fold, so `bull` shows as `bull:emails` and the lone cache key
    // collapses to its whole path -- one row each instead of three levels of expanding.
    assert!(
        out.contains("bull:emails"),
        "chain should be folded:\n{out}"
    );
    assert!(out.contains("cache:user:7"), "{out}");

    // Still collapsed below the fold: the two job ids must not be visible yet.
    let tree_pane: String = out
        .lines()
        .map(|l| l.split("││").next().unwrap_or(l))
        .collect();
    assert!(
        !tree_pane.contains("emails:1"),
        "tree should start collapsed:\n{out}"
    );
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
        app.apply(Update::Detail {
            meta: meta("k", Kind::String),
            value: Box::new(value),
            stream: None,
        });
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
        stream: None,
    });

    let out = render(&mut app, 100, 24);
    // Minified JSON has no space after the colon; pretty-printed does.
    assert!(
        out.contains("\"to\": \"a@b.c\""),
        "payload should be pretty-printed:\n{out}"
    );
}

#[test]
fn ttl_and_memory_appear_in_the_detail_header() {
    let (mut app, _rx) = app_with(&["k"]);
    app.set_pending_key(Some("k".into()));
    app.apply(Update::Detail {
        meta: meta("k", Kind::Hash),
        value: Box::new(KeyValue::Hash(vec![("f".into(), "v".into())])),
        stream: None,
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
        stream: None,
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
    assert!(
        out.contains("no keys"),
        "an empty pane should say what to do next:\n{out}"
    );
}

#[test]
fn every_view_renders() {
    for view in View::ALL {
        let (mut app, _rx) = app_with(&["bull:emails:1"]);
        app.view = view;
        let out = render(&mut app, 110, 30);
        assert!(
            out.contains(view.label()),
            "tab bar missing {}:\n{out}",
            view.label()
        );
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
    assert!(
        !out.to_lowercase().contains("failed"),
        "must not read as a failure:\n{out}"
    );
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
    assert!(
        out.contains("keys=501"),
        "per-db keyspace should be listed:\n{out}"
    );

    // The dashboard is taller than most terminals, so scrolling has to work: the keyspace
    // section sits below the fold at a realistic height.
    let short = render(&mut app, 110, 30);
    assert!(!short.contains("keys=501"));
    app.pane_scroll = 24;
    let scrolled = render(&mut app, 110, 30);
    assert!(
        scrolled.contains("keys=501"),
        "scrolling should reach the keyspace:\n{scrolled}"
    );
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
    assert!(
        out.contains("slowlog-log-slower-than"),
        "should hint at the threshold:\n{out}"
    );
}

fn with_bullmq(keys: &[&str]) -> (App, Receiver<Request>) {
    let (mut app, rx) = app_with(keys);
    app.apply(Update::Detected(vec![Detection {
        lens_id: "bullmq",
        confidence: Confidence::Certain,
        version: Some("6.0.2".into()),
        prefix: "bull".into(),
        summary: "bullmq 6.0.2 - 2 queues".into(),
        targets: vec!["emails".into()],
    }]));
    app.view = View::Queues;
    (app, rx)
}

fn queue(name: &str, paused: bool, failed: u64) -> QueueSummary {
    QueueSummary {
        name: name.into(),
        paused,
        counts: State::ALL
            .iter()
            .map(|s| (*s, if *s == State::Failed { failed } else { 0 }))
            .collect(),
    }
}

#[test]
fn queue_table_shows_counts_and_true_paused_state() {
    let (mut app, _rx) = with_bullmq(&[]);
    app.apply(Update::Queues(PaneState::Ready(vec![
        queue("emails", false, 18),
        queue("reports", true, 0),
    ])));

    let out = render(&mut app, 120, 24);
    assert!(
        out.contains("bullmq 6.0.2"),
        "detection summary belongs in the title:\n{out}"
    );
    assert!(out.contains("emails"));
    assert!(out.contains("running"));
    assert!(
        out.contains("paused"),
        "paused state must be visible:\n{out}"
    );
    assert!(out.contains("18"));
    assert!(out.contains("1 paused"));
}

#[test]
fn queue_columns_do_not_run_into_each_other() {
    // `prioritized` and `waiting-children` at full length overflowed their columns.
    let (mut app, _rx) = with_bullmq(&[]);
    app.apply(Update::Queues(PaneState::Ready(vec![queue(
        "image-processing",
        false,
        500,
    )])));

    let out = render(&mut app, 120, 24);
    let header = out
        .lines()
        .find(|l| l.contains("queue") && l.contains("status"))
        .unwrap();
    assert!(header.contains("prio"), "short labels expected:\n{header}");
    assert!(
        !header.contains("prioritized"),
        "full label overflows:\n{header}"
    );
    // The name column needs a gap before status.
    let row = out
        .lines()
        .find(|l| l.contains("image-processing"))
        .unwrap();
    assert!(
        !row.contains("image-processingrunning"),
        "name butts into status:\n{row}"
    );
}

#[test]
fn throughput_column_distinguishes_idle_from_not_yet_watching() {
    let (mut app, _rx) = with_bullmq(&[]);
    app.apply(Update::Queues(PaneState::Ready(vec![queue(
        "emails", false, 0,
    )])));

    // Before the reader attaches, the graph is unknown -- not idle.
    let out = render(&mut app, 130, 24);
    assert!(out.contains("attaching to event streams"), "{out}");

    app.apply(Update::EventsStatus(EventsStatus::Live));
    let out = render(&mut app, 130, 24);
    assert!(out.contains("live"), "{out}");
    assert!(
        out.contains("idle"),
        "an attached-but-silent queue reads as idle:\n{out}"
    );
}

#[test]
fn throughput_column_draws_a_sparkline_from_stream_events() {
    let (mut app, _rx) = with_bullmq(&[]);
    app.apply(Update::Queues(PaneState::Ready(vec![queue(
        "emails", false, 0,
    )])));
    app.apply(Update::EventsStatus(EventsStatus::Live));

    // Events timestamped "now" so they land inside the rendered window.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    app.apply(Update::Events(
        (0..12)
            .map(|i| StreamEvent {
                queue: "emails".into(),
                kind: EventKind::Completed,
                at_ms: now_ms - i * 100,
            })
            .collect(),
    ));

    let out = render(&mut app, 130, 24);
    assert!(
        out.contains('█') || out.contains('▇') || out.contains('▄'),
        "a burst should draw sparkline blocks:\n{out}"
    );

    // Assert on the summary line specifically -- a substring search for "0.0" over the
    // whole screen matches the `127.0.0.1` in the connection URL.
    let summary = out
        .lines()
        .find(|l| l.contains("events/sec"))
        .expect("summary line");
    assert!(summary.contains("live"), "{summary}");
    assert!(
        !summary.contains("0.0 events/sec"),
        "rate should be non-zero after a burst: {summary}"
    );
}

#[test]
fn the_queue_table_fits_its_pane() {
    // The sparkline is sized from the remaining width; getting that budget wrong clipped
    // the ev/s column off the right-hand edge.
    let (mut app, _rx) = with_bullmq(&[]);
    app.apply(Update::Queues(PaneState::Ready(vec![queue(
        "image-processing",
        false,
        500,
    )])));
    app.apply(Update::EventsStatus(EventsStatus::Live));

    for width in [80u16, 100, 120, 160, 200] {
        let out = render(&mut app, width, 20);

        // No rendered line may exceed the terminal width at any size.
        for line in out.lines() {
            assert!(
                line.chars().count() <= width as usize,
                "line overflows at width {width}: {line}"
            );
        }

        // The columns that justify the view must survive every width.
        let header = out
            .lines()
            .find(|l| l.contains("queue") && l.contains("status"))
            .unwrap();
        for required in ["wait", "active", "failed"] {
            assert!(
                header.contains(required),
                "lost `{required}` at width {width}:\n{header}"
            );
        }

        // Once there's room, the graph and rate appear.
        if width >= 140 {
            assert!(
                out.contains("ev/s"),
                "graph should fit at width {width}:\n{out}"
            );
        }
    }
}

#[test]
fn a_narrow_pane_drops_columns_instead_of_clipping_them() {
    let (mut app, _rx) = with_bullmq(&[]);
    app.apply(Update::Queues(PaneState::Ready(vec![queue(
        "emails", false, 7,
    )])));

    let narrow = render(&mut app, 80, 20);
    let wide = render(&mut app, 200, 20);

    // `children` is the least useful column, so it goes first.
    assert!(
        !narrow.contains("children"),
        "narrow pane should drop it:\n{narrow}"
    );
    assert!(
        wide.contains("children"),
        "wide pane has room for it:\n{wide}"
    );
    // But the failed count is never dropped -- it's why the view exists.
    assert!(narrow.contains("failed"));
}

#[test]
fn job_detail_renders_a_stack_trace_per_attempt() {
    let (mut app, _rx) = with_bullmq(&[]);
    app.apply(Update::Queues(PaneState::Ready(vec![queue(
        "image-processing",
        false,
        1,
    )])));
    app.apply(Update::Jobs {
        state: State::Failed,
        data: PaneState::Ready(vec![JobRef {
            id: "7012".into(),
            score: Some(1.0),
        }]),
    });

    let job = keylens_bullmq::job::from_fields(
        "7012",
        &field_values(&[
            ("name", "image-processing"),
            ("atm", "2"),
            ("opts", r#"{"attempts":2}"#),
            ("failedReason", "offset is out of bounds"),
            (
                "stacktrace",
                r#"["RangeError: boom\n    at decodeFrame (file:///app/p.mjs:59:9)","RangeError: boom again\n    at decodeFrame (file:///app/p.mjs:59:9)"]"#,
            ),
            ("data", r#"{"assetId":"asset_1","width":2560}"#),
            ("timestamp", "1000"),
            ("processedOn", "1500"),
            ("finishedOn", "2200"),
        ]),
    );
    app.apply(Update::Job(PaneState::Ready(Some(Box::new(JobDetail {
        job,
        logs: vec![],
    })))));
    app.level = keylens::app::QueueLevel::Job;

    let out = render(&mut app, 120, 40);
    assert!(
        out.contains("2/2"),
        "attempts should show made/allowed:\n{out}"
    );
    assert!(out.contains("waited"), "{out}");
    assert!(out.contains("ran for"), "{out}");
    assert!(out.contains("offset is out of bounds"));
    assert!(
        out.contains("attempt 1") && out.contains("attempt 2"),
        "one trace per attempt:\n{out}"
    );
    assert!(
        out.contains("at decodeFrame"),
        "frames should be real lines, not escaped:\n{out}"
    );
    assert!(
        !out.contains("\\n"),
        "escaped newlines mean the JSON array wasn't parsed:\n{out}"
    );
    assert!(out.contains("\"assetId\""), "payload should render:\n{out}");
}

#[test]
fn a_job_removed_by_retention_explains_itself() {
    let (mut app, _rx) = with_bullmq(&[]);
    app.apply(Update::Job(PaneState::Ready(None)));
    app.level = keylens::app::QueueLevel::Job;

    let out = render(&mut app, 120, 24);
    assert!(out.contains("no longer exists"), "{out}");
}

fn field_values(pairs: &[(&str, &str)]) -> Vec<Option<String>> {
    keylens_bullmq::job::JOB_FIELDS
        .iter()
        .map(|f| {
            pairs
                .iter()
                .find(|(k, _)| k == f)
                .map(|(_, v)| v.to_string())
        })
        .collect()
}

fn stream_with_groups() -> Box<keylens_conn::StreamInfo> {
    use keylens_conn::{ConsumerInfo, GroupInfo, StreamInfo};
    Box::new(StreamInfo {
        length: 1154,
        entries_added: Some(1154),
        last_generated_id: "1785518057220-0".into(),
        groups: vec![GroupInfo {
            name: "processors".into(),
            consumer_count: 2,
            pending: 27,
            last_delivered_id: "1785517628634-0".into(),
            entries_read: Some(95),
            lag: Some(0),
            pending_min_id: "1785517610863-0".into(),
            pending_max_id: "1785517615743-0".into(),
            consumers: vec![
                ConsumerInfo {
                    name: "worker-healthy".into(),
                    pending: 0,
                    idle_ms: 232,
                    inactive_ms: Some(240),
                },
                ConsumerInfo {
                    name: "worker-stuck".into(),
                    pending: 27,
                    idle_ms: 441_000,
                    inactive_ms: Some(441_000),
                },
            ],
        }],
        ..Default::default()
    })
}

fn render_stream(app: &mut App, stream: Box<keylens_conn::StreamInfo>, width: u16) -> String {
    app.set_pending_key(Some("keylens:audit".into()));
    app.apply(Update::Detail {
        meta: meta("keylens:audit", Kind::Stream),
        value: Box::new(KeyValue::Stream(vec![StreamEntry {
            id: "1785517610835-0".into(),
            fields: vec![("actor".into(), "admin".into())],
        }])),
        stream: Some(stream),
    });
    render(app, width, 40)
}

#[test]
fn stream_viewer_surfaces_the_stuck_consumer() {
    // The reason this pane exists: not "what's in the stream" but "who stopped acking".
    let (mut app, _rx) = app_with(&["keylens:audit"]);
    let out = render_stream(&mut app, stream_with_groups(), 120);

    assert!(out.contains("consumer groups"), "{out}");
    assert!(out.contains("processors"));
    assert!(out.contains("worker-stuck"));
    assert!(
        out.contains("pending range"),
        "the outstanding id range matters:\n{out}"
    );
    // The flagged consumer's idle time is humanised, not raw milliseconds.
    assert!(out.contains("7m21s"), "{out}");
    // Entries still render, below the group state.
    assert!(out.contains("entries"));
}

#[test]
fn the_stuck_flag_does_not_wrap_onto_its_own_line() {
    // A trailing note wrapped in a narrow value pane -- exactly where it matters most.
    let (mut app, _rx) = app_with(&["keylens:audit"]);
    for width in [90u16, 110, 140] {
        let out = render_stream(&mut app, stream_with_groups(), width);
        let row = out
            .lines()
            .find(|l| l.contains("worker-stuck"))
            .unwrap_or_else(|| panic!("consumer row missing at width {width}"));
        assert!(
            row.contains('!'),
            "stuck marker missing at width {width}: {row}"
        );
        assert!(
            row.contains("27"),
            "pending count wrapped away at width {width}: {row}"
        );
    }
}

#[test]
fn consumers_are_ordered_worst_first() {
    let (mut app, _rx) = app_with(&["keylens:audit"]);
    let out = render_stream(&mut app, stream_with_groups(), 120);

    let stuck = out
        .lines()
        .position(|l| l.contains("worker-stuck"))
        .unwrap();
    let healthy = out
        .lines()
        .position(|l| l.contains("worker-healthy"))
        .unwrap();
    assert!(
        stuck < healthy,
        "the consumer holding entries should come first:\n{out}"
    );
}

#[test]
fn an_unknown_lag_is_not_reported_as_zero() {
    // Redis returns nil for lag after trimming or XSETID. Showing 0 would claim the group
    // is caught up when the truth is that Redis cannot tell.
    let (mut app, _rx) = app_with(&["keylens:audit"]);
    let mut stream = stream_with_groups();
    stream.groups[0].lag = None;

    let out = render_stream(&mut app, stream, 120);
    assert!(out.contains("lag unknown"), "{out}");
}

#[test]
fn a_stream_without_groups_says_so() {
    use keylens_conn::StreamInfo;
    let (mut app, _rx) = app_with(&["bull:emails:events"]);
    let stream = Box::new(StreamInfo {
        length: 10_044,
        entries_added: Some(63_914),
        last_generated_id: "1785517561250-1".into(),
        ..Default::default()
    });

    let out = render_stream(&mut app, stream, 120);
    assert!(out.contains("no consumer groups"), "{out}");
    assert!(out.contains("XREAD"), "explain why, not just that:\n{out}");
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
    assert!(
        out.contains("read-only"),
        "the safety promise belongs on the splash:\n{out}"
    );

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
    let mut app = App::new(
        ServerInfo::parse("redis_version:8.0.0\r\n"),
        "redis://x".into(),
        tx,
    );

    let out = render(&mut app, 40, 20);
    assert!(out.contains("KEYLENS"));
    assert!(!out.contains("█"), "should fall back to plain text:\n{out}");
}

#[test]
fn a_failed_key_does_not_hide_the_next_key_that_loads() {
    // The value pane renders `error` *instead of* the value, so an error that outlives the
    // key it belongs to blanks every key selected afterwards. This used to persist until
    // the next full rescan: one bad key made the pane look permanently broken.
    let (mut app, _rx) = app_with(&["k1", "k2"]);

    app.set_pending_key(Some("k1".into()));
    app.apply(Update::Error("command `TYPE` failed: boom".into()));
    assert!(
        render(&mut app, 100, 24).contains("boom"),
        "error should show"
    );

    app.set_pending_key(Some("k2".into()));
    app.apply(Update::Detail {
        meta: meta("k2", Kind::String),
        value: Box::new(KeyValue::String("fresh-value".into())),
        stream: None,
    });

    let out = render(&mut app, 100, 24);
    assert!(
        out.contains("fresh-value"),
        "a key that loaded must replace the previous key's error:\n{out}"
    );
    assert!(
        !out.contains("boom"),
        "the stale error must be gone:\n{out}"
    );
}

#[tokio::test]
async fn moving_the_cursor_clears_the_previous_key_s_error() {
    // Not just on arrival of the next value -- while it loads, too. Otherwise the old
    // error sits under the new selection and reads as though *that* key failed.
    let (mut app, _rx) = app_with(&["k1", "k2"]);
    app.apply(Update::Error("boom".into()));

    app.handle_key(KeyEvent::from(KeyCode::Char('j'))).await;

    let out = render(&mut app, 100, 24);
    assert!(
        !out.contains("boom"),
        "navigating retires the error:\n{out}"
    );
    assert!(
        out.contains("loading"),
        "and shows the pending load:\n{out}"
    );
}

#[test]
fn an_unsupported_bounded_read_explains_the_missing_capability() {
    let (mut app, _rx) = app_with(&["big"]);
    app.set_pending_key(Some("big".into()));
    app.apply(Update::Detail {
        meta: meta("big", Kind::String),
        value: Box::new(KeyValue::Unsupported(
            "this server has no bounded string read (GETRANGE)".into(),
        )),
        stream: None,
    });

    let out = render(&mut app, 100, 24);
    assert!(out.contains("no bounded string read"), "{out}");
    assert!(out.contains("GETRANGE"), "{out}");
}

#[test]
fn the_view_hint_counts_the_tabs_that_actually_exist() {
    // Hardcoding `1-6` meant the hint contradicted the tab bar the moment a lens matched
    // and the queues tab appeared.
    let (mut app, _rx) = app_with(&["cache:1"]);
    assert_eq!(app.views().len(), 6);
    assert!(render(&mut app, 130, 24).contains("1-6 view"));

    let (mut app, _rx) = with_bullmq(&["bull:emails:1"]);
    assert_eq!(app.views().len(), 7);
    let out = render(&mut app, 130, 24);
    assert!(out.contains("1-7 view"), "{out}");
    assert!(!out.contains("1-6 view"), "{out}");
}

#[test]
fn a_server_without_streams_says_so_instead_of_attaching_forever() {
    // `attached: bool` collapsed "connected and quiet" into "not connected yet", so a
    // server that can never deliver an event sat on "attaching…" for the whole session.
    let (mut app, _rx) = with_bullmq(&[]);
    app.apply(Update::Queues(PaneState::Ready(vec![queue(
        "emails", false, 0,
    )])));
    app.apply(Update::EventsStatus(EventsStatus::Unavailable(
        "NOPERM cannot run 'xread'".into(),
    )));

    let out = render(&mut app, 130, 24);
    assert!(out.contains("live throughput unavailable"), "{out}");
    assert!(out.contains("NOPERM"), "name the reason:\n{out}");
    assert!(
        !out.contains("attaching to event streams"),
        "it is not still attaching:\n{out}"
    );
    // The counts above are still real -- only the graph is unavailable.
    assert!(out.contains("emails"), "{out}");
}

/// Drain the worker channel and report which requests were queued.
fn drained(rx: &mut Receiver<Request>) -> Vec<Request> {
    let mut out = Vec::new();
    while let Ok(req) = rx.try_recv() {
        out.push(req);
    }
    out
}

fn selects(reqs: &[Request]) -> Vec<String> {
    reqs.iter()
        .filter_map(|r| match r {
            Request::Select { key } => Some(key.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn scrolling_costs_one_fetch_not_one_per_row() {
    // Reading a key is several round trips. Firing one per cursor move meant holding `j`
    // queued a fetch per row, and against a remote server every reply but the last is
    // discarded as stale on arrival -- a backlog paid for and thrown away.
    let (mut app, mut rx) = app_with(&["k1", "k2", "k3", "k4", "k5"]);
    let _ = drained(&mut rx);

    for _ in 0..4 {
        app.handle_key(KeyEvent::from(KeyCode::Char('j'))).await;
    }

    assert!(
        selects(&drained(&mut rx)).is_empty(),
        "cursor movement alone must not hit the server"
    );
    assert!(app.selection_pending(), "but it must be remembered");

    // The event loop flushes once the keys stop coming.
    app.flush_selection().await;
    assert_eq!(
        selects(&drained(&mut rx)),
        vec!["k5"],
        "exactly one fetch, for the row actually landed on"
    );
    assert!(!app.selection_pending());
}

#[tokio::test]
async fn enter_fetches_immediately_without_waiting_for_the_debounce() {
    // Debouncing is for movement. `enter` is the user naming the key they want, and it
    // should not feel like it lagged.
    let (mut app, mut rx) = app_with(&["solo"]);
    let _ = drained(&mut rx);

    app.handle_key(KeyEvent::from(KeyCode::Enter)).await;
    assert_eq!(selects(&drained(&mut rx)), vec!["solo"]);
    assert!(!app.selection_pending());
}

#[tokio::test]
async fn re_selecting_the_same_row_does_not_re_fetch() {
    let (mut app, mut rx) = app_with(&["a", "b"]);
    let _ = drained(&mut rx);

    app.handle_key(KeyEvent::from(KeyCode::Char('j'))).await;
    app.flush_selection().await;
    assert_eq!(selects(&drained(&mut rx)), vec!["b"]);

    // Pressing enter on the row already in flight must not queue a duplicate.
    app.handle_key(KeyEvent::from(KeyCode::Enter)).await;
    assert!(
        selects(&drained(&mut rx)).is_empty(),
        "the same key is already pending"
    );
}

#[tokio::test]
async fn a_saturated_worker_never_blocks_the_ui() {
    // `send().await` on a bounded channel parks the *UI task* until the worker drains one,
    // so a busy worker froze keystrokes and redraws entirely -- indistinguishable from a
    // hang. The request is dropped and retried instead.
    let (tx, _rx) = mpsc::channel(1);
    let mut app = App::new(
        ServerInfo::parse("redis_version:8.0.0\r\n"),
        "redis://x".into(),
        tx,
    );
    app.apply(Update::Batch {
        keys: vec![("k1".into(), None), ("k2".into(), None)],
        reset: true,
        complete: true,
        scanned_pages: 1,
    });

    // Fill the single channel slot, then keep going. Each of these would previously have
    // parked forever; the test simply completing is the assertion.
    for _ in 0..20 {
        app.handle_key(KeyEvent::from(KeyCode::Char('j'))).await;
        app.flush_selection().await;
        app.handle_key(KeyEvent::from(KeyCode::Char('k'))).await;
        app.flush_selection().await;
    }

    assert!(
        app.selection_pending(),
        "a dropped fetch stays pending so the next idle tick retries it"
    );
    assert!(
        app.error.is_none(),
        "a full queue is backpressure, not an error: {:?}",
        app.error
    );
}

#[tokio::test]
async fn a_dropped_fetch_does_not_leave_the_pane_claiming_to_load_forever() {
    // If the request never went out, `pending_key` must not be set -- otherwise the value
    // pane sits on `loading…` waiting for a reply to something nobody asked.
    let (tx, mut rx) = mpsc::channel(1);
    let mut app = App::new(
        ServerInfo::parse("redis_version:8.0.0\r\n"),
        "redis://x".into(),
        tx,
    );
    app.apply(Update::Batch {
        keys: vec![("k1".into(), None), ("k2".into(), None)],
        reset: true,
        complete: true,
        scanned_pages: 1,
    });

    // One slot, already occupied.
    app.handle_key(KeyEvent::from(KeyCode::Char('j'))).await;
    app.flush_selection().await;
    assert_eq!(selects(&drained(&mut rx)).len(), 1, "first one got through");

    // Occupy the slot again and try to move on without draining.
    app.handle_key(KeyEvent::from(KeyCode::Char('k'))).await;
    app.flush_selection().await;
    app.handle_key(KeyEvent::from(KeyCode::Char('j'))).await;
    app.flush_selection().await;

    assert!(app.selection_pending(), "the retry is queued");

    // Drain and retry: the fetch now lands.
    let _ = drained(&mut rx);
    app.flush_selection().await;
    assert_eq!(selects(&drained(&mut rx)), vec!["k2"]);
}
