//! Adversarial data against real servers.
//!
//! keylens is pointed at keyspaces it does not control. Everything below is something a
//! producer, a tenant or an attacker can write: hostile key names, oversized values,
//! pathological collections, malformed BullMQ records, thousands of channels and
//! consumers. The assertions are the promises: no panic, bounded retention, honest
//! counts, and nothing that reaches the terminal as a control sequence.
//!
//! ```sh
//! docker compose up -d --wait
//! cargo test --test adversarial -- --ignored --test-threads 1
//! KEYLENS_TEST_URL=redis://127.0.0.1:6380 cargo test --test adversarial -- --ignored --test-threads 1
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use keylens::app::App;
use keylens::worker::{Request, ScanProgress, Update, Worker};
use keylens_conn::bounds::{MAX_ELEMENT_BYTES, MAX_ENTRY_FIELDS, MAX_LISTED};
use keylens_conn::{Conn, Coverage, KeyValue, MAX_STRING_BYTES, PAGE, ServerInfo, Value};
use ratatui::{Terminal, backend::TestBackend};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

fn url() -> String {
    std::env::var("KEYLENS_TEST_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

async fn conn() -> Conn {
    Conn::connect(&url(), "adversarial")
        .await
        .expect("fixtures up? `docker compose up -d`")
}

/// Bytes kept by a decoded value: what keylens holds in memory and renders.
fn retained(v: &KeyValue) -> usize {
    match v {
        KeyValue::String(s) => s.len(),
        KeyValue::List(x) | KeyValue::Set(x) => x.iter().map(String::len).sum(),
        KeyValue::Hash(x) => x.iter().map(|(a, b)| a.len() + b.len()).sum(),
        KeyValue::ZSet(x) => x.iter().map(|(a, _)| a.len() + 8).sum(),
        KeyValue::Stream(x) => x
            .iter()
            .map(|e| {
                e.id.len()
                    + e.fields
                        .iter()
                        .map(|(a, b)| a.len() + b.len())
                        .sum::<usize>()
            })
            .sum(),
        KeyValue::Missing | KeyValue::Changed | KeyValue::Unsupported(_) => 0,
    }
}

fn render(app: &mut App) -> Vec<String> {
    let mut t = Terminal::new(TestBackend::new(140, 50)).unwrap();
    t.draw(|f| keylens::ui::draw(f, app)).unwrap();
    t.backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol().to_string())
        .collect()
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn hostile_key_names_are_listed_read_and_rendered_safely() {
    let conn = conn().await;
    let names: Vec<Vec<u8>> = vec![
        b"keylens:adv:name:esc\x1b]0;PWNED\x07\x1b[2J".to_vec(),
        b"keylens:adv:name:newline\nfake row".to_vec(),
        b"keylens:adv:name:bidi\xe2\x80\xaereversed".to_vec(),
        [b"keylens:adv:name:long:".as_slice(), &vec![b'x'; 64 * 1024]].concat(),
        b"keylens:adv:name:bin\xff\xfe".to_vec(), // not UTF-8: counted, not listed
    ];
    for n in &names {
        conn.test_cmd("UNLINK", vec![Value::Bytes(n.clone().into())])
            .await
            .ok();
        conn.test_cmd(
            "SET",
            vec![
                Value::Bytes(n.clone().into()),
                Value::from("payload\x1b[31m"),
            ],
        )
        .await
        .unwrap();
    }

    let (req_tx, req_rx) = mpsc::channel(8);
    let (up_tx, mut up_rx) = mpsc::channel(8);
    tokio::spawn(Worker::new(conn).run(req_rx, up_tx));
    let (app_tx, _app_rx) = mpsc::channel(8);
    let mut app = App::new(ServerInfo::unknown(), url(), app_tx);

    // The app's own generation: a batch for a generation it never issued is dropped.
    let scan = app.scan_generation();
    req_tx
        .send(Request::Rescan {
            scan,
            pattern: Some("keylens:adv:name:*".into()),
            kind: None,
        })
        .await
        .unwrap();
    let mut keys = Vec::new();
    let mut unlisted: u64;
    loop {
        let update = tokio::time::timeout(Duration::from_secs(20), up_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let Update::Batch {
            keys: batch,
            progress,
            unlisted: u,
            ..
        } = &update
        else {
            continue;
        };
        keys.extend(batch.iter().map(|(k, _)| k.clone()));
        unlisted = *u;
        let done = *progress == ScanProgress::Complete;
        app.apply(update);
        if done {
            break;
        }
        req_tx.send(Request::More { scan }).await.unwrap();
    }
    assert_eq!(keys.len(), 4, "every UTF-8 name listed: {keys:?}");
    assert_eq!(unlisted, 1, "the binary name is counted, not silently lost");
    assert!(app.status.contains("non-UTF-8"), "{}", app.status);

    // Every listed key reads, and nothing control-like reaches a terminal cell.
    let reader = Conn::connect(&url(), "reader").await.unwrap();
    for k in &keys {
        let read = reader.read_key(k).await.unwrap();
        assert!(matches!(read.value, KeyValue::String(_)), "{k:?}");
        let id = app.expect_detail(k.clone());
        app.apply(Update::Detail {
            id,
            meta: Box::new(read.meta),
            value: Box::new(read.value),
            coverage: read.coverage,
            stream: None,
        });
        let cells = render(&mut app);
        let bad: Vec<_> = cells
            .iter()
            .filter(|c| c.chars().any(char::is_control))
            .collect();
        assert!(
            bad.is_empty(),
            "control characters reached the screen: {bad:?}"
        );
    }

    for n in names {
        reader
            .test_cmd("UNLINK", vec![Value::Bytes(n.into())])
            .await
            .ok();
    }
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn oversized_values_are_read_with_bounded_retention() {
    let conn = conn().await;
    // 256 KiB elements: 16x the per-element cap, and a 50 MiB page that stays well
    // inside the command timeout on a loaded CI runner.
    let mb = "m".repeat(256 * 1024);

    // 32 MiB string.
    conn.test_cmd(
        "SET",
        vec!["keylens:adv:bigstr".into(), "s".repeat(32 << 20).into()],
    )
    .await
    .unwrap();
    // 200 list elements of 256 KiB: a full page of oversized elements.
    conn.test_cmd("UNLINK", vec!["keylens:adv:fatlist".into()])
        .await
        .ok();
    for _ in 0..4 {
        let mut argv: Vec<Value> = vec!["keylens:adv:fatlist".into()];
        argv.extend((0..50).map(|_| Value::from(mb.as_str())));
        conn.test_cmd("RPUSH", argv).await.unwrap();
    }
    // A stream entry with 5,000 fields.
    let mut xadd: Vec<Value> = vec!["keylens:adv:widestream".into(), "*".into()];
    for i in 0..5_000 {
        xadd.push(format!("f{i}").into());
        xadd.push("v".into());
    }
    conn.test_cmd("UNLINK", vec!["keylens:adv:widestream".into()])
        .await
        .ok();
    conn.test_cmd("XADD", xadd).await.unwrap();

    let s = conn.read_key("keylens:adv:bigstr").await.unwrap();
    assert!(
        retained(&s.value) <= MAX_STRING_BYTES + 64,
        "{}",
        retained(&s.value)
    );
    assert_eq!(
        s.coverage,
        Coverage::Partial {
            shown: MAX_STRING_BYTES
        }
    );
    assert_eq!(s.meta.size, Some(32 << 20));

    let l = conn.read_key("keylens:adv:fatlist").await.unwrap();
    // 50 MiB went over the wire -- LRANGE cannot bound element size -- but what is
    // kept is at most a page of capped elements.
    let budget = PAGE * (MAX_ELEMENT_BYTES + 64);
    assert!(
        retained(&l.value) <= budget,
        "{} > {budget}",
        retained(&l.value)
    );
    assert_eq!(l.value.len(), PAGE);

    let st = conn.read_key("keylens:adv:widestream").await.unwrap();
    let KeyValue::Stream(entries) = &st.value else {
        panic!("{:?}", st.value)
    };
    assert_eq!(
        entries[0].fields.len(),
        MAX_ENTRY_FIELDS + 1,
        "capped + a note row"
    );
    assert!(
        entries[0]
            .fields
            .last()
            .unwrap()
            .1
            .contains("more fields not shown")
    );

    for k in [
        "keylens:adv:bigstr",
        "keylens:adv:fatlist",
        "keylens:adv:widestream",
    ] {
        conn.test_cmd("UNLINK", vec![k.into()]).await.ok();
    }
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn thousands_of_consumers_and_channels_are_kept_bounded_and_counted() {
    let conn = conn().await;
    let key = "keylens:adv:consumers";
    conn.test_cmd("UNLINK", vec![key.into()]).await.ok();
    conn.test_cmd("XADD", vec![key.into(), "*".into(), "f".into(), "v".into()])
        .await
        .unwrap();
    conn.test_cmd(
        "XGROUP",
        vec!["CREATE".into(), key.into(), "g".into(), "0".into()],
    )
    .await
    .unwrap();
    let total = MAX_LISTED + 500;
    let cmds: Vec<(&'static str, Vec<Value>)> = (0..total)
        .map(|i| {
            (
                "XGROUP",
                vec![
                    "CREATECONSUMER".into(),
                    key.into(),
                    "g".into(),
                    format!("c{i}").into(),
                ],
            )
        })
        .collect();
    conn.test_pipeline(&cmds).await.unwrap();

    let info = conn.stream_info(key).await.unwrap();
    let g = &info.groups[0];
    assert_eq!(g.consumers.len(), MAX_LISTED);
    assert_eq!(g.consumers_total, total, "the true count survives the cap");
    conn.test_cmd("UNLINK", vec![key.into()]).await.ok();

    // 1,500 channels with one subscriber each, over a raw RESP connection that stays
    // subscribed for the duration.
    let host = url().trim_start_matches("redis://").to_string();
    let mut sub = tokio::net::TcpStream::connect(&host).await.unwrap();
    let n = MAX_LISTED + 500;
    let mut cmd = format!("*{}\r\n$9\r\nSUBSCRIBE\r\n", n + 1);
    for i in 0..n {
        let ch = format!("keylens:adv:ch:{i}");
        cmd.push_str(&format!("${}\r\n{ch}\r\n", ch.len()));
    }
    sub.write_all(cmd.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    if conn.capabilities().has(keylens_conn::Feature::PubSub) {
        let channels = conn.pubsub_channels(200).await.unwrap();
        assert_eq!(channels.len(), 200);
        assert!(channels.total >= n, "total {} < {n}", channels.total);
        assert!(channels.is_truncated());
    }
    drop(sub);
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn malformed_bullmq_records_degrade_instead_of_failing() {
    use keylens_bullmq::{BullMqLens, State};
    let conn = conn().await;
    let p = "keylens-adv";
    let meta = format!("{p}:q:meta");
    let job = format!("{p}:q:1");
    let failed = format!("{p}:q:failed");
    for k in [&meta, &job, &failed] {
        conn.test_cmd("UNLINK", vec![k.as_str().into()]).await.ok();
    }
    conn.test_cmd(
        "HSET",
        vec![
            meta.as_str().into(),
            "version".into(),
            "\x1b[2J:garbage".into(),
        ],
    )
    .await
    .unwrap();
    conn.test_cmd(
        "HSET",
        vec![
            job.as_str().into(),
            "timestamp".into(),
            "not-a-number".into(),
            "atm".into(),
            "-5".into(),
            "opts".into(),
            "{not json".into(),
            "stacktrace".into(),
            format!("[\"{}\"]", "t".repeat(2 << 20)).into(),
            "processedOn".into(),
            "99999999999999999999999".into(),
        ],
    )
    .await
    .unwrap();
    for score in ["inf", "-inf", "1e308"] {
        conn.test_cmd(
            "ZADD",
            vec![
                failed.as_str().into(),
                score.into(),
                format!("j{score}").into(),
            ],
        )
        .await
        .unwrap();
    }

    let lens = BullMqLens::new(p);
    let queues = lens.all_queues(&conn).await.unwrap();
    assert!(queues.iter().any(|q| q.name == "q"));
    let page = lens.jobs(&conn, "q", State::Failed, 0, 200).await.unwrap();
    assert_eq!(page.len(), 3, "odd scores still list");
    let j = lens
        .job(&conn, "q", "1")
        .await
        .unwrap()
        .expect("job exists");
    assert_eq!(j.timestamp, None);
    assert_eq!(
        j.processed_on, None,
        "an out-of-range number is unknown, not wrapped"
    );
    assert_eq!(j.attempts_allowed, None);
    assert!(
        j.stacktrace.iter().map(String::len).sum::<usize>() <= 64 * 1024 + 64,
        "the 2 MiB trace is capped"
    );

    for k in [&meta, &job, &failed] {
        conn.test_cmd("UNLINK", vec![k.as_str().into()]).await.ok();
    }
}
