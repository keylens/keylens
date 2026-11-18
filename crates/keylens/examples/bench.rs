//! Release benchmarks: one scenario per process, so `/usr/bin/time` attributes peak RSS
//! and CPU to it. Prints one JSON line.
//!
//! ```sh
//! cargo run --release --example bench -- seed
//! cargo run --release --example bench -- <scenario> [rtt_ms]
//! ```
//!
//! Point it at a server with no other traffic (`KEYLENS_BENCH_URL`, default the compose
//! Valkey on :6380): command counts are read from the server's `INFO commandstats`, so
//! another client's commands would be counted too. `bench/run.sh` runs the full matrix.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "../tests/support/mod.rs"]
mod support;

use std::time::{Duration, Instant};

use keylens::app::App;
use keylens::worker::{RequestId, ScanProgress, Update};
use keylens_conn::{Conn, KeyValue, ServerInfo, Value};

const VERSION: &str = "0.1.8";

fn url() -> String {
    std::env::var("KEYLENS_BENCH_URL").unwrap_or_else(|_| "redis://127.0.0.1:6380".into())
}

fn hostport() -> String {
    url().trim_start_matches("redis://").to_string()
}

/// Every command the server has executed, minus INFO (which only the bench sends).
async fn commands(admin: &Conn) -> u64 {
    let raw = keylens_conn::value::display_string(
        &admin
            .test_cmd("INFO", vec!["commandstats".into()])
            .await
            .unwrap(),
    );
    raw.lines()
        .filter(|l| l.starts_with("cmdstat_") && !l.starts_with("cmdstat_info:"))
        .filter_map(|l| {
            l.split("calls=")
                .nth(1)?
                .split(',')
                .next()?
                .parse::<u64>()
                .ok()
        })
        .sum()
}

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
        _ => 0,
    }
}

async fn seed(admin: &Conn) {
    let del = |k: &str| admin.test_cmd("UNLINK", vec![k.into()]);
    let mb = "m".repeat(1 << 20);
    for k in [
        "kb:list",
        "kb:hash",
        "kb:set",
        "kb:zset",
        "kb:stream",
        "kb:str",
        "kb:fat",
        "kb:big",
        "kb:groups",
    ] {
        del(k).await.ok();
    }
    for chunk in (0..5_000).collect::<Vec<_>>().chunks(1_000) {
        let mut l: Vec<Value> = vec!["kb:list".into()];
        let mut h: Vec<Value> = vec!["kb:hash".into()];
        let mut z: Vec<Value> = vec!["kb:zset".into()];
        for i in chunk {
            l.push(format!("item-{i}").into());
            h.push(format!("f{i}").into());
            h.push(format!("value-{i}").into());
            z.push((*i as i64).into());
            z.push(format!("m{i}").into());
        }
        admin.test_cmd("RPUSH", l).await.unwrap();
        admin.test_cmd("HSET", h).await.unwrap();
        admin.test_cmd("ZADD", z).await.unwrap();
    }
    let mut s: Vec<Value> = vec!["kb:set".into()];
    s.extend((0..500).map(|i: i64| Value::from(i)));
    admin.test_cmd("SADD", s).await.unwrap();
    let xs: Vec<(&'static str, Vec<Value>)> = (0..5_000)
        .map(|i| {
            (
                "XADD",
                vec!["kb:stream".into(), "*".into(), "n".into(), i.into()],
            )
        })
        .collect();
    admin.test_pipeline(&xs).await.unwrap();
    admin
        .test_cmd("SET", vec!["kb:str".into(), "s".repeat(32 << 20).into()])
        .await
        .unwrap();
    for _ in 0..4 {
        let mut argv: Vec<Value> = vec!["kb:fat".into()];
        argv.extend((0..50).map(|_| Value::from(mb.as_str())));
        admin.test_cmd("RPUSH", argv).await.unwrap();
    }
    for chunk in (0..1_000_000).collect::<Vec<_>>().chunks(10_000) {
        let mut argv: Vec<Value> = vec!["kb:big".into()];
        argv.extend(chunk.iter().map(|i| Value::from(*i as i64)));
        admin.test_cmd("RPUSH", argv).await.unwrap();
    }
    admin
        .test_cmd(
            "XADD",
            vec!["kb:groups".into(), "*".into(), "f".into(), "v".into()],
        )
        .await
        .unwrap();
    for g in 0..20 {
        admin
            .test_cmd(
                "XGROUP",
                vec![
                    "CREATE".into(),
                    "kb:groups".into(),
                    format!("g{g}").into(),
                    "0".into(),
                ],
            )
            .await
            .unwrap();
        admin
            .test_cmd(
                "XGROUP",
                vec![
                    "CREATECONSUMER".into(),
                    "kb:groups".into(),
                    format!("g{g}").into(),
                    "c".into(),
                ],
            )
            .await
            .unwrap();
    }
    // 20 BullMQ-shaped queues.
    for q in 0..20 {
        admin
            .test_cmd(
                "HSET",
                vec![
                    format!("bull:bq{q}:meta").into(),
                    "version".into(),
                    "bullmq:6.0.2".into(),
                ],
            )
            .await
            .unwrap();
        admin
            .test_cmd(
                "RPUSH",
                vec![format!("bull:bq{q}:wait").into(), "1".into(), "2".into()],
            )
            .await
            .unwrap();
    }
}

/// The events reader through an outage: refuse every connection for `secs`, then restore.
///
/// Reports connection attempts the proxy saw during the outage (the reader's whole cost
/// while the server is down) and how long after the restore live events flowed again.
/// Needs a server with BullMQ traffic: the compose Redis with its producer.
async fn outage(secs: u64) {
    if std::env::var_os("KEYLENS_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_env("KEYLENS_LOG"))
            .with_writer(std::io::stderr)
            .init();
    }
    let upstream =
        std::env::var("KEYLENS_BENCH_EVENTS_HOSTPORT").unwrap_or_else(|_| "127.0.0.1:6379".into());
    let proxy = support::FaultProxy::start(&upstream).await;
    let conn = Conn::connect(&proxy.url(), "events").await.unwrap();
    let queues: Vec<String> = [
        "emails",
        "reports",
        "image-processing",
        "exports",
        "webhooks",
    ]
    .map(String::from)
    .to_vec();
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let reader = tokio::spawn(keylens::events::run(conn, "bull".into(), queues, tx));
    let is_events = |u: &Update| matches!(u, Update::Events(e) if !e.is_empty());
    loop {
        if is_events(&rx.recv().await.unwrap()) {
            break;
        }
    }

    proxy.set_mode(support::Mode::Refuse);
    proxy.sever().await;
    let accepted_before = proxy.accepted();
    let down = Instant::now();
    let mut statuses = Vec::new();
    while down.elapsed() < Duration::from_secs(secs) {
        if let Ok(Some(Update::EventsStatus(s))) =
            tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
        {
            statuses.push(format!("{s:?}"));
        }
    }
    let attempts = proxy.accepted() - accepted_before;
    let gave_up = statuses.iter().any(|s| s.starts_with("Unavailable"));

    proxy.set_mode(support::Mode::Forward);
    let up = Instant::now();
    let recovered = tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(u) = rx.recv().await {
            if is_events(&u) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    reader.abort();
    println!(
        "{{\"version\":\"{VERSION}\",\"scenario\":\"outage:{secs}\",\"connection_attempts\":{attempts},\"gave_up\":{gave_up},\"recovered\":{recovered},\"recovery_s\":{}}}",
        if recovered {
            format!("{:.1}", up.elapsed().as_secs_f64())
        } else {
            "null".into()
        }
    );
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let scenario = args.next().expect("scenario");
    let rtt_ms: u64 = args.next().map_or(0, |a| a.parse().unwrap());
    let admin = Conn::connect(&url(), "admin").await.unwrap();

    if scenario == "seed" {
        seed(&admin).await;
        println!("{{\"seeded\":true}}");
        return;
    }

    if let Some(n) = scenario.strip_prefix("tree:") {
        let n: usize = n.parse().unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let mut app = App::new(ServerInfo::unknown(), "redis://x".into(), tx);
        let mut worst = Duration::ZERO;
        let started = Instant::now();
        let mut term = ratatui::Terminal::new(ratatui::backend::TestBackend::new(160, 50)).unwrap();
        for b in 0..n.div_ceil(500) {
            let keys = (0..500)
                .map(|i| (format!("app:{}:{}:{i}", b % 97, b), None))
                .collect();
            let t = Instant::now();
            app.apply(Update::Batch {
                scan: RequestId::default(),
                keys,
                reset: b == 0,
                progress: ScanProgress::Partial,
                scanned_pages: 1,
                unlisted: 0,
            });
            term.draw(|f| keylens::ui::draw(f, &mut app)).unwrap();
            worst = worst.max(t.elapsed());
        }
        println!(
            "{{\"version\":\"{VERSION}\",\"scenario\":\"{scenario}\",\"keys\":{},\"wall_ms\":{},\"worst_frame_ms\":{:.2}}}",
            app.tree.len(),
            started.elapsed().as_millis(),
            worst.as_secs_f64() * 1e3
        );
        return;
    }

    // The events reader connected straight to the server for `secs`, while something
    // outside (a `docker stop`/`start`) causes the outage. Reports whether live events
    // came back afterwards; CPU comes from /usr/bin/time.
    if let Some(secs) = scenario.strip_prefix("watch:") {
        let conn = Conn::connect(
            &format!(
                "redis://{}",
                std::env::var("KEYLENS_BENCH_EVENTS_HOSTPORT")
                    .unwrap_or_else(|_| "127.0.0.1:6379".into())
            ),
            "events",
        )
        .await
        .unwrap();
        let queues: Vec<String> = [
            "emails",
            "reports",
            "image-processing",
            "exports",
            "webhooks",
        ]
        .map(String::from)
        .to_vec();
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(keylens::events::run(conn, "bull".into(), queues, tx));
        let deadline = Instant::now() + Duration::from_secs(secs.parse().unwrap());
        let (mut statuses, mut last_events) = (Vec::new(), None);
        while Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
                Ok(Some(Update::EventsStatus(s))) => {
                    statuses.push(format!("{s:?}").chars().take(40).collect::<String>())
                }
                Ok(Some(Update::Events(e))) if !e.is_empty() => last_events = Some(Instant::now()),
                _ => {}
            }
        }
        println!(
            "{{\"version\":\"{VERSION}\",\"scenario\":\"watch:{secs}\",\"statuses\":{},\"events_in_last_10s\":{}}}",
            statuses.len(),
            last_events.is_some_and(|t| t.elapsed() < Duration::from_secs(10))
        );
        return;
    }

    if let Some(secs) = scenario.strip_prefix("outage_idle:") {
        let proxy = support::FaultProxy::start("127.0.0.1:6379").await;
        let _conn = Conn::connect(&proxy.url(), "idle").await.unwrap();
        proxy.set_mode(support::Mode::Refuse);
        proxy.sever().await;
        tokio::time::sleep(Duration::from_secs(secs.parse().unwrap())).await;
        println!(
            "{{\"scenario\":\"outage_idle\",\"accepted\":{}}}",
            proxy.accepted()
        );
        return;
    }
    if let Some(secs) = scenario.strip_prefix("outage:") {
        outage(secs.parse().unwrap()).await;
        return;
    }

    let proxy = support::FaultProxy::start(&hostport()).await;
    proxy.set_delay(Duration::from_millis(rtt_ms / 2));
    let conn = Conn::connect(&proxy.url(), "bench").await.unwrap();

    let before = commands(&admin).await;
    let started = Instant::now();
    let (result, kept) = match scenario.as_str() {
        s if s.starts_with("open:") => {
            let key = format!("kb:{}", &s[5..]);
            match conn.read_key(&key).await {
                Ok(r) => ("ok".to_string(), retained(&r.value)),
                Err(e) => (format!("err: {e}"), 0),
            }
        }
        "stream_info" => {
            let i = conn.stream_info("kb:groups").await.unwrap();
            assert_eq!(i.groups.len(), 20);
            ("ok".into(), 0)
        }
        "queues" => {
            let q = keylens_bullmq::BullMqLens::default()
                .all_queues(&conn)
                .await
                .unwrap();
            (format!("ok ({} queues)", q.len()), 0)
        }
        "lrange_all" => match conn
            .cmd("LRANGE", vec!["kb:big".into(), 0.into(), (-1).into()])
            .await
        {
            Ok(Value::Array(a)) => ("accepted".to_string(), a.len()),
            Ok(_) => ("accepted".to_string(), 0),
            Err(e) => (format!("refused: {e}"), 0),
        },
        other => panic!("unknown scenario {other}"),
    };
    let wall = started.elapsed();
    let after = commands(&admin).await;
    println!(
        "{{\"version\":\"{VERSION}\",\"scenario\":\"{scenario}\",\"rtt_ms\":{rtt_ms},\"wall_ms\":{:.1},\"commands\":{},\"retained_bytes\":{kept},\"result\":{:?}}}",
        wall.as_secs_f64() * 1e3,
        after.saturating_sub(before),
        result
    );
}
