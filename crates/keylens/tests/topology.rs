//! Redis Cluster and Sentinel, against real topologies.
//!
//! Ignored and env-gated. With `docker compose --profile topology up -d --wait`:
//!
//! ```sh
//! KEYLENS_TEST_CLUSTER_URL=redis-cluster://127.0.0.1:7100 \
//! KEYLENS_TEST_SENTINEL_URL='redis-sentinel://:sentinel-pw@127.0.0.1:26390/0?sentinelServiceName=keylens' \
//!   cargo test --test topology -- --ignored --test-threads 1
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use keylens::worker::Update;
use keylens_bullmq::EventsStatus;
use keylens_conn::{Conn, ConnOptions, Coverage, KeyValue, Kind, Value, redact_url};
use tokio::sync::mpsc;

fn cluster_url() -> Option<String> {
    std::env::var("KEYLENS_TEST_CLUSTER_URL").ok()
}

fn sentinel_url() -> Option<String> {
    std::env::var("KEYLENS_TEST_SENTINEL_URL").ok()
}

const N: usize = 3_000;

fn key(i: usize) -> String {
    format!("keylens:topo:{i}")
}

async fn seeded_cluster() -> Option<Conn> {
    let url = cluster_url()?;
    let conn = Conn::connect(&url, "cluster").await.expect("cluster up?");
    assert!(conn.is_clustered(), "{url} is not a cluster");
    let cmds: Vec<(&'static str, Vec<Value>)> = (0..N)
        .map(|i| ("SET", vec![key(i).into(), format!("v{i}").into()]))
        .collect();
    for r in conn.test_pipeline(&cmds).await.unwrap() {
        r.unwrap();
    }
    Some(conn)
}

async fn cleanup(conn: &Conn) {
    let cmds: Vec<(&'static str, Vec<Value>)> =
        (0..N).map(|i| ("UNLINK", vec![key(i).into()])).collect();
    conn.test_pipeline(&cmds).await.ok();
}

#[tokio::test]
#[ignore = "requires the topology profile"]
async fn a_cluster_scan_visits_every_primary_exactly_once() {
    let Some(conn) = seeded_cluster().await else {
        return;
    };
    let mut scanner = conn.key_scanner(Some("keylens:topo:*"), 500, None).unwrap();
    let mut seen = Vec::new();
    while let Some(page) = scanner.next_page().await.unwrap() {
        seen.extend(page);
    }
    let unique: BTreeSet<_> = seen.iter().cloned().collect();
    let want: BTreeSet<_> = (0..N).map(key).collect();
    assert_eq!(unique, want, "keys missing or extra across primaries");
    assert_eq!(seen.len(), N, "a key was reported twice");
    cleanup(&conn).await;
}

#[tokio::test]
#[ignore = "requires the topology profile"]
async fn cross_slot_pipelines_stay_aligned_and_reads_route() {
    let Some(conn) = seeded_cluster().await else {
        return;
    };
    // GET is not on the allowlist, so read through the bounded value path.
    let cmds: Vec<(&'static str, Vec<Value>)> =
        (0..300).map(|i| ("STRLEN", vec![key(i).into()])).collect();
    let replies = conn.pipeline(&cmds).await.unwrap();
    for (i, r) in replies.iter().enumerate() {
        let want = format!("v{i}").len() as u64;
        assert_eq!(
            r.as_ref().unwrap().as_u64(),
            Some(want),
            "slot {i} answered for another key"
        );
    }
    for i in [0usize, 1, 2, 1_000, 2_999] {
        let read = conn.read_key(&key(i)).await.unwrap();
        assert_eq!(read.meta.kind, Kind::String);
        assert_eq!(read.coverage, Coverage::Complete);
        assert!(matches!(&read.value, KeyValue::String(v) if *v == format!("v{i}")));
    }
    // One cursor cannot scan a cluster; the single-cursor API says so.
    assert!(conn.scan_page("0", None, 100, None).await.is_err());
    cleanup(&conn).await;
}

#[tokio::test]
#[ignore = "requires the topology profile"]
async fn cluster_events_need_a_shared_hash_tag() {
    let Some(url) = cluster_url() else { return };
    let run = |prefix: &str| {
        let url = url.clone();
        let prefix = prefix.to_string();
        async move {
            let conn = Conn::connect(&url, "events").await.unwrap();
            let (tx, mut rx) = mpsc::channel(8);
            let task = tokio::spawn(keylens::events::run(
                conn,
                prefix,
                vec!["a".into(), "b".into(), "c".into()],
                tx,
            ));
            let status = match tokio::time::timeout(Duration::from_secs(10), rx.recv()).await {
                Ok(Some(Update::EventsStatus(s))) => s,
                other => panic!("{other:?}"),
            };
            task.abort();
            status
        }
    };
    assert!(matches!(
        run("bull").await,
        EventsStatus::Unavailable(why) if why.contains("hash slot")
    ));
    assert_eq!(run("{bull}").await, EventsStatus::Live);
}

#[tokio::test]
#[ignore = "requires the topology profile"]
async fn a_cluster_scan_that_loses_a_primary_is_not_complete() {
    // Every node is scanned concurrently; one that stops answering must make the scan
    // fail, not let the others finish and call the keyspace walked.
    let Some(conn) = seeded_cluster().await else {
        return;
    };
    let url = cluster_url().unwrap();
    let opts = ConnOptions {
        command_timeout: Duration::from_secs(2),
        ..ConnOptions::default()
    };
    let scanning = Conn::connect_with(&url, "scan", opts).await.unwrap();

    // Pause one primary for longer than the page timeout.
    let node = Conn::connect("redis://127.0.0.1:7102", "node")
        .await
        .unwrap();
    node.test_cmd("CLIENT", vec!["PAUSE".into(), 6_000.into(), "ALL".into()])
        .await
        .unwrap();

    let started = Instant::now();
    let mut scanner = scanning.key_scanner(None, 100, None).unwrap();
    let outcome = loop {
        match scanner.next_page().await {
            Ok(Some(_)) => continue,
            Ok(None) => break Ok(()),
            Err(e) => break Err(e),
        }
    };
    assert!(
        outcome.is_err(),
        "a scan missing a paused primary reported itself complete"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    tokio::time::sleep(Duration::from_secs(5)).await; // let the pause lapse
    cleanup(&conn).await;
}

#[tokio::test]
#[ignore = "requires the topology profile"]
async fn sentinel_with_a_password_connects_and_never_shows_it() {
    let Some(url) = sentinel_url() else { return };
    // The userinfo password: fred authenticates to the sentinel and the primary with it.
    let secret = url
        .split_once("://")
        .and_then(|(_, rest)| rest.rsplit_once('@'))
        .and_then(|(userinfo, _)| userinfo.split_once(':'))
        .map(|(_, pw)| pw.to_string())
        .filter(|pw| !pw.is_empty())
        .expect("the sentinel url carries a password");

    let conn = Conn::connect(&url, "sentinel").await.expect("sentinel up?");
    conn.test_cmd(
        "SET",
        vec!["keylens:sentinel".into(), "through-sentinel".into()],
    )
    .await
    .unwrap();
    let read = conn.read_key("keylens:sentinel").await.unwrap();
    assert!(matches!(&read.value, KeyValue::String(v) if v == "through-sentinel"));
    conn.test_cmd("DEL", vec!["keylens:sentinel".into()])
        .await
        .ok();

    assert!(!redact_url(&url).contains(&secret), "{}", redact_url(&url));

    // The CLI, end to end: probe output and a failed connection's error.
    let probe = std::process::Command::new(env!("CARGO_BIN_EXE_keylens"))
        .args(["--url", &url, "probe"])
        .output()
        .unwrap();
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&probe.stdout),
        String::from_utf8_lossy(&probe.stderr)
    );
    assert!(probe.status.success(), "{all}");
    assert!(
        !all.contains(&secret),
        "probe printed the sentinel password:\n{all}"
    );

    let wrong = url.replace(&format!(":{secret}@"), ":wrong-sentinel-pw@");
    let failed = std::process::Command::new(env!("CARGO_BIN_EXE_keylens"))
        .args(["--url", &wrong, "probe"])
        .env("KEYLENS_LOG", "debug")
        .output()
        .unwrap();
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&failed.stdout),
        String::from_utf8_lossy(&failed.stderr)
    );
    assert!(
        !all.contains("wrong-sentinel-pw"),
        "a failed sentinel connect echoed the password:\n{all}"
    );
}
