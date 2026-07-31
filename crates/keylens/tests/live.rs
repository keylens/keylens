//! Live integration tests against the docker-compose fixtures.
//!
//! Ignored by default so `cargo test` stays hermetic. Run them with the fixtures up:
//!
//! ```sh
//! docker compose up -d
//! cargo test --test live -- --ignored --nocapture
//! ```
//!
//! These are the tests that catch upstream drift and wrong assumptions about replies --
//! the things a mock would happily confirm for you.

use keylens_conn::{Conn, KeyValue, Kind};

fn url() -> String {
    std::env::var("KEYLENS_TEST_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

fn valkey_url() -> String {
    std::env::var("KEYLENS_TEST_VALKEY_URL").unwrap_or_else(|_| "redis://127.0.0.1:6380".into())
}

async fn conn() -> Conn {
    Conn::connect(&url(), "test").await.expect("fixtures up? `docker compose up -d`")
}

/// Walk the keyspace the way the browser does, with a bound.
async fn scan_all(conn: &Conn, pattern: Option<&str>, kind: Option<&str>) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut keys = Vec::new();
    for _ in 0..200 {
        let page = conn.scan_page(&cursor, pattern, 500, kind).await.unwrap();
        keys.extend(page.keys);
        cursor = page.cursor;
        if cursor == "0" {
            break;
        }
    }
    keys
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn scans_the_bullmq_keyspace() {
    let conn = conn().await;
    let keys = scan_all(&conn, Some("bull:*"), None).await;
    assert!(!keys.is_empty(), "no bull:* keys -- is the producer running?");
    assert!(keys.iter().any(|k| k.ends_with(":meta")));
    assert!(keys.iter().any(|k| k.ends_with(":events")));
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn type_filter_is_applied_server_side() {
    let conn = conn().await;
    let streams = scan_all(&conn, Some("bull:*"), Some("stream")).await;
    assert!(!streams.is_empty(), "BullMQ writes an events stream per queue");

    // Every returned key must genuinely be a stream, or the filter is a lie.
    for key in streams.iter().take(10) {
        let meta = conn.key_meta(key).await.unwrap();
        assert_eq!(meta.kind, Kind::Stream, "{key} is not a stream");
    }
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn reads_every_type_the_fixture_produces() {
    let conn = conn().await;

    // meta is a hash, and it carries the version marker the lens keys off.
    let meta = conn.key_meta("bull:emails:meta").await.unwrap();
    assert_eq!(meta.kind, Kind::Hash);
    let KeyValue::Hash(fields) = conn.read_value("bull:emails:meta", Kind::Hash, 0).await.unwrap()
    else {
        panic!("expected a hash");
    };
    let version = fields.iter().find(|(f, _)| f == "version").map(|(_, v)| v.clone());
    assert!(
        version.is_some_and(|v| v.starts_with("bullmq:")),
        "meta.version should carry `bullmq:<version>`"
    );

    // failed is a ZSET scored by finish timestamp.
    let failed = conn.key_meta("bull:emails:failed").await.unwrap();
    assert_eq!(failed.kind, Kind::ZSet);
    let KeyValue::ZSet(entries) = conn.read_value("bull:emails:failed", Kind::ZSet, 0).await.unwrap()
    else {
        panic!("expected a zset");
    };
    assert!(!entries.is_empty(), "producer should have failed some jobs by now");
    assert!(entries[0].1 > 1.0e12, "score should be a ms timestamp, got {}", entries[0].1);

    // events is a stream.
    let events = conn.read_value("bull:emails:events", Kind::Stream, 0).await.unwrap();
    let KeyValue::Stream(entries) = events else { panic!("expected a stream") };
    assert!(!entries.is_empty());
    assert!(entries[0].fields.iter().any(|(f, _)| f == "event"));
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn failed_jobs_carry_a_real_stack_trace() {
    // The whole point of the failed-job viewer. If stacktrace is empty, the pane is
    // useless no matter how it's rendered.
    let conn = conn().await;
    let KeyValue::ZSet(failed) =
        conn.read_value("bull:image-processing:failed", Kind::ZSet, 0).await.unwrap()
    else {
        panic!("expected a zset");
    };
    assert!(!failed.is_empty(), "no failed image-processing jobs yet");

    let job_id = &failed[0].0;
    let key = format!("bull:image-processing:{job_id}");
    let KeyValue::Hash(fields) = conn.read_value(&key, Kind::Hash, 0).await.unwrap() else {
        panic!("expected a hash");
    };

    let get = |name: &str| fields.iter().find(|(f, _)| f == name).map(|(_, v)| v.clone());

    let reason = get("failedReason").expect("failedReason missing");
    assert!(!reason.is_empty());

    let stacktrace = get("stacktrace").expect("stacktrace missing");
    assert!(
        stacktrace.contains("at "),
        "stacktrace should have real frames, got: {stacktrace}"
    );
    assert!(get("data").is_some_and(|d| d.starts_with('{')), "payload should be JSON");
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn pipelined_typing_matches_individual_typing() {
    // The browser types a whole page in one round trip. If the pipeline reply order ever
    // drifted from the request order, every key in the tree would show the wrong type.
    let conn = conn().await;
    let keys = scan_all(&conn, Some("bull:emails:*"), None).await;
    let sample: Vec<_> = keys.into_iter().take(25).collect();
    assert!(sample.len() > 5, "need a few keys to compare");

    let cmds: Vec<(&'static str, Vec<keylens_conn::Value>)> = sample
        .iter()
        .map(|k| ("TYPE", vec![keylens_conn::Value::from(k.as_str())]))
        .collect();
    let piped = conn.pipeline(&cmds).await.unwrap();
    assert_eq!(piped.len(), sample.len(), "pipeline must return one reply per command");

    for (i, key) in sample.iter().enumerate() {
        let individual = conn.key_meta(key).await.unwrap().kind;
        let pipelined = Kind::parse(&keylens_conn::value::display_string(&piped[i]));
        assert_eq!(pipelined, individual, "type mismatch for {key} at index {i}");
    }
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn client_list_parses_against_a_real_server() {
    // `CLIENT LIST` is a hand-rolled text format that drifts between versions and vendors.
    // Parsing it correctly against real output is the only way to know.
    let conn = conn().await;
    let clients = conn.client_list().await.unwrap();
    assert!(!clients.is_empty(), "we are ourselves a client");

    // Our own connection must be in there, with the fields we actually render.
    assert!(clients.iter().all(|c| !c.id.is_empty()), "every row needs an id");
    assert!(clients.iter().all(|c| !c.addr.is_empty()), "every row needs an addr");
    assert!(
        clients.iter().any(|c| !c.cmd.is_empty()),
        "at least one client should report its last command"
    );
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn slowlog_is_readable_even_when_empty() {
    // An empty slowlog is the common case on a healthy server; it must parse to an empty
    // list rather than erroring.
    let conn = conn().await;
    let entries = conn.slowlog(64).await.unwrap();
    for e in &entries {
        assert!(!e.command.is_empty(), "a logged entry should carry its command");
    }
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn standalone_reports_cluster_disabled_without_erroring() {
    let conn = conn().await;
    let topology = conn.cluster_topology().await.unwrap();
    assert!(!topology.enabled, "the fixture is standalone");
    // And we must not have gone on to ask a standalone server for CLUSTER NODES.
    assert!(topology.nodes.is_empty());
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn pubsub_channels_parse() {
    let conn = conn().await;
    let channels = conn.pubsub_channels(50).await.unwrap();
    // BullMQ workers subscribe for delayed-job wakeups, so there is usually something
    // here -- but an empty list is legitimate and must not be an error.
    for c in &channels {
        assert!(!c.name.is_empty());
    }
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn valkey_is_detected_as_valkey() {
    let conn = Conn::connect(&valkey_url(), "test").await.expect("valkey fixture up?");
    assert_eq!(conn.server().vendor, keylens_conn::Vendor::Valkey);
    // Vendor must come from INFO, not from the port or the URL scheme.
    assert!(!conn.server().version.is_empty());
}
