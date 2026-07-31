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

    let mut compared = 0;
    for (i, key) in sample.iter().enumerate() {
        let individual = conn.key_meta(key).await.unwrap().kind;
        let pipelined = Kind::parse(&keylens_conn::value::display_string(&piped[i]));

        // The fixture churns constantly: a job can be removed by retention between the
        // pipelined read and the individual one. A key that has since vanished says
        // nothing about reply ordering, which is the invariant under test.
        if individual == Kind::None || pipelined == Kind::None {
            continue;
        }

        assert_eq!(pipelined, individual, "type mismatch for {key} at index {i}");
        compared += 1;
    }
    assert!(compared >= 5, "only {compared} keys survived long enough to compare");
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
async fn pipelined_queue_counts_match_individual_counts() {
    // The queue table is one pipeline of 8 commands per queue. If the reply order ever
    // drifted, every count in the dashboard would be attributed to the wrong queue --
    // and it would look completely plausible.
    use keylens_bullmq::{BullMqLens, State};
    use keylens_conn::Value;

    let conn = conn().await;
    let lens = BullMqLens::default();
    let queues = lens.all_queues(&conn).await.unwrap();
    assert!(queues.len() >= 2, "need a few queues to catch a misalignment");

    for q in &queues {
        for state in State::ALL {
            let key = format!("bull:{}:{}", q.name, state.suffix());
            let direct = conn
                .cmd(state.count_cmd(), vec![Value::from(key.as_str())])
                .await
                .unwrap()
                .as_u64()
                .unwrap_or(0);

            // Counts move constantly on a live queue, so compare loosely -- the failure
            // this guards against is a wholesale mix-up, not a few jobs of drift.
            let piped = q.count(state);
            let delta = piped.abs_diff(direct);
            assert!(
                delta < 500,
                "{}/{} pipelined {piped} vs direct {direct} -- reply misalignment?",
                q.name,
                state.label()
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn reads_a_failed_job_end_to_end() {
    use keylens_bullmq::{BullMqLens, State};

    let conn = conn().await;
    let lens = BullMqLens::default();

    let jobs = lens.jobs(&conn, "image-processing", State::Failed, 0, 5).await.unwrap();
    assert!(!jobs.is_empty(), "producer should have failed some jobs");
    // Failed is a ZSET scored by finish time.
    assert!(jobs[0].score.is_some_and(|s| s > 1.0e12), "expected a ms timestamp score");

    let job = lens
        .job(&conn, "image-processing", &jobs[0].id)
        .await
        .unwrap()
        .expect("job should still exist");

    assert_eq!(job.id, jobs[0].id);
    assert!(job.has_failed());
    assert!(!job.failed_reason.is_empty());
    // The trap this guards: `stacktrace` is a JSON array, and `attemptsMade` is `atm`.
    assert!(!job.stacktrace.is_empty(), "stacktrace should parse out of the JSON array");
    assert!(job.stacktrace[0].contains("at "), "frames expected: {}", job.stacktrace[0]);
    assert!(job.attempts_made > 0, "v6 stores attempts as `atm`, not `attemptsMade`");
    assert!(job.data.starts_with('{'), "payload should be JSON");
    assert!(job.duration_ms().is_some(), "a finished job has both timestamps");
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn list_backed_states_return_ids_without_scores() {
    use keylens_bullmq::{BullMqLens, State};

    let conn = conn().await;
    let lens = BullMqLens::default();
    // `reports` carries a deep backlog thanks to the pause cycle.
    let jobs = lens.jobs(&conn, "reports", State::Waiting, 0, 5).await.unwrap();
    for j in &jobs {
        assert!(j.score.is_none(), "wait is a LIST and has no score");
        assert!(!j.id.is_empty());
    }
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn a_missing_job_reads_as_none_not_an_error() {
    use keylens_bullmq::BullMqLens;
    let conn = conn().await;
    let lens = BullMqLens::default();
    assert!(lens.job(&conn, "emails", "does-not-exist-99999").await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn the_events_stream_delivers_live_throughput() {
    // The M5 kill gate, tested rather than eyeballed: attach to the events streams and
    // confirm real events arrive with usable timestamps within a few seconds.
    use keylens::events::run;
    use keylens::worker::Update;
    use keylens_bullmq::events::EventKind;
    use tokio::sync::mpsc;

    let conn = conn().await;
    let queues = vec![
        "emails".to_string(),
        "image-processing".to_string(),
        "webhooks".to_string(),
    ];

    let (tx, mut rx) = mpsc::channel::<Update>(64);
    let reader = tokio::spawn(run(conn, "bull".into(), queues.clone(), tx));

    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);

    while events.len() < 20 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(Update::Events(batch))) => events.extend(batch),
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }
    reader.abort();

    assert!(
        events.len() >= 20,
        "expected a stream of events from a running producer, got {}",
        events.len()
    );

    // Events must be attributed to a watched queue, not to the wrong stream.
    for e in &events {
        assert!(queues.contains(&e.queue), "unexpected queue {}", e.queue);
    }

    // Timestamps come from the entry ids, so they must be plausible wall-clock ms.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    for e in &events {
        let age = now_ms - e.at_ms;
        assert!(
            (-5_000..60_000).contains(&age),
            "event timestamp {} is {age}ms from now -- entry id parsing is wrong",
            e.at_ms
        );
    }

    // The producer completes and fails jobs constantly, so both should appear.
    assert!(
        events.iter().any(|e| e.kind == EventKind::Completed),
        "expected completed events"
    );
    assert!(
        events.iter().any(|e| matches!(e.kind, EventKind::Active | EventKind::Added)),
        "expected lifecycle events"
    );
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn throughput_buckets_populate_from_a_live_stream() {
    use keylens::events::run;
    use keylens::worker::Update;
    use keylens_bullmq::Throughput;
    use tokio::sync::mpsc;

    let conn = conn().await;
    let (tx, mut rx) = mpsc::channel::<Update>(64);
    let reader = tokio::spawn(run(conn, "bull".into(), vec!["emails".into()], tx));

    let mut throughput = Throughput::default();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(Update::Events(batch))) => {
                for e in batch {
                    throughput.record(&e.queue, e.kind, e.at_ms);
                }
                if throughput.series("emails").is_some_and(|s| s.seen >= 10) {
                    break;
                }
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }
    reader.abort();

    let series = throughput.series("emails").expect("emails should have produced events");
    assert!(series.seen >= 10, "only saw {} events", series.seen);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // The window must actually contain the events -- this is what caught the graph
    // rendering empty while the rate counter read non-zero.
    let window = series.window(now, 30, |b| b.total);
    assert!(
        window.iter().sum::<u64>() > 0,
        "events were recorded but the 30s window is empty -- bucket/now mismatch"
    );
    assert!(series.rate(now, 10) > 0.0, "rate should be non-zero right after a burst");
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn valkey_is_detected_as_valkey() {
    let conn = Conn::connect(&valkey_url(), "test").await.expect("valkey fixture up?");
    assert_eq!(conn.server().vendor, keylens_conn::Vendor::Valkey);
    // Vendor must come from INFO, not from the port or the URL scheme.
    assert!(!conn.server().version.is_empty());
}
