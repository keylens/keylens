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

use std::time::Duration;

use keylens_conn::{Conn, KeyValue, Kind};

fn url() -> String {
    std::env::var("KEYLENS_TEST_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

fn valkey_url() -> String {
    std::env::var("KEYLENS_TEST_VALKEY_URL").unwrap_or_else(|_| "redis://127.0.0.1:6380".into())
}

async fn conn() -> Conn {
    Conn::connect(&url(), "test")
        .await
        .expect("fixtures up? `docker compose up -d`")
}

/// How long to wait for the producer to terminally fail a job.
const FAILED_JOB_WAIT: Duration = Duration::from_secs(90);

/// Wait until some queue has a terminally failed job, and return that queue's `failed` set.
///
/// A job only reaches `failed` once every attempt is exhausted, and the producer fails
/// each attempt at a decaying probability -- which for `emails` works out to roughly one
/// job in a thousand. The seed creates 38 emails jobs and the tick adds about one a
/// second, so `bull:emails:failed` does not exist for the first *quarter of an hour* after
/// the fixtures come up. Naming that queue and asserting the key was already there was
/// asserting that a 0.1% event had happened yet; it passed only when CI was slow enough.
///
/// `image-processing` and `reports` fail terminally about one job in twenty, so they get
/// there within seconds of the seed. Ordered accordingly, and returning whichever queue
/// arrives first keeps the test about the reply shape rather than about the odds.
async fn await_failed_jobs(conn: &Conn) -> (String, Vec<(String, f64)>) {
    const QUEUES: [&str; 5] = [
        "image-processing",
        "reports",
        "exports",
        "emails",
        "webhooks",
    ];

    let started = std::time::Instant::now();
    loop {
        for queue in QUEUES {
            let key = format!("bull:{queue}:failed");
            // A queue with no failures yet has no key at all, and `ZRANGE` on a missing
            // key is an empty array rather than an error -- so this is one check, not two.
            if let Ok(KeyValue::ZSet(entries)) = conn.read_value(&key, Kind::ZSet, 0).await
                && !entries.is_empty()
            {
                return (queue.to_string(), entries);
            }
        }

        assert!(
            started.elapsed() < FAILED_JOB_WAIT,
            "no queue accumulated a terminally failed job within {FAILED_JOB_WAIT:?} -- is \
             the producer running? (`docker compose up -d`)"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
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
    assert!(
        !keys.is_empty(),
        "no bull:* keys -- is the producer running?"
    );
    assert!(keys.iter().any(|k| k.ends_with(":meta")));
    assert!(keys.iter().any(|k| k.ends_with(":events")));
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn type_filter_is_applied_server_side() {
    let conn = conn().await;
    let streams = scan_all(&conn, Some("bull:*"), Some("stream")).await;
    assert!(
        !streams.is_empty(),
        "BullMQ writes an events stream per queue"
    );

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
    let KeyValue::Hash(fields) = conn
        .read_value("bull:emails:meta", Kind::Hash, 0)
        .await
        .unwrap()
    else {
        panic!("expected a hash");
    };
    let version = fields
        .iter()
        .find(|(f, _)| f == "version")
        .map(|(_, v)| v.clone());
    assert!(
        version.is_some_and(|v| v.starts_with("bullmq:")),
        "meta.version should carry `bullmq:<version>`"
    );

    // failed is a ZSET scored by finish timestamp.
    let (queue, entries) = await_failed_jobs(&conn).await;
    let failed = conn
        .key_meta(&format!("bull:{queue}:failed"))
        .await
        .unwrap();
    assert_eq!(failed.kind, Kind::ZSet);
    assert!(
        entries[0].1 > 1.0e12,
        "score should be a ms timestamp, got {}",
        entries[0].1
    );

    // events is a stream.
    let events = conn
        .read_value("bull:emails:events", Kind::Stream, 0)
        .await
        .unwrap();
    let KeyValue::Stream(entries) = events else {
        panic!("expected a stream")
    };
    assert!(!entries.is_empty());
    assert!(entries[0].fields.iter().any(|(f, _)| f == "event"));
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn failed_jobs_carry_a_real_stack_trace() {
    // The whole point of the failed-job viewer. If stacktrace is empty, the pane is
    // useless no matter how it's rendered.
    let conn = conn().await;
    // Same hazard as above: pinning one queue asserts that *that* queue has already lost a
    // job, which is a race against the producer rather than a property of the viewer.
    let (queue, failed) = await_failed_jobs(&conn).await;

    let job_id = &failed[0].0;
    let key = format!("bull:{queue}:{job_id}");
    let KeyValue::Hash(fields) = conn.read_value(&key, Kind::Hash, 0).await.unwrap() else {
        panic!("expected a hash");
    };

    let get = |name: &str| {
        fields
            .iter()
            .find(|(f, _)| f == name)
            .map(|(_, v)| v.clone())
    };

    let reason = get("failedReason").expect("failedReason missing");
    assert!(!reason.is_empty());

    let stacktrace = get("stacktrace").expect("stacktrace missing");
    assert!(
        stacktrace.contains("at "),
        "stacktrace should have real frames, got: {stacktrace}"
    );
    assert!(
        get("data").is_some_and(|d| d.starts_with('{')),
        "payload should be JSON"
    );
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
    assert_eq!(
        piped.len(),
        sample.len(),
        "pipeline must return one reply per command"
    );

    let mut compared = 0;
    for (i, key) in sample.iter().enumerate() {
        let individual = conn.key_meta(key).await.unwrap().kind;
        let pipelined = Kind::parse(&keylens_conn::value::display_string(
            piped[i].as_ref().unwrap(),
        ));

        // The fixture churns constantly: a job can be removed by retention between the
        // pipelined read and the individual one. A key that has since vanished says
        // nothing about reply ordering, which is the invariant under test.
        if individual == Kind::None || pipelined == Kind::None {
            continue;
        }

        assert_eq!(
            pipelined, individual,
            "type mismatch for {key} at index {i}"
        );
        compared += 1;
    }
    assert!(
        compared >= 5,
        "only {compared} keys survived long enough to compare"
    );
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
    assert!(
        clients.iter().all(|c| !c.id.is_empty()),
        "every row needs an id"
    );
    assert!(
        clients.iter().all(|c| !c.addr.is_empty()),
        "every row needs an addr"
    );
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
        assert!(
            !e.command.is_empty(),
            "a logged entry should carry its command"
        );
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
    assert!(
        queues.len() >= 2,
        "need a few queues to catch a misalignment"
    );

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

    let jobs = lens
        .jobs(&conn, "image-processing", State::Failed, 0, 5)
        .await
        .unwrap();
    assert!(!jobs.is_empty(), "producer should have failed some jobs");
    // Failed is a ZSET scored by finish time.
    assert!(
        jobs[0].score.is_some_and(|s| s > 1.0e12),
        "expected a ms timestamp score"
    );

    let job = lens
        .job(&conn, "image-processing", &jobs[0].id)
        .await
        .unwrap()
        .expect("job should still exist");

    assert_eq!(job.id, jobs[0].id);
    assert!(job.has_failed());
    assert!(!job.failed_reason.is_empty());
    // The trap this guards: `stacktrace` is a JSON array, and `attemptsMade` is `atm`.
    assert!(
        !job.stacktrace.is_empty(),
        "stacktrace should parse out of the JSON array"
    );
    assert!(
        job.stacktrace[0].contains("at "),
        "frames expected: {}",
        job.stacktrace[0]
    );
    assert!(
        job.attempts_made > 0,
        "v6 stores attempts as `atm`, not `attemptsMade`"
    );
    assert!(job.data.starts_with('{'), "payload should be JSON");
    assert!(
        job.duration_ms().is_some(),
        "a finished job has both timestamps"
    );
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn list_backed_states_return_ids_without_scores() {
    use keylens_bullmq::{BullMqLens, State};

    let conn = conn().await;
    let lens = BullMqLens::default();
    // `reports` carries a deep backlog thanks to the pause cycle.
    let jobs = lens
        .jobs(&conn, "reports", State::Waiting, 0, 5)
        .await
        .unwrap();
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
    assert!(
        lens.job(&conn, "emails", "does-not-exist-99999")
            .await
            .unwrap()
            .is_none()
    );
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
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::Active | EventKind::Added)),
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

    let series = throughput
        .series("emails")
        .expect("emails should have produced events");
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
    assert!(
        series.rate(now, 10) > 0.0,
        "rate should be non-zero right after a burst"
    );
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn reads_consumer_groups_and_pending_state() {
    // BullMQ's own events streams have no groups -- workers use XREAD -- so the fixture
    // adds a stream with a healthy consumer and one that reads without acknowledging.
    let conn = conn().await;
    let info = conn.stream_info("keylens:audit").await.unwrap();

    assert!(info.length > 0, "audit stream should have entries");
    assert!(info.entries_added.is_some_and(|n| n > 0));
    assert!(!info.last_generated_id.is_empty());

    let group = info
        .groups
        .iter()
        .find(|g| g.name == "processors")
        .expect("the fixture creates a `processors` group");

    assert_eq!(group.consumer_count, 2);
    assert!(
        group.pending > 0,
        "worker-stuck never acks, so entries stay pending"
    );
    assert!(
        !group.pending_min_id.is_empty(),
        "XPENDING summary should give an id range"
    );
    assert!(!group.last_delivered_id.is_empty());

    let stuck = group
        .consumers
        .iter()
        .find(|c| c.name == "worker-stuck")
        .expect("stuck consumer should be listed");
    assert!(stuck.pending > 0, "it holds unacknowledged entries");

    let healthy = group
        .consumers
        .iter()
        .find(|c| c.name == "worker-healthy")
        .expect("healthy consumer should be listed");
    assert_eq!(healthy.pending, 0, "it acknowledges everything it reads");

    // Ranking is what makes the pane useful on a group with many consumers.
    let ranked = group.stuck_consumers();
    assert_eq!(
        ranked.first().map(|c| c.name.as_str()),
        Some("worker-stuck")
    );
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn a_stream_without_groups_reports_none_rather_than_failing() {
    // XINFO GROUPS on a group-less stream returns an empty array, not an error.
    let conn = conn().await;
    let info = conn.stream_info("bull:emails:events").await.unwrap();
    assert!(info.length > 0);
    assert!(
        info.groups.is_empty(),
        "BullMQ events streams use XREAD, not consumer groups"
    );
}

/// Recached runs behind a compose profile that needs a sibling checkout, so these skip
/// unless the URL is provided:
///
/// ```sh
/// docker compose --profile recached up -d --build
/// KEYLENS_TEST_RECACHED_URL=redis://127.0.0.1:6381 \
///   cargo test --test live -- --ignored
/// ```
fn recached_url() -> Option<String> {
    std::env::var("KEYLENS_TEST_RECACHED_URL").ok()
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn connects_and_browses_whatever_info_the_server_offers() {
    // The regression this guards: keylens used to call INFO during connect and return Err
    // if it failed, so a server that doesn't implement INFO was unreachable entirely.
    //
    // Deliberately asserts consistency rather than "Recached has no INFO" -- Recached
    // gained INFO in 0.2.3, and a test that pins a dependency's missing feature starts
    // failing the moment that dependency improves.
    let Some(url) = recached_url() else {
        eprintln!("skipping: set KEYLENS_TEST_RECACHED_URL");
        return;
    };

    let conn = Conn::connect(&url, "test")
        .await
        .expect("must connect either way");

    if conn.has_server_info() {
        assert!(
            !conn.server().fields.is_empty(),
            "INFO reported available but produced no fields"
        );
    } else {
        assert!(
            conn.server().fields.is_empty(),
            "no INFO, so there should be nothing parsed from it"
        );
    }

    // Either way the browser must work, which is the point.
    let keys = scan_all(&conn, None, None).await;
    assert!(!keys.is_empty(), "SCAN should still walk the keyspace");
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn reads_values_without_hscan_sscan_or_getrange() {
    let Some(url) = recached_url() else {
        eprintln!("skipping: set KEYLENS_TEST_RECACHED_URL");
        return;
    };
    let conn = Conn::connect(&url, "test").await.unwrap();

    use keylens_conn::Feature;
    assert!(!conn.capabilities().has(Feature::GetRange));
    assert!(!conn.capabilities().has(Feature::CursorCollectionScan));

    // Seed one key of each type the server supports.
    conn.cmd("SET", vec!["keylens:t:str".into(), "hello".into()])
        .await
        .unwrap();
    conn.cmd(
        "HSET",
        vec!["keylens:t:hash".into(), "f".into(), "v".into()],
    )
    .await
    .unwrap();
    conn.cmd("SADD", vec!["keylens:t:set".into(), "m".into()])
        .await
        .unwrap();

    // Each of these takes the size-checked fallback path.
    let s = conn
        .read_value("keylens:t:str", Kind::String, 0)
        .await
        .unwrap();
    assert!(
        matches!(s, KeyValue::String(ref v) if v == "hello"),
        "{s:?}"
    );

    let h = conn
        .read_value("keylens:t:hash", Kind::Hash, 0)
        .await
        .unwrap();
    let KeyValue::Hash(fields) = h else {
        panic!("expected a hash")
    };
    assert_eq!(fields, vec![("f".to_string(), "v".to_string())]);

    let set = conn
        .read_value("keylens:t:set", Kind::Set, 0)
        .await
        .unwrap();
    let KeyValue::Set(members) = set else {
        panic!("expected a set")
    };
    assert_eq!(members, vec!["m".to_string()]);
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn an_oversized_collection_is_declined_rather_than_read_whole() {
    // The safety property: without HSCAN, keylens measures first and refuses a big hash
    // instead of falling back to HGETALL.
    let Some(url) = recached_url() else {
        eprintln!("skipping: set KEYLENS_TEST_RECACHED_URL");
        return;
    };
    let conn = Conn::connect(&url, "test").await.unwrap();

    let key = "keylens:t:bighash";
    conn.cmd("DEL", vec![key.into()]).await.ok();
    for i in 0..(keylens_conn::value::PAGE + 50) {
        conn.cmd("HSET", vec![key.into(), format!("f{i}").into(), "v".into()])
            .await
            .unwrap();
    }

    let v = conn.read_value(key, Kind::Hash, 0).await.unwrap();
    assert!(
        matches!(v, KeyValue::TooLarge { what: "hash", .. }),
        "an oversized hash must be declined, got {v:?}"
    );
    conn.cmd("DEL", vec![key.into()]).await.ok();
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn valkey_is_detected_as_valkey() {
    let conn = Conn::connect(&valkey_url(), "test")
        .await
        .expect("valkey fixture up?");
    assert_eq!(conn.server().vendor, keylens_conn::Vendor::Valkey);
    // Vendor must come from INFO, not from the port or the URL scheme.
    assert!(!conn.server().version.is_empty());
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn one_bad_command_does_not_fail_the_whole_pipeline() {
    // `pipe.all()` collapsed a pipeline into a single Result, so one WRONGTYPE took the
    // rest of the batch down with it: a single mistyped queue key blanked the entire queue
    // table, and one bad key dropped the types of all 500 keys in a scan batch.
    let conn = conn().await;
    let good = "keylens:pipe:good";
    let wrong = "keylens:pipe:wrongtype";

    conn.cmd("DEL", vec![good.into(), wrong.into()]).await.ok();
    conn.cmd("SET", vec![good.into(), "v".into()])
        .await
        .unwrap();
    // A string, so LLEN against it is a genuine WRONGTYPE from a real server.
    conn.cmd("SET", vec![wrong.into(), "v".into()])
        .await
        .unwrap();

    let replies = conn
        .pipeline(&[
            ("TYPE", vec![good.into()]),
            ("LLEN", vec![wrong.into()]),
            ("TYPE", vec![good.into()]),
        ])
        .await
        .expect("the pipeline itself succeeded; only one command in it failed");

    assert_eq!(replies.len(), 3, "one slot per command, always");
    assert!(replies[0].is_ok(), "a good command keeps its reply");
    assert!(replies[1].is_err(), "the bad one reports its own failure");
    assert!(
        replies[2].is_ok(),
        "and a command *after* the failure still lands: {:?}",
        replies[2]
    );

    conn.cmd("DEL", vec![good.into(), wrong.into()]).await.ok();
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn a_queue_with_a_mistyped_key_still_renders_its_other_counts() {
    // The end-to-end version of the above: `summaries` maps a flat pipeline back onto
    // queues by position, so a failure has to occupy its slot rather than void the table.
    use keylens_bullmq::{BullMqLens, State};

    let conn = conn().await;
    let lens = BullMqLens::new("keylens-pipetest");
    let meta = "keylens-pipetest:q:meta";
    let wait = "keylens-pipetest:q:wait";
    let failed = "keylens-pipetest:q:failed";

    conn.cmd("DEL", vec![meta.into(), wait.into(), failed.into()])
        .await
        .ok();
    conn.cmd("HSET", vec![meta.into(), "paused".into(), "1".into()])
        .await
        .unwrap();
    // `wait` should be a LIST; make it a string so its LLEN is a WRONGTYPE.
    conn.cmd("SET", vec![wait.into(), "corrupt".into()])
        .await
        .unwrap();
    conn.cmd("ZADD", vec![failed.into(), 1.into(), "job-1".into()])
        .await
        .unwrap();

    let queues = lens
        .all_queues(&conn)
        .await
        .expect("one bad key must not fail the whole listing");

    let q = queues.iter().find(|q| q.name == "q").expect("queue listed");
    assert!(q.paused, "meta still read correctly");
    assert_eq!(q.count(State::Failed), 1, "the good count survives");
    assert_eq!(q.count(State::Waiting), 0, "the bad one degrades to zero");

    conn.cmd("DEL", vec![meta.into(), wait.into(), failed.into()])
        .await
        .ok();
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn a_zero_limit_page_reads_nothing_rather_than_everything() {
    // Against a real server, because the bug was Redis semantics: `LRANGE key 0 -1` is the
    // whole list, and `offset + limit - 1` produces exactly that when limit is 0.
    use keylens_bullmq::{BullMqLens, State};

    let conn = conn().await;
    let lens = BullMqLens::new("keylens-zerotest");
    let wait = "keylens-zerotest:q:wait";
    let logs = "keylens-zerotest:q:job-1:logs";

    conn.cmd("DEL", vec![wait.into(), logs.into()]).await.ok();
    for i in 0..5 {
        conn.cmd("RPUSH", vec![wait.into(), format!("job-{i}").into()])
            .await
            .unwrap();
        conn.cmd("RPUSH", vec![logs.into(), format!("line-{i}").into()])
            .await
            .unwrap();
    }

    let none = lens.jobs(&conn, "q", State::Waiting, 0, 0).await.unwrap();
    assert!(none.is_empty(), "limit 0 must not return all 5 jobs");

    let no_logs = lens.job_logs(&conn, "q", "job-1", 0).await.unwrap();
    assert!(no_logs.is_empty(), "limit 0 must not return the whole log");

    // A real limit still works, so the guard didn't just break paging.
    let two = lens.jobs(&conn, "q", State::Waiting, 0, 2).await.unwrap();
    assert_eq!(two.len(), 2);
    assert_eq!(
        lens.job_logs(&conn, "q", "job-1", 3).await.unwrap().len(),
        3
    );

    conn.cmd("DEL", vec![wait.into(), logs.into()]).await.ok();
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn a_long_binary_value_is_marked_truncated() {
    // The truncation check used to measure the *rendered* string, which abbreviates binary
    // payloads to ~200 chars -- so a value that was cut came back looking complete.
    let conn = conn().await;
    let key = "keylens:t:binary-big";

    conn.cmd("DEL", vec![key.into()]).await.ok();
    // Non-UTF8 bytes, well past the 64KB cap.
    let payload: Vec<u8> = (0..80_000).map(|i| 0xF0u8.wrapping_add(i as u8)).collect();
    conn.cmd("SET", vec![key.into(), payload.as_slice().into()])
        .await
        .unwrap();

    let value = conn.read_value(key, Kind::String, 0).await.unwrap();
    let KeyValue::String(s) = value else {
        panic!("expected a string value");
    };
    assert!(
        s.contains("truncated"),
        "an 80KB value read with a 64KB cap must say it was cut: {}",
        &s[..s.len().min(120)]
    );

    conn.cmd("DEL", vec![key.into()]).await.ok();
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn stream_info_is_skipped_rather_than_failed_without_stream_support() {
    // On a server that has streams this is the positive control: the gate must not have
    // accidentally disabled the viewer on servers that work.
    use keylens_conn::Feature;

    let conn = conn().await;
    assert!(
        conn.capabilities().has(Feature::Streams),
        "the fixture server supports streams"
    );
    assert!(conn.supports_streams());

    // A stream that exists reads normally through the gate.
    let info = conn.stream_info("bull:emails:events").await.unwrap();
    assert!(
        info.length > 0 || !info.last_generated_id.is_empty(),
        "the fixture events stream should have entries"
    );
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn every_request_is_answered_even_when_the_scan_is_already_complete() {
    // The UI sets `loading` when it asks for more keys and clears it when a batch arrives.
    // The worker used to `continue` past a `More` on a finished scan, replying with
    // nothing at all -- leaving the status bar spinning on a scan that had ended.
    use keylens::worker::{Request, Update, Worker};
    use std::time::Duration;
    use tokio::sync::mpsc;

    let conn = conn().await;
    let (req_tx, req_rx) = mpsc::channel(8);
    let (up_tx, mut up_rx) = mpsc::channel(8);
    tokio::spawn(Worker::new(conn).run(req_rx, up_tx));

    // A pattern that matches nothing, so the scan reaches the end of the keyspace.
    req_tx
        .send(Request::Rescan {
            pattern: Some("keylens:no-such-prefix:*".into()),
            kind: None,
        })
        .await
        .unwrap();

    let mut complete = false;
    for _ in 0..20 {
        match tokio::time::timeout(Duration::from_secs(10), up_rx.recv())
            .await
            .expect("worker answered the rescan")
        {
            Some(Update::Batch { complete: done, .. }) => {
                if done {
                    complete = true;
                    break;
                }
                req_tx.send(Request::More).await.unwrap();
            }
            other => panic!("unexpected update: {other:?}"),
        }
    }
    assert!(complete, "the scan should reach the end of the keyspace");

    // Now ask for more past the end. This must still produce a reply.
    req_tx.send(Request::More).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(5), up_rx.recv())
        .await
        .expect("`More` past the end must be answered, not silently dropped")
        .expect("worker is still alive");

    match reply {
        Update::Batch { complete, keys, .. } => {
            assert!(complete, "still complete");
            assert!(keys.is_empty(), "and there was nothing left to send");
        }
        other => panic!("expected an empty final batch, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn a_configured_prefix_reaches_the_lens() {
    // `prefix` is documented in the README but used to be parsed and thrown away, so a
    // keyspace on a custom prefix looked like a server with no queues -- silently, since
    // "no queues here" is a legitimate answer and produces no error.
    use keylens::worker::{Request, Update, Worker};
    use std::time::Duration;
    use tokio::sync::mpsc;

    let setup = conn().await;
    let meta = "myapp:orders:meta";
    setup.cmd("DEL", vec![meta.into()]).await.ok();
    setup
        .cmd(
            "HSET",
            vec![meta.into(), "version".into(), "bullmq:6.0.2".into()],
        )
        .await
        .unwrap();

    async fn detect(conn: Conn, prefix: Option<String>) -> Vec<keylens_lens::Detection> {
        let (req_tx, req_rx) = mpsc::channel(4);
        let (up_tx, mut up_rx) = mpsc::channel(4);
        tokio::spawn(Worker::with_prefix(conn, prefix).run(req_rx, up_tx));
        req_tx.send(Request::Detect).await.unwrap();
        match tokio::time::timeout(Duration::from_secs(10), up_rx.recv())
            .await
            .expect("detection answered")
        {
            Some(Update::Detected(d)) => d,
            other => panic!("unexpected update: {other:?}"),
        }
    }

    // With the prefix configured, the custom keyspace is found.
    let found = detect(conn().await, Some("myapp".into())).await;
    let d = found
        .iter()
        .find(|d| d.lens_id == "bullmq")
        .expect("a configured prefix must be scanned");
    assert_eq!(d.prefix, "myapp");
    assert!(
        d.targets.iter().any(|t| t == "orders"),
        "queue should be listed: {:?}",
        d.targets
    );

    // Without it, the default `bull` prefix is used and `myapp:*` is invisible -- which is
    // exactly the failure the option exists to fix.
    let default = detect(conn().await, None).await;
    assert!(
        default
            .iter()
            .all(|d| !d.targets.iter().any(|t| t == "orders")),
        "the default prefix must not pick up a custom keyspace"
    );

    setup.cmd("DEL", vec![meta.into()]).await.ok();
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn the_batched_key_read_agrees_with_the_sequential_one() {
    // `TYPE`/`PTTL`/`MEMORY USAGE` moved into one pipeline and the size now overlaps the
    // value read. That is a change to *when* commands are issued, so the thing to prove is
    // that it did not change *what* they return.
    let conn = conn().await;

    for (key, seed) in [
        ("keylens:rtt:hash", vec![("HSET", vec!["f", "v"])]),
        ("keylens:rtt:list", vec![("RPUSH", vec!["a", "b", "c"])]),
        ("keylens:rtt:str", vec![("SET", vec!["hello"])]),
    ] {
        conn.cmd("DEL", vec![key.into()]).await.ok();
        for (cmd, args) in seed {
            let mut full: Vec<keylens_conn::Value> = vec![key.into()];
            full.extend(args.iter().map(|a| keylens_conn::Value::from(*a)));
            conn.cmd(cmd, full).await.unwrap();
        }
        conn.cmd("PEXPIRE", vec![key.into(), 600_000.into()])
            .await
            .unwrap();

        let sequential = conn.key_meta(key).await.unwrap();

        let head = conn.key_head(key).await.unwrap();
        let size = conn.key_size(key, head.kind).await.unwrap();

        assert_eq!(head.kind, sequential.kind, "{key}: type");
        assert_eq!(size, sequential.size, "{key}: size");
        assert!(head.ttl_ms.is_some(), "{key}: ttl should be read");
        assert_eq!(
            head.memory.is_some(),
            sequential.memory.is_some(),
            "{key}: memory availability must not change"
        );

        conn.cmd("DEL", vec![key.into()]).await.ok();
    }
}

/// The one-round-trip read must agree with the sequential path it replaced, for every type.
///
/// This is the test that matters for `read_key`. Its whole design is that five of its six
/// speculative commands fail with `WRONGTYPE`, and a slot-mapping mistake does not produce
/// an error -- it produces a *plausible* answer, a hash reporting a list's length, with
/// every individual reply well-formed. Only comparing against the slow path catches that.
#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn a_one_round_trip_read_agrees_with_the_sequential_one() {
    let conn = conn().await;

    let seed: Vec<(&'static str, &'static str, Vec<&'static str>)> = vec![
        ("keylens:rk:str", "SET", vec!["hello world"]),
        ("keylens:rk:hash", "HSET", vec!["a", "1", "b", "2"]),
        ("keylens:rk:list", "RPUSH", vec!["x", "y", "z"]),
        ("keylens:rk:set", "SADD", vec!["m1", "m2"]),
        ("keylens:rk:zset", "ZADD", vec!["1.5", "alpha"]),
        ("keylens:rk:stream", "XADD", vec!["*", "event", "completed"]),
    ];
    for (key, cmd, args) in &seed {
        let mut argv: Vec<keylens_conn::Value> = vec![(*key).into()];
        argv.extend(args.iter().map(|a| (*a).into()));
        conn.cmd(cmd, argv).await.unwrap();
    }
    conn.cmd("EXPIRE", vec!["keylens:rk:str".into(), 600.into()])
        .await
        .unwrap();

    for (key, _, _) in &seed {
        let (meta, value) = conn.read_key(key).await.unwrap();

        // The sequential path, computed independently.
        let head = conn.key_head(key).await.unwrap();
        let size = conn.key_size(key, head.kind).await.unwrap();
        let expected = conn.read_value(key, head.kind, 0).await.unwrap();

        assert_eq!(meta.kind, head.kind, "{key}: wrong type");
        assert_eq!(
            meta.size, size,
            "{key}: wrong size -- check the slot mapping"
        );
        assert_eq!(meta.ttl_ms.is_some(), head.ttl_ms.is_some(), "{key}: ttl");
        assert_eq!(
            format!("{value:?}"),
            format!("{expected:?}"),
            "{key}: speculative value differs from the sequential read"
        );
        assert!(!value.is_empty(), "{key}: read nothing back");
    }

    // A key that does not exist reads as missing, not as an error and not as an empty
    // value of whatever type the previous key happened to be.
    let (meta, value) = conn.read_key("keylens:rk:nope").await.unwrap();
    assert_eq!(meta.kind, Kind::None);
    assert_eq!(meta.size, 0);
    assert!(meta.ttl_ms.is_none());
    assert!(matches!(value, KeyValue::Missing), "{value:?}");

    for (key, _, _) in &seed {
        conn.cmd("UNLINK", vec![(*key).into()]).await.ok();
    }
}
