//! Fault-injection tests: real servers behind a proxy that breaks the link on demand.
//!
//! Ignored by default, like `live.rs`. Run with the compose fixtures up:
//!
//! ```sh
//! cargo test --test faults -- --ignored --test-threads 1
//! ```
//!
//! Connections here use a 2s command timeout so a dead link is declared dead in seconds.
//! The production default is 20s; the behaviour under test is the same.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::time::{Duration, Instant};

use keylens::worker::{Request, RequestId, ScanProgress, Update, Worker};
use keylens_bullmq::EventsStatus;
use keylens_conn::{Conn, ConnError, ConnOptions, Value};
use support::{FaultProxy, Mode};
use tokio::sync::mpsc;

const FAST: ConnOptions = ConnOptions {
    connect_timeout: Duration::from_secs(5),
    command_timeout: Duration::from_secs(2),
};

async fn admin() -> Conn {
    Conn::connect(&format!("redis://{}", support::upstream()), "admin")
        .await
        .expect("fixtures up? `docker compose up -d`")
}

/// Wait for the next update matching `pick`, or fail after `within`.
async fn next_matching<T>(
    rx: &mut mpsc::Receiver<Update>,
    within: Duration,
    mut pick: impl FnMut(&Update) -> Option<T>,
) -> T {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let update = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("nothing matching within {within:?}"))
            .expect("sender alive");
        if let Some(t) = pick(&update) {
            return t;
        }
    }
}

/// Field `name` from an `INFO` section, as a number.
async fn info_u64(conn: &Conn, section: &str, name: &str) -> u64 {
    let raw = keylens_conn::value::display_string(
        &conn.test_cmd("INFO", vec![section.into()]).await.unwrap(),
    );
    raw.lines()
        .find_map(|l| l.strip_prefix(&format!("{name}:")))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn a_scan_on_a_half_open_link_fails_instead_of_wedging_the_worker() {
    // Measured on 0.1.7: on a blackholed link an ordinary command failed at the 20s
    // command timeout, but a SCAN page was still waiting after 60s -- fred sends scan
    // pages without a response timeout. The worker serves requests in order, so every
    // queue/pane/`m` request behind it waited too.
    // Enough keys for several SCAN pages on any fixture, so the dead link is met mid-scan
    // rather than after a complete one.
    let admin = admin().await;
    for chunk in (0..3_000).collect::<Vec<_>>().chunks(500) {
        let mut argv: Vec<Value> = Vec::new();
        for i in chunk {
            argv.push(format!("keylens:fault:{i}").into());
            argv.push("v".into());
        }
        admin.test_cmd("MSET", argv).await.unwrap();
    }

    let proxy = FaultProxy::start(&support::upstream()).await;
    let conn = Conn::connect_with(&proxy.url(), "fault", FAST)
        .await
        .unwrap();

    let (req_tx, req_rx) = mpsc::channel(8);
    let (up_tx, mut up_rx) = mpsc::channel(8);
    tokio::spawn(Worker::new(conn).run(req_rx, up_tx));

    let scan = RequestId::new(1);
    req_tx
        .send(Request::Rescan {
            scan,
            pattern: None,
            kind: None,
        })
        .await
        .unwrap();
    next_matching(&mut up_rx, Duration::from_secs(10), |u| {
        matches!(u, Update::Batch { .. }).then_some(())
    })
    .await;

    proxy.set_mode(Mode::Blackhole);
    let started = Instant::now();
    // fred prefetches one page, so the first `More` may still be served from it; keep
    // asking until the scan reaches the dead link.
    let progress = loop {
        req_tx.send(Request::More { scan }).await.unwrap();
        let progress = next_matching(&mut up_rx, Duration::from_secs(15), |u| match u {
            Update::Batch { progress, .. } => Some(progress.clone()),
            _ => None,
        })
        .await;
        if progress != ScanProgress::Partial {
            break progress;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "never reached the dead link"
        );
    };
    let took = started.elapsed();
    assert!(
        matches!(progress, ScanProgress::Failed(_)),
        "a half-open scan must fail, not report {progress:?}"
    );
    assert!(took < Duration::from_secs(8), "took {took:?} to notice");

    // The worker is not wedged: the next request is answered once the link is back.
    proxy.set_mode(Mode::Forward);
    proxy.sever().await;
    req_tx
        .send(Request::LoadSlowlog {
            id: RequestId::new(2),
        })
        .await
        .unwrap();
    next_matching(&mut up_rx, Duration::from_secs(20), |u| {
        matches!(u, Update::Slowlog { .. }).then_some(())
    })
    .await;

    for chunk in (0..3_000).collect::<Vec<_>>().chunks(500) {
        let keys: Vec<Value> = chunk
            .iter()
            .map(|i| format!("keylens:fault:{i}").into())
            .collect();
        admin.test_cmd("UNLINK", keys).await.ok();
    }
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn the_events_reader_survives_an_outage_longer_than_the_old_retry_budget() {
    // 0.1.7 gave up permanently after five consecutive XREAD failures (~15s plus
    // timeouts). A Redis restart, a failover or a sleeping laptop then left the live
    // graph dead for the rest of the session.
    let proxy = FaultProxy::start(&support::upstream()).await;
    let conn = Conn::connect_with(&proxy.url(), "events", FAST)
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
    let (tx, mut rx) = mpsc::channel(64);
    let reader = tokio::spawn(keylens::events::run(conn, "bull".into(), queues, tx));

    let events = |u: &Update| matches!(u, Update::Events(e) if !e.is_empty()).then_some(());
    next_matching(&mut rx, Duration::from_secs(30), events).await;

    // Down: every new connection refused, every open one cut.
    proxy.set_mode(Mode::Refuse);
    proxy.sever().await;
    let outage = Duration::from_secs(25);
    let down_at = Instant::now();
    let mut reconnecting = 0u32;
    while down_at.elapsed() < outage {
        match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
            Ok(Some(Update::EventsStatus(EventsStatus::Reconnecting { .. }))) => {
                reconnecting += 1;
            }
            Ok(Some(Update::EventsStatus(EventsStatus::Unavailable(why)))) => {
                panic!(
                    "gave up on a transient outage after {:?}: {why}",
                    down_at.elapsed()
                )
            }
            _ => {}
        }
    }
    assert!(
        reconnecting > 0,
        "the outage was never reported as reconnecting"
    );

    // Up again. Backoff is capped at 30s, so recovery must follow within about that.
    proxy.set_mode(Mode::Forward);
    let up_at = Instant::now();
    next_matching(&mut rx, Duration::from_secs(45), |u| {
        matches!(u, Update::EventsStatus(EventsStatus::Live)).then_some(())
    })
    .await;
    next_matching(&mut rx, Duration::from_secs(30), events).await;
    eprintln!(
        "events recovered {:?} after the link returned ({reconnecting} reconnecting reports)",
        up_at.elapsed()
    );
    reader.abort();
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn an_xread_acl_denial_stops_at_once() {
    // Identical on every attempt, so retrying it is pure cost. 0.1.7 spent five attempts
    // and ~15s on it before saying anything.
    let admin = admin().await;
    let user = "keylens-test-noxread";
    admin
        .test_cmd(
            "ACL",
            vec![
                "SETUSER".into(),
                user.into(),
                "reset".into(),
                "on".into(),
                ">pw".into(),
                "~*".into(),
                "+@all".into(),
                "-xread".into(),
            ],
        )
        .await
        .unwrap();
    let conn = Conn::connect_with(
        &format!("redis://{user}:pw@{}", support::upstream()),
        "events",
        FAST,
    )
    .await
    .unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    let started = Instant::now();
    let reader = tokio::spawn(keylens::events::run(
        conn,
        "bull".into(),
        vec!["emails".into()],
        tx,
    ));

    let mut retried = false;
    let why = loop {
        match tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("an answer")
        {
            Some(Update::EventsStatus(EventsStatus::Unavailable(why))) => break why,
            Some(Update::EventsStatus(EventsStatus::Reconnecting { .. })) => retried = true,
            _ => {}
        }
    };
    admin
        .test_cmd("ACL", vec!["DELUSER".into(), user.into()])
        .await
        .ok();
    assert!(!retried, "an ACL denial was retried");
    assert!(why.contains("NOPERM"), "{why}");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert!(
        reader.await.is_ok(),
        "the reader returns rather than looping"
    );
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn rotated_credentials_stop_reconnecting_and_say_so() {
    // Measured on 0.1.7: after the password changed and the connection dropped, every
    // command failed with a bare "Request timed out", while fred retried the stale
    // password in the background forever.
    let admin = admin().await;
    let user = "keylens-test-rotate";
    admin
        .test_cmd(
            "ACL",
            vec![
                "SETUSER".into(),
                user.into(),
                "reset".into(),
                "on".into(),
                ">old".into(),
                "~*".into(),
                "+@all".into(),
            ],
        )
        .await
        .unwrap();
    let conn = Conn::connect_with(
        &format!("redis://{user}:old@{}", support::upstream()),
        "rotate",
        FAST,
    )
    .await
    .unwrap();
    conn.cmd("TYPE", vec!["k".into()]).await.unwrap();

    admin
        .test_cmd(
            "ACL",
            vec![
                "SETUSER".into(),
                user.into(),
                "resetpass".into(),
                ">new".into(),
            ],
        )
        .await
        .unwrap();
    admin
        .test_cmd("CLIENT", vec!["KILL".into(), "USER".into(), user.into()])
        .await
        .unwrap();

    // The first command after the drop may race the reconnect; within a few it must be
    // the specific, terminal error.
    let mut last = None;
    for _ in 0..5 {
        match conn.cmd("TYPE", vec!["k".into()]).await {
            Err(ConnError::AuthLost(why)) => {
                last = Some(why);
                break;
            }
            other => {
                last = None;
                let _ = other;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    let why = last.expect("credentials rejected on reconnect must surface as AuthLost");
    assert!(why.contains("WRONGPASS"), "{why}");

    // ...and it stops trying: the server's auth-failure counter holds still.
    let before = info_u64(&admin, "stats", "acl_access_denied_auth").await;
    tokio::time::sleep(Duration::from_secs(12)).await; // past fred's 10s backoff cap
    let after = info_u64(&admin, "stats", "acl_access_denied_auth").await;
    admin
        .test_cmd("ACL", vec!["DELUSER".into(), user.into()])
        .await
        .ok();
    assert_eq!(before, after, "still retrying a rejected password");

    // Commands fail fast now, not after a timeout.
    let started = Instant::now();
    assert!(matches!(
        conn.cmd("TYPE", vec![Value::from("k")]).await,
        Err(ConnError::AuthLost(_))
    ));
    assert!(started.elapsed() < Duration::from_millis(100));
}

#[tokio::test]
#[ignore = "requires docker compose fixtures"]
async fn a_dropped_connection_reconnects_on_its_own() {
    // The baseline the others build on: a severed link (a restart, an LB reset) comes
    // back without the user doing anything.
    let proxy = FaultProxy::start(&support::upstream()).await;
    let conn = Conn::connect_with(&proxy.url(), "fault", FAST)
        .await
        .unwrap();
    conn.cmd("TYPE", vec!["k".into()]).await.unwrap();
    proxy.sever().await;
    let started = Instant::now();
    let mut ok = false;
    while started.elapsed() < Duration::from_secs(20) {
        if conn.cmd("TYPE", vec!["k".into()]).await.is_ok() {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(ok, "never reconnected");
    eprintln!("reconnected after {:?}", started.elapsed());
}
