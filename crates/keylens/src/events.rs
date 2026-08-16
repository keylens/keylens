//! The live event-stream reader.
//!
//! Runs on its own task with its own connection, deliberately: `XREAD BLOCK` occupies the
//! connection for the duration of the block, so sharing the worker's connection would
//! stall every key lookup behind it.
//!
//! One `XREAD` covers every queue at once on standalone Redis. In Cluster, Redis requires
//! all streams in that call to share a hash slot; keylens reports live events unavailable
//! when the configured BullMQ keys do not provide a shared hash tag.

use std::time::Duration;

use keylens_bullmq::EventsStatus;
use keylens_bullmq::QueueKeys;
use keylens_bullmq::events::{EventKind, entry_id_ms};
use keylens_conn::{Conn, Value, key_slot};
use tokio::sync::mpsc::Sender;
use tracing::{debug, warn};

use crate::worker::Update;

/// How long a single `XREAD` blocks before returning empty-handed. Short enough that a
/// shutdown is noticed promptly, long enough that an idle server sees ~one command/sec.
const BLOCK_MS: i64 = 1_000;
/// Entries per stream per read. A burst larger than this is simply picked up next loop.
const COUNT: i64 = 500;
/// Consecutive `XREAD` failures before the reader stops trying.
///
/// A retry loop with no ceiling is not resilience, it's a leak: against a server that
/// denies `XREAD` by ACL, every attempt fails identically forever, and because the
/// interactive build sends logs to a sink the user never learns why the graph is empty.
/// Give up, say so, and stop spending a command a second on it.
const MAX_CONSECUTIVE_FAILURES: u32 = 5;
/// Backoff between failed reads, doubled per failure and capped.
const RETRY_BASE: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(8);

/// One observed event.
#[derive(Debug, Clone)]
pub struct StreamEvent {
    pub queue: String,
    pub kind: EventKind,
    pub at_ms: i64,
}

/// Follow every queue's events stream until the channel closes.
pub async fn run(conn: Conn, prefix: String, queues: Vec<String>, tx: Sender<Update>) {
    if queues.is_empty() {
        return;
    }

    // Ask before parking on it. `XREAD` is the one command here with no fallback, and a
    // server that doesn't have it would otherwise be probed once a second, silently,
    // for as long as keylens is open.
    if !conn.supports_streams() {
        let why = conn
            .capabilities()
            .get(keylens_conn::Feature::Streams)
            .reason()
            .unwrap_or("XREAD is not available")
            .to_string();
        tx.send(Update::EventsStatus(EventsStatus::Unavailable(why)))
            .await
            .ok();
        return;
    }

    let keys: Vec<String> = queues
        .iter()
        .map(|q| QueueKeys::new(&prefix, q).events())
        .collect();

    if conn.is_clustered()
        && keys
            .iter()
            .map(|key| key_slot(key))
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            > 1
    {
        tx.send(Update::EventsStatus(EventsStatus::Unavailable(
            "live throughput needs the watched streams in one Redis Cluster hash slot; use a shared BullMQ hash-tag prefix"
                .into(),
        )))
        .await
        .ok();
        return;
    }

    // `$` means "only entries added from now on". Reading history would replay hours of
    // events into the first second of the graph and draw a spike that never happened.
    let mut ids: Vec<String> = vec!["$".to_string(); keys.len()];

    if tx
        .send(Update::EventsStatus(EventsStatus::Live))
        .await
        .is_err()
    {
        return;
    }

    let mut failures: u32 = 0;

    loop {
        let reply = match conn.xread(&keys, &ids, COUNT as u64, BLOCK_MS as u64).await {
            Ok(v) => v,
            Err(e) => {
                // A blocking read that times out is not an error, but a dropped connection
                // is. Back off and retry -- the graph degrades rather than the app dying --
                // but only so many times, because a failure that repeats identically is a
                // permanent one and retrying it forever helps nobody.
                failures += 1;
                warn!(error = %e, failures, "xread failed");

                if failures >= MAX_CONSECUTIVE_FAILURES {
                    warn!("giving up on the events stream after {failures} failures");
                    tx.send(Update::EventsStatus(EventsStatus::Unavailable(
                        e.to_string(),
                    )))
                    .await
                    .ok();
                    return;
                }

                tokio::time::sleep(retry_delay(failures)).await;
                continue;
            }
        };

        // A read that came back resets the budget: a single blip mid-session must not
        // count towards a ceiling meant for permanent failures.
        failures = 0;

        let events = parse_xread(&reply, &keys, &queues, &mut ids);
        if events.is_empty() {
            continue;
        }

        debug!(count = events.len(), "stream events");
        if tx.send(Update::Events(events)).await.is_err() {
            return; // UI is gone
        }
    }
}

/// Parse an `XREAD` reply and advance each stream's cursor.
///
/// RESP2 returns `[[key, [[id, [f, v, ...]], ...]], ...]`; RESP3 returns a map keyed by
/// stream name. Both shapes appear in the wild depending on the negotiated protocol, so
/// both are handled.
fn parse_xread(
    reply: &Value,
    keys: &[String],
    queues: &[String],
    ids: &mut [String],
) -> Vec<StreamEvent> {
    let mut out = Vec::new();

    let streams: Vec<(String, &Value)> = match reply {
        Value::Array(items) => items
            .iter()
            .filter_map(|s| match s {
                Value::Array(pair) if pair.len() == 2 => {
                    Some((crate::events::as_text(&pair[0]), &pair[1]))
                }
                _ => None,
            })
            .collect(),
        Value::Map(map) => map
            .iter()
            .map(|(k, v)| (k.as_str().unwrap_or_default().to_string(), v))
            .collect(),
        _ => return out,
    };

    for (key, entries) in streams {
        let Some(idx) = keys.iter().position(|k| *k == key) else {
            continue;
        };
        let Value::Array(entries) = entries else {
            continue;
        };

        for entry in entries {
            let Value::Array(pair) = entry else { continue };
            if pair.len() != 2 {
                continue;
            }

            let id = as_text(&pair[0]);
            // Advance the cursor even for entries we can't classify, otherwise a single
            // unparseable entry would be re-read forever.
            ids[idx] = id.clone();

            let Some(at_ms) = entry_id_ms(&id) else {
                continue;
            };
            let Value::Array(fields) = &pair[1] else {
                continue;
            };

            // Fields are a flat [name, value, ...] list; BullMQ puts the event name under
            // the `event` key.
            let kind = fields
                .chunks_exact(2)
                .find(|c| as_text(&c[0]) == "event")
                .map(|c| EventKind::parse(&as_text(&c[1])));

            if let Some(kind) = kind {
                out.push(StreamEvent {
                    queue: queues[idx].clone(),
                    kind,
                    at_ms,
                });
            }
        }
    }

    out
}

fn as_text(v: &Value) -> String {
    v.as_string().unwrap_or_default()
}

/// Exponential backoff, capped. The cap matters more than the curve: the point is to stop
/// hammering a server that is refusing us, not to converge on some ideal interval.
fn retry_delay(failures: u32) -> Duration {
    RETRY_BASE
        .saturating_mul(1u32 << failures.min(5).saturating_sub(1))
        .min(RETRY_MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, event: &str) -> Value {
        Value::Array(vec![
            Value::from(id),
            Value::Array(vec![
                Value::from("event"),
                Value::from(event),
                Value::from("jobId"),
                Value::from("42"),
            ]),
        ])
    }

    fn setup() -> (Vec<String>, Vec<String>, Vec<String>) {
        (
            vec!["bull:emails:events".into(), "bull:reports:events".into()],
            vec!["emails".into(), "reports".into()],
            vec!["$".into(), "$".into()],
        )
    }

    #[test]
    fn parses_resp2_shape_and_maps_streams_to_queues() {
        let (keys, queues, mut ids) = setup();
        let reply = Value::Array(vec![Value::Array(vec![
            Value::from("bull:reports:events"),
            Value::Array(vec![entry("1785515393600-0", "failed")]),
        ])]);

        let events = parse_xread(&reply, &keys, &queues, &mut ids);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].queue, "reports",
            "stream key must map to the right queue"
        );
        assert_eq!(events[0].kind, EventKind::Failed);
        assert_eq!(events[0].at_ms, 1_785_515_393_600);
    }

    #[test]
    fn advances_only_the_cursor_for_the_stream_that_delivered() {
        let (keys, queues, mut ids) = setup();
        let reply = Value::Array(vec![Value::Array(vec![
            Value::from("bull:reports:events"),
            Value::Array(vec![
                entry("100-0", "completed"),
                entry("101-0", "completed"),
            ]),
        ])]);

        parse_xread(&reply, &keys, &queues, &mut ids);
        assert_eq!(ids[0], "$", "a silent stream keeps waiting for new entries");
        assert_eq!(
            ids[1], "101-0",
            "the cursor advances to the last entry seen"
        );
    }

    #[test]
    fn parses_resp3_map_shape() {
        let (keys, queues, mut ids) = setup();
        let map = keylens_conn::Map::try_from(vec![(
            "bull:emails:events",
            Value::Array(vec![entry("200-0", "completed")]),
        )])
        .unwrap();
        let reply = Value::Map(map);

        let events = parse_xread(&reply, &keys, &queues, &mut ids);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].queue, "emails");
    }

    #[test]
    fn an_unparseable_entry_still_advances_the_cursor() {
        // Otherwise one malformed entry would be re-read forever and the reader would
        // livelock on it.
        let (keys, queues, mut ids) = setup();
        let reply = Value::Array(vec![Value::Array(vec![
            Value::from("bull:emails:events"),
            Value::Array(vec![Value::Array(vec![
                Value::from("300-0"),
                Value::Array(vec![Value::from("unrelated"), Value::from("x")]),
            ])]),
        ])]);

        let events = parse_xread(&reply, &keys, &queues, &mut ids);
        assert!(events.is_empty());
        assert_eq!(ids[0], "300-0");
    }

    #[test]
    fn retries_back_off_and_stop_climbing() {
        // The point of the cap is to stop hammering a server that is refusing us. An
        // uncapped doubling would also overflow the shift long before it got useful.
        let delays: Vec<u64> = (1..=MAX_CONSECUTIVE_FAILURES)
            .map(|f| retry_delay(f).as_secs())
            .collect();

        assert_eq!(
            delays[0],
            RETRY_BASE.as_secs(),
            "first retry is immediate-ish"
        );
        assert!(
            delays.windows(2).all(|w| w[1] >= w[0]),
            "delays must never shrink: {delays:?}"
        );
        assert!(
            delays.iter().all(|d| *d <= RETRY_MAX.as_secs()),
            "capped at {}s: {delays:?}",
            RETRY_MAX.as_secs()
        );
    }

    #[test]
    fn the_retry_budget_is_finite() {
        // A loop with no ceiling is not resilience: against a permanent failure it spends
        // one command a second, forever, with the log going to a sink.
        const { assert!(MAX_CONSECUTIVE_FAILURES > 0) };
        let total: u64 = (1..MAX_CONSECUTIVE_FAILURES)
            .map(|f| retry_delay(f).as_secs())
            .sum();
        assert!(
            total <= 60,
            "the reader should give up inside a minute, not {total}s"
        );
    }

    #[test]
    fn unknown_streams_and_junk_replies_are_ignored() {
        let (keys, queues, mut ids) = setup();
        let reply = Value::Array(vec![Value::Array(vec![
            Value::from("bull:not-watched:events"),
            Value::Array(vec![entry("400-0", "completed")]),
        ])]);
        assert!(parse_xread(&reply, &keys, &queues, &mut ids).is_empty());
        assert!(parse_xread(&Value::from("nope"), &keys, &queues, &mut ids).is_empty());
    }

    #[test]
    fn cluster_event_streams_need_one_shared_hash_tag() {
        assert_eq!(
            key_slot("bull:{tenant}:emails:events"),
            key_slot("bull:{tenant}:reports:events")
        );
        assert_ne!(
            key_slot("bull:emails:events"),
            key_slot("bull:reports:events")
        );
    }
}
