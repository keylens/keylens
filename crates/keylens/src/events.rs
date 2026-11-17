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
use keylens_conn::{Conn, ConnOptions, Recovery, Value, key_slot};
use tokio::sync::mpsc::Sender;
use tracing::{debug, warn};

use crate::worker::Update;

/// How long a single `XREAD` blocks before returning empty-handed. Short enough that a
/// shutdown is noticed promptly, long enough that an idle server sees ~one command/sec.
const BLOCK_MS: i64 = 1_000;
/// Entries per stream per read. A burst larger than this is simply picked up next loop.
const COUNT: i64 = 500;
/// Backoff between failed attempts, doubled per failure, capped, and jittered.
///
/// 0.1.7 gave up for good after five consecutive failures -- about fifteen seconds of
/// outage -- so a Redis restart, a failover or a laptop waking from sleep left the graph
/// dead for the rest of the session. Failures are now classified instead: a denial stops
/// at once and says why ([`Recovery::Stop`]); anything else retries forever, at most one
/// attempt per [`RETRY_MAX`], which is the whole cost of being wrong about "transient".
const RETRY_BASE: Duration = Duration::from_millis(500);
const RETRY_MAX: Duration = Duration::from_secs(30);

/// One observed event.
#[derive(Debug, Clone)]
pub struct StreamEvent {
    pub queue: String,
    pub kind: EventKind,
    pub at_ms: i64,
}

/// Exponential backoff with jitter.
///
/// Jitter so that several keylens sessions (or several readers) knocked off by the same
/// restart do not reconnect in lockstep. Drawn from the standard library's per-process
/// random hasher keys -- no RNG dependency for a few bits of spread.
#[derive(Debug)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            attempt: 0,
        }
    }

    /// Delay before the next attempt: `base * 2^n` capped at `max`, then jittered into
    /// `[d/2, d]` so the cap is never exceeded.
    pub fn next_delay(&mut self) -> Duration {
        let exp = self
            .base
            .saturating_mul(1u32 << self.attempt.min(16))
            .min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        let half = exp / 2;
        let spread = u64::try_from(half.as_millis()).unwrap_or(u64::MAX).max(1);
        let jitter = Duration::from_millis(random_u64() % spread);
        (half + jitter).min(self.max)
    }

    /// Failures since the last success.
    pub fn attempts(&self) -> u32 {
        self.attempt
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

fn random_u64() -> u64 {
    use std::hash::BuildHasher;
    std::collections::hash_map::RandomState::new().hash_one(std::time::Instant::now())
}

/// Open the reader's own connection, retrying transient failures, then follow.
///
/// Its own connection because `XREAD BLOCK` holds the connection for the duration of the
/// block, so sharing the worker's would stall every key lookup behind it.
pub async fn follow(
    url: String,
    options: ConnOptions,
    prefix: String,
    queues: Vec<String>,
    tx: Sender<Update>,
) {
    let mut backoff = Backoff::new(RETRY_BASE, RETRY_MAX);
    loop {
        match Conn::connect_with(&url, "events", options).await {
            Ok(conn) => return run(conn, prefix, queues, tx).await,
            Err(e) => {
                warn!(error = %e, "could not open a connection for the event stream");
                let status = match e.recovery() {
                    // Say so in the UI. Interactive logs go to a sink, so a silent return
                    // leaves the queue table claiming it is still "attaching…" forever.
                    Recovery::Stop(why) => {
                        tx.send(Update::EventsStatus(EventsStatus::Unavailable(why)))
                            .await
                            .ok();
                        return;
                    }
                    Recovery::Retry => EventsStatus::Reconnecting {
                        attempt: backoff.attempts().saturating_add(1),
                        error: e.to_string(),
                    },
                };
                if tx.send(Update::EventsStatus(status)).await.is_err() {
                    return;
                }
                tokio::time::sleep(backoff.next_delay()).await;
            }
        }
    }
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
    //
    // After an outage the cursors keep the last id each stream delivered, so the reader
    // resumes exactly where it stopped -- no gap and no replay. A stream still at `$`
    // (nothing seen yet) cannot be resumed; what it wrote during the outage is not shown.
    let mut ids: Vec<String> = vec!["$".to_string(); keys.len()];

    if tx
        .send(Update::EventsStatus(EventsStatus::Live))
        .await
        .is_err()
    {
        return;
    }

    let mut backoff = Backoff::new(RETRY_BASE, RETRY_MAX);

    loop {
        let reply = match conn.xread(&keys, &ids, COUNT as u64, BLOCK_MS as u64).await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, attempt = backoff.attempts(), "xread failed");
                let status = match e.recovery() {
                    // An ACL denial, an unknown command, a wrong-typed key: identical on
                    // every attempt. Stop now -- 0.1.7 spent five attempts and ~15s on it.
                    Recovery::Stop(why) => {
                        tx.send(Update::EventsStatus(EventsStatus::Unavailable(why)))
                            .await
                            .ok();
                        return;
                    }
                    Recovery::Retry => EventsStatus::Reconnecting {
                        attempt: backoff.attempts().saturating_add(1),
                        error: e.to_string(),
                    },
                };
                if tx.send(Update::EventsStatus(status)).await.is_err() {
                    return;
                }
                tokio::time::sleep(backoff.next_delay()).await;
                continue;
            }
        };

        // A read that came back is recovery. Say so once, and reset the backoff so the
        // next blip starts from the bottom again.
        if backoff.attempts() > 0 {
            backoff.reset();
            if tx
                .send(Update::EventsStatus(EventsStatus::Live))
                .await
                .is_err()
            {
                return;
            }
        }

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
                .as_chunks::<2>()
                .0
                .iter()
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
    fn backoff_climbs_to_its_cap_and_never_past_it() {
        let mut b = Backoff::new(RETRY_BASE, RETRY_MAX);
        let delays: Vec<Duration> = (0..40).map(|_| b.next_delay()).collect();
        assert!(delays.iter().all(|d| *d <= RETRY_MAX), "{delays:?}");
        assert!(
            delays[0] <= RETRY_BASE,
            "the first retry is quick: {:?}",
            delays[0]
        );
        // Jitter keeps each delay within [cap/2, cap] once the cap is reached.
        assert!(
            delays[20..].iter().all(|d| *d >= RETRY_MAX / 2),
            "{delays:?}"
        );
        assert_eq!(b.attempts(), 40);
        b.reset();
        assert_eq!(b.attempts(), 0);
        assert!(b.next_delay() <= RETRY_BASE, "reset starts from the bottom");
    }

    #[test]
    fn an_outage_of_any_length_costs_at_most_one_attempt_per_cap() {
        // The bound that replaced "give up after five": command volume during an outage.
        let mut b = Backoff::new(RETRY_BASE, RETRY_MAX);
        let hour: Duration = (0..200).map(|_| b.next_delay()).sum();
        assert!(
            hour > Duration::from_secs(60 * 45),
            "200 attempts took only {hour:?}"
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
