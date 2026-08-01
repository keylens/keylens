//! The live event-stream reader.
//!
//! Runs on its own task with its own connection, deliberately: `XREAD BLOCK` occupies the
//! connection for the duration of the block, so sharing the worker's connection would
//! stall every key lookup behind it.
//!
//! One `XREAD` covers every queue at once — Redis takes multiple streams in a single call
//! — so watching 40 queues costs one blocked connection, not 40.

use keylens_bullmq::QueueKeys;
use keylens_bullmq::events::{EventKind, entry_id_ms};
use keylens_conn::{Conn, Value};
use tokio::sync::mpsc::Sender;
use tracing::{debug, warn};

use crate::worker::Update;

/// How long a single `XREAD` blocks before returning empty-handed. Short enough that a
/// shutdown is noticed promptly, long enough that an idle server sees ~one command/sec.
const BLOCK_MS: i64 = 1_000;
/// Entries per stream per read. A burst larger than this is simply picked up next loop.
const COUNT: i64 = 500;

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

    let keys: Vec<String> = queues
        .iter()
        .map(|q| QueueKeys::new(&prefix, q).events())
        .collect();

    // `$` means "only entries added from now on". Reading history would replay hours of
    // events into the first second of the graph and draw a spike that never happened.
    let mut ids: Vec<String> = vec!["$".to_string(); keys.len()];

    if tx.send(Update::EventsAttached).await.is_err() {
        return;
    }

    loop {
        let mut args: Vec<Value> = vec![
            "COUNT".into(),
            COUNT.into(),
            "BLOCK".into(),
            BLOCK_MS.into(),
            "STREAMS".into(),
        ];
        args.extend(keys.iter().map(|k| Value::from(k.as_str())));
        args.extend(ids.iter().map(|i| Value::from(i.as_str())));

        let reply = match conn.cmd("XREAD", args).await {
            Ok(v) => v,
            Err(e) => {
                // A blocking read that times out is not an error, but a dropped connection
                // is. Either way, backing off and retrying is the right move -- the graph
                // degrades rather than the app dying.
                warn!(error = %e, "xread failed; retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

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
    fn unknown_streams_and_junk_replies_are_ignored() {
        let (keys, queues, mut ids) = setup();
        let reply = Value::Array(vec![Value::Array(vec![
            Value::from("bull:not-watched:events"),
            Value::Array(vec![entry("400-0", "completed")]),
        ])]);
        assert!(parse_xread(&reply, &keys, &queues, &mut ids).is_empty());
        assert!(parse_xread(&Value::from("nope"), &keys, &queues, &mut ids).is_empty());
    }
}
