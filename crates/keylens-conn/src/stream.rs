//! Stream introspection: `XINFO` and `XPENDING`.
//!
//! Badly served by every existing tool, and the place Redis and Valkey usage is growing.
//! The question a stream viewer has to answer is not "what's in this stream" but "which
//! consumer is stuck, and how far behind is it" -- which needs `XINFO GROUPS`,
//! `XINFO CONSUMERS` and `XPENDING` together, not the entries.

use fred::prelude::Value;

use crate::conn::Conn;
use crate::error::Result;
use crate::value::display_string;

/// Cap on groups inspected per stream. A stream with hundreds of groups would otherwise
/// turn one key selection into hundreds of round trips.
const MAX_GROUPS: usize = 32;

#[derive(Debug, Clone, Default)]
pub struct StreamInfo {
    pub length: u64,
    pub entries_added: Option<u64>,
    pub last_generated_id: String,
    pub max_deleted_id: String,
    pub first_entry_id: String,
    pub groups: Vec<GroupInfo>,
    /// True when more groups exist than were inspected.
    pub groups_truncated: bool,
}

#[derive(Debug, Clone, Default)]
pub struct GroupInfo {
    pub name: String,
    pub consumer_count: u64,
    pub pending: u64,
    pub last_delivered_id: String,
    pub entries_read: Option<i64>,
    /// Entries added to the stream that this group has not yet read.
    ///
    /// `None` is a real answer, not a failure: Redis cannot compute lag after entries are
    /// trimmed or the id is set manually, and reporting 0 there would be a lie.
    pub lag: Option<i64>,
    pub consumers: Vec<ConsumerInfo>,
    pub pending_min_id: String,
    pub pending_max_id: String,
}

impl GroupInfo {
    /// Consumers holding pending entries, worst first -- the ones worth looking at.
    pub fn stuck_consumers(&self) -> Vec<&ConsumerInfo> {
        let mut stuck: Vec<&ConsumerInfo> =
            self.consumers.iter().filter(|c| c.pending > 0).collect();
        stuck.sort_by_key(|c| std::cmp::Reverse(c.idle_ms));
        stuck
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConsumerInfo {
    pub name: String,
    pub pending: u64,
    /// Milliseconds since this consumer's last interaction.
    pub idle_ms: i64,
    /// Redis 7.2+; milliseconds since it last *successfully read* something.
    pub inactive_ms: Option<i64>,
}

impl Conn {
    /// Everything about a stream that isn't its entries.
    pub async fn stream_info(&self, key: &str) -> Result<StreamInfo> {
        let raw = self.cmd("XINFO", vec!["STREAM".into(), key.into()]).await?;
        let f = fields(&raw);

        let mut info = StreamInfo {
            length: num(&f, "length").unwrap_or(0) as u64,
            entries_added: num(&f, "entries-added").map(|n| n as u64),
            last_generated_id: text(&f, "last-generated-id"),
            max_deleted_id: text(&f, "max-deleted-entry-id"),
            first_entry_id: text(&f, "recorded-first-entry-id"),
            ..Default::default()
        };

        let groups_raw = self.cmd("XINFO", vec!["GROUPS".into(), key.into()]).await?;
        let Value::Array(groups) = groups_raw else {
            return Ok(info);
        };

        info.groups_truncated = groups.len() > MAX_GROUPS;

        for g in groups.iter().take(MAX_GROUPS) {
            let gf = fields(g);
            let name = text(&gf, "name");
            if name.is_empty() {
                continue;
            }

            let mut group = GroupInfo {
                consumer_count: num(&gf, "consumers").unwrap_or(0) as u64,
                pending: num(&gf, "pending").unwrap_or(0) as u64,
                last_delivered_id: text(&gf, "last-delivered-id"),
                entries_read: num(&gf, "entries-read"),
                lag: num(&gf, "lag"),
                name: name.clone(),
                ..Default::default()
            };

            if let Ok(c) = self
                .cmd(
                    "XINFO",
                    vec!["CONSUMERS".into(), key.into(), name.as_str().into()],
                )
                .await
            {
                group.consumers = parse_consumers(&c);
            }

            // XPENDING's summary form gives the id range still outstanding, which is what
            // tells you *how far back* the stall goes.
            if group.pending > 0
                && let Ok(p) = self
                    .cmd("XPENDING", vec![key.into(), name.as_str().into()])
                    .await
                && let Value::Array(summary) = p
                && summary.len() >= 3
            {
                group.pending_min_id = display_string(&summary[1]);
                group.pending_max_id = display_string(&summary[2]);
            }

            info.groups.push(group);
        }

        Ok(info)
    }
}

fn parse_consumers(reply: &Value) -> Vec<ConsumerInfo> {
    let Value::Array(items) = reply else {
        return Vec::new();
    };
    items
        .iter()
        .map(|c| {
            let f = fields(c);
            ConsumerInfo {
                name: text(&f, "name"),
                pending: num(&f, "pending").unwrap_or(0) as u64,
                idle_ms: num(&f, "idle").unwrap_or(0),
                inactive_ms: num(&f, "inactive"),
            }
        })
        .filter(|c| !c.name.is_empty())
        .collect()
}

/// `XINFO` returns a flat `[field, value, ...]` array under RESP2 and a map under RESP3.
/// Both shapes appear depending on the negotiated protocol.
fn fields(v: &Value) -> Vec<(String, Value)> {
    match v {
        Value::Array(items) => items
            .chunks_exact(2)
            .map(|c| (display_string(&c[0]), c[1].clone()))
            .collect(),
        Value::Map(map) => map
            .iter()
            .map(|(k, val)| (k.as_str().unwrap_or_default().to_string(), val.clone()))
            .collect(),
        _ => Vec::new(),
    }
}

fn find<'a>(f: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    f.iter().find(|(k, _)| k == name).map(|(_, v)| v)
}

fn text(f: &[(String, Value)], name: &str) -> String {
    find(f, name).map(display_string).unwrap_or_default()
}

/// `None` for a missing field *and* for an explicit nil -- Redis returns nil for `lag`
/// when it genuinely cannot compute it.
fn num(f: &[(String, Value)], name: &str) -> Option<i64> {
    match find(f, name)? {
        Value::Null => None,
        v => v.as_i64(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flat(pairs: &[(&str, Value)]) -> Value {
        let mut out = Vec::new();
        for (k, v) in pairs {
            out.push(Value::from(*k));
            out.push(v.clone());
        }
        Value::Array(out)
    }

    #[test]
    fn parses_resp2_flat_field_arrays() {
        let v = flat(&[
            ("length", Value::from(10_044i64)),
            ("last-generated-id", Value::from("1785517561250-1")),
        ]);
        let f = fields(&v);
        assert_eq!(num(&f, "length"), Some(10_044));
        assert_eq!(text(&f, "last-generated-id"), "1785517561250-1");
    }

    #[test]
    fn a_nil_lag_stays_none_rather_than_becoming_zero() {
        // Redis cannot compute lag after trimming or XSETID. Reporting 0 would claim the
        // group is caught up when the truth is "unknown".
        let f = fields(&flat(&[("lag", Value::Null)]));
        assert_eq!(num(&f, "lag"), None);

        let f = fields(&flat(&[("lag", Value::from(0i64))]));
        assert_eq!(num(&f, "lag"), Some(0), "an explicit zero is still zero");
    }

    #[test]
    fn missing_fields_are_absent_not_defaulted() {
        let f = fields(&flat(&[("length", Value::from(1i64))]));
        assert_eq!(num(&f, "entries-added"), None);
        assert_eq!(text(&f, "nope"), "");
    }

    #[test]
    fn parses_consumers_with_idle_and_inactive() {
        let reply = Value::Array(vec![
            flat(&[
                ("name", Value::from("worker-healthy")),
                ("pending", Value::from(0i64)),
                ("idle", Value::from(395i64)),
                ("inactive", Value::from(397i64)),
            ]),
            flat(&[
                ("name", Value::from("worker-stuck")),
                ("pending", Value::from(27i64)),
                ("idle", Value::from(13_288i64)),
                ("inactive", Value::from(13_288i64)),
            ]),
        ]);

        let consumers = parse_consumers(&reply);
        assert_eq!(consumers.len(), 2);
        assert_eq!(consumers[1].name, "worker-stuck");
        assert_eq!(consumers[1].pending, 27);
        assert_eq!(consumers[1].idle_ms, 13_288);
        assert_eq!(consumers[1].inactive_ms, Some(13_288));
    }

    #[test]
    fn older_servers_without_inactive_still_parse() {
        // `inactive` is Redis 7.2+; its absence must not drop the consumer.
        let reply = Value::Array(vec![flat(&[
            ("name", Value::from("old")),
            ("pending", Value::from(3i64)),
            ("idle", Value::from(10i64)),
        ])]);
        let consumers = parse_consumers(&reply);
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].inactive_ms, None);
    }

    #[test]
    fn nameless_consumers_are_dropped() {
        let reply = Value::Array(vec![flat(&[("pending", Value::from(1i64))])]);
        assert!(parse_consumers(&reply).is_empty());
    }

    #[test]
    fn stuck_consumers_are_ranked_by_idle_time() {
        let group = GroupInfo {
            consumers: vec![
                ConsumerInfo {
                    name: "ok".into(),
                    pending: 0,
                    idle_ms: 999_999,
                    inactive_ms: None,
                },
                ConsumerInfo {
                    name: "a".into(),
                    pending: 2,
                    idle_ms: 500,
                    inactive_ms: None,
                },
                ConsumerInfo {
                    name: "b".into(),
                    pending: 27,
                    idle_ms: 13_288,
                    inactive_ms: None,
                },
            ],
            ..Default::default()
        };

        let stuck = group.stuck_consumers();
        // An idle consumer holding nothing is not stuck, however long it has been quiet.
        assert_eq!(stuck.len(), 2);
        assert_eq!(stuck[0].name, "b", "longest-idle holder comes first");
        assert_eq!(stuck[1].name, "a");
    }
}
