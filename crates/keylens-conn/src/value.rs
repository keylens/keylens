//! Reading key metadata and values, safely.
//!
//! The rule here mirrors the no-`KEYS` rule: **never issue an unbounded collection read.**
//! `HGETALL` on a hash with two million fields blocks the server for the duration, and a
//! browsing tool that does it once on the wrong key causes an outage. Every read below is
//! either cursor-paged (`HSCAN`/`SSCAN`) or explicitly ranged (`LRANGE`, `ZRANGE`,
//! `XRANGE`, `GETRANGE`), so cost is bounded by the viewport, not by the key.

use fred::prelude::Value;

use crate::capability::Feature;
use crate::conn::Conn;
use crate::error::{ConnError, Result};

/// How much of a value a single fetch pulls back. Sized for a viewport, not for the key.
pub const PAGE: usize = 200;
/// Cap on a single string value. Longer strings are truncated with a marker.
pub const MAX_STRING_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    String,
    Hash,
    List,
    Set,
    ZSet,
    Stream,
    None,
    Other,
}

impl Kind {
    pub fn parse(s: &str) -> Self {
        match s {
            "string" => Kind::String,
            "hash" => Kind::Hash,
            "list" => Kind::List,
            "set" => Kind::Set,
            "zset" => Kind::ZSet,
            "stream" => Kind::Stream,
            "none" => Kind::None,
            _ => Kind::Other,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Kind::String => "string",
            Kind::Hash => "hash",
            Kind::List => "list",
            Kind::Set => "set",
            Kind::ZSet => "zset",
            Kind::Stream => "stream",
            Kind::None => "none",
            Kind::Other => "other",
        }
    }

    /// Short tag for the tree, where horizontal space is scarce.
    pub fn tag(&self) -> &'static str {
        match self {
            Kind::String => "str",
            Kind::Hash => "hash",
            Kind::List => "list",
            Kind::Set => "set",
            Kind::ZSet => "zset",
            Kind::Stream => "strm",
            Kind::None => "-",
            Kind::Other => "?",
        }
    }

    /// Command that reports how many elements the key holds.
    fn size_cmd(&self) -> Option<&'static str> {
        match self {
            Kind::String => Some("STRLEN"),
            Kind::Hash => Some("HLEN"),
            Kind::List => Some("LLEN"),
            Kind::Set => Some("SCARD"),
            Kind::ZSet => Some("ZCARD"),
            Kind::Stream => Some("XLEN"),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KeyMeta {
    pub key: String,
    pub kind: Kind,
    /// Remaining TTL in milliseconds. `None` means no expiry set.
    pub ttl_ms: Option<i64>,
    /// Element count, or byte length for strings.
    pub size: u64,
    /// `MEMORY USAGE`, when the server permits it.
    pub memory: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub enum KeyValue {
    String(String),
    Hash(Vec<(String, String)>),
    List(Vec<String>),
    Set(Vec<String>),
    ZSet(Vec<(String, f64)>),
    Stream(Vec<StreamEntry>),
    /// Key vanished between listing and reading -- normal on a live keyspace.
    Missing,
    Unsupported(String),
}

impl KeyValue {
    /// Number of rows this value renders as.
    pub fn len(&self) -> usize {
        match self {
            KeyValue::String(s) => s.lines().count(),
            KeyValue::Hash(v) => v.len(),
            KeyValue::List(v) => v.len(),
            KeyValue::Set(v) => v.len(),
            KeyValue::ZSet(v) => v.len(),
            KeyValue::Stream(v) => v.len(),
            KeyValue::Missing | KeyValue::Unsupported(_) => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Render a Redis reply as text, tolerating binary payloads.
///
/// Redis values are byte strings, not UTF-8. A viewer that assumes UTF-8 renders nothing
/// for msgpack or protobuf payloads -- and BullMQ itself can store msgpack.
pub fn display_string(v: &Value) -> String {
    if let Some(s) = v.as_string() {
        return s;
    }
    match v.as_bytes() {
        Some(b) => {
            let lossy = String::from_utf8_lossy(b);
            format!("<{} bytes> {}", b.len(), lossy.chars().take(200).collect::<String>())
        }
        None => format!("{v:?}"),
    }
}

impl Conn {
    /// Type, TTL, size and (when permitted) memory for one key.
    pub async fn key_meta(&self, key: &str) -> Result<KeyMeta> {
        let kind = Kind::parse(&display_string(&self.cmd("TYPE", vec![key.into()]).await?));

        if kind == Kind::None {
            return Ok(KeyMeta { key: key.into(), kind, ttl_ms: None, size: 0, memory: None });
        }

        // PTTL: -1 = no expiry, -2 = key gone.
        let ttl_raw = self.cmd("PTTL", vec![key.into()]).await?.as_i64().unwrap_or(-1);
        let ttl_ms = (ttl_raw >= 0).then_some(ttl_raw);

        let size = match kind.size_cmd() {
            Some(cmd) => self.cmd(cmd, vec![key.into()]).await?.as_u64().unwrap_or(0),
            None => 0,
        };

        // MEMORY USAGE is blocked on several managed hosts; absence is not an error.
        let memory = if self.capabilities().has(Feature::MemoryStats) {
            self.cmd("MEMORY", vec!["USAGE".into(), key.into()]).await.ok().and_then(|v| v.as_u64())
        } else {
            None
        };

        Ok(KeyMeta { key: key.into(), kind, ttl_ms, size, memory })
    }

    /// Read up to [`PAGE`] elements starting at `offset`.
    ///
    /// `offset` is honoured exactly for ordered types (list, zset, stream). Hash and set
    /// are unordered, so it is used as a page counter over `HSCAN`/`SSCAN` cursors --
    /// which is why the viewer labels those pages "next", not by index.
    pub async fn read_value(&self, key: &str, kind: Kind, offset: usize) -> Result<KeyValue> {
        let start = offset as i64;
        let stop = start + PAGE as i64 - 1;

        let value = match kind {
            Kind::None => KeyValue::Missing,

            Kind::String => {
                // GETRANGE, not GET: a 500MB string must not be pulled into the TUI.
                let reply = self
                    .cmd(
                        "GETRANGE",
                        vec![key.into(), 0.into(), (MAX_STRING_BYTES as i64 - 1).into()],
                    )
                    .await?;
                let mut s = display_string(&reply);
                if s.len() >= MAX_STRING_BYTES {
                    s.push_str("\n\n... truncated ...");
                }
                KeyValue::String(s)
            }

            Kind::List => {
                let reply =
                    self.cmd("LRANGE", vec![key.into(), start.into(), stop.into()]).await?;
                KeyValue::List(as_string_vec(&reply))
            }

            Kind::ZSet => {
                let reply = self
                    .cmd(
                        "ZRANGE",
                        vec![key.into(), start.into(), stop.into(), "WITHSCORES".into()],
                    )
                    .await?;
                KeyValue::ZSet(as_scored_pairs(&reply))
            }

            Kind::Hash => {
                let reply = self
                    .cmd(
                        "HSCAN",
                        vec![key.into(), "0".into(), "COUNT".into(), (PAGE as i64).into()],
                    )
                    .await?;
                KeyValue::Hash(as_field_pairs(&scan_items(&reply)?))
            }

            Kind::Set => {
                let reply = self
                    .cmd(
                        "SSCAN",
                        vec![key.into(), "0".into(), "COUNT".into(), (PAGE as i64).into()],
                    )
                    .await?;
                KeyValue::Set(scan_items(&reply)?.iter().map(display_string).collect())
            }

            Kind::Stream => {
                let reply = self
                    .cmd(
                        "XRANGE",
                        vec![key.into(), "-".into(), "+".into(), "COUNT".into(), (PAGE as i64).into()],
                    )
                    .await?;
                KeyValue::Stream(as_stream_entries(&reply))
            }

            Kind::Other => KeyValue::Unsupported(
                "module type -- no viewer for this key yet".to_string(),
            ),
        };

        Ok(value)
    }
}

fn as_string_vec(v: &Value) -> Vec<String> {
    match v {
        Value::Array(items) => items.iter().map(display_string).collect(),
        _ => Vec::new(),
    }
}

fn as_field_pairs(items: &[Value]) -> Vec<(String, String)> {
    items
        .chunks_exact(2)
        .map(|c| (display_string(&c[0]), display_string(&c[1])))
        .collect()
}

fn as_scored_pairs(v: &Value) -> Vec<(String, f64)> {
    let Value::Array(items) = v else { return Vec::new() };
    items
        .chunks_exact(2)
        .map(|c| {
            let member = display_string(&c[0]);
            // Scores come back as bulk strings in RESP2 and doubles in RESP3.
            let score = c[1].as_f64().unwrap_or(0.0);
            (member, score)
        })
        .collect()
}

/// `HSCAN`/`SSCAN` reply is `[cursor, [items...]]`.
fn scan_items(v: &Value) -> Result<Vec<Value>> {
    match v {
        Value::Array(parts) if parts.len() == 2 => match &parts[1] {
            Value::Array(items) => Ok(items.clone()),
            other => Err(ConnError::Reply {
                cmd: "SCAN",
                detail: format!("items not an array: {other:?}"),
            }),
        },
        other => Err(ConnError::Reply {
            cmd: "SCAN",
            detail: format!("expected [cursor, items], got {other:?}"),
        }),
    }
}

fn as_stream_entries(v: &Value) -> Vec<StreamEntry> {
    let Value::Array(entries) = v else { return Vec::new() };
    entries
        .iter()
        .filter_map(|e| {
            let Value::Array(pair) = e else { return None };
            if pair.len() != 2 {
                return None;
            }
            let id = display_string(&pair[0]);
            let fields = match &pair[1] {
                Value::Array(kv) => as_field_pairs(kv),
                _ => Vec::new(),
            };
            Some(StreamEntry { id, fields })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kinds() {
        assert_eq!(Kind::parse("zset"), Kind::ZSet);
        assert_eq!(Kind::parse("stream"), Kind::Stream);
        assert_eq!(Kind::parse("none"), Kind::None);
        // An unknown type is a module type, not a bug -- render a placeholder, don't panic.
        assert_eq!(Kind::parse("ReJSON-RL"), Kind::Other);
    }

    #[test]
    fn every_collection_kind_has_a_size_command() {
        for k in [Kind::String, Kind::Hash, Kind::List, Kind::Set, Kind::ZSet, Kind::Stream] {
            assert!(k.size_cmd().is_some(), "{k:?} has no size command");
        }
        assert!(Kind::None.size_cmd().is_none());
    }

    #[test]
    fn pairs_ignore_a_trailing_odd_element() {
        // A truncated reply must not panic the viewer.
        let items = vec![Value::from("a"), Value::from("1"), Value::from("orphan")];
        assert_eq!(as_field_pairs(&items), vec![("a".into(), "1".into())]);
    }

    #[test]
    fn zset_scores_parse_from_bulk_strings() {
        let reply = Value::Array(vec![
            Value::from("member-a"),
            Value::from("1.5"),
            Value::from("member-b"),
            Value::from("-2"),
        ]);
        assert_eq!(
            as_scored_pairs(&reply),
            vec![("member-a".into(), 1.5), ("member-b".into(), -2.0)]
        );
    }

    #[test]
    fn scan_items_rejects_malformed_replies() {
        assert!(scan_items(&Value::from("nope")).is_err());
        let ok = Value::Array(vec![Value::from("0"), Value::Array(vec![Value::from("x")])]);
        assert_eq!(scan_items(&ok).unwrap().len(), 1);
    }

    #[test]
    fn parses_stream_entries() {
        let reply = Value::Array(vec![Value::Array(vec![
            Value::from("1712-0"),
            Value::Array(vec![Value::from("event"), Value::from("completed")]),
        ])]);
        let entries = as_stream_entries(&reply);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "1712-0");
        assert_eq!(entries[0].fields, vec![("event".into(), "completed".into())]);
    }

    #[test]
    fn value_len_counts_rows() {
        assert_eq!(KeyValue::List(vec!["a".into(), "b".into()]).len(), 2);
        assert!(KeyValue::Missing.is_empty());
    }
}
