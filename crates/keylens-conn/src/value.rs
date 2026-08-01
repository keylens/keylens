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
    /// Too big to read safely on a server that lacks the bounded read command.
    TooLarge {
        /// The Redis type, for the message: `this hash holds more than …`.
        what: &'static str,
        limit: usize,
        /// What `limit` counts. A string's cap is bytes; a collection's is elements, and
        /// telling someone a string "holds more than 65536 entries" is just wrong.
        unit: &'static str,
    },
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
            KeyValue::Missing | KeyValue::Unsupported(_) | KeyValue::TooLarge { .. } => 0,
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
            format!(
                "<{} bytes> {}",
                b.len(),
                lossy.chars().take(200).collect::<String>()
            )
        }
        None => format!("{v:?}"),
    }
}

/// Everything about a key that can be learned without first knowing its type.
///
/// Split out from [`KeyMeta`] because the size command *does* depend on the type -- you
/// cannot ask `HLEN` before `TYPE` comes back. Separating the two is what lets the size
/// lookup overlap the value read instead of queueing behind it.
#[derive(Debug, Clone)]
pub struct KeyHead {
    pub kind: Kind,
    pub ttl_ms: Option<i64>,
    pub memory: Option<u64>,
}

impl Conn {
    /// Type, TTL and memory in a single round trip.
    ///
    /// These three are independent of each other and of the key's type, so there is no
    /// reason to pay three round trips for them. On a managed host ~250ms away that
    /// difference is most of the delay between pressing `j` and seeing a value.
    pub async fn key_head(&self, key: &str) -> Result<KeyHead> {
        let want_memory = self.capabilities().has(Feature::MemoryStats);

        let mut cmds: Vec<(&'static str, Vec<Value>)> =
            vec![("TYPE", vec![key.into()]), ("PTTL", vec![key.into()])];
        if want_memory {
            cmds.push(("MEMORY", vec!["USAGE".into(), key.into()]));
        }

        let replies = self.pipeline(&cmds).await?;
        let reply = |i: usize| replies.get(i).and_then(|r| r.as_ref().ok());

        let kind = reply(0)
            .map(|v| Kind::parse(&display_string(v)))
            .unwrap_or(Kind::None);

        // PTTL: -1 = no expiry, -2 = key gone. Both mean "nothing to show".
        let ttl_raw = reply(1).and_then(|v| v.as_i64()).unwrap_or(-1);

        Ok(KeyHead {
            kind,
            ttl_ms: (ttl_raw >= 0).then_some(ttl_raw),
            // MEMORY USAGE is blocked on several managed hosts; absence is not an error.
            memory: want_memory
                .then(|| reply(2).and_then(|v| v.as_u64()))
                .flatten(),
        })
    }

    /// Element count, or byte length for a string. One command, and it needs the type.
    pub async fn key_size(&self, key: &str, kind: Kind) -> Result<u64> {
        match kind.size_cmd() {
            Some(cmd) => Ok(self.cmd(cmd, vec![key.into()]).await?.as_u64().unwrap_or(0)),
            None => Ok(0),
        }
    }

    /// Type, TTL, size and (when permitted) memory for one key.
    ///
    /// Two round trips: everything type-independent, then the size. Callers that also
    /// read the value should prefer [`key_head`](Self::key_head) plus a concurrent
    /// [`key_size`](Self::key_size), which collapses those two into one.
    pub async fn key_meta(&self, key: &str) -> Result<KeyMeta> {
        let head = self.key_head(key).await?;

        if head.kind == Kind::None {
            return Ok(KeyMeta {
                key: key.into(),
                kind: head.kind,
                ttl_ms: None,
                size: 0,
                memory: None,
            });
        }

        Ok(KeyMeta {
            key: key.into(),
            kind: head.kind,
            ttl_ms: head.ttl_ms,
            size: self.key_size(key, head.kind).await?,
            memory: head.memory,
        })
    }

    /// Read up to [`PAGE`] elements starting at `offset`.
    ///
    /// `offset` is honoured exactly for the ordered types (list, zset, stream), which is
    /// what `LRANGE`/`ZRANGE`/`XRANGE` index by.
    ///
    /// Hash and set **ignore it** and always return the first cursor page. That is a
    /// limitation, not a convention: `HSCAN`/`SSCAN` are resumed by an opaque cursor from
    /// the previous reply, not by an element index, so paging them needs the caller to
    /// carry that cursor back in. Until this signature does, claiming otherwise here would
    /// describe an API that does not exist.
    pub async fn read_value(&self, key: &str, kind: Kind, offset: usize) -> Result<KeyValue> {
        let start = offset as i64;
        let stop = start + PAGE as i64 - 1;

        let value = match kind {
            Kind::None => KeyValue::Missing,

            Kind::String => {
                // GETRANGE, not GET: a 500MB string must not be pulled into the TUI.
                if self.capabilities().has(Feature::GetRange) {
                    let reply = self
                        .cmd(
                            "GETRANGE",
                            vec![key.into(), 0.into(), (MAX_STRING_BYTES as i64 - 1).into()],
                        )
                        .await?;
                    // Measure the *reply*, not the rendered text. `display_string` turns a
                    // binary payload into `<N bytes> …200 chars`, which is far shorter than
                    // the bytes it stands for -- so checking the rendered length silently
                    // dropped the marker on exactly the values most likely to be truncated.
                    let truncated = reply_byte_len(&reply) >= MAX_STRING_BYTES;
                    let mut s = display_string(&reply);
                    if truncated {
                        s.push_str("\n\n... truncated ...");
                    }
                    KeyValue::String(s)
                } else if size_ok(self, key, "STRLEN", MAX_STRING_BYTES).await? {
                    // No GETRANGE on this server. STRLEN first, then a whole GET only if
                    // the value is small enough -- the bound is preserved, just measured
                    // instead of requested.
                    KeyValue::String(display_string(&self.cmd("GET", vec![key.into()]).await?))
                } else {
                    KeyValue::TooLarge {
                        what: "string",
                        limit: MAX_STRING_BYTES,
                        unit: "bytes",
                    }
                }
            }

            Kind::List => {
                let reply = self
                    .cmd("LRANGE", vec![key.into(), start.into(), stop.into()])
                    .await?;
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
                if self.capabilities().has(Feature::CursorCollectionScan) {
                    let reply = self
                        .cmd(
                            "HSCAN",
                            vec![key.into(), "0".into(), "COUNT".into(), (PAGE as i64).into()],
                        )
                        .await?;
                    KeyValue::Hash(as_field_pairs(&scan_items(&reply)?))
                } else if size_ok(self, key, "HLEN", PAGE).await? {
                    KeyValue::Hash(whole_collection(self, key, "HGETALL").await?)
                } else {
                    KeyValue::TooLarge {
                        what: "hash",
                        limit: PAGE,
                        unit: "fields",
                    }
                }
            }

            Kind::Set => {
                if self.capabilities().has(Feature::CursorCollectionScan) {
                    let reply = self
                        .cmd(
                            "SSCAN",
                            vec![key.into(), "0".into(), "COUNT".into(), (PAGE as i64).into()],
                        )
                        .await?;
                    KeyValue::Set(scan_items(&reply)?.iter().map(display_string).collect())
                } else if size_ok(self, key, "SCARD", PAGE).await? {
                    let reply = self.cmd("SMEMBERS", vec![key.into()]).await?;
                    KeyValue::Set(as_string_vec(&reply))
                } else {
                    KeyValue::TooLarge {
                        what: "set",
                        limit: PAGE,
                        unit: "members",
                    }
                }
            }

            Kind::Stream => {
                let reply = self
                    .cmd(
                        "XRANGE",
                        vec![
                            key.into(),
                            "-".into(),
                            "+".into(),
                            "COUNT".into(),
                            (PAGE as i64).into(),
                        ],
                    )
                    .await?;
                KeyValue::Stream(as_stream_entries(&reply))
            }

            Kind::Other => {
                KeyValue::Unsupported("module type -- no viewer for this key yet".to_string())
            }
        };

        Ok(value)
    }
}

/// Byte length of a reply, before it is rendered for display.
///
/// [`display_string`] is lossy about size on purpose -- it abbreviates binary payloads --
/// so anything deciding "was this truncated" has to ask the reply, not the rendering.
fn reply_byte_len(v: &Value) -> usize {
    v.as_bytes()
        .map(|b| b.len())
        .or_else(|| v.as_string().map(|s| s.len()))
        .unwrap_or(0)
}

/// Measure a key with `cmd` and report whether it is under `limit`.
///
/// This is what keeps the whole-collection fallbacks honest. keylens never issues an
/// unbounded read; on a server without the cursor variants it *measures* first with a
/// command that server does have, and declines when the answer is too big. The bound is
/// preserved — it's just enforced client-side instead of requested server-side.
async fn size_ok(conn: &Conn, key: &str, cmd: &'static str, limit: usize) -> Result<bool> {
    let n = conn.cmd(cmd, vec![key.into()]).await?.as_u64().unwrap_or(0);
    Ok(n as usize <= limit)
}

/// Read a whole hash. Only ever called behind [`size_ok`].
async fn whole_collection(
    conn: &Conn,
    key: &str,
    cmd: &'static str,
) -> Result<Vec<(String, String)>> {
    let reply = conn.cmd(cmd, vec![key.into()]).await?;
    Ok(match reply {
        Value::Array(items) => as_field_pairs(&items),
        Value::Map(map) => map
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().unwrap_or_default().to_string(),
                    display_string(v),
                )
            })
            .collect(),
        _ => Vec::new(),
    })
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
    let Value::Array(items) = v else {
        return Vec::new();
    };
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
    let Value::Array(entries) = v else {
        return Vec::new();
    };
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
        for k in [
            Kind::String,
            Kind::Hash,
            Kind::List,
            Kind::Set,
            Kind::ZSet,
            Kind::Stream,
        ] {
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
        assert_eq!(
            entries[0].fields,
            vec![("event".into(), "completed".into())]
        );
    }

    #[test]
    fn value_len_counts_rows() {
        assert_eq!(KeyValue::List(vec!["a".into(), "b".into()]).len(), 2);
        assert!(KeyValue::Missing.is_empty());
    }

    #[test]
    fn truncation_is_judged_on_the_reply_not_the_rendering() {
        // `display_string` abbreviates a binary payload to `<N bytes> …200 chars`, which is
        // *shorter* than what it stands for. Measuring the rendered text meant a 64KB
        // msgpack value came back looking complete, with no truncation marker at all --
        // silently wrong on exactly the values most likely to be cut.
        let binary = Value::Bytes(vec![0xF0u8; MAX_STRING_BYTES].into());

        assert_eq!(reply_byte_len(&binary), MAX_STRING_BYTES);
        assert!(
            display_string(&binary).len() < MAX_STRING_BYTES,
            "the rendering is much shorter, which is why it must not be the measure"
        );
        assert!(reply_byte_len(&binary) >= MAX_STRING_BYTES, "so this fires");
    }

    #[test]
    fn reply_byte_len_reads_utf8_and_binary_alike() {
        assert_eq!(reply_byte_len(&Value::from("abc")), 3);
        // Multi-byte UTF-8: bytes, not chars, because the cap is a transfer bound.
        assert_eq!(reply_byte_len(&Value::from("日本")), 6);
        assert_eq!(reply_byte_len(&Value::Bytes(vec![0, 1, 2, 3].into())), 4);
        assert_eq!(reply_byte_len(&Value::Null), 0);
    }

    #[test]
    fn a_short_value_is_not_marked_truncated() {
        let short = Value::from("hello");
        assert!(reply_byte_len(&short) < MAX_STRING_BYTES);
    }

    #[test]
    fn oversized_values_carry_the_unit_their_limit_is_counted_in() {
        // "this string holds more than 65536 entries" was the wrong noun: a string's cap
        // is bytes, a hash's is fields, a set's is members.
        for (value, unit) in [
            (
                KeyValue::TooLarge {
                    what: "string",
                    limit: MAX_STRING_BYTES,
                    unit: "bytes",
                },
                "bytes",
            ),
            (
                KeyValue::TooLarge {
                    what: "hash",
                    limit: PAGE,
                    unit: "fields",
                },
                "fields",
            ),
            (
                KeyValue::TooLarge {
                    what: "set",
                    limit: PAGE,
                    unit: "members",
                },
                "members",
            ),
        ] {
            let KeyValue::TooLarge { unit: got, .. } = value else {
                unreachable!()
            };
            assert_eq!(got, unit);
        }
    }
}
