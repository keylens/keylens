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

/// The same two bounds as Redis range arguments.
///
/// Redis reads a negative bound as an offset from the end, so a page size that wrapped
/// on the way to `i64` would ask for the whole collection. These are const-evaluated and
/// checked below, so the cast happens once at compile time and never at runtime.
const PAGE_BOUND: i64 = PAGE as i64;
const MAX_STRING_BOUND: i64 = MAX_STRING_BYTES as i64;
const _: () = assert!(
    PAGE_BOUND > 0 && MAX_STRING_BOUND > 0,
    "page constants must stay positive as Redis range bounds"
);

/// The types [`Conn::read_key`] speculates over, in the order its pipeline sends them.
///
/// Both the size block and the value block follow this order and [`Kind::slot`] indexes
/// into both, so the ordering lives here and nowhere else.
const SPECULATIVE_KINDS: [Kind; 6] = [
    Kind::String,
    Kind::Hash,
    Kind::List,
    Kind::Set,
    Kind::ZSet,
    Kind::Stream,
];

// Fixed slots in the speculative pipeline. `MEMORY USAGE` is conditional, so it goes
// *last* — that way including or omitting it cannot shift anything else.
const SLOT_TYPE: usize = 0;
const SLOT_PTTL: usize = 1;
const SIZE_BASE: usize = 2;
const VALUE_BASE: usize = SIZE_BASE + SPECULATIVE_KINDS.len();
const SLOT_MEMORY: usize = VALUE_BASE + SPECULATIVE_KINDS.len();

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

    /// Position of this type in [`SPECULATIVE_KINDS`], and so in both blocks of the
    /// speculative pipeline. `None` for the types it does not speculate over.
    fn slot(&self) -> Option<usize> {
        SPECULATIVE_KINDS.iter().position(|k| k == self)
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
    /// The server cannot provide a safely paged representation of this value.
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

    /// Everything the detail pane needs about a key, in **one round trip**.
    ///
    /// Type, TTL, memory, size and the first page of the value all come back together.
    /// Done sequentially this is three round trips, because both the size command and the
    /// read command depend on the type — which is why this asks for *every* type's size
    /// and value at once and keeps only the pair the type turns out to justify.
    ///
    /// The five wrong ones fail with `WRONGTYPE`, which Redis rejects on the type check
    /// before doing any work at all: they cost a few bytes on the wire and nothing on the
    /// server. [`Conn::pipeline`] already reports per-command results, so their failure is
    /// ordinary rather than fatal.
    ///
    /// At 35ms this saves 70ms nobody notices. Against a managed endpoint measured at
    /// 390ms it is the difference between a key opening in 0.4s and in 1.2s, on every
    /// single keypress — which is what makes browsing feel broken rather than distant.
    pub async fn read_key(&self, key: &str) -> Result<(KeyMeta, KeyValue)> {
        // Speculation only pays when the bounded reads exist for every type. Servers
        // missing one take the sequential path so the unsupported result is specific to
        // the selected type. No measure-then-read fallback is safe: the key can grow
        // between those commands.
        if !(self.capabilities().has(Feature::GetRange)
            && self.capabilities().has(Feature::CursorCollectionScan))
        {
            return self.read_key_sequentially(key).await;
        }

        let want_memory = self.capabilities().has(Feature::MemoryStats);

        let mut cmds: Vec<(&'static str, Vec<Value>)> = Vec::with_capacity(SLOT_MEMORY + 1);
        cmds.push(("TYPE", vec![key.into()]));
        cmds.push(("PTTL", vec![key.into()]));
        for kind in SPECULATIVE_KINDS {
            cmds.push((size_cmd_or_filler(kind), vec![key.into()]));
        }
        for kind in SPECULATIVE_KINDS {
            cmds.push(value_cmd(kind, key));
        }
        if want_memory {
            cmds.push(("MEMORY", vec!["USAGE".into(), key.into()]));
        }

        let replies = self.pipeline(&cmds).await?;
        let at = |i: usize| replies.get(i).and_then(|r| r.as_ref().ok());

        let kind = at(SLOT_TYPE)
            .map(|v| Kind::parse(&display_string(v)))
            .unwrap_or(Kind::None);
        // PTTL: -1 = no expiry, -2 = key gone. Both mean "nothing to show".
        let ttl_raw = at(SLOT_PTTL).and_then(|v| v.as_i64()).unwrap_or(-1);
        let slot = kind.slot();

        let meta = KeyMeta {
            key: key.into(),
            kind,
            ttl_ms: (ttl_raw >= 0).then_some(ttl_raw),
            // A size that failed is a missing number, not a missing key.
            size: slot
                .and_then(|s| at(SIZE_BASE + s))
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            // MEMORY USAGE is blocked on several managed hosts; absence is not an error.
            memory: want_memory
                .then(|| at(SLOT_MEMORY).and_then(|v| v.as_u64()))
                .flatten(),
        };

        let value = match slot.and_then(|s| at(VALUE_BASE + s)) {
            Some(reply) => decode_value(kind, reply)?,
            None => absent(kind),
        };

        Ok((meta, value))
    }

    /// The two-round-trip path, for servers without every bounded read command.
    ///
    /// Still collapsed where it can be: the size and the value both need the type but not
    /// each other, so they go out together.
    async fn read_key_sequentially(&self, key: &str) -> Result<(KeyMeta, KeyValue)> {
        let head = self.key_head(key).await?;

        if head.kind == Kind::None {
            return Ok((
                KeyMeta {
                    key: key.into(),
                    kind: head.kind,
                    ttl_ms: None,
                    size: 0,
                    memory: None,
                },
                KeyValue::Missing,
            ));
        }

        let (size, value) = tokio::join!(
            self.key_size(key, head.kind),
            self.read_value(key, head.kind, 0),
        );

        Ok((
            KeyMeta {
                key: key.into(),
                kind: head.kind,
                ttl_ms: head.ttl_ms,
                size: size.unwrap_or(0),
                memory: head.memory,
            },
            value?,
        ))
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
        // Checked, not cast: an offset past `i64::MAX` wraps to a negative, which Redis
        // reads as "from the end" and turns a bounded page into the whole collection.
        let (start, stop) = page_bounds(offset, kind)?;

        let value = match kind {
            Kind::None => KeyValue::Missing,

            Kind::String => {
                // GETRANGE, not GET: a 500MB string must not be pulled into the TUI.
                if self.capabilities().has(Feature::GetRange) {
                    let reply = self
                        .cmd(
                            "GETRANGE",
                            vec![key.into(), 0.into(), (MAX_STRING_BOUND - 1).into()],
                        )
                        .await?;
                    decode_string(&reply)
                } else {
                    // Measuring first and issuing GET second has a race: another client can
                    // replace the value with a huge string between those commands. On a
                    // server without GETRANGE there is no genuinely bounded read.
                    KeyValue::Unsupported(
                        "this server has no bounded string read (GETRANGE)".into(),
                    )
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
                            vec![key.into(), "0".into(), "COUNT".into(), PAGE_BOUND.into()],
                        )
                        .await?;
                    decode_hash(&reply)?
                } else {
                    KeyValue::Unsupported("this server has no bounded hash read (HSCAN)".into())
                }
            }

            Kind::Set => {
                if self.capabilities().has(Feature::CursorCollectionScan) {
                    let reply = self
                        .cmd(
                            "SSCAN",
                            vec![key.into(), "0".into(), "COUNT".into(), PAGE_BOUND.into()],
                        )
                        .await?;
                    decode_set(&reply)?
                } else {
                    KeyValue::Unsupported("this server has no bounded set read (SSCAN)".into())
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
                            PAGE_BOUND.into(),
                        ],
                    )
                    .await?;
                KeyValue::Stream(as_stream_entries(&reply))
            }

            Kind::Other => absent(Kind::Other),
        };

        Ok(value)
    }
}

/// Inclusive `[start, stop]` bounds for one [`PAGE`] starting at `offset`.
///
/// `kind` only names the command in the error; the bounds themselves are the same for
/// every ranged type.
fn page_bounds(offset: usize, kind: Kind) -> Result<(i64, i64)> {
    let cmd = kind.size_cmd().unwrap_or("RANGE");
    let range_err = |detail: &'static str| ConnError::Range {
        cmd,
        offset,
        detail,
    };

    let start = i64::try_from(offset).map_err(|_| range_err("offset exceeds i64::MAX"))?;
    let stop = start
        .checked_add(PAGE_BOUND - 1)
        .ok_or_else(|| range_err("offset + page size overflows i64"))?;
    Ok((start, stop))
}

/// The size command for a type that [`Conn::read_key`] speculates over.
///
/// Every [`SPECULATIVE_KINDS`] entry has one, so the filler is unreachable in practice.
/// It exists because the pipeline's slots are positional: returning nothing here would
/// shorten the reply list and silently misalign every field after it. A harmless `TYPE`
/// holds the slot, and [`Kind::slot`] never points at it anyway.
fn size_cmd_or_filler(kind: Kind) -> &'static str {
    kind.size_cmd().unwrap_or("TYPE")
}

/// The bounded first-page read for one type. Same slot-holding rule as
/// [`size_cmd_or_filler`].
fn value_cmd(kind: Kind, key: &str) -> (&'static str, Vec<Value>) {
    let stop = PAGE_BOUND - 1;
    let count = PAGE_BOUND;
    match kind {
        // GETRANGE, not GET: a 500MB string must not be pulled into the TUI.
        Kind::String => (
            "GETRANGE",
            vec![key.into(), 0.into(), (MAX_STRING_BOUND - 1).into()],
        ),
        Kind::Hash => (
            "HSCAN",
            vec![key.into(), "0".into(), "COUNT".into(), count.into()],
        ),
        Kind::List => ("LRANGE", vec![key.into(), 0.into(), stop.into()]),
        Kind::Set => (
            "SSCAN",
            vec![key.into(), "0".into(), "COUNT".into(), count.into()],
        ),
        Kind::ZSet => (
            "ZRANGE",
            vec![key.into(), 0.into(), stop.into(), "WITHSCORES".into()],
        ),
        Kind::Stream => (
            "XRANGE",
            vec![
                key.into(),
                "-".into(),
                "+".into(),
                "COUNT".into(),
                count.into(),
            ],
        ),
        _ => ("TYPE", vec![key.into()]),
    }
}

/// Turn one type's reply into a value. The single decoder per type, shared by the
/// speculative read and the sequential one.
fn decode_value(kind: Kind, reply: &Value) -> Result<KeyValue> {
    Ok(match kind {
        Kind::String => decode_string(reply),
        Kind::Hash => decode_hash(reply)?,
        Kind::List => KeyValue::List(as_string_vec(reply)),
        Kind::Set => decode_set(reply)?,
        Kind::ZSet => KeyValue::ZSet(as_scored_pairs(reply)),
        Kind::Stream => KeyValue::Stream(as_stream_entries(reply)),
        Kind::None | Kind::Other => absent(kind),
    })
}

/// What to show when there is no value to decode.
fn absent(kind: Kind) -> KeyValue {
    match kind {
        Kind::Other => {
            KeyValue::Unsupported("module type -- no viewer for this key yet".to_string())
        }
        // Either the key is gone, or its read failed because the type changed between the
        // `TYPE` in the pipeline and the read a few commands later. Both mean there is
        // nothing to render for the type we were told about.
        _ => KeyValue::Missing,
    }
}

fn decode_string(reply: &Value) -> KeyValue {
    // Measure the *reply*, not the rendered text. `display_string` turns a binary payload
    // into `<N bytes> …200 chars`, which is far shorter than the bytes it stands for -- so
    // checking the rendered length silently dropped the marker on exactly the values most
    // likely to be truncated.
    let truncated = reply_byte_len(reply) >= MAX_STRING_BYTES;
    let mut s = display_string(reply);
    if truncated {
        s.push_str("\n\n... truncated ...");
    }
    KeyValue::String(s)
}

fn decode_hash(reply: &Value) -> Result<KeyValue> {
    let items = scan_items(reply)?;
    // SCAN COUNT is a target, not a hard limit. Keep the UI/API page bounded even if the
    // server returns a larger compact-encoding bucket in one step.
    Ok(KeyValue::Hash(as_field_pairs(
        &items[..items.len().min(PAGE * 2)],
    )))
}

fn decode_set(reply: &Value) -> Result<KeyValue> {
    Ok(KeyValue::Set(
        scan_items(reply)?
            .iter()
            .take(PAGE)
            .map(display_string)
            .collect(),
    ))
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

fn as_string_vec(v: &Value) -> Vec<String> {
    match v {
        Value::Array(items) => items.iter().map(display_string).collect(),
        _ => Vec::new(),
    }
}

fn as_field_pairs(items: &[Value]) -> Vec<(String, String)> {
    items
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| (display_string(&c[0]), display_string(&c[1])))
        .collect()
}

fn as_scored_pairs(v: &Value) -> Vec<(String, f64)> {
    let Value::Array(items) = v else {
        return Vec::new();
    };
    items
        .as_chunks::<2>()
        .0
        .iter()
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
    fn speculative_slots_line_up_with_the_pipeline_that_fills_them() {
        // The reply list is positional: slot `SIZE_BASE + n` is only the size of
        // `SPECULATIVE_KINDS[n]` for as long as both blocks are built from that array in
        // that order. Get this wrong and a hash reports a list's length -- with no error
        // anywhere, because every reply is individually well-formed.
        assert_eq!(SIZE_BASE, 2, "TYPE and PTTL come first");
        assert_eq!(VALUE_BASE, SIZE_BASE + SPECULATIVE_KINDS.len());
        assert_eq!(SLOT_MEMORY, VALUE_BASE + SPECULATIVE_KINDS.len());

        for (i, kind) in SPECULATIVE_KINDS.iter().enumerate() {
            assert_eq!(kind.slot(), Some(i), "{kind:?} indexes the wrong slot");
        }

        // The types with no slot must never index into either block.
        for kind in [Kind::None, Kind::Other] {
            assert_eq!(kind.slot(), None, "{kind:?} must not claim a slot");
        }
    }

    #[test]
    fn every_speculated_kind_has_a_real_size_and_value_command() {
        // Both helpers fall back to a `TYPE` filler so a missing entry cannot shorten the
        // pipeline and misalign it. That filler is a safety net, not a plan: if one of
        // these types ever reaches it, the pane silently shows a type name where a value
        // should be.
        for kind in SPECULATIVE_KINDS {
            assert_ne!(
                size_cmd_or_filler(kind),
                "TYPE",
                "{kind:?} fell back to the slot filler instead of a real size command"
            );
            assert_ne!(
                value_cmd(kind, "k").0,
                "TYPE",
                "{kind:?} fell back to the slot filler instead of a real read command"
            );
        }
    }

    #[test]
    fn speculative_reads_are_all_bounded() {
        // The crate's one hard rule: no unbounded collection read, ever. Speculating over
        // six types at once multiplies the cost of getting this wrong by six.
        // The workspace guard owns the collection-command list. Keep the two shapes that
        // require argument-aware checks here: an unbounded string read and a full range.
        const UNBOUNDED: [&str; 2] = ["GET", "LRANGE_ALL"];
        for kind in SPECULATIVE_KINDS {
            let (cmd, args) = value_cmd(kind, "k");
            assert!(
                !UNBOUNDED.contains(&cmd),
                "{kind:?} speculates with the unbounded command {cmd}"
            );
            let rendered: Vec<String> = args.iter().map(display_string).collect();
            assert!(
                rendered.iter().any(|a| a == "COUNT")
                    || rendered.iter().any(|a| a.parse::<i64>().is_ok()),
                "{kind:?} has neither a COUNT nor a range bound: {rendered:?}"
            );
        }
    }

    #[test]
    fn a_module_type_is_unsupported_not_missing() {
        // These render very differently, and confusing them tells someone their key is
        // gone when it is merely a type keylens has no viewer for.
        assert!(matches!(absent(Kind::Other), KeyValue::Unsupported(_)));
        assert!(matches!(absent(Kind::None), KeyValue::Missing));
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
    fn scan_count_overshoot_is_capped_in_the_decoded_page() {
        let set_reply = Value::Array(vec![
            Value::from("1"),
            Value::Array((0..PAGE + 20).map(|i| Value::from(i as i64)).collect()),
        ]);
        let KeyValue::Set(set) = decode_set(&set_reply).unwrap() else {
            unreachable!()
        };
        assert_eq!(set.len(), PAGE);

        let hash_items = (0..PAGE + 20)
            .flat_map(|i| [Value::from(format!("f{i}")), Value::from("v")])
            .collect();
        let hash_reply = Value::Array(vec![Value::from("1"), Value::Array(hash_items)]);
        let KeyValue::Hash(hash) = decode_hash(&hash_reply).unwrap() else {
            unreachable!()
        };
        assert_eq!(hash.len(), PAGE);
    }
}
