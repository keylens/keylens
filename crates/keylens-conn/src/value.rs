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
/// Bytes requested by the bounded string read: one more than is ever kept.
///
/// The extra byte is how truncation is *known* rather than guessed. Asking for exactly
/// [`MAX_STRING_BYTES`] cannot tell a value of exactly that length from a longer one, so
/// 0.1.7 marked every exactly-64 KiB value as truncated when nothing had been cut.
const STRING_PROBE_BYTES: usize = MAX_STRING_BYTES + 1;

/// The same two bounds as Redis range arguments.
///
/// Redis reads a negative bound as an offset from the end, so a page size that wrapped
/// on the way to `i64` would ask for the whole collection. These are const-evaluated and
/// checked below, so the cast happens once at compile time and never at runtime.
const PAGE_BOUND: i64 = PAGE as i64;
/// Inclusive `GETRANGE` end: `0..=MAX_STRING_BYTES` is [`STRING_PROBE_BYTES`] bytes.
const STRING_PROBE_END: i64 = (STRING_PROBE_BYTES - 1) as i64;
const _: () = assert!(
    PAGE_BOUND > 0 && STRING_PROBE_END > 0,
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
    ///
    /// `None` means the size could not be read -- not that the key is empty. 0.1.7 stored
    /// a failed `HLEN` as `0`, which rendered a two-million-field hash as `size 0`.
    pub size: Option<u64>,
    /// `MEMORY USAGE`, when the server permits it.
    pub memory: Option<u64>,
}

/// How much of a value one read returned.
///
/// keylens reads one bounded page of every value. Whether that page is the whole value
/// is a fact about the read, and it is only `Complete` when the reply proves it: a range
/// that came back short, a cursor that came back `0`, a string shorter than the cap.
/// Anything else is `Partial`, including "full page, size unknown", because a full page
/// is exactly what a larger value looks like.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coverage {
    /// Every element, or every byte, the server held at read time.
    Complete,
    /// The first `shown` rows (or bytes, for a string). More exist, or may exist.
    Partial { shown: usize },
}

/// One bounded read of a key: what it is, what was read, and whether that was all of it.
#[derive(Debug, Clone)]
pub struct KeyRead {
    pub meta: KeyMeta,
    pub value: KeyValue,
    pub coverage: Coverage,
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
    /// The key changed type between `TYPE` and the read (`WRONGTYPE`). It exists, so
    /// "no longer exists" would be false; reading it again will show the new type.
    Changed,
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
            KeyValue::Missing | KeyValue::Changed | KeyValue::Unsupported(_) => 0,
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

        let kind = type_from(&replies, 0)?;

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
    ///
    /// `Ok(None)` for a type with no size command, or a reply that is not a count.
    pub async fn key_size(&self, key: &str, kind: Kind) -> Result<Option<u64>> {
        match kind.size_cmd() {
            Some(cmd) => Ok(self.cmd(cmd, vec![key.into()]).await?.as_u64()),
            None => Ok(None),
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
                size: Some(0),
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
    pub async fn read_key(&self, key: &str) -> Result<KeyRead> {
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

        let mut replies = self.pipeline(&cmds).await?;

        // A failed `TYPE` is an error, not an absent key. 0.1.7 read it as `none` and
        // told the user the key no longer existed -- on an ACL that merely denies TYPE.
        let kind = type_from(&replies, SLOT_TYPE)?;
        let at = |i: usize| replies.get(i).and_then(|r| r.as_ref().ok());
        // PTTL: -1 = no expiry, -2 = key gone. Both mean "nothing to show".
        let ttl_raw = at(SLOT_PTTL).and_then(|v| v.as_i64()).unwrap_or(-1);
        let slot = kind.slot();

        let meta = KeyMeta {
            key: key.into(),
            kind,
            ttl_ms: (ttl_raw >= 0).then_some(ttl_raw),
            // A size that failed is a missing number, not a zero.
            size: match kind {
                Kind::None => Some(0),
                _ => slot
                    .and_then(|s| at(SIZE_BASE + s))
                    .and_then(|v| v.as_u64()),
            },
            // MEMORY USAGE is blocked on several managed hosts; absence is not an error.
            memory: want_memory
                .then(|| at(SLOT_MEMORY).and_then(|v| v.as_u64()))
                .flatten(),
        };

        let (value, coverage) = match slot {
            None => (absent(kind), Coverage::Complete),
            Some(s) => match take_slot(&mut replies, VALUE_BASE + s) {
                Some(Ok(reply)) => decode_value(kind, &reply, meta.size)?,
                // The type changed between `TYPE` and this read, a few commands later.
                Some(Err(e)) if is_wrongtype(&e) => (KeyValue::Changed, Coverage::Complete),
                // Anything else -- an ACL denying `HSCAN`, say -- is a failure to report,
                // not an absent value. 0.1.7 rendered it as "key no longer exists".
                Some(Err(e)) => return Err(e),
                None => {
                    return Err(ConnError::Reply {
                        cmd: "PIPELINE",
                        detail: "value slot missing from the reply".into(),
                    });
                }
            },
        };

        Ok(KeyRead {
            meta,
            value,
            coverage,
        })
    }

    /// The two-round-trip path, for servers without every bounded read command.
    ///
    /// Still collapsed where it can be: the size and the value both need the type but not
    /// each other, so they go out together.
    async fn read_key_sequentially(&self, key: &str) -> Result<KeyRead> {
        let head = self.key_head(key).await?;

        if head.kind == Kind::None {
            return Ok(KeyRead {
                meta: KeyMeta {
                    key: key.into(),
                    kind: head.kind,
                    ttl_ms: None,
                    size: Some(0),
                    memory: None,
                },
                value: KeyValue::Missing,
                coverage: Coverage::Complete,
            });
        }

        let (size, page) = tokio::join!(
            self.key_size(key, head.kind),
            self.read_page(key, head.kind),
        );
        // A failed size is unknown, not zero, and not a reason to drop the value.
        let size = size.ok().flatten();
        let (value, coverage) = match page {
            Ok(page) => page,
            Err(e) if is_wrongtype(&e) => (KeyValue::Changed, Coverage::Complete),
            Err(e) => return Err(e),
        };
        let coverage = match (coverage, size) {
            // The ranged reads judge completeness from the reply alone; with the size in
            // hand a full page that happens to be the whole value can be called complete.
            (Coverage::Partial { shown }, Some(n))
                if fits(shown, n) && !head.kind.is_cursor_paged() =>
            {
                Coverage::Complete
            }
            (c, _) => c,
        };

        Ok(KeyRead {
            meta: KeyMeta {
                key: key.into(),
                kind: head.kind,
                ttl_ms: head.ttl_ms,
                size,
                memory: head.memory,
            },
            value,
            coverage,
        })
    }

    /// Read up to [`PAGE`] elements starting at `offset`.
    ///
    /// `offset` is honoured exactly for the ordered types (list, zset), which is what
    /// `LRANGE`/`ZRANGE` index by.
    ///
    /// # Errors
    ///
    /// [`ConnError::Range`] for a non-zero `offset` on a hash, set or stream. Those are
    /// paged by an opaque cursor or an entry id, not an index. 0.1.7 silently ignored the
    /// offset and returned the first page again, so a caller paging by offset saw page
    /// one repeated forever and believed it had read the whole value.
    pub async fn read_value(&self, key: &str, kind: Kind, offset: usize) -> Result<KeyValue> {
        if offset != 0 && (kind.is_cursor_paged() || kind == Kind::Stream) {
            return Err(ConnError::Range {
                cmd: kind.size_cmd().unwrap_or("RANGE"),
                offset,
                detail: "this type is paged by cursor or entry id, not by offset",
            });
        }
        Ok(self.read_range(key, kind, offset).await?.0)
    }

    /// The first page of a value, and whether it was all of it.
    async fn read_page(&self, key: &str, kind: Kind) -> Result<(KeyValue, Coverage)> {
        self.read_range(key, kind, 0).await
    }

    async fn read_range(
        &self,
        key: &str,
        kind: Kind,
        offset: usize,
    ) -> Result<(KeyValue, Coverage)> {
        // Checked, not cast: an offset past `i64::MAX` wraps to a negative, which Redis
        // reads as "from the end" and turns a bounded page into the whole collection.
        let (start, stop) = page_bounds(offset, kind)?;

        let page = match kind {
            Kind::None => (KeyValue::Missing, Coverage::Complete),

            Kind::String => {
                // GETRANGE, not GET: a 500MB string must not be pulled into the TUI.
                if self.capabilities().has(Feature::GetRange) {
                    let reply = self
                        .cmd(
                            "GETRANGE",
                            vec![key.into(), 0.into(), STRING_PROBE_END.into()],
                        )
                        .await?;
                    decode_string(&reply)
                } else {
                    // Measuring first and issuing GET second has a race: another client can
                    // replace the value with a huge string between those commands. On a
                    // server without GETRANGE there is no genuinely bounded read.
                    (
                        KeyValue::Unsupported(
                            "this server has no bounded string read (GETRANGE)".into(),
                        ),
                        Coverage::Complete,
                    )
                }
            }

            Kind::List => {
                let reply = self
                    .cmd("LRANGE", vec![key.into(), start.into(), stop.into()])
                    .await?;
                ranged(KeyValue::List(as_string_vec(&reply)))
            }

            Kind::ZSet => {
                let reply = self
                    .cmd(
                        "ZRANGE",
                        vec![key.into(), start.into(), stop.into(), "WITHSCORES".into()],
                    )
                    .await?;
                ranged(KeyValue::ZSet(as_scored_pairs(&reply)))
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
                    (
                        KeyValue::Unsupported(
                            "this server has no bounded hash read (HSCAN)".into(),
                        ),
                        Coverage::Complete,
                    )
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
                    (
                        KeyValue::Unsupported("this server has no bounded set read (SSCAN)".into()),
                        Coverage::Complete,
                    )
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
                ranged(KeyValue::Stream(as_stream_entries(&reply)))
            }

            Kind::Other => (absent(Kind::Other), Coverage::Complete),
        };

        Ok(page)
    }
}

impl Kind {
    /// Hash and set are read by `HSCAN`/`SSCAN`, whose position is an opaque cursor.
    fn is_cursor_paged(&self) -> bool {
        matches!(self, Kind::Hash | Kind::Set)
    }
}

/// Whether `shown` rows account for a value of `size` elements.
fn fits(shown: usize, size: u64) -> bool {
    u64::try_from(shown).is_ok_and(|shown| shown >= size)
}

/// Coverage of a ranged (`LRANGE`/`ZRANGE`/`XRANGE … COUNT`) first page, from the reply
/// alone: a page that came back short reached the end; a full one may not have.
fn ranged(value: KeyValue) -> (KeyValue, Coverage) {
    let shown = value.len();
    let coverage = if shown < PAGE {
        Coverage::Complete
    } else {
        Coverage::Partial { shown }
    };
    (value, coverage)
}

/// Move one reply out of a pipeline result without disturbing the others' positions.
fn take_slot(replies: &mut [Result<Value>], i: usize) -> Option<Result<Value>> {
    replies
        .get_mut(i)
        .map(|r| std::mem::replace(r, Ok(Value::Null)))
}

/// The key's type from a pipeline's `TYPE` slot. An error in that slot is the read's
/// error: "the key is gone" and "we could not ask" are different answers.
fn type_from(replies: &[Result<Value>], i: usize) -> Result<Kind> {
    match replies.get(i) {
        Some(Ok(v)) => Ok(Kind::parse(&display_string(v))),
        Some(Err(ConnError::Command { source, .. })) => Err(ConnError::Command {
            cmd: "TYPE",
            source: source.clone(),
        }),
        Some(Err(e)) => Err(ConnError::Reply {
            cmd: "TYPE",
            detail: e.to_string(),
        }),
        None => Err(ConnError::Reply {
            cmd: "TYPE",
            detail: "missing from the pipeline reply".into(),
        }),
    }
}

/// Whether a command failed because the key holds another type.
fn is_wrongtype(e: &ConnError) -> bool {
    matches!(e, ConnError::Command { source, .. } if source.details().starts_with("WRONGTYPE"))
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
            vec![key.into(), 0.into(), STRING_PROBE_END.into()],
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
///
/// `size` is the element count read alongside, when it was; with it, a full first page
/// that is in fact the whole value can be called complete.
fn decode_value(kind: Kind, reply: &Value, size: Option<u64>) -> Result<(KeyValue, Coverage)> {
    let (value, coverage) = match kind {
        Kind::String => decode_string(reply),
        Kind::Hash => decode_hash(reply)?,
        Kind::List => ranged(KeyValue::List(as_string_vec(reply))),
        Kind::Set => decode_set(reply)?,
        Kind::ZSet => ranged(KeyValue::ZSet(as_scored_pairs(reply))),
        Kind::Stream => ranged(KeyValue::Stream(as_stream_entries(reply))),
        Kind::None | Kind::Other => (absent(kind), Coverage::Complete),
    };
    let coverage = match (coverage, size) {
        (Coverage::Partial { shown }, Some(n))
            if fits(shown, n) && !kind.is_cursor_paged() && kind != Kind::String =>
        {
            Coverage::Complete
        }
        (c, _) => c,
    };
    Ok((value, coverage))
}

/// What to show when there is no value to decode.
fn absent(kind: Kind) -> KeyValue {
    match kind {
        Kind::Other => {
            KeyValue::Unsupported("module type -- no viewer for this key yet".to_string())
        }
        _ => KeyValue::Missing,
    }
}

/// Decode a bounded `GETRANGE` reply of up to [`STRING_PROBE_BYTES`] bytes.
///
/// Truncated means the reply was *longer* than [`MAX_STRING_BYTES`] -- which the extra
/// probe byte makes knowable. What is kept is at most `MAX_STRING_BYTES`.
fn decode_string(reply: &Value) -> (KeyValue, Coverage) {
    // Measure the *reply*, not the rendered text. `display_string` turns a binary payload
    // into `<N bytes> …200 chars`, which is far shorter than the bytes it stands for.
    if reply_byte_len(reply) <= MAX_STRING_BYTES {
        return (KeyValue::String(display_string(reply)), Coverage::Complete);
    }
    let kept = match reply {
        Value::String(s) => Value::from(prefix_on_char_boundary(s, MAX_STRING_BYTES)),
        other => match other.as_bytes() {
            Some(b) => Value::Bytes(b.get(..MAX_STRING_BYTES).unwrap_or(b).to_vec().into()),
            None => other.clone(),
        },
    };
    let mut s = display_string(&kept);
    s.push_str("\n\n... truncated ...");
    (
        KeyValue::String(s),
        Coverage::Partial {
            shown: MAX_STRING_BYTES,
        },
    )
}

/// The longest prefix of `s` that is at most `max` bytes and ends on a char boundary.
fn prefix_on_char_boundary(s: &str, max: usize) -> &str {
    let mut end = max.min(s.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s.get(..end).unwrap_or_default()
}

fn decode_hash(reply: &Value) -> Result<(KeyValue, Coverage)> {
    let (cursor, items) = scan_items(reply)?;
    // SCAN COUNT is a target, not a hard limit: a compact-encoded hash comes back whole in
    // one step whatever COUNT says. Keep at most one page, and say when that cut anything.
    let capped = items.len() > PAGE * 2;
    let value = KeyValue::Hash(as_field_pairs(items.get(..PAGE * 2).unwrap_or(&items)));
    Ok(cursor_coverage(value, &cursor, capped))
}

fn decode_set(reply: &Value) -> Result<(KeyValue, Coverage)> {
    let (cursor, items) = scan_items(reply)?;
    let capped = items.len() > PAGE;
    let value = KeyValue::Set(items.iter().take(PAGE).map(display_string).collect());
    Ok(cursor_coverage(value, &cursor, capped))
}

/// A cursor-paged first page is complete only when the server said so (cursor `0`) and
/// nothing it sent was dropped by the page cap.
fn cursor_coverage(value: KeyValue, cursor: &str, capped: bool) -> (KeyValue, Coverage) {
    let coverage = if cursor == "0" && !capped {
        Coverage::Complete
    } else {
        Coverage::Partial { shown: value.len() }
    };
    (value, coverage)
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
fn scan_items(v: &Value) -> Result<(String, Vec<Value>)> {
    match v {
        Value::Array(parts) if parts.len() == 2 => match &parts[1] {
            Value::Array(items) => Ok((display_string(&parts[0]), items.clone())),
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
        let (cursor, items) = scan_items(&ok).unwrap();
        assert_eq!((cursor.as_str(), items.len()), ("0", 1));
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
            Value::from("0"),
            Value::Array((0..PAGE + 20).map(|i| Value::from(i as i64)).collect()),
        ]);
        let (KeyValue::Set(set), coverage) = decode_set(&set_reply).unwrap() else {
            unreachable!()
        };
        assert_eq!(set.len(), PAGE);
        // Cursor 0 says the server sent everything, but the cap dropped 20 of it.
        assert_eq!(coverage, Coverage::Partial { shown: PAGE });

        let hash_items = (0..PAGE + 20)
            .flat_map(|i| [Value::from(format!("f{i}")), Value::from("v")])
            .collect();
        let hash_reply = Value::Array(vec![Value::from("0"), Value::Array(hash_items)]);
        let (KeyValue::Hash(hash), coverage) = decode_hash(&hash_reply).unwrap() else {
            unreachable!()
        };
        assert_eq!(hash.len(), PAGE);
        assert_eq!(coverage, Coverage::Partial { shown: PAGE });
    }

    fn scan_reply(cursor: &str, n: usize) -> Value {
        Value::Array(vec![
            Value::from(cursor),
            Value::Array((0..n).map(|i| Value::from(format!("m{i}"))).collect()),
        ])
    }

    #[test]
    fn a_cursor_page_is_complete_only_when_the_cursor_says_so() {
        // 0.1.7 discarded the cursor: a 10-member first page of a 1,000-member set looked
        // exactly like a 10-member set.
        let (_, more) = decode_set(&scan_reply("1792", 10)).unwrap();
        assert_eq!(more, Coverage::Partial { shown: 10 });
        let (_, done) = decode_set(&scan_reply("0", 10)).unwrap();
        assert_eq!(done, Coverage::Complete);
    }

    #[test]
    fn a_full_ranged_page_is_partial_unless_the_size_proves_otherwise() {
        let full = Value::Array((0..PAGE).map(|i| Value::from(i as i64)).collect());
        let short = Value::Array((0..3).map(|i| Value::from(i as i64)).collect());

        assert_eq!(
            decode_value(Kind::List, &short, None).unwrap().1,
            Coverage::Complete
        );
        // A full page is what a larger list looks like, so without a size it is partial...
        assert_eq!(
            decode_value(Kind::List, &full, None).unwrap().1,
            Coverage::Partial { shown: PAGE }
        );
        assert_eq!(
            decode_value(Kind::List, &full, Some(5_000)).unwrap().1,
            Coverage::Partial { shown: PAGE }
        );
        // ...and complete when the size says it is all there is.
        assert_eq!(
            decode_value(Kind::List, &full, Some(PAGE as u64))
                .unwrap()
                .1,
            Coverage::Complete
        );
    }

    #[test]
    fn a_string_of_exactly_the_cap_is_not_marked_truncated() {
        // 0.1.7 asked for exactly MAX bytes and called `>= MAX` truncated, so a value of
        // exactly 64 KiB carried a "truncated" marker with nothing cut.
        let exact = Value::from("a".repeat(MAX_STRING_BYTES));
        let (KeyValue::String(s), coverage) = decode_string(&exact) else {
            unreachable!()
        };
        assert_eq!(coverage, Coverage::Complete);
        assert!(!s.contains("truncated"));

        // One byte over -- which only the probe byte can reveal -- is truncated, and what
        // is kept never exceeds the cap.
        let over = Value::from("a".repeat(STRING_PROBE_BYTES));
        let (KeyValue::String(s), coverage) = decode_string(&over) else {
            unreachable!()
        };
        assert_eq!(
            coverage,
            Coverage::Partial {
                shown: MAX_STRING_BYTES
            }
        );
        assert!(s.ends_with("... truncated ..."));
        let kept = s.split("\n\n").next().unwrap_or_default();
        assert_eq!(
            kept.len(),
            MAX_STRING_BYTES,
            "kept more or less than the cap"
        );
    }

    #[test]
    fn truncating_a_utf8_string_keeps_it_valid() {
        let raw = format!("{}é", "a".repeat(MAX_STRING_BYTES - 1)); // é straddles the cap
        let (KeyValue::String(s), _) = decode_string(&Value::from(raw)) else {
            unreachable!()
        };
        assert!(s.starts_with(&"a".repeat(MAX_STRING_BYTES - 1)));
        assert!(
            !s.contains('é'),
            "a split char must be dropped, not mangled"
        );
    }

    #[test]
    fn the_speculative_string_read_asks_for_one_probe_byte() {
        let (cmd, args) = value_cmd(Kind::String, "k");
        assert_eq!(cmd, "GETRANGE");
        assert_eq!(
            args.get(2).and_then(Value::as_i64),
            Some(MAX_STRING_BYTES as i64)
        );
    }

    #[test]
    fn a_failed_type_is_an_error_not_an_absent_key() {
        let denied = Err(ConnError::Command {
            cmd: "TYPE",
            source: fred::error::Error::new(
                fred::error::ErrorKind::Unknown,
                "NOPERM this user has no permissions to run the 'type' command",
            ),
        });
        assert!(type_from(&[denied], 0).is_err());
        assert_eq!(
            type_from(&[Ok(Value::from("none"))], 0).unwrap(),
            Kind::None
        );
    }

    #[test]
    fn wrongtype_is_recognised_and_other_failures_are_not() {
        let err = |msg: &'static str| ConnError::Command {
            cmd: "HSCAN",
            source: fred::error::Error::new(fred::error::ErrorKind::Unknown, msg),
        };
        assert!(is_wrongtype(&err(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )));
        assert!(!is_wrongtype(&err("NOPERM no permissions")));
    }
}
