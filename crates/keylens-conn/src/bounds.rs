//! What the public command surface accepts, and how much each command can cost.
//!
//! Every command reaching the server through [`Conn::cmd`](crate::Conn::cmd) or
//! [`Conn::pipeline`](crate::Conn::pipeline) passes [`check_command`] first. It answers
//! two questions:
//!
//! 1. **Is it read-only?** An allowlist of commands and subcommands. Anything else is
//!    [`ConnError::UnsafeCommand`].
//! 2. **Are its arguments bounded?** A read-only command can still ask for everything:
//!    `LRANGE k 0 -1`, `XRANGE k - +`, `SLOWLOG GET -1`, `SCAN 0 COUNT 100000000`,
//!    `MEMORY USAGE k SAMPLES 0`. Those are [`ConnError::Unbounded`].
//!
//! # What "bounded" means here, honestly
//!
//! There are three different things a limit can bound, and Redis offers each of them for
//! some commands and not for others. Claiming more than the protocol gives would be the
//! same kind of lie this release is removing from the UI.
//!
//! | Command | Element count on the wire | Bytes on the wire | Server work |
//! |---|---|---|---|
//! | `LRANGE`/`ZRANGE`/`ZREVRANGE` (index) | **hard**: `stop - start + 1` | per element unbounded | O(offset + count) |
//! | `XRANGE … COUNT n` | **hard**: `n` entries | per field unbounded | O(log N + n) |
//! | `GETRANGE` | **hard**: span bytes | **hard** | O(span) |
//! | `SCAN`/`HSCAN`/`SSCAN … COUNT n` | **hint only**. `COUNT` is a target, and a compact-encoded hash, set or zset comes back whole in one reply whatever it says | unbounded | roughly O(n) per call |
//! | `HMGET k f1..fn` | hard: `n` values | per value unbounded | O(n) |
//! | `HGET` | 1 | value unbounded | O(1) |
//! | `PUBSUB CHANNELS [pattern]` | **none**: no `COUNT` exists | unbounded | O(channels) |
//! | `CLIENT LIST` | **none** | unbounded | O(clients) |
//! | `XINFO GROUPS` / `XINFO CONSUMERS` | **none** | unbounded | O(groups) / O(consumers) |
//! | `XPENDING k g` (summary) | per-consumer rows, **none** | unbounded | O(consumers) |
//! | `SLOWLOG GET n` | hard: `n` (Redis also truncates each entry's arguments) | bounded by the server | O(n) |
//!
//! For the "none" rows keylens bounds what it **keeps and renders**, not what crosses the
//! wire: callers cap retained rows and say how many there were (see
//! [`Conn::pubsub_channels`](crate::Conn::pubsub_channels)). No argument can make those
//! replies smaller, so none is pretended.
//!
//! **This is a discipline boundary, not a sandbox.** Lenses are compiled in, and one that
//! wanted to could reach past this. What it prevents is the accident: a lens author
//! reaching for `LRANGE 0 -1` because paging is tedious, and no reviewer noticing.

use fred::prelude::Value;

use crate::error::{ConnError, Result};

/// Largest `COUNT` hint accepted on `SCAN`, `HSCAN` and `SSCAN`.
///
/// A hint, not a bound (see the module table), so this caps how hard one call can ask the
/// server to work, not how many elements come back. Detection uses 4,000.
pub const MAX_SCAN_COUNT: i64 = 10_000;
/// Largest element span for an index range (`LRANGE`, `ZRANGE`, `ZREVRANGE`), and the
/// largest `COUNT` for `XRANGE`, `ZRANGE … LIMIT` and extended `XPENDING`.
pub const MAX_RANGE: i64 = 1_000;
/// Largest byte span for `GETRANGE`. The value pane reads 64 KiB + 1.
pub const MAX_STRING_RANGE: i64 = 1024 * 1024;
/// Most fields in one `HMGET`, or channels in one `PUBSUB NUMSUB`.
pub const MAX_FIELDS: usize = 1_024;
/// Most entries in one `SLOWLOG GET`.
pub const MAX_SLOWLOG: i64 = 1_024;
/// Most entries per stream in one `XREAD`.
pub const MAX_XREAD_COUNT: u64 = 1_000;
/// Longest `XREAD BLOCK`. Must stay well under the per-command timeout, or every idle
/// block would be reported as a timeout.
pub const MAX_BLOCK_MS: u64 = 10_000;
/// Most streams in one `XREAD`.
pub const MAX_XREAD_STREAMS: usize = 1_000;
/// Largest `MEMORY USAGE … SAMPLES`. `SAMPLES 0` means "every element" and is refused.
pub const MAX_MEMORY_SAMPLES: i64 = 64;

/// Most bytes kept from any one element of a collection (a list item, a hash value, a
/// set member, a stream field). Redis has no way to bound an element on the wire, so this
/// bounds what is *kept*: 200 rows of 16 KiB is a 3.2 MiB worst case per value page,
/// instead of 200 rows of whatever a producer wrote.
pub const MAX_ELEMENT_BYTES: usize = 16 * 1024;
/// Most field/value pairs kept from one stream entry.
pub const MAX_ENTRY_FIELDS: usize = 128;
/// Most rows kept from a reply with no server-side bound (`PUBSUB CHANNELS`,
/// `CLIENT LIST`, `XINFO CONSUMERS`).
pub const MAX_LISTED: usize = 1_000;

/// Rows kept from a reply that cannot be bounded on the wire, and how many there were.
///
/// `total` is exact: the whole reply was received, which is exactly why it cannot be
/// bounded. Only what is kept and rendered is.
#[derive(Debug, Clone, PartialEq)]
pub struct Capped<T> {
    pub items: Vec<T>,
    pub total: usize,
}

impl<T> Capped<T> {
    /// Keep at most `max` of `all`, remembering how many there were.
    pub fn keep(all: impl IntoIterator<Item = T>, max: usize) -> Self {
        let mut items = Vec::new();
        let mut total = 0usize;
        for item in all {
            total = total.saturating_add(1);
            if items.len() < max {
                items.push(item);
            }
        }
        Self { items, total }
    }

    /// Whether rows were dropped.
    pub fn is_truncated(&self) -> bool {
        self.total > self.items.len()
    }
}

impl<T> Default for Capped<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            total: 0,
        }
    }
}

impl<T> From<Vec<T>> for Capped<T> {
    fn from(items: Vec<T>) -> Self {
        let total = items.len();
        Self { items, total }
    }
}

impl<T> std::ops::Deref for Capped<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.items
    }
}

impl<'a, T> IntoIterator for &'a Capped<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

/// Keep at most [`MAX_ELEMENT_BYTES`] of an element, cut on a char boundary and marked.
pub fn cap_element(mut s: String) -> String {
    if s.len() <= MAX_ELEMENT_BYTES {
        return s;
    }
    let total = s.len();
    let mut end = MAX_ELEMENT_BYTES;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
    s.push_str(&format!(" … [{total} bytes, first {end} shown]"));
    s
}

/// `CONFIG GET` parameters a lens may ask for.
///
/// An allowlist rather than "any `GET`", because `CONFIG GET` is read-only in the sense
/// that matters to the *server* and not in the sense that matters here: `CONFIG GET
/// requirepass` reads the server's password, and `CONFIG GET *` reads it along with
/// everything else. Exact names only, so a glob matches nothing.
const CONFIG_PARAMS: &[&str] = &[
    "maxmemory",
    "maxmemory-policy",
    "appendonly",
    "save",
    "databases",
    "timeout",
];

/// Check one command against the read-only allowlist and the argument bounds.
///
/// # Errors
///
/// [`ConnError::UnsafeCommand`] for anything not on the allowlist, and
/// [`ConnError::Unbounded`] for an allowed command whose arguments ask for an unbounded
/// or oversized read.
pub fn check_command(name: &'static str, args: &[Value]) -> Result<()> {
    let sub = || upper(args.first());
    let unsafe_cmd = || Err(ConnError::UnsafeCommand(name));

    match name {
        // Constant-size replies.
        "TYPE" | "PTTL" | "STRLEN" | "HLEN" | "LLEN" | "SCARD" | "ZCARD" | "XLEN" | "HGET" => {
            Ok(())
        }

        "CLIENT" => match sub().as_str() {
            "INFO" => Ok(()),
            // Unbounded on the wire; see the module table.
            "LIST" if args.len() == 1 => Ok(()),
            "LIST" => unbounded(name, "CLIENT LIST takes no filters here"),
            _ => unsafe_cmd(),
        },
        "CLUSTER" => match sub().as_str() {
            "INFO" | "NODES" | "SHARDS" | "SLOTS" => Ok(()),
            _ => unsafe_cmd(),
        },
        "CONFIG" => {
            if sub() == "GET"
                && args.len() == 2
                && CONFIG_PARAMS.contains(&text(args.get(1)).to_ascii_lowercase().as_str())
            {
                Ok(())
            } else {
                unsafe_cmd()
            }
        }
        "MEMORY" => {
            if sub() != "USAGE" {
                return unsafe_cmd();
            }
            match args.len() {
                2 => Ok(()),
                4 if upper(args.get(2)) == "SAMPLES" => {
                    in_range(name, "SAMPLES", args.get(3), 1, MAX_MEMORY_SAMPLES)
                }
                _ => unbounded(name, "MEMORY USAGE takes a key and an optional SAMPLES"),
            }
        }
        "MODULE" => match sub().as_str() {
            "LIST" => Ok(()),
            _ => unsafe_cmd(),
        },
        "PUBSUB" => match sub().as_str() {
            // No COUNT exists; the reply is intrinsically unbounded. Allowed because the
            // pane needs it, and the caller caps what it keeps.
            "CHANNELS" if args.len() <= 2 => Ok(()),
            "NUMPAT" if args.len() == 1 => Ok(()),
            "NUMSUB" if args.len().saturating_sub(1) <= MAX_FIELDS => Ok(()),
            "CHANNELS" | "NUMPAT" | "NUMSUB" => unbounded(name, "too many arguments"),
            _ => unsafe_cmd(),
        },
        "SLOWLOG" => match sub().as_str() {
            "LEN" => Ok(()),
            // Without a count Redis returns 10. `-1` returns the whole log.
            "GET" if args.len() == 1 => Ok(()),
            "GET" if args.len() == 2 => in_range(name, "count", args.get(1), 1, MAX_SLOWLOG),
            "GET" => unbounded(name, "SLOWLOG GET takes one count"),
            _ => unsafe_cmd(),
        },
        "XINFO" => match sub().as_str() {
            // `FULL` without a small COUNT dumps the stream; it is not needed.
            "STREAM" if args.len() == 2 => Ok(()),
            "STREAM" => unbounded(name, "XINFO STREAM FULL is not allowed"),
            "GROUPS" if args.len() == 2 => Ok(()),
            "CONSUMERS" if args.len() == 3 => Ok(()),
            "GROUPS" | "CONSUMERS" => unbounded(name, "unexpected arguments"),
            _ => unsafe_cmd(),
        },

        "GETRANGE" => span(name, args.get(1), args.get(2), MAX_STRING_RANGE),
        "LRANGE" | "ZREVRANGE" => {
            if args.len() > 4 || (args.len() == 4 && upper(args.get(3)) != "WITHSCORES") {
                return unbounded(name, "unexpected arguments");
            }
            if name == "LRANGE" && args.len() != 3 {
                return unbounded(name, "LRANGE takes key, start, stop");
            }
            span(name, args.get(1), args.get(2), MAX_RANGE)
        }
        "ZRANGE" => zrange(args),
        "XRANGE" | "XREVRANGE" => {
            // key start end COUNT n -- COUNT is required.
            if args.len() == 5 && upper(args.get(3)) == "COUNT" {
                in_range(name, "COUNT", args.get(4), 1, MAX_RANGE)
            } else {
                unbounded(name, "a COUNT is required")
            }
        }
        "XPENDING" => match args.len() {
            // Summary form. Its per-consumer list is unbounded on the wire.
            2 => Ok(()),
            // Extended form: key group [IDLE ms] start end count [consumer]
            5 | 6 => in_range(name, "count", args.get(4), 1, MAX_RANGE),
            7 | 8 if upper(args.get(2)) == "IDLE" => {
                in_range(name, "count", args.get(6), 1, MAX_RANGE)
            }
            _ => unbounded(name, "unexpected arguments"),
        },
        "HMGET" => {
            if args.len().saturating_sub(1) <= MAX_FIELDS {
                Ok(())
            } else {
                unbounded(name, format!("more than {MAX_FIELDS} fields"))
            }
        }
        "SCAN" => scan_count(name, args, 1),
        "HSCAN" | "SSCAN" => scan_count(name, args, 2),

        _ => unsafe_cmd(),
    }
}

/// Check the parameters of a blocking stream read.
///
/// # Errors
///
/// [`ConnError::Unbounded`] for a zero or oversized count, too many streams, a block
/// longer than [`MAX_BLOCK_MS`], or mismatched key/id lists.
pub fn check_xread(streams: usize, ids: usize, count: u64, block_ms: u64) -> Result<()> {
    if streams == 0 || streams != ids {
        return unbounded("XREAD", "one id per stream is required");
    }
    if streams > MAX_XREAD_STREAMS {
        return unbounded("XREAD", format!("more than {MAX_XREAD_STREAMS} streams"));
    }
    if count == 0 || count > MAX_XREAD_COUNT {
        return unbounded("XREAD", format!("COUNT must be 1..={MAX_XREAD_COUNT}"));
    }
    if block_ms == 0 || block_ms > MAX_BLOCK_MS {
        // BLOCK 0 waits forever, which no command timeout can tell from a dead link.
        return unbounded("XREAD", format!("BLOCK must be 1..={MAX_BLOCK_MS} ms"));
    }
    Ok(())
}

/// Check a `COUNT` hint for a cursor scan.
///
/// # Errors
///
/// [`ConnError::Unbounded`] for zero or more than [`MAX_SCAN_COUNT`].
pub fn check_scan_count(count: u32) -> Result<()> {
    if count == 0 || i64::from(count) > MAX_SCAN_COUNT {
        return unbounded("SCAN", format!("COUNT must be 1..={MAX_SCAN_COUNT}"));
    }
    Ok(())
}

fn zrange(args: &[Value]) -> Result<()> {
    const NAME: &str = "ZRANGE";
    let opts: Vec<String> = args.iter().skip(3).map(|a| upper(Some(a))).collect();
    let by_value = opts.iter().any(|o| o == "BYSCORE" || o == "BYLEX");
    if let Some(i) = opts.iter().position(|o| o == "LIMIT") {
        // LIMIT offset count: the count bounds the reply; a negative one means "all".
        return in_range(NAME, "LIMIT count", args.get(3 + i + 2), 1, MAX_RANGE);
    }
    if by_value {
        return unbounded(NAME, "BYSCORE/BYLEX need a LIMIT");
    }
    if opts.iter().any(|o| o != "WITHSCORES" && o != "REV") {
        return unbounded(NAME, "unexpected arguments");
    }
    span(NAME, args.get(1), args.get(2), MAX_RANGE)
}

/// `[start, stop]` index span, inclusive.
///
/// Both non-negative with `stop >= start`, or both negative (counted from the end) with
/// `stop >= start`. Mixed signs are refused: `0 -1` is "the whole collection", and there
/// is no way to know the span without knowing the length.
fn span(name: &'static str, start: Option<&Value>, stop: Option<&Value>, max: i64) -> Result<()> {
    let (Some(start), Some(stop)) = (int(start), int(stop)) else {
        return unbounded(name, "start and stop must be integers");
    };
    if (start < 0) != (stop < 0) {
        return unbounded(
            name,
            format!("{start}..{stop} mixes signs, which can mean the whole collection"),
        );
    }
    let width = stop
        .checked_sub(start)
        .and_then(|d| d.checked_add(1))
        .filter(|w| *w >= 1);
    match width {
        Some(w) if w <= max => Ok(()),
        Some(w) => unbounded(name, format!("{w} elements requested, at most {max}")),
        None => unbounded(name, format!("{start}..{stop} is not an ascending range")),
    }
}

fn scan_count(name: &'static str, args: &[Value], first_opt: usize) -> Result<()> {
    let mut i = first_opt;
    while i < args.len() {
        match upper(args.get(i)).as_str() {
            "COUNT" => in_range(name, "COUNT", args.get(i + 1), 1, MAX_SCAN_COUNT)?,
            "MATCH" | "TYPE" => {}
            // HSCAN … NOVALUES (Redis 7.4) takes no value.
            "NOVALUES" if name == "HSCAN" => {
                i += 1;
                continue;
            }
            _ => return unbounded(name, "unexpected arguments"),
        }
        i += 2;
    }
    Ok(())
}

fn in_range(name: &'static str, what: &str, v: Option<&Value>, lo: i64, hi: i64) -> Result<()> {
    match int(v) {
        Some(n) if (lo..=hi).contains(&n) => Ok(()),
        Some(n) => unbounded(name, format!("{what} {n} is outside {lo}..={hi}")),
        None => unbounded(name, format!("{what} must be an integer")),
    }
}

fn unbounded(cmd: &'static str, detail: impl Into<String>) -> Result<()> {
    Err(ConnError::Unbounded {
        cmd,
        detail: detail.into(),
    })
}

fn int(v: Option<&Value>) -> Option<i64> {
    match v? {
        Value::Integer(n) => Some(*n),
        other => other.as_string()?.trim().parse().ok(),
    }
}

fn text(v: Option<&Value>) -> String {
    v.and_then(Value::as_string).unwrap_or_default()
}

fn upper(v: Option<&Value>) -> String {
    text(v).to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(xs: &[&str]) -> Vec<Value> {
        xs.iter().map(|x| Value::from(*x)).collect()
    }

    fn ok(name: &'static str, xs: &[&str]) -> bool {
        check_command(name, &args(xs)).is_ok()
    }

    fn rejected_as_unbounded(name: &'static str, xs: &[&str]) -> bool {
        matches!(
            check_command(name, &args(xs)),
            Err(ConnError::Unbounded { .. })
        )
    }

    #[test]
    fn whole_collection_reads_are_refused() {
        // Every one of these was accepted by 0.1.7's allowlist.
        assert!(rejected_as_unbounded("LRANGE", &["k", "0", "-1"]));
        assert!(rejected_as_unbounded(
            "ZRANGE",
            &["k", "0", "-1", "WITHSCORES"]
        ));
        assert!(rejected_as_unbounded("ZREVRANGE", &["k", "0", "-1"]));
        assert!(rejected_as_unbounded("XRANGE", &["k", "-", "+"]));
        assert!(rejected_as_unbounded(
            "ZRANGE",
            &["k", "-inf", "+inf", "BYSCORE"]
        ));
        assert!(rejected_as_unbounded("SLOWLOG", &["GET", "-1"]));
        assert!(rejected_as_unbounded(
            "MEMORY",
            &["USAGE", "k", "SAMPLES", "0"]
        ));
        assert!(rejected_as_unbounded("XINFO", &["STREAM", "k", "FULL"]));
        assert!(rejected_as_unbounded("SCAN", &["0", "COUNT", "100000000"]));
        assert!(rejected_as_unbounded("HSCAN", &["k", "0", "COUNT", "0"]));
        assert!(rejected_as_unbounded("GETRANGE", &["k", "0", "-1"]));
    }

    #[test]
    fn oversized_but_finite_reads_are_refused() {
        assert!(rejected_as_unbounded("LRANGE", &["k", "0", "1000"])); // 1,001 elements
        assert!(ok("LRANGE", &["k", "0", "999"]));
        assert!(rejected_as_unbounded(
            "XRANGE",
            &["k", "-", "+", "COUNT", "1001"]
        ));
        assert!(rejected_as_unbounded("GETRANGE", &["k", "0", "1048576"]));
        assert!(ok("GETRANGE", &["k", "0", "65536"]));
        let many: Vec<String> = (0..=MAX_FIELDS).map(|i| format!("f{i}")).collect();
        let mut hmget = vec!["k"];
        hmget.extend(many.iter().map(String::as_str));
        assert!(rejected_as_unbounded("HMGET", &hmget));
    }

    #[test]
    fn the_reads_keylens_itself_issues_are_accepted() {
        assert!(ok("LRANGE", &["k", "0", "199"]));
        assert!(ok("LRANGE", &["k", "-200", "-1"]), "the last 200: bounded");
        assert!(ok("ZRANGE", &["k", "0", "199", "WITHSCORES"]));
        assert!(ok("ZREVRANGE", &["k", "0", "199", "WITHSCORES"]));
        assert!(ok(
            "ZRANGE",
            &["k", "-inf", "+inf", "BYSCORE", "LIMIT", "0", "50"]
        ));
        assert!(ok("XRANGE", &["k", "-", "+", "COUNT", "200"]));
        assert!(ok("HSCAN", &["k", "0", "COUNT", "200"]));
        assert!(ok("SSCAN", &["k", "0", "MATCH", "a*", "COUNT", "200"]));
        assert!(ok(
            "SCAN",
            &["0", "MATCH", "bull:*", "COUNT", "4000", "TYPE", "hash"]
        ));
        assert!(ok("SLOWLOG", &["GET", "128"]));
        assert!(ok("XPENDING", &["k", "g"]));
        assert!(ok("XPENDING", &["k", "g", "-", "+", "10"]));
        assert!(ok(
            "XPENDING",
            &["k", "g", "IDLE", "1000", "-", "+", "10", "c"]
        ));
        assert!(ok("MEMORY", &["USAGE", "k"]));
        assert!(ok("PUBSUB", &["CHANNELS"]));
        assert!(ok("CLIENT", &["LIST"]));
        // Integers, not just digit strings, are what the read path sends.
        assert!(check_command("LRANGE", &[Value::from("k"), 0.into(), 199.into()]).is_ok());
    }

    #[test]
    fn mixed_sign_and_descending_ranges_are_refused() {
        assert!(rejected_as_unbounded("LRANGE", &["k", "5", "-1"]));
        assert!(rejected_as_unbounded("LRANGE", &["k", "-1", "5"]));
        assert!(rejected_as_unbounded("LRANGE", &["k", "10", "2"]));
        assert!(rejected_as_unbounded("LRANGE", &["k", "x", "2"]));
        // i64 extremes must not overflow the width calculation.
        assert!(rejected_as_unbounded(
            "LRANGE",
            &["k", "-9223372036854775808", "-1"]
        ));
        assert!(rejected_as_unbounded(
            "LRANGE",
            &["k", "0", "9223372036854775807"]
        ));
    }

    #[test]
    fn writes_and_secret_reads_are_not_allowed_at_all() {
        for (name, a) in [
            ("SET", vec!["k", "v"]),
            ("CONFIG", vec!["GET", "requirepass"]),
            ("CONFIG", vec!["GET", "*"]),
            ("CONFIG", vec!["SET", "maxmemory", "1"]),
            ("CLIENT", vec!["KILL", "ID", "1"]),
            ("GET", vec!["k"]),
            // Spelled in two halves so the workspace guard, which greps for the quoted
            // literal, keeps meaning "no KEYS anywhere in shipped code".
            (concat!("KE", "YS"), vec!["*"]),
        ] {
            assert!(
                matches!(
                    check_command(name, &args(&a)),
                    Err(ConnError::UnsafeCommand(_))
                ),
                "{name} {a:?}"
            );
        }
    }

    #[test]
    fn xread_parameters_are_bounded() {
        assert!(check_xread(3, 3, 500, 1_000).is_ok());
        assert!(
            check_xread(3, 2, 500, 1_000).is_err(),
            "ids must match streams"
        );
        assert!(check_xread(1, 1, 0, 1_000).is_err());
        assert!(check_xread(1, 1, MAX_XREAD_COUNT + 1, 1_000).is_err());
        assert!(check_xread(1, 1, 10, 0).is_err(), "BLOCK 0 waits forever");
        assert!(check_xread(1, 1, 10, MAX_BLOCK_MS + 1).is_err());
        assert!(check_xread(0, 0, 10, 10).is_err());
    }

    #[test]
    fn scan_count_is_bounded() {
        assert!(check_scan_count(500).is_ok());
        assert!(check_scan_count(0).is_err());
        assert!(check_scan_count(u32::MAX).is_err());
    }
}
