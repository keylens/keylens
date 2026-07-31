//! Display formatting.

use std::fmt::Write as _;

/// Human TTL from milliseconds. `None` renders as a dash, not "0" -- "no expiry" and
/// "expires now" are very different facts.
pub fn ttl(ms: Option<i64>) -> String {
    let Some(ms) = ms else { return "-".into() };
    if ms < 1000 {
        return format!("{ms}ms");
    }

    let secs = ms / 1000;
    let (d, h, m, s) = (secs / 86_400, (secs % 86_400) / 3600, (secs % 3600) / 60, secs % 60);

    let mut out = String::new();
    if d > 0 {
        let _ = write!(out, "{d}d");
    }
    if h > 0 {
        let _ = write!(out, "{h}h");
    }
    // Minutes are noise once we're past a day.
    if m > 0 && d == 0 {
        let _ = write!(out, "{m}m");
    }
    if s > 0 && d == 0 && h == 0 {
        let _ = write!(out, "{s}s");
    }
    if out.is_empty() { format!("{secs}s") } else { out }
}

pub fn bytes(n: Option<u64>) -> String {
    match n {
        Some(n) => bytesize::ByteSize(n).to_string(),
        None => "-".into(),
    }
}

/// Thousands separators, because a raw `1048576` in a count column is unreadable.
pub fn count(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Pretty-print a JSON payload, or return `None` when it isn't JSON.
///
/// Job payloads are JSON far more often than not, and reading a minified blob in a pane
/// is the difference between the viewer being useful and being a hex dump.
pub fn pretty_json(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    // Cheap reject before paying for a parse on every value.
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return None;
    }
    let parsed: serde_json::Value = serde_json::from_str(trimmed).ok()?;
    serde_json::to_string_pretty(&parsed).ok()
}

/// Truncate to `max` chars with an ellipsis, counting characters rather than bytes so a
/// multi-byte payload can't panic the renderer mid-codepoint.
pub fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let keep = max.saturating_sub(1);
    let mut out: String = s.chars().take(keep).collect();
    out.push('…');
    out
}

/// Collapse newlines so a multi-line value still occupies one row in a table.
pub fn single_line(s: &str) -> String {
    s.replace(['\n', '\r'], " ")
}

/// A text meter, e.g. `████████░░░░░░░░`.
///
/// `fraction` is clamped: a `used_memory` above `maxmemory` is a real state Redis can be
/// in, and it must render as a full bar rather than overflowing the column.
pub fn bar(fraction: f64, width: usize) -> String {
    let f = fraction.clamp(0.0, 1.0);
    let filled = (f * width as f64).round() as usize;
    let filled = filled.min(width);
    format!("{}{}", "█".repeat(filled), "░".repeat(width - filled))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_distinguishes_no_expiry_from_zero() {
        assert_eq!(ttl(None), "-");
        assert_eq!(ttl(Some(0)), "0ms");
    }

    #[test]
    fn ttl_scales() {
        assert_eq!(ttl(Some(500)), "500ms");
        assert_eq!(ttl(Some(45_000)), "45s");
        assert_eq!(ttl(Some(90_000)), "1m30s");
        assert_eq!(ttl(Some(3_600_000)), "1h");
        assert_eq!(ttl(Some(90_000_000)), "1d1h");
    }

    #[test]
    fn counts_get_separators() {
        assert_eq!(count(0), "0");
        assert_eq!(count(999), "999");
        assert_eq!(count(1_000), "1,000");
        assert_eq!(count(1_048_576), "1,048,576");
    }

    #[test]
    fn pretty_json_only_for_json() {
        assert!(pretty_json(r#"{"a":1}"#).unwrap().contains("\"a\": 1"));
        assert!(pretty_json("[1,2]").is_some());
        assert_eq!(pretty_json("plain text"), None);
        // Looks like JSON, isn't.
        assert_eq!(pretty_json("{not json}"), None);
    }

    #[test]
    fn truncate_counts_chars_not_bytes() {
        // Byte-slicing this would panic mid-codepoint.
        let s = "日本語のテキストです";
        assert_eq!(truncate(s, 4), "日本語…");
        assert_eq!(truncate("short", 10), "short");
    }

    #[test]
    fn single_line_flattens() {
        assert_eq!(single_line("a\nb\r\nc"), "a b  c");
    }

    #[test]
    fn bar_is_always_exactly_width_cells() {
        for f in [-1.0, 0.0, 0.33, 0.5, 1.0, 2.5] {
            assert_eq!(bar(f, 10).chars().count(), 10, "fraction {f} broke the width");
        }
    }

    #[test]
    fn bar_fills_proportionally() {
        assert_eq!(bar(0.0, 4), "░░░░");
        assert_eq!(bar(0.5, 4), "██░░");
        assert_eq!(bar(1.0, 4), "████");
        // Over 100% happens when used_memory exceeds maxmemory; clamp, don't overflow.
        assert_eq!(bar(1.8, 4), "████");
    }
}
