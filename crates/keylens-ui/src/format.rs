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
    let (d, h, m, s) = (
        secs / 86_400,
        (secs % 86_400) / 3600,
        (secs % 3600) / 60,
        secs % 60,
    );

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
    if out.is_empty() {
        format!("{secs}s")
    } else {
        out
    }
}

/// Relative time between two millisecond timestamps.
///
/// A raw epoch is unreadable and an absolute clock time makes you do the subtraction
/// yourself; "3m ago" is the thing you actually wanted to know. Future timestamps read as
/// "in 3m", which is what `delayed` jobs need.
pub fn ago(timestamp_ms: i64, now_ms: i64) -> String {
    let delta = now_ms - timestamp_ms;
    let magnitude = ttl(Some(delta.abs()));
    if delta >= 0 {
        format!("{magnitude} ago")
    } else {
        format!("in {magnitude}")
    }
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

/// Eight levels of vertical block, plus a blank for "no data".
const SPARK_LEVELS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

/// A unicode sparkline over `values`, scaled to the largest value present.
///
/// Scaling is per-series and relative, which is the point: the question a throughput graph
/// answers is "is this queue spiking *right now*", not "how does it compare to a fixed
/// axis". A flat series renders flat rather than maxed out.
pub fn sparkline(values: &[u64]) -> String {
    let max = values.iter().copied().max().unwrap_or(0);
    if max == 0 {
        // All-zero is a real state and must not render as a full bar.
        return "·".repeat(values.len());
    }

    values
        .iter()
        .map(|v| {
            if *v == 0 {
                return '·';
            }
            // -1 so the max lands on the top level rather than overflowing the index.
            let idx = ((*v as f64 / max as f64) * (SPARK_LEVELS.len() - 1) as f64).round() as usize;
            SPARK_LEVELS[idx.min(SPARK_LEVELS.len() - 1)]
        })
        .collect()
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
    fn relative_time_reads_forwards_and_backwards() {
        let now = 1_785_515_393_000_i64;
        assert_eq!(ago(now - 5_000, now), "5s ago");
        assert_eq!(ago(now - 180_000, now), "3m ago");
        assert_eq!(ago(now - 7_200_000, now), "2h ago");
        // Delayed jobs are scheduled for the future.
        assert_eq!(ago(now + 180_000, now), "in 3m");
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
    fn sparkline_is_one_cell_per_value() {
        assert_eq!(sparkline(&[1, 2, 3]).chars().count(), 3);
        assert_eq!(sparkline(&[]).chars().count(), 0);
    }

    #[test]
    fn an_idle_series_is_not_a_full_bar() {
        // Scaling to a zero max would divide by zero or paint every cell solid; an idle
        // queue must look idle.
        assert_eq!(sparkline(&[0, 0, 0]), "···");
    }

    #[test]
    fn sparkline_scales_to_the_series_maximum() {
        let s: Vec<char> = sparkline(&[0, 1, 10]).chars().collect();
        assert_eq!(s[0], '·', "zero is a gap, not a low bar");
        assert_eq!(s[2], '█', "the max reaches the top level");
        assert!(s[1] < s[2], "a smaller value renders lower");
    }

    #[test]
    fn a_flat_nonzero_series_renders_flat() {
        let s = sparkline(&[5, 5, 5]);
        assert!(s.chars().all(|c| c == '█'), "got {s}");
    }

    #[test]
    fn bar_is_always_exactly_width_cells() {
        for f in [-1.0, 0.0, 0.33, 0.5, 1.0, 2.5] {
            assert_eq!(
                bar(f, 10).chars().count(),
                10,
                "fraction {f} broke the width"
            );
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
