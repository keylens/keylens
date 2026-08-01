//! The `KEYLENS` wordmark and startup splash.
//!
//! The splash covers the gap between "process started" and "first batch of keys arrived",
//! which on a cold remote keyspace is a real second or two of otherwise-blank screen.

use ratatui::text::{Line, Span};

use crate::theme::Theme;

/// Block letters, 7 glyphs at 8 columns each plus a trailing column.
const WORDMARK: [&str; 6] = [
    "██╗  ██╗███████╗██╗   ██╗██╗     ███████╗███╗   ██╗███████╗",
    "██║ ██╔╝██╔════╝╚██╗ ██╔╝██║     ██╔════╝████╗  ██║██╔════╝",
    "█████╔╝ █████╗   ╚████╔╝ ██║     █████╗  ██╔██╗ ██║███████╗",
    "██╔═██╗ ██╔══╝    ╚██╔╝  ██║     ██╔══╝  ██║╚██╗██║╚════██║",
    "██║  ██╗███████╗   ██║   ███████╗███████╗██║ ╚████║███████║",
    "╚═╝  ╚═╝╚══════╝   ╚═╝   ╚══════╝╚══════╝╚═╝  ╚═══╝╚══════╝",
];

/// Width of the block wordmark in columns.
pub const WORDMARK_WIDTH: u16 = 59;
/// Columns covered by `KEY`; the rest is `LENS`. Splitting here is what gives the
/// two-tone brand read rather than one flat block of colour.
const KEY_COLUMNS: usize = 24;

/// The wordmark as styled lines, or a compact fallback when the terminal is too narrow.
///
/// Falling back matters: a 40-column terminal would otherwise show the block letters
/// sliced mid-glyph, which looks broken rather than minimal.
pub fn wordmark(available_width: u16) -> Vec<Line<'static>> {
    if available_width < WORDMARK_WIDTH {
        return vec![Line::from(vec![
            Span::styled("KEY", Theme::brand_a()),
            Span::styled("LENS", Theme::brand_b()),
        ])];
    }

    WORDMARK
        .iter()
        .map(|row| {
            // Split on character count, not bytes: these are multi-byte box glyphs.
            let chars: Vec<char> = row.chars().collect();
            let split = KEY_COLUMNS.min(chars.len());
            let (key, lens): (String, String) = (
                chars[..split].iter().collect(),
                chars[split..].iter().collect(),
            );

            Line::from(vec![
                Span::styled(key, Theme::brand_a()),
                Span::styled(lens, Theme::brand_b()),
            ])
        })
        .collect()
}

/// Full splash: wordmark, tagline, and whatever we know so far about the connection.
pub fn splash(
    width: u16,
    url: &str,
    server: Option<(&str, &str)>,
    status: &str,
) -> Vec<Line<'static>> {
    let mut lines = vec![Line::raw(""), Line::raw("")];
    lines.extend(wordmark(width));

    lines.push(Line::raw(""));
    lines.push(Line::from(Span::styled(
        "a TUI for Redis and Valkey that understands your keys",
        Theme::label(),
    )));
    lines.push(Line::raw(""));

    match server {
        Some((vendor, version)) => lines.push(Line::from(vec![
            Span::styled(format!(" {vendor} "), Theme::chip(Theme::BRAND_B)),
            Span::raw(" "),
            Span::styled(version.to_string(), Theme::number()),
            Span::styled("  ", Theme::base()),
            Span::styled(url.to_string(), Theme::dim()),
        ])),
        None => lines.push(Line::from(Span::styled(url.to_string(), Theme::dim()))),
    }

    lines.push(Line::raw(""));
    lines.push(Line::from(Span::styled(
        status.to_string(),
        Theme::accent(),
    )));
    lines.push(Line::raw(""));
    lines.push(Line::from(Span::styled(
        "read-only — safe to point at production",
        Theme::ok(),
    )));

    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(lines: &[Line]) -> String {
        lines
            .iter()
            .map(|l| {
                l.spans
                    .iter()
                    .map(|s| s.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn wordmark_is_two_tone() {
        let lines = wordmark(100);
        assert_eq!(lines.len(), WORDMARK.len());
        for line in &lines {
            assert_eq!(line.spans.len(), 2, "each row splits into KEY and LENS");
            assert_eq!(line.spans[0].style.fg, Some(Theme::BRAND_A));
            assert_eq!(line.spans[1].style.fg, Some(Theme::BRAND_B));
        }
    }

    #[test]
    fn every_wordmark_row_is_the_same_width() {
        // Ragged rows would shear the letters. Count chars, not bytes.
        let widths: Vec<usize> = WORDMARK.iter().map(|r| r.chars().count()).collect();
        assert!(
            widths.iter().all(|w| *w == WORDMARK_WIDTH as usize),
            "rows have differing widths: {widths:?}"
        );
    }

    #[test]
    fn narrow_terminals_get_a_compact_wordmark() {
        // Slicing block glyphs mid-letter looks broken, not minimal.
        let lines = wordmark(40);
        assert_eq!(lines.len(), 1);
        assert_eq!(text(&lines), "KEYLENS");
    }

    #[test]
    fn splash_shows_the_url_before_the_server_is_known() {
        let lines = splash(100, "redis://example:6379", None, "connecting...");
        let out = text(&lines);
        assert!(out.contains("redis://example:6379"));
        assert!(out.contains("connecting..."));
    }

    #[test]
    fn splash_shows_vendor_once_detected() {
        let lines = splash(100, "redis://x", Some(("Valkey", "8.1.0")), "scanning...");
        let out = text(&lines);
        assert!(out.contains("Valkey"));
        assert!(out.contains("8.1.0"));
    }
}
