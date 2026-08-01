//! Colours and chrome.
//!
//! Indexed ANSI colours rather than RGB, deliberately: keylens inherits whatever palette
//! the user's terminal already uses instead of fighting it. A tool that looks wrong in
//! someone's carefully-configured terminal gets uninstalled.
//!
//! The brand pair is magenta → cyan. Everything else is semantic: green means healthy,
//! yellow means look at this, red means it's wrong.

use std::sync::atomic::{AtomicBool, Ordering};

use keylens_conn::Kind;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, BorderType};

static COLOR_ENABLED: AtomicBool = AtomicBool::new(true);

/// Turn colour off, per <https://no-color.org> or an explicit `--no-color`.
pub fn set_color_enabled(enabled: bool) {
    COLOR_ENABLED.store(enabled, Ordering::Relaxed);
}

pub fn color_enabled() -> bool {
    COLOR_ENABLED.load(Ordering::Relaxed)
}

/// Strip colour from a style while keeping its modifiers.
///
/// The NO_COLOR convention is about *colour*, not about typography — bold and dim still
/// carry the hierarchy, so a monochrome terminal keeps a readable layout instead of a flat
/// wall of identical text.
fn paint(style: Style) -> Style {
    paint_with(style, color_enabled())
}

/// The pure form, so tests can exercise both modes without touching the global — which
/// would otherwise race against every other test running in parallel.
fn paint_with(style: Style, enabled: bool) -> Style {
    if enabled {
        style
    } else {
        Style::new().add_modifier(style.add_modifier)
    }
}

pub struct Theme;

impl Theme {
    // ---- brand -----------------------------------------------------------------

    pub const BRAND_A: Color = Color::Magenta;
    pub const BRAND_B: Color = Color::Cyan;

    /// The two halves of the block wordmark: `KEY` then `LENS`.
    pub fn brand_a() -> Style {
        paint(Style::new().fg(Self::BRAND_A).add_modifier(Modifier::BOLD))
    }

    pub fn brand_b() -> Style {
        paint(Style::new().fg(Self::BRAND_B).add_modifier(Modifier::BOLD))
    }

    /// The `KEYLENS` wordmark in the status bar.
    pub fn brand() -> Style {
        paint(
            Style::new()
                .fg(Color::Black)
                .bg(Self::BRAND_A)
                .add_modifier(Modifier::BOLD),
        )
    }

    // ---- text ------------------------------------------------------------------

    pub const fn base() -> Style {
        Style::new()
    }

    pub fn dim() -> Style {
        paint(Style::new().fg(Color::DarkGray))
    }

    pub fn label() -> Style {
        paint(Style::new().fg(Color::Gray))
    }

    pub fn title() -> Style {
        paint(Style::new().fg(Self::BRAND_B).add_modifier(Modifier::BOLD))
    }

    /// Section heading inside a pane.
    pub fn heading() -> Style {
        paint(Style::new().fg(Self::BRAND_A).add_modifier(Modifier::BOLD))
    }

    pub fn selected() -> Style {
        paint(
            Style::new()
                .fg(Color::Black)
                .bg(Self::BRAND_B)
                .add_modifier(Modifier::BOLD),
        )
    }

    pub fn branch() -> Style {
        paint(Style::new().fg(Color::Blue).add_modifier(Modifier::BOLD))
    }

    pub fn key_name() -> Style {
        paint(Style::new().fg(Color::White))
    }

    pub fn field() -> Style {
        paint(Style::new().fg(Color::Yellow))
    }

    pub fn value() -> Style {
        paint(Style::new().fg(Color::White))
    }

    pub fn error() -> Style {
        paint(Style::new().fg(Color::Red).add_modifier(Modifier::BOLD))
    }

    pub fn ok() -> Style {
        paint(Style::new().fg(Color::Green))
    }

    pub fn warn() -> Style {
        paint(Style::new().fg(Color::Yellow))
    }

    pub fn accent() -> Style {
        paint(Style::new().fg(Self::BRAND_A))
    }

    pub fn number() -> Style {
        paint(Style::new().fg(Color::LightCyan))
    }

    // ---- chrome ----------------------------------------------------------------

    /// A filled badge, e.g. the vendor name or an active filter.
    pub fn chip(color: Color) -> Style {
        paint(
            Style::new()
                .fg(Color::Black)
                .bg(color)
                .add_modifier(Modifier::BOLD),
        )
    }

    pub fn tab_active() -> Style {
        paint(
            Style::new()
                .fg(Color::Black)
                .bg(Self::BRAND_B)
                .add_modifier(Modifier::BOLD),
        )
    }

    pub fn tab_inactive() -> Style {
        paint(Style::new().fg(Color::DarkGray))
    }

    /// Rounded borders throughout -- softer than the default square corners and the main
    /// thing that stops a ratatui app looking like a default ratatui app.
    pub fn panel(title: &str, focused: bool) -> Block<'static> {
        Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(if focused { Self::title() } else { Self::dim() })
            .title(ratatui::text::Span::styled(
                format!(" {title} "),
                if focused {
                    Self::title()
                } else {
                    Self::label()
                },
            ))
    }

    /// One colour per Redis type, so the tree is scannable without reading the tags.
    pub fn kind(kind: Option<Kind>) -> Style {
        let color = match kind {
            Some(Kind::String) => Color::Green,
            Some(Kind::Hash) => Color::Yellow,
            Some(Kind::List) => Color::Cyan,
            Some(Kind::Set) => Color::Magenta,
            Some(Kind::ZSet) => Color::LightMagenta,
            Some(Kind::Stream) => Color::LightBlue,
            _ => Color::DarkGray,
        };
        paint(Style::new().fg(color))
    }

    /// Green below 60%, yellow to 85%, red above -- used for memory and hit-rate bars.
    pub fn gauge(fraction: f64) -> Style {
        let color = if fraction >= 0.85 {
            Color::Red
        } else if fraction >= 0.6 {
            Color::Yellow
        } else {
            Color::Green
        };
        paint(Style::new().fg(color))
    }

    /// Hit rate reads the opposite way round: high is good.
    pub fn gauge_inverted(fraction: f64) -> Style {
        Self::gauge(1.0 - fraction)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gauge_colours_escalate_with_pressure() {
        assert_eq!(Theme::gauge(0.10).fg, Some(Color::Green));
        assert_eq!(Theme::gauge(0.70).fg, Some(Color::Yellow));
        assert_eq!(Theme::gauge(0.95).fg, Some(Color::Red));
    }

    #[test]
    fn hit_rate_reads_the_other_way_round() {
        // A 95% hit rate is healthy; the plain gauge would paint it red.
        assert_eq!(Theme::gauge_inverted(0.95).fg, Some(Color::Green));
        assert_eq!(Theme::gauge_inverted(0.05).fg, Some(Color::Red));
    }

    #[test]
    fn no_color_strips_colour_but_keeps_typography() {
        // The NO_COLOR convention is about colour, not about layout: dropping bold too
        // would flatten every heading and selection into identical text.
        let styled = Style::new()
            .fg(Color::Red)
            .bg(Color::Black)
            .add_modifier(Modifier::BOLD);

        let plain = paint_with(styled, false);
        assert_eq!(plain.fg, None);
        assert_eq!(plain.bg, None);
        assert!(plain.add_modifier.contains(Modifier::BOLD));

        assert_eq!(paint_with(styled, true), styled, "colour mode is untouched");
    }

    #[test]
    fn every_redis_type_gets_a_distinct_colour() {
        let kinds = [
            Kind::String,
            Kind::Hash,
            Kind::List,
            Kind::Set,
            Kind::ZSet,
            Kind::Stream,
        ];
        let mut seen = Vec::new();
        for k in kinds {
            let c = Theme::kind(Some(k)).fg.unwrap();
            assert!(!seen.contains(&c), "{k:?} reuses a colour");
            seen.push(c);
        }
    }
}
