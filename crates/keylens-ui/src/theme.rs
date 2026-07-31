//! Colours and chrome.
//!
//! Indexed ANSI colours rather than RGB, deliberately: keylens inherits whatever palette
//! the user's terminal already uses instead of fighting it. A tool that looks wrong in
//! someone's carefully-configured terminal gets uninstalled.
//!
//! The brand pair is magenta → cyan. Everything else is semantic: green means healthy,
//! yellow means look at this, red means it's wrong.

use keylens_conn::Kind;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, BorderType};

pub struct Theme;

impl Theme {
    // ---- brand -----------------------------------------------------------------

    pub const BRAND_A: Color = Color::Magenta;
    pub const BRAND_B: Color = Color::Cyan;

    /// The `KEYLENS` wordmark in the status bar.
    pub fn brand() -> Style {
        Style::new().fg(Color::Black).bg(Self::BRAND_A).add_modifier(Modifier::BOLD)
    }

    // ---- text ------------------------------------------------------------------

    pub const fn base() -> Style {
        Style::new()
    }

    pub fn dim() -> Style {
        Style::new().fg(Color::DarkGray)
    }

    pub fn label() -> Style {
        Style::new().fg(Color::Gray)
    }

    pub fn title() -> Style {
        Style::new().fg(Self::BRAND_B).add_modifier(Modifier::BOLD)
    }

    /// Section heading inside a pane.
    pub fn heading() -> Style {
        Style::new().fg(Self::BRAND_A).add_modifier(Modifier::BOLD)
    }

    pub fn selected() -> Style {
        Style::new().fg(Color::Black).bg(Self::BRAND_B).add_modifier(Modifier::BOLD)
    }

    pub fn branch() -> Style {
        Style::new().fg(Color::Blue).add_modifier(Modifier::BOLD)
    }

    pub fn key_name() -> Style {
        Style::new().fg(Color::White)
    }

    pub fn field() -> Style {
        Style::new().fg(Color::Yellow)
    }

    pub fn value() -> Style {
        Style::new().fg(Color::White)
    }

    pub fn error() -> Style {
        Style::new().fg(Color::Red).add_modifier(Modifier::BOLD)
    }

    pub fn ok() -> Style {
        Style::new().fg(Color::Green)
    }

    pub fn warn() -> Style {
        Style::new().fg(Color::Yellow)
    }

    pub fn accent() -> Style {
        Style::new().fg(Self::BRAND_A)
    }

    pub fn number() -> Style {
        Style::new().fg(Color::LightCyan)
    }

    // ---- chrome ----------------------------------------------------------------

    /// A filled badge, e.g. the vendor name or an active filter.
    pub fn chip(color: Color) -> Style {
        Style::new().fg(Color::Black).bg(color).add_modifier(Modifier::BOLD)
    }

    pub fn tab_active() -> Style {
        Style::new().fg(Color::Black).bg(Self::BRAND_B).add_modifier(Modifier::BOLD)
    }

    pub fn tab_inactive() -> Style {
        Style::new().fg(Color::DarkGray)
    }

    /// Rounded borders throughout -- softer than the default square corners and the main
    /// thing that stops a ratatui app looking like a default ratatui app.
    pub fn panel(title: &str, focused: bool) -> Block<'static> {
        Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(if focused { Self::title() } else { Self::dim() })
            .title(ratatui::text::Span::styled(
                format!(" {title} "),
                if focused { Self::title() } else { Self::label() },
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
        Style::new().fg(color)
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
        Style::new().fg(color)
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
    fn every_redis_type_gets_a_distinct_colour() {
        let kinds = [Kind::String, Kind::Hash, Kind::List, Kind::Set, Kind::ZSet, Kind::Stream];
        let mut seen = Vec::new();
        for k in kinds {
            let c = Theme::kind(Some(k)).fg.unwrap();
            assert!(!seen.contains(&c), "{k:?} reuses a colour");
            seen.push(c);
        }
    }
}
