//! Shared UI model and widgets for keylens.
//!
//! The tree model here is pure -- keys in, rows out, no I/O -- so the trickiest logic in
//! the browser is testable without a terminal or a Redis.

pub mod banner;
pub mod format;
pub mod pane;
pub mod theme;
pub mod tree;

pub use pane::PaneState;
pub use theme::Theme;
pub use tree::{KeyTree, Row};
