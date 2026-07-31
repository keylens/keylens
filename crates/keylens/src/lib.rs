//! keylens as a library, so the app logic and rendering are testable without a terminal.
//!
//! The binary in `main.rs` is a thin shell over this.

pub mod app;
pub mod browse;
pub mod config;
pub mod panes;
pub mod probe;
pub mod ui;
pub mod worker;
