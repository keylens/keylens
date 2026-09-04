//! keylens as a library, so the app logic and rendering are testable without a terminal.
//!
//! The binary in `main.rs` is a thin shell over this.
// `unwrap` in a test is a deliberate assertion, not a reachable panic: the lint that
// guards the production paths would otherwise force `?` into every fixture.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod app;
pub mod browse;
pub mod config;
pub mod events;
pub mod panes;
pub mod probe;
pub mod queues;
pub mod ui;
pub mod worker;
