//! BullMQ lens.
//!
//! Key layout and semantics verified against `taskforcesh/bullmq@master` on 2026-07-31.
//! Two upstream facts drive the design and are easy to get wrong:
//!
//! * **Latest BullMQ is v6**, and `meta.version` holds `` `${libName}:${version}` ``.
//! * **Pause does not rename `wait` to `paused`.** It sets `meta.paused = 1` and deletes
//!   the marker key. See [`keys::is_paused`].
//!
//! Mutations are deliberately absent in v0.1. When they land, they must use BullMQ's own
//! ported Lua scripts -- composing `ZREM` + `LPUSH` by hand corrupts queues under
//! concurrent workers.
// `unwrap` in a test is a deliberate assertion, not a reachable panic: the lint that
// guards the production paths would otherwise force `?` into every fixture.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod events;
pub mod job;
pub mod keys;
pub mod lens;

pub use events::{EventKind, EventsStatus, Throughput};
pub use job::{Job, JobRef};
pub use keys::{QueueKeys, State};
pub use lens::{BullMqLens, QueueSummary};
