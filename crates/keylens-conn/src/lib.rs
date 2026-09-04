//! Connection, capability probing and vendor detection for keylens.
//!
//! Everything that talks to a Redis-compatible server -- Redis, Valkey, Recached and
//! friends -- goes through [`Conn`]. See its docs for the
//! invariants that buys us.
// `unwrap` in a test is a deliberate assertion, not a reachable panic: the lint that
// guards the production paths would otherwise force `?` into every fixture.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod capability;
pub mod conn;
pub mod error;
pub mod server;
pub mod server_info;
pub mod stream;
pub mod url;
pub mod value;

/// The one place fred's types surface outside this crate.
///
/// `Conn` exists so the client stays swappable; a command argument type still has to
/// cross the boundary. Keeping the re-export here means a future client swap touches this
/// line and this crate, not every call site.
pub use fred::prelude::Value;
/// Re-exported for building RESP3 map replies (tests, and any caller that needs one).
pub use fred::types::Map;

pub use capability::{Availability, Capabilities, Feature};
pub use conn::{Conn, KeyScanner, ScanPage};
pub use error::{ConnError, Result};
pub use server::{ClientInfo, ClusterNode, ClusterTopology, PubSubChannel, SlowEntry};
pub use server_info::{ServerInfo, Vendor};
pub use stream::{ConsumerInfo, GroupInfo, StreamInfo};
pub use url::redact_url;
pub use value::{KeyMeta, KeyValue, Kind, StreamEntry};

/// Compute the Redis Cluster hash slot for a key, including `{hash-tag}` semantics.
pub fn key_slot(key: &str) -> u16 {
    fred::util::redis_keyslot(key.as_bytes())
}
