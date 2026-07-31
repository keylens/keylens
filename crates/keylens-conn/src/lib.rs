//! Connection, capability probing and vendor detection for keylens.
//!
//! Everything that talks to Redis or Valkey goes through [`Conn`]. See its docs for the
//! invariants that buys us.

pub mod capability;
pub mod conn;
pub mod error;
pub mod server;
pub mod server_info;
pub mod value;

/// The one place fred's types surface outside this crate.
///
/// `Conn` exists so the client stays swappable; a command argument type still has to
/// cross the boundary. Keeping the re-export here means a future client swap touches this
/// line and this crate, not every call site.
pub use fred::prelude::Value;

pub use capability::{Availability, Capabilities, Feature};
pub use conn::{Conn, ScanPage};
pub use error::{ConnError, Result};
pub use server::{ClientInfo, ClusterNode, ClusterTopology, PubSubChannel, SlowEntry};
pub use server_info::{ServerInfo, Vendor};
pub use value::{Kind, KeyMeta, KeyValue, StreamEntry};
