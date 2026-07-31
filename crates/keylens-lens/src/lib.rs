//! The lens extension point.
//!
//! Every Redis client shows you keys. None of them understand what your keys *mean*.
//! `bull:emails:failed` is a ZSET to `redis-cli`; it's a dead-letter queue to you.
//!
//! A lens is three things and nothing more:
//!
//! 1. a **detector** -- a cheap keyspace probe that says "this looks like BullMQ v6",
//! 2. a **model** -- the domain objects that pattern implies (queues, jobs, states),
//! 3. a **view** -- how to render them (lives in the UI layer, keyed by lens id).
//!
//! That shape is deliberate: a contributor can add Sidekiq or Celery support without
//! touching core. See `docs/LENS.md`.

use std::sync::Arc;

use async_trait::async_trait;
use keylens_conn::Conn;
use thiserror::Error;
use tracing::{debug, warn};

#[derive(Debug, Error)]
pub enum LensError {
    #[error(transparent)]
    Conn(#[from] keylens_conn::ConnError),

    #[error("lens `{lens}` could not build a model: {detail}")]
    Model { lens: &'static str, detail: String },
}

pub type Result<T> = std::result::Result<T, LensError>;

/// How sure a detector is. Surfaced in the UI so we say "BullMQ v6 detected -- 12 queues"
/// rather than silently guessing and being wrong in a way the user can't see.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Confidence {
    /// Shape matches, but so would other things. Offer it; don't auto-open.
    Weak,
    /// Structure is distinctive enough to name.
    Likely,
    /// Version markers or unambiguous keys present.
    Certain,
}

/// The result of a successful detection.
#[derive(Debug, Clone)]
pub struct Detection {
    pub lens_id: &'static str,
    pub confidence: Confidence,
    /// Detected version of the *upstream library*, e.g. BullMQ's major. `None` when the
    /// keyspace doesn't advertise one.
    pub version: Option<String>,
    /// Key prefix the lens is scoped to, e.g. `bull`.
    pub prefix: String,
    /// One-line summary for the lens picker, e.g. "12 queues, 3 paused".
    pub summary: String,
    /// Roots the lens found -- queue names, namespaces, whatever the domain calls them.
    pub targets: Vec<String>,
}

#[async_trait]
pub trait Lens: Send + Sync {
    /// Stable identifier used in config and to key views. Never change it.
    fn id(&self) -> &'static str;

    /// Human-facing name for the lens picker.
    fn name(&self) -> &'static str;

    /// Cheap keyspace probe.
    ///
    /// Contract, and it is a hard contract:
    /// * must never issue `KEYS`,
    /// * must bound its `SCAN` work -- detection runs on every connect,
    /// * must tolerate restricted servers by checking [`keylens_conn::Capabilities`],
    /// * returns `Ok(None)` for "not present", reserving `Err` for real failures.
    async fn detect(&self, conn: &Conn) -> Result<Option<Detection>>;
}

/// Holds the built-in lenses and runs detection across all of them.
#[derive(Default, Clone)]
pub struct Registry {
    lenses: Vec<Arc<dyn Lens>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, lens: Arc<dyn Lens>) -> &mut Self {
        self.lenses.push(lens);
        self
    }

    pub fn lenses(&self) -> &[Arc<dyn Lens>] {
        &self.lenses
    }

    pub fn get(&self, id: &str) -> Option<&Arc<dyn Lens>> {
        self.lenses.iter().find(|l| l.id() == id)
    }

    /// Run every detector, strongest confidence first.
    ///
    /// A detector that errors is logged and skipped -- one broken lens must not stop the
    /// user from connecting, because the general browser still works without any lens.
    pub async fn detect_all(&self, conn: &Conn) -> Vec<Detection> {
        let mut found = Vec::new();
        for lens in &self.lenses {
            match lens.detect(conn).await {
                Ok(Some(d)) => {
                    debug!(lens = lens.id(), confidence = ?d.confidence, "lens detected");
                    found.push(d);
                }
                Ok(None) => {}
                Err(e) => warn!(lens = lens.id(), error = %e, "lens detection failed; skipping"),
            }
        }
        found.sort_by_key(|d| std::cmp::Reverse(d.confidence));
        found
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake(&'static str);

    #[async_trait]
    impl Lens for Fake {
        fn id(&self) -> &'static str {
            self.0
        }
        fn name(&self) -> &'static str {
            self.0
        }
        async fn detect(&self, _: &Conn) -> Result<Option<Detection>> {
            unreachable!("registry lookup tests don't hit the network")
        }
    }

    #[test]
    fn registry_lookup_by_id() {
        let mut r = Registry::new();
        r.register(Arc::new(Fake("bullmq"))).register(Arc::new(Fake("sidekiq")));

        assert_eq!(r.lenses().len(), 2);
        assert_eq!(r.get("sidekiq").map(|l| l.id()), Some("sidekiq"));
        assert!(r.get("celery").is_none());
    }

    #[test]
    fn confidence_orders_certain_highest() {
        assert!(Confidence::Certain > Confidence::Likely);
        assert!(Confidence::Likely > Confidence::Weak);
    }
}
