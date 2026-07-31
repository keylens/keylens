use async_trait::async_trait;
use fred::prelude::Value;
use keylens_conn::Conn;
use keylens_lens::{Confidence, Detection, Lens, Result};
use tracing::debug;

use crate::keys::{
    is_paused, parse_meta_version, queue_name_from_meta_key, QueueKeys, State,
};

/// Detection walks at most this many SCAN pages. Detection runs on every connect, so it
/// must stay cheap even on a keyspace with millions of unrelated keys.
const MAX_DETECT_PAGES: usize = 40;
const SCAN_COUNT: u32 = 500;
/// Cap on queues surfaced by a single detection pass.
const MAX_QUEUES: usize = 500;

#[derive(Debug, Clone)]
pub struct QueueSummary {
    pub name: String,
    pub paused: bool,
    /// Counts per state, in [`State::ALL`] order.
    pub counts: Vec<(State, u64)>,
}

impl QueueSummary {
    pub fn count(&self, s: State) -> u64 {
        self.counts.iter().find(|(k, _)| *k == s).map(|(_, v)| *v).unwrap_or(0)
    }

    pub fn total(&self) -> u64 {
        self.counts.iter().map(|(_, v)| v).sum()
    }
}

pub struct BullMqLens {
    prefix: String,
}

impl Default for BullMqLens {
    fn default() -> Self {
        Self::new("bull")
    }
}

impl BullMqLens {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self { prefix: prefix.into() }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Find candidate queues by scanning for `<prefix>:*:meta` hashes.
    ///
    /// `TYPE hash` is passed when the server supports it; [`Conn::scan_page`] drops the
    /// option otherwise, so we re-validate below rather than trusting the filter.
    async fn scan_meta_keys(&self, conn: &Conn) -> Result<Vec<String>> {
        let pattern = format!("{}:*:meta", self.prefix);
        let mut cursor = "0".to_string();
        let mut names = Vec::new();

        for _ in 0..MAX_DETECT_PAGES {
            let page = conn.scan_page(&cursor, Some(&pattern), SCAN_COUNT, Some("hash")).await?;

            for key in &page.keys {
                if let Some(name) = queue_name_from_meta_key(&self.prefix, key) {
                    names.push(name);
                }
            }

            cursor = page.cursor.clone();
            if page.is_complete() || names.len() >= MAX_QUEUES {
                break;
            }
        }

        names.sort();
        names.dedup();
        names.truncate(MAX_QUEUES);
        Ok(names)
    }

    /// Read `version` and `paused` from a queue's meta hash in one round trip.
    async fn read_meta(&self, conn: &Conn, name: &str) -> Result<(Option<String>, bool)> {
        let keys = QueueKeys::new(&self.prefix, name);
        let reply = conn
            .cmd(
                "HMGET",
                vec![Value::from(keys.meta()), Value::from("version"), Value::from("paused")],
            )
            .await?;

        let Value::Array(fields) = reply else {
            return Ok((None, false));
        };

        let version = fields.first().and_then(|v| v.as_string());
        let paused = is_paused(fields.get(1).and_then(|v| v.as_string()).as_deref());
        Ok((version, paused))
    }

    /// Per-state counts for one queue.
    ///
    /// Sequential for now -- M4 should batch these into a pipeline, since this is
    /// 7 round trips per queue and the queue list refreshes on a tick.
    pub async fn queue_summary(&self, conn: &Conn, name: &str) -> Result<QueueSummary> {
        let keys = QueueKeys::new(&self.prefix, name);
        let (_, paused) = self.read_meta(conn, name).await?;

        let mut counts = Vec::with_capacity(State::ALL.len());
        for state in State::ALL {
            let reply = conn.cmd(state.count_cmd(), vec![Value::from(keys.state(state))]).await?;
            // A missing key counts as zero; only a genuinely odd reply is worth flagging.
            let n = reply.as_u64().unwrap_or(0);
            counts.push((state, n));
        }

        Ok(QueueSummary { name: name.to_string(), paused, counts })
    }

    pub async fn all_queues(&self, conn: &Conn) -> Result<Vec<QueueSummary>> {
        let names = self.scan_meta_keys(conn).await?;
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            out.push(self.queue_summary(conn, &name).await?);
        }
        Ok(out)
    }
}

#[async_trait]
impl Lens for BullMqLens {
    fn id(&self) -> &'static str {
        "bullmq"
    }

    fn name(&self) -> &'static str {
        "BullMQ"
    }

    async fn detect(&self, conn: &Conn) -> Result<Option<Detection>> {
        let names = self.scan_meta_keys(conn).await?;
        if names.is_empty() {
            return Ok(None);
        }

        // Sample meta from the first few queues to pin the version and confidence,
        // instead of paying for every queue during detection.
        let mut version: Option<String> = None;
        let mut lib: Option<String> = None;
        let mut sampled = 0usize;

        for name in names.iter().take(5) {
            let (raw_version, _) = self.read_meta(conn, name).await?;
            sampled += 1;
            if let Some(raw) = raw_version
                && let Some((l, v)) = parse_meta_version(&raw)
            {
                lib = Some(l);
                version = Some(v);
                break;
            }
        }

        debug!(queues = names.len(), sampled, ?version, "bullmq detection");

        // `meta.version` is written by BullMQ v4+. Its absence means either an older
        // BullMQ or a Bull v3 keyspace -- still worth offering, but don't claim certainty.
        let confidence = match lib.as_deref() {
            Some(l) if l.starts_with("bullmq") => Confidence::Certain,
            Some(_) => Confidence::Likely,
            None => Confidence::Weak,
        };

        let summary = match (&lib, &version) {
            (Some(l), Some(v)) => format!("{} {} - {} queues", l, v, names.len()),
            _ => format!("{} queues (version not advertised)", names.len()),
        };

        Ok(Some(Detection {
            lens_id: "bullmq",
            confidence,
            version,
            prefix: self.prefix.clone(),
            summary,
            targets: names,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(counts: Vec<(State, u64)>, paused: bool) -> QueueSummary {
        QueueSummary { name: "emails".into(), paused, counts }
    }

    #[test]
    fn missing_states_count_as_zero() {
        let s = summary(vec![(State::Failed, 3)], false);
        assert_eq!(s.count(State::Failed), 3);
        assert_eq!(s.count(State::Waiting), 0);
        assert_eq!(s.total(), 3);
    }

    #[test]
    fn default_prefix_is_bull() {
        assert_eq!(BullMqLens::default().prefix(), "bull");
        assert_eq!(BullMqLens::new("myapp").prefix(), "myapp");
    }
}
