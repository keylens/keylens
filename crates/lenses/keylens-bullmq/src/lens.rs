use async_trait::async_trait;
use fred::prelude::Value;
use keylens_conn::Conn;
use keylens_lens::{Confidence, Detection, Lens, Result};
use tracing::debug;

use crate::job::{self, Job, JobRef};
use crate::keys::{QueueKeys, State, is_paused, parse_meta_version, queue_name_from_meta_key};

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
        self.counts
            .iter()
            .find(|(k, _)| *k == s)
            .map(|(_, v)| *v)
            .unwrap_or(0)
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
        Self {
            prefix: prefix.into(),
        }
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
            let page = conn
                .scan_page(&cursor, Some(&pattern), SCAN_COUNT, Some("hash"))
                .await?;

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
                vec![
                    Value::from(keys.meta()),
                    Value::from("version"),
                    Value::from("paused"),
                ],
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
    pub async fn queue_summary(&self, conn: &Conn, name: &str) -> Result<QueueSummary> {
        Ok(self
            .summaries(conn, std::slice::from_ref(&name.to_string()))
            .await?
            .pop()
            .unwrap_or_else(|| QueueSummary {
                name: name.to_string(),
                paused: false,
                counts: Vec::new(),
            }))
    }

    /// Counts and paused state for many queues in a single round trip.
    ///
    /// Sequentially this is 8 commands per queue; on a queue system with 40 queues and a
    /// server 60ms away that's 20 seconds for one refresh. The queue list refreshes on a
    /// tick, so it has to be one pipeline.
    async fn summaries(&self, conn: &Conn, names: &[String]) -> Result<Vec<QueueSummary>> {
        if names.is_empty() {
            return Ok(Vec::new());
        }

        // Per queue: one HMGET for meta, then one count per state. Order is what maps the
        // flat reply list back onto queues, so it must match exactly on the way out.
        const PER_QUEUE: usize = 1 + State::ALL.len();
        let mut cmds: Vec<(&'static str, Vec<Value>)> = Vec::with_capacity(names.len() * PER_QUEUE);

        for name in names {
            let keys = QueueKeys::new(&self.prefix, name);
            cmds.push((
                "HMGET",
                vec![
                    Value::from(keys.meta()),
                    Value::from("version"),
                    Value::from("paused"),
                ],
            ));
            for state in State::ALL {
                cmds.push((state.count_cmd(), vec![Value::from(keys.state(state))]));
            }
        }

        let replies = conn.pipeline(&cmds).await?;

        Ok(names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let base = i * PER_QUEUE;

                let paused = match replies.get(base) {
                    Some(Value::Array(fields)) => {
                        is_paused(fields.get(1).and_then(|v| v.as_string()).as_deref())
                    }
                    _ => false,
                };

                let counts = State::ALL
                    .iter()
                    .enumerate()
                    .map(|(j, state)| {
                        let n = replies
                            .get(base + 1 + j)
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        (*state, n)
                    })
                    .collect();

                QueueSummary {
                    name: name.clone(),
                    paused,
                    counts,
                }
            })
            .collect())
    }

    pub async fn all_queues(&self, conn: &Conn) -> Result<Vec<QueueSummary>> {
        let names = self.scan_meta_keys(conn).await?;
        self.summaries(conn, &names).await
    }

    /// A page of job ids from one state.
    ///
    /// List-backed states (`wait`, `active`) use `LRANGE`; the rest are ZSETs and carry a
    /// score worth showing -- finish time for `completed`/`failed`, due time for `delayed`.
    pub async fn jobs(
        &self,
        conn: &Conn,
        queue: &str,
        state: State,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<JobRef>> {
        let keys = QueueKeys::new(&self.prefix, queue);
        let key = keys.state(state);
        let start = offset as i64;
        let stop = start + limit as i64 - 1;

        let reply = match state.count_cmd() {
            "LLEN" => {
                conn.cmd("LRANGE", vec![key.into(), start.into(), stop.into()])
                    .await?
            }
            _ => {
                conn.cmd(
                    // Newest first: for completed and failed, the recent end is the one
                    // anyone actually wants to look at.
                    "ZREVRANGE",
                    vec![key.into(), start.into(), stop.into(), "WITHSCORES".into()],
                )
                .await?
            }
        };

        let Value::Array(items) = reply else {
            return Ok(Vec::new());
        };

        Ok(match state.count_cmd() {
            "LLEN" => items
                .iter()
                .filter_map(|v| v.as_string())
                .map(|id| JobRef { id, score: None })
                .collect(),
            _ => items
                .chunks_exact(2)
                .filter_map(|c| {
                    c[0].as_string().map(|id| JobRef {
                        id,
                        score: c[1].as_f64(),
                    })
                })
                .collect(),
        })
    }

    /// One job's fields. Returns `None` when the job has been removed, which happens
    /// constantly on a live queue with retention configured.
    pub async fn job(&self, conn: &Conn, queue: &str, id: &str) -> Result<Option<Job>> {
        let keys = QueueKeys::new(&self.prefix, queue);
        let mut args: Vec<Value> = vec![Value::from(keys.job(id))];
        args.extend(job::JOB_FIELDS.iter().map(|f| Value::from(*f)));

        let reply = conn.cmd("HMGET", args).await?;
        let Value::Array(values) = reply else {
            return Ok(None);
        };

        let fields: Vec<Option<String>> = values.iter().map(|v| v.as_string()).collect();
        if fields.iter().all(|f| f.is_none()) {
            return Ok(None);
        }

        Ok(Some(job::from_fields(id, &fields)))
    }

    /// Per-job logs, newest last.
    pub async fn job_logs(
        &self,
        conn: &Conn,
        queue: &str,
        id: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        let keys = QueueKeys::new(&self.prefix, queue);
        let reply = conn
            .cmd(
                "LRANGE",
                vec![
                    Value::from(keys.job_logs(id)),
                    0.into(),
                    (limit as i64 - 1).into(),
                ],
            )
            .await?;
        let Value::Array(items) = reply else {
            return Ok(Vec::new());
        };
        Ok(items.iter().filter_map(|v| v.as_string()).collect())
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
        QueueSummary {
            name: "emails".into(),
            paused,
            counts,
        }
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
