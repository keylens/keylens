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
/// Keys examined per `SCAN` page during detection.
///
/// Deliberately large. Detection walks the keyspace looking for `<prefix>:*:meta`, and
/// every page is a round trip — against a managed host ~1.4s away, a small `COUNT` turns
/// detection into half a minute of waiting before the queues tab appears. `COUNT` costs
/// the server a bounded amount of work per call, so trading pages for page size is close
/// to free for it and a large win for us.
const SCAN_COUNT: u32 = 4_000;
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
        let mut names = Vec::new();
        let type_filter = conn
            .capabilities()
            .has(keylens_conn::Feature::ScanTypeFilter)
            .then_some("hash");
        let mut scanner = conn.key_scanner(Some(&pattern), SCAN_COUNT, type_filter);

        for _ in 0..MAX_DETECT_PAGES {
            let Some(keys) = scanner.next_page().await? else {
                break;
            };

            for key in &keys {
                if let Some(name) = queue_name_from_meta_key(&self.prefix, key) {
                    names.push(name);
                }
            }

            if names.len() >= MAX_QUEUES {
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

        // Per-command results: one queue's key being the wrong type, or its meta hash
        // having been deleted mid-refresh, must cost that one cell -- not the whole table.
        let replies = conn.pipeline(&cmds).await?;
        let reply = |i: usize| replies.get(i).and_then(|r| r.as_ref().ok());

        Ok(names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let base = i * PER_QUEUE;

                let paused = match reply(base) {
                    Some(Value::Array(fields)) => {
                        is_paused(fields.get(1).and_then(|v| v.as_string()).as_deref())
                    }
                    _ => false,
                };

                let counts = State::ALL
                    .iter()
                    .enumerate()
                    .map(|(j, state)| {
                        let n = reply(base + 1 + j).and_then(|v| v.as_u64()).unwrap_or(0);
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
        let Some((start, stop)) = page_range(offset, limit) else {
            return Ok(Vec::new());
        };

        let keys = QueueKeys::new(&self.prefix, queue);
        let key = keys.state(state);

        let reply = if state.is_list_backed() {
            conn.cmd("LRANGE", vec![key.into(), start.into(), stop.into()])
                .await?
        } else {
            conn.cmd(
                // Newest first: for completed and failed, the recent end is the one
                // anyone actually wants to look at.
                "ZREVRANGE",
                vec![key.into(), start.into(), stop.into(), "WITHSCORES".into()],
            )
            .await?
        };

        let Value::Array(items) = reply else {
            return Ok(Vec::new());
        };

        Ok(if state.is_list_backed() {
            items
                .iter()
                .filter_map(|v| v.as_string())
                .map(|id| JobRef { id, score: None })
                .collect()
        } else {
            items
                .chunks_exact(2)
                .filter_map(|c| {
                    c[0].as_string().map(|id| JobRef {
                        id,
                        score: c[1].as_f64(),
                    })
                })
                .collect()
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
        let Some((start, stop)) = page_range(0, limit) else {
            return Ok(Vec::new());
        };

        let keys = QueueKeys::new(&self.prefix, queue);
        let reply = conn
            .cmd(
                "LRANGE",
                vec![Value::from(keys.job_logs(id)), start.into(), stop.into()],
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

/// Inclusive `[start, stop]` for a page, or `None` when nothing was asked for.
///
/// The `None` is the whole reason this is a function. Redis range commands treat a
/// negative `stop` as an offset from the end, so the naive `offset + limit - 1` turns a
/// zero limit into `LRANGE key 0 -1` — the entire list, which is precisely the unbounded
/// read this crate exists to never issue. Asking for nothing gets nothing back.
fn page_range(offset: usize, limit: usize) -> Option<(i64, i64)> {
    if limit == 0 {
        return None;
    }
    let start = offset as i64;
    Some((start, start + limit as i64 - 1))
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

    #[test]
    fn a_zero_limit_never_becomes_a_whole_list_read() {
        // `0 + 0 - 1` is `LRANGE key 0 -1`, which Redis reads as "everything". A page of
        // nothing must stay a page of nothing.
        assert_eq!(page_range(0, 0), None);
        assert_eq!(page_range(500, 0), None);
    }

    #[test]
    fn page_range_is_inclusive_and_offset_relative() {
        assert_eq!(page_range(0, 200), Some((0, 199)));
        assert_eq!(page_range(200, 200), Some((200, 399)));
        assert_eq!(page_range(0, 1), Some((0, 0)), "one element, not zero");
    }

    #[test]
    fn every_page_range_stays_non_negative() {
        // A negative bound is what silently reinterprets the request as "to the end".
        for offset in [0usize, 1, 200, 10_000] {
            for limit in [1usize, 2, 200, 5_000] {
                let (start, stop) = page_range(offset, limit).expect("non-zero limit");
                assert!(
                    start >= 0 && stop >= start,
                    "{offset}/{limit} → {start}..{stop}"
                );
            }
        }
    }
}
