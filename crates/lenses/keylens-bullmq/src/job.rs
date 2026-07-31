//! Reading jobs out of a BullMQ queue.
//!
//! Field names verified against a live BullMQ v6 keyspace on 2026-08-01. Two traps:
//!
//! * v6 stores `attemptsMade` as **`atm`** (and `attemptsStarted` as `ats`). Older
//!   versions wrote the long names, so both are read.
//! * `stacktrace` is a **JSON array of strings**, not a newline-joined blob. Rendering it
//!   raw shows escaped `\n` sequences instead of a stack.

/// A job id plus the score it carried in its state's ZSET.
///
/// For `completed`/`failed` the score is the finish timestamp; for `delayed` it packs the
/// due time; for list-backed states there is no score at all.
#[derive(Debug, Clone, PartialEq)]
pub struct JobRef {
    pub id: String,
    pub score: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Job {
    pub id: String,
    pub name: String,
    /// Raw JSON payload, pretty-printed by the viewer.
    pub data: String,
    pub opts: String,
    pub attempts_made: u32,
    /// From `opts.attempts`, so the UI can show `2/3`.
    pub attempts_allowed: Option<u32>,
    pub stacktrace: Vec<String>,
    pub failed_reason: String,
    pub return_value: String,
    pub progress: String,
    /// Milliseconds since the epoch.
    pub timestamp: Option<i64>,
    pub processed_on: Option<i64>,
    pub finished_on: Option<i64>,
    pub delay: Option<i64>,
    pub priority: Option<i64>,
    /// Set on a child in a flow.
    pub parent_key: String,
}

impl Job {
    /// Wall-clock processing time, when the job has both timestamps.
    pub fn duration_ms(&self) -> Option<i64> {
        match (self.processed_on, self.finished_on) {
            (Some(start), Some(end)) if end >= start => Some(end - start),
            _ => None,
        }
    }

    /// How long the job sat in the queue before a worker picked it up.
    pub fn wait_ms(&self) -> Option<i64> {
        match (self.timestamp, self.processed_on) {
            (Some(added), Some(started)) if started >= added => Some(started - added),
            _ => None,
        }
    }

    pub fn has_failed(&self) -> bool {
        !self.failed_reason.is_empty() || !self.stacktrace.is_empty()
    }

    pub fn attempts_label(&self) -> String {
        match self.attempts_allowed {
            Some(max) => format!("{}/{}", self.attempts_made, max),
            None => self.attempts_made.to_string(),
        }
    }
}

/// The exact hash fields to read.
///
/// An explicit list rather than `HGETALL`: a job hash is small, but the rule that no
/// unbounded collection read ever reaches the server holds everywhere, without exception.
pub const JOB_FIELDS: &[&str] = &[
    "name",
    "data",
    "opts",
    "atm",
    "attemptsMade",
    "stacktrace",
    "failedReason",
    "returnvalue",
    "progress",
    "timestamp",
    "processedOn",
    "finishedOn",
    "delay",
    "priority",
    "parentKey",
];

/// Build a [`Job`] from replies aligned to [`JOB_FIELDS`].
pub fn from_fields(id: &str, values: &[Option<String>]) -> Job {
    let get = |name: &str| -> Option<String> {
        JOB_FIELDS
            .iter()
            .position(|f| *f == name)
            .and_then(|i| values.get(i).cloned().flatten())
            .filter(|s| !s.is_empty())
    };

    let opts = get("opts").unwrap_or_default();
    Job {
        id: id.to_string(),
        name: get("name").unwrap_or_default(),
        data: get("data").unwrap_or_default(),
        attempts_allowed: attempts_from_opts(&opts),
        opts,
        // v6 writes `atm`; older versions wrote `attemptsMade`.
        attempts_made: get("atm")
            .or_else(|| get("attemptsMade"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        stacktrace: parse_stacktrace(get("stacktrace").as_deref().unwrap_or("")),
        failed_reason: get("failedReason").unwrap_or_default(),
        return_value: get("returnvalue").unwrap_or_default(),
        progress: get("progress").unwrap_or_default(),
        timestamp: get("timestamp").and_then(|v| v.parse().ok()),
        processed_on: get("processedOn").and_then(|v| v.parse().ok()),
        finished_on: get("finishedOn").and_then(|v| v.parse().ok()),
        delay: get("delay").and_then(|v| v.parse().ok()),
        priority: get("priority").and_then(|v| v.parse().ok()),
        parent_key: get("parentKey").unwrap_or_default(),
    }
}

/// `stacktrace` is a JSON array of strings, each holding a full multi-line trace.
///
/// Falls back to treating the raw value as one trace, so a malformed or legacy field still
/// renders something rather than silently vanishing.
pub fn parse_stacktrace(raw: &str) -> Vec<String> {
    if raw.trim().is_empty() {
        return Vec::new();
    }
    match serde_json::from_str::<Vec<String>>(raw) {
        Ok(traces) => traces,
        Err(_) => vec![raw.to_string()],
    }
}

/// Pull `attempts` out of the serialised job opts.
fn attempts_from_opts(opts: &str) -> Option<u32> {
    let parsed: serde_json::Value = serde_json::from_str(opts).ok()?;
    parsed.get("attempts")?.as_u64().map(|n| n as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(pairs: &[(&str, &str)]) -> Vec<Option<String>> {
        JOB_FIELDS
            .iter()
            .map(|f| pairs.iter().find(|(k, _)| k == f).map(|(_, v)| v.to_string()))
            .collect()
    }

    #[test]
    fn reads_the_abbreviated_v6_attempts_field() {
        // v6 writes `atm`. Reading only `attemptsMade` reports every job as attempt 0.
        let job = from_fields("1", &values(&[("atm", "2")]));
        assert_eq!(job.attempts_made, 2);
    }

    #[test]
    fn falls_back_to_the_long_attempts_field_on_older_versions() {
        let job = from_fields("1", &values(&[("attemptsMade", "3")]));
        assert_eq!(job.attempts_made, 3);
    }

    #[test]
    fn attempts_label_uses_the_limit_from_opts() {
        let job = from_fields(
            "1",
            &values(&[("atm", "2"), ("opts", r#"{"attempts":3,"delay":0}"#)]),
        );
        assert_eq!(job.attempts_label(), "2/3");

        // No limit in opts: show the count alone rather than inventing a denominator.
        let bare = from_fields("1", &values(&[("atm", "2")]));
        assert_eq!(bare.attempts_label(), "2");
    }

    #[test]
    fn stacktrace_is_a_json_array_of_traces() {
        let raw = r#"["RangeError: boom\n    at decodeFrame (file:///app/p.mjs:59:9)\n    at resizeImage (file:///app/p.mjs:63:10)"]"#;
        let traces = parse_stacktrace(raw);
        assert_eq!(traces.len(), 1);
        assert!(traces[0].contains("at decodeFrame"));
        // Real newlines, not escaped ones -- otherwise the viewer shows `\n` literals.
        assert!(traces[0].contains('\n'));
    }

    #[test]
    fn multiple_attempts_produce_multiple_traces() {
        let traces = parse_stacktrace(r#"["first failure","second failure"]"#);
        assert_eq!(traces.len(), 2);
    }

    #[test]
    fn a_malformed_stacktrace_still_renders() {
        let traces = parse_stacktrace("not json at all");
        assert_eq!(traces, vec!["not json at all"]);
        assert!(parse_stacktrace("").is_empty());
        assert!(parse_stacktrace("   ").is_empty());
    }

    #[test]
    fn computes_wait_and_run_durations() {
        let job = from_fields(
            "1",
            &values(&[
                ("timestamp", "1000"),
                ("processedOn", "1500"),
                ("finishedOn", "2200"),
            ]),
        );
        assert_eq!(job.wait_ms(), Some(500));
        assert_eq!(job.duration_ms(), Some(700));
    }

    #[test]
    fn durations_are_none_when_a_job_has_not_finished() {
        let job = from_fields("1", &values(&[("timestamp", "1000")]));
        assert_eq!(job.duration_ms(), None);
        assert_eq!(job.wait_ms(), None);
    }

    #[test]
    fn clock_skew_does_not_produce_negative_durations() {
        // finishedOn before processedOn happens across restarts and clock changes.
        let job = from_fields(
            "1",
            &values(&[("processedOn", "2000"), ("finishedOn", "1000")]),
        );
        assert_eq!(job.duration_ms(), None);
    }

    #[test]
    fn failure_is_detected_from_either_signal() {
        assert!(from_fields("1", &values(&[("failedReason", "nope")])).has_failed());
        assert!(from_fields("1", &values(&[("stacktrace", r#"["x"]"#)])).has_failed());
        assert!(!from_fields("1", &values(&[("name", "ok")])).has_failed());
    }

    #[test]
    fn empty_fields_are_treated_as_absent() {
        let job = from_fields("1", &values(&[("failedReason", ""), ("name", "emails")]));
        assert!(!job.has_failed());
        assert_eq!(job.name, "emails");
    }
}
