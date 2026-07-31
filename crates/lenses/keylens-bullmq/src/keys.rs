//! BullMQ's Redis key layout.
//!
//! Mirrors `src/classes/queue-keys.ts` upstream, verified against `master` on 2026-07-31.
//! If BullMQ adds a key, add it here *and* to the conformance test -- silent drift is the
//! single most likely way this lens starts lying.

/// Every per-queue key suffix BullMQ creates, with its Redis type.
///
/// Kept as data rather than scattered string literals so the key browser can label
/// anything under a detected prefix, including keys this lens doesn't model yet.
pub const QUEUE_KEYS: &[(&str, &str)] = &[
    ("active", "list"),
    ("wait", "list"),
    ("waiting-children", "zset"),
    ("paused", "list"), // legacy; see `is_paused` below
    ("id", "string"),
    ("delayed", "zset"),
    ("prioritized", "zset"),
    ("stalled-check", "string"),
    ("completed", "zset"),
    ("failed", "zset"),
    ("stalled", "set"),
    ("repeat", "zset"),
    ("limiter", "string"),
    ("meta", "hash"),
    ("events", "stream"),
    ("pc", "string"),     // priority counter
    ("marker", "zset"),   // worker wakeup marker
    ("de", "hash"),       // deduplication
];

/// Job states as BullMQ defines them (`src/types/job-type.ts`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Waiting,
    Active,
    Prioritized,
    Delayed,
    WaitingChildren,
    Completed,
    Failed,
}

impl State {
    pub const ALL: [State; 7] = [
        State::Waiting,
        State::Active,
        State::Prioritized,
        State::Delayed,
        State::WaitingChildren,
        State::Completed,
        State::Failed,
    ];

    /// The key suffix backing this state. Note `Waiting` lives in `wait`, not `waiting`.
    pub fn suffix(&self) -> &'static str {
        match self {
            State::Waiting => "wait",
            State::Active => "active",
            State::Prioritized => "prioritized",
            State::Delayed => "delayed",
            State::WaitingChildren => "waiting-children",
            State::Completed => "completed",
            State::Failed => "failed",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            State::Waiting => "waiting",
            State::Active => "active",
            State::Prioritized => "prioritized",
            State::Delayed => "delayed",
            State::WaitingChildren => "waiting-children",
            State::Completed => "completed",
            State::Failed => "failed",
        }
    }

    /// Abbreviated label for the queue table, where seven columns have to share a row.
    ///
    /// `prioritized` and `waiting-children` are 11 and 16 characters; at full length they
    /// run into the neighbouring column.
    pub fn short_label(&self) -> &'static str {
        match self {
            State::Waiting => "wait",
            State::Active => "active",
            State::Prioritized => "prio",
            State::Delayed => "delayed",
            State::WaitingChildren => "children",
            State::Completed => "done",
            State::Failed => "failed",
        }
    }

    /// What this state's ZSET score actually means, so the job list can label the column
    /// instead of showing a bare float.
    pub fn score_label(&self) -> &'static str {
        match self {
            State::Waiting | State::Active => "position",
            State::Completed | State::Failed => "finished",
            State::Delayed => "due",
            State::Prioritized => "priority",
            State::WaitingChildren => "added",
        }
    }

    /// The command that counts this state, which depends on the underlying Redis type.
    pub fn count_cmd(&self) -> &'static str {
        match self {
            State::Waiting | State::Active => "LLEN",
            _ => "ZCARD",
        }
    }
}

/// Builds keys for one queue under one prefix.
#[derive(Debug, Clone)]
pub struct QueueKeys {
    pub prefix: String,
    pub name: String,
}

impl QueueKeys {
    pub fn new(prefix: impl Into<String>, name: impl Into<String>) -> Self {
        Self { prefix: prefix.into(), name: name.into() }
    }

    pub fn base(&self) -> String {
        format!("{}:{}", self.prefix, self.name)
    }

    pub fn key(&self, suffix: &str) -> String {
        format!("{}:{}", self.base(), suffix)
    }

    pub fn state(&self, s: State) -> String {
        self.key(s.suffix())
    }

    pub fn meta(&self) -> String {
        self.key("meta")
    }

    pub fn events(&self) -> String {
        self.key("events")
    }

    pub fn job(&self, id: &str) -> String {
        self.key(id)
    }

    pub fn job_logs(&self, id: &str) -> String {
        format!("{}:{}:logs", self.base(), id)
    }
}

/// Recover a queue name from a `<prefix>:<name>:meta` key.
///
/// Queue names may themselves contain colons (`bull:emails:transactional:meta`), so this
/// strips the known prefix and the known suffix rather than splitting on `:`.
pub fn queue_name_from_meta_key(prefix: &str, key: &str) -> Option<String> {
    let head = format!("{prefix}:");
    let name = key.strip_prefix(&head)?.strip_suffix(":meta")?;
    (!name.is_empty()).then(|| name.to_string())
}

/// Parse the `version` field of the meta hash, which upstream writes as
/// `` `${libName}:${version}` `` -- e.g. `bullmq:6.0.2` or `bullmq-pro:7.1.0`.
pub fn parse_meta_version(raw: &str) -> Option<(String, String)> {
    let (lib, ver) = raw.rsplit_once(':')?;
    (!lib.is_empty() && !ver.is_empty()).then(|| (lib.to_string(), ver.to_string()))
}

/// Whether a queue is paused, given its `meta` hash `paused` field.
///
/// **This is the field that matters.** Current BullMQ pauses by setting `meta.paused = 1`
/// and deleting the marker key -- it does *not* rename `wait` to `paused`. The `paused`
/// LIST is a legacy artifact that `resume` drains back into `wait`. Inferring paused
/// state from that list's existence reports paused queues as running.
pub fn is_paused(meta_paused_field: Option<&str>) -> bool {
    matches!(meta_paused_field, Some(v) if v != "0" && !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_keys() {
        let k = QueueKeys::new("bull", "emails");
        assert_eq!(k.base(), "bull:emails");
        assert_eq!(k.meta(), "bull:emails:meta");
        assert_eq!(k.events(), "bull:emails:events");
        assert_eq!(k.state(State::Waiting), "bull:emails:wait");
        assert_eq!(k.job("42"), "bull:emails:42");
        assert_eq!(k.job_logs("42"), "bull:emails:42:logs");
    }

    #[test]
    fn waiting_state_maps_to_wait_key_not_waiting() {
        // `waiting` is the state name; `wait` is the key. Conflating them yields an
        // always-zero waiting count.
        assert_eq!(State::Waiting.label(), "waiting");
        assert_eq!(State::Waiting.suffix(), "wait");
    }

    #[test]
    fn short_labels_fit_the_queue_table_column() {
        // The table gives each state 10 columns including the gap; a longer label runs
        // into its neighbour, which is exactly what full names did.
        for s in State::ALL {
            assert!(
                s.short_label().len() <= 8,
                "{} is too wide at {} chars",
                s.short_label(),
                s.short_label().len()
            );
        }
    }

    #[test]
    fn short_labels_are_unique() {
        let mut seen = Vec::new();
        for s in State::ALL {
            assert!(!seen.contains(&s.short_label()), "{:?} reuses a label", s);
            seen.push(s.short_label());
        }
    }

    #[test]
    fn list_states_count_with_llen_zset_states_with_zcard() {
        assert_eq!(State::Waiting.count_cmd(), "LLEN");
        assert_eq!(State::Active.count_cmd(), "LLEN");
        assert_eq!(State::Delayed.count_cmd(), "ZCARD");
        assert_eq!(State::Failed.count_cmd(), "ZCARD");
        assert_eq!(State::WaitingChildren.count_cmd(), "ZCARD");
    }

    #[test]
    fn recovers_queue_names_containing_colons() {
        assert_eq!(
            queue_name_from_meta_key("bull", "bull:emails:meta").as_deref(),
            Some("emails")
        );
        assert_eq!(
            queue_name_from_meta_key("bull", "bull:emails:transactional:meta").as_deref(),
            Some("emails:transactional")
        );
        assert_eq!(queue_name_from_meta_key("bull", "other:emails:meta"), None);
        assert_eq!(queue_name_from_meta_key("bull", "bull:emails:wait"), None);
        // `bull::meta` would yield an empty name -- not a queue.
        assert_eq!(queue_name_from_meta_key("bull", "bull::meta"), None);
    }

    #[test]
    fn parses_lib_and_version_from_meta() {
        assert_eq!(
            parse_meta_version("bullmq:6.0.2"),
            Some(("bullmq".into(), "6.0.2".into()))
        );
        assert_eq!(
            parse_meta_version("bullmq-pro:7.1.0"),
            Some(("bullmq-pro".into(), "7.1.0".into()))
        );
        assert_eq!(parse_meta_version("garbage"), None);
    }

    #[test]
    fn paused_reads_from_meta_field_only() {
        assert!(is_paused(Some("1")));
        assert!(!is_paused(Some("0")));
        assert!(!is_paused(None)); // resume does HDEL, so absent means running
        assert!(!is_paused(Some("")));
    }
}
