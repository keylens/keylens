//! Live throughput from the BullMQ events stream.
//!
//! This is the whole differentiation thesis. BullBoard polls `getJobCounts` on a timer,
//! which is why its graphs are coarse and why it feels laggy. BullMQ already writes every
//! state transition to a Redis STREAM at `<prefix>:<queue>:events`, so a single blocking
//! `XREAD` gives true event-level throughput at sub-second resolution and near-zero server
//! load — no polling, no counting, no extra round trips.
//!
//! What arrives is a stream of `(queue, event, entry-id)`. This module turns that into
//! per-second buckets the UI can draw.

use std::collections::HashMap;
use std::collections::VecDeque;

/// Seconds of history kept per queue.
pub const WINDOW_SECS: usize = 120;

/// The BullMQ event names worth distinguishing. Everything else still counts toward
/// total throughput but isn't broken out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Added,
    Active,
    Completed,
    Failed,
    Delayed,
    Progress,
    Removed,
    Paused,
    Resumed,
    Drained,
    Stalled,
    Other,
}

impl EventKind {
    pub fn parse(raw: &str) -> Self {
        match raw {
            "added" => EventKind::Added,
            "active" => EventKind::Active,
            "completed" => EventKind::Completed,
            "failed" => EventKind::Failed,
            "delayed" => EventKind::Delayed,
            "progress" => EventKind::Progress,
            "removed" => EventKind::Removed,
            "paused" => EventKind::Paused,
            "resumed" => EventKind::Resumed,
            "drained" => EventKind::Drained,
            "stalled" => EventKind::Stalled,
            _ => EventKind::Other,
        }
    }
}

/// One second of activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Bucket {
    pub second: i64,
    pub completed: u64,
    pub failed: u64,
    pub added: u64,
    pub total: u64,
}

/// A rolling per-second history for one queue.
#[derive(Debug, Clone, Default)]
pub struct Series {
    buckets: VecDeque<Bucket>,
    /// Cumulative since the stream was attached, for a "seen so far" readout.
    pub seen: u64,
    pub failed_total: u64,
}

impl Series {
    fn record(&mut self, kind: EventKind, second: i64) {
        let newest = self.buckets.back().map(|b| b.second).unwrap_or(second);

        // Only entries that have fallen out of the window entirely are dropped. Events
        // *within* the window may arrive slightly out of order -- a single `XREAD` returns
        // several streams, and a batch can interleave -- so they get placed in their own
        // bucket rather than discarded or folded into the wrong second.
        if second + WINDOW_SECS as i64 <= newest {
            return;
        }

        let idx = match self.buckets.iter().rposition(|b| b.second <= second) {
            Some(i) if self.buckets[i].second == second => i,
            Some(i) => {
                self.buckets.insert(
                    i + 1,
                    Bucket {
                        second,
                        ..Default::default()
                    },
                );
                i + 1
            }
            None => {
                self.buckets.push_front(Bucket {
                    second,
                    ..Default::default()
                });
                0
            }
        };

        let bucket = &mut self.buckets[idx];
        bucket.total += 1;
        match kind {
            EventKind::Completed => bucket.completed += 1,
            EventKind::Failed => {
                bucket.failed += 1;
                self.failed_total += 1;
            }
            EventKind::Added => bucket.added += 1,
            _ => {}
        }
        self.seen += 1;

        while self.buckets.len() > WINDOW_SECS {
            self.buckets.pop_front();
        }
    }

    /// The last `width` seconds ending at `now`, zero-filled.
    ///
    /// Zero-filling is what makes the graph honest: a queue that went quiet has to show a
    /// flat tail, not a stale spike frozen at the right-hand edge.
    pub fn window(&self, now: i64, width: usize, pick: impl Fn(&Bucket) -> u64) -> Vec<u64> {
        if width == 0 {
            return Vec::new();
        }
        let start = now - width as i64 + 1;
        let mut out = vec![0u64; width];
        for b in &self.buckets {
            if b.second >= start && b.second <= now {
                out[(b.second - start) as usize] = pick(b);
            }
        }
        out
    }

    /// Events per second over the last `secs`, counting the elapsed window rather than
    /// the buckets present, so silence pulls the rate down.
    pub fn rate(&self, now: i64, secs: i64) -> f64 {
        if secs <= 0 {
            return 0.0;
        }
        let start = now - secs + 1;
        let total: u64 = self
            .buckets
            .iter()
            .filter(|b| b.second >= start && b.second <= now)
            .map(|b| b.total)
            .sum();
        total as f64 / secs as f64
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

/// Where the live events reader has got to.
///
/// Three states, not a bool, because "we are not seeing events" has three very different
/// causes and the UI has to say which: still connecting, connected and quiet, or never
/// going to work on this server. A boolean `attached` collapsed the last two, so a server
/// without stream support sat on "attaching…" forever.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum EventsStatus {
    /// Opening the reader connection. An empty graph means "not watching yet".
    #[default]
    Attaching,
    /// Reader is on the streams. An empty graph means the queues are genuinely idle.
    Live,
    /// The reader gave up, with the reason. Nothing will arrive; say so.
    Unavailable(String),
}

/// Throughput across every queue being watched.
#[derive(Debug, Clone, Default)]
pub struct Throughput {
    series: HashMap<String, Series>,
    pub status: EventsStatus,
}

impl Throughput {
    /// Whether events are actually flowing, so an empty graph reads as "idle".
    pub fn is_live(&self) -> bool {
        self.status == EventsStatus::Live
    }

    /// `at_ms` is the event's own timestamp, taken from the stream entry id.
    pub fn record(&mut self, queue: &str, kind: EventKind, at_ms: i64) {
        self.series
            .entry(queue.to_string())
            .or_default()
            .record(kind, at_ms / 1000);
    }

    pub fn series(&self, queue: &str) -> Option<&Series> {
        self.series.get(queue)
    }

    pub fn total_rate(&self, now: i64, secs: i64) -> f64 {
        self.series.values().map(|s| s.rate(now, secs)).sum()
    }

    pub fn watching(&self) -> usize {
        self.series.len()
    }
}

/// Milliseconds out of a stream entry id (`<ms>-<seq>`).
///
/// Using the entry's own timestamp rather than arrival time means a burst that was
/// written while we were blocked still lands in the second it happened.
pub fn entry_id_ms(id: &str) -> Option<i64> {
    id.split('-').next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series_with(events: &[(EventKind, i64)]) -> Series {
        let mut s = Series::default();
        for (kind, sec) in events {
            s.record(*kind, *sec);
        }
        s
    }

    #[test]
    fn parses_bullmq_event_names() {
        assert_eq!(EventKind::parse("completed"), EventKind::Completed);
        assert_eq!(EventKind::parse("failed"), EventKind::Failed);
        assert_eq!(EventKind::parse("waiting-children"), EventKind::Other);
    }

    #[test]
    fn extracts_ms_from_a_stream_entry_id() {
        assert_eq!(entry_id_ms("1785515393123-0"), Some(1_785_515_393_123));
        assert_eq!(entry_id_ms("nonsense"), None);
    }

    #[test]
    fn events_land_in_per_second_buckets() {
        let s = series_with(&[
            (EventKind::Completed, 100),
            (EventKind::Completed, 100),
            (EventKind::Failed, 101),
        ]);
        let completed = s.window(101, 2, |b| b.completed);
        assert_eq!(completed, vec![2, 0]);
        let failed = s.window(101, 2, |b| b.failed);
        assert_eq!(failed, vec![0, 1]);
    }

    #[test]
    fn a_quiet_queue_shows_a_flat_tail_not_a_frozen_spike() {
        // The graph has to reflect *now*, not the last time something happened.
        let s = series_with(&[(EventKind::Completed, 100)]);
        assert_eq!(s.window(105, 6, |b| b.total), vec![1, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn window_is_always_exactly_the_requested_width() {
        let s = series_with(&[(EventKind::Completed, 100)]);
        for width in [1usize, 5, 30, 120] {
            assert_eq!(s.window(100, width, |b| b.total).len(), width);
        }
        assert!(s.window(100, 0, |b| b.total).is_empty());
    }

    #[test]
    fn history_is_capped_to_the_window() {
        let mut s = Series::default();
        for sec in 0..(WINDOW_SECS as i64 * 3) {
            s.record(EventKind::Completed, sec);
        }
        assert_eq!(s.buckets.len(), WINDOW_SECS);
    }

    #[test]
    fn events_older_than_the_window_are_dropped() {
        // A clock-skewed entry from an hour ago would otherwise be folded into the wrong
        // bucket and draw a spike that never happened.
        let mut s = series_with(&[(EventKind::Completed, 200), (EventKind::Completed, 201)]);
        let before = s.window(201, 3, |b| b.total);
        s.record(EventKind::Completed, 201 - WINDOW_SECS as i64);
        assert_eq!(s.window(201, 3, |b| b.total), before);
    }

    #[test]
    fn out_of_order_events_inside_the_window_are_kept() {
        // A single XREAD covers several streams and a batch can interleave, so events do
        // arrive slightly out of order. Dropping them undercounts the rate.
        let mut s = Series::default();
        s.record(EventKind::Completed, 205); // newest first
        s.record(EventKind::Completed, 203);
        s.record(EventKind::Completed, 204);
        s.record(EventKind::Completed, 205);

        assert_eq!(s.seen, 4, "no event may be discarded inside the window");
        assert_eq!(s.window(205, 3, |b| b.total), vec![1, 1, 2]);
    }

    #[test]
    fn an_out_of_order_burst_keeps_the_full_count() {
        let mut s = Series::default();
        for i in 0..12 {
            // Descending timestamps, the pathological ordering.
            s.record(EventKind::Completed, 1000 - i);
        }
        assert_eq!(s.seen, 12);
        assert_eq!(s.window(1000, 12, |b| b.total).iter().sum::<u64>(), 12);
    }

    #[test]
    fn rate_falls_off_when_a_queue_goes_quiet() {
        let s = series_with(&[
            (EventKind::Completed, 100),
            (EventKind::Completed, 100),
            (EventKind::Completed, 101),
        ]);
        // 3 events over a 3s window.
        assert!((s.rate(102, 3) - 1.0).abs() < f64::EPSILON);
        // Ten seconds later nothing has happened, so the rate must decay.
        assert!(s.rate(112, 3) < f64::EPSILON);
    }

    #[test]
    fn throughput_buckets_by_event_timestamp_not_arrival() {
        let mut t = Throughput::default();
        t.record("emails", EventKind::Failed, 1_785_515_393_600);
        t.record("emails", EventKind::Failed, 1_785_515_393_900);
        // Both are in the same second despite different milliseconds.
        let s = t.series("emails").unwrap();
        assert_eq!(s.window(1_785_515_393, 1, |b| b.failed), vec![2]);
        assert_eq!(s.failed_total, 2);
    }

    #[test]
    fn events_status_distinguishes_quiet_from_never_coming() {
        // As a bool this was one bit for two very different facts, and the UI printed
        // "attaching…" forever on a server that simply has no XREAD.
        let mut t = Throughput::default();
        assert_eq!(t.status, EventsStatus::Attaching, "nothing attached yet");
        assert!(!t.is_live());

        t.status = EventsStatus::Live;
        assert!(t.is_live(), "an empty graph now means idle");

        t.status = EventsStatus::Unavailable("NOPERM cannot run 'xread'".into());
        assert!(!t.is_live());
        assert!(
            matches!(&t.status, EventsStatus::Unavailable(why) if why.contains("NOPERM")),
            "the reason has to survive to the UI"
        );
    }

    #[test]
    fn queues_are_tracked_independently() {
        let mut t = Throughput::default();
        t.record("emails", EventKind::Completed, 100_000);
        t.record("reports", EventKind::Completed, 100_000);
        t.record("reports", EventKind::Completed, 100_000);

        assert_eq!(t.series("emails").unwrap().seen, 1);
        assert_eq!(t.series("reports").unwrap().seen, 2);
        assert!(t.series("unknown").is_none());
        assert_eq!(t.watching(), 2);
    }
}
