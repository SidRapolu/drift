//The windowed buffer + watermark (single stream)
// Determining when it's safe to say we've seen everything at a point in time

// Watermark (W) = Highest event time seen so far (H) - Ticks (L)

use std::collections::BTreeMap;

use crate::event::{ BookUpdate, EventTime };

pub type LevelState = std::collections::HashMap<i64, u64>;

#[derive(Debug, PartialEq, Eq)]
pub enum Observed {
    Accepted,
    TooLate,
}

pub struct StreamWindow {
    // Allowed lateness, L, in ticks
    allowed_lateness: u64,
    // Highest event time observed so far, H
    highest_seen: Option<u64>,
    live: BTreeMap<u64, LevelState>,
}

impl StreamWindow {
    pub fn new(allowed_lateness: u64) -> Self {
        StreamWindow {
            allowed_lateness,
            highest_seen: None,
            live: BTreeMap::new(),
        }
    }

    // The current watermark, W = H - L, or None if no events seen yet
    pub fn watermark(&self) -> Option<u64> {
        self.highest_seen.map(|h| h.saturating_sub(self.allowed_lateness))
    }

    // Feed one event from this stream into the window.
    // Returns whether it was accepted or dropped as too-late
    pub fn observe(
        &mut self,
        event_time: EventTime,
        update: BookUpdate,
        finalized: &mut Vec<(u64, LevelState)>
    ) -> Observed {
        let t = event_time.0;

        // Compare the event time against the watermark before to determine if the event is too late
        if let Some(w) = self.watermark() {
            if t <= w {
                return Observed::TooLate;
            }
        }

        // Accept last written event into the live window
        self.live.entry(t).or_insert_with(LevelState::new).insert(update.price_level, update.size);

        // Update H, taking the latest event into account
        self.highest_seen = Some(match self.highest_seen {
            Some(h) => h.max(t),
            None => t,
        });

        // Set events <= W in the finalized map and let events with time > W pend decision
        if let Some(w) = self.watermark() {
            let still_live = self.live.split_off(&(w + 1));
            let finalized_map = std::mem::replace(&mut self.live, still_live);
            for (time, state) in finalized_map {
                finalized.push((time, state));
            }
        }

        Observed::Accepted
    }

    // Flush everything remaining as finalized when no more events are coming
    pub fn flush(&mut self, finalized: &mut Vec<(u64, LevelState)>) {
        let remaining = std::mem::take(&mut self.live);
        for (time, state) in remaining {
            finalized.push((time, state));
        }
    }

    // Number of event-times currently buffered (used for tests)
    pub fn live_len(&self) -> usize {
        self.live.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::BookUpdate;

    fn upd(level: i64, size: u64) -> BookUpdate {
        BookUpdate { price_level: level, size }
    }

    #[test]
    fn watermark_trails_highest_by_allowed_lateness() {
        let mut w = StreamWindow::new(3); // L = 3
        let mut fin = Vec::new();
        assert_eq!(w.watermark(), None); // nothing seen yet

        w.observe(EventTime(10), upd(0, 100), &mut fin);
        // highest = 10, L = 3 -> watermark = 7
        assert_eq!(w.watermark(), Some(7));

        w.observe(EventTime(20), upd(0, 100), &mut fin);
        // highest = 20 -> watermark = 17
        assert_eq!(w.watermark(), Some(17));
    }

    #[test]
    fn in_order_events_finalize_as_watermark_advances() {
        let mut w = StreamWindow::new(2);
        let mut fin = Vec::new();

        // All times should be finalized
        for t in 1..=10u64 {
            w.observe(EventTime(t), upd(0, t * 10), &mut fin);
        }
        // Seen up to 10, watermark = 8
        // Times 9 and 10 should live
        let finalized_times: Vec<u64> = fin
            .iter()
            .map(|(t, _)| *t)
            .collect();
        assert_eq!(finalized_times, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(w.live_len(), 2); // Times 9, 10 still in the window
    }

    #[test]
    fn out_of_order_within_lateness_is_accepted() {
        let mut w = StreamWindow::new(5);
        let mut fin = Vec::new();

        w.observe(EventTime(10), upd(0, 1), &mut fin); // H=10, W=5
        // Time 7 arrives AFTER time 10, but 7 > watermark(5), so it's a
        // legitimate out-of-order event to accept
        let r = w.observe(EventTime(7), upd(0, 2), &mut fin);
        assert_eq!(r, Observed::Accepted);
    }

    #[test]
    fn straggler_beyond_lateness_is_too_late() {
        let mut w = StreamWindow::new(2);
        let mut fin = Vec::new();

        w.observe(EventTime(10), upd(0, 1), &mut fin); // H=10, W=8
        // Time 5 arrives but watermark is already 8 -> too late.
        let r = w.observe(EventTime(5), upd(0, 2), &mut fin);
        assert_eq!(r, Observed::TooLate);
    }

    #[test]
    fn memory_stays_bounded_over_a_long_stream() {
        // Live buffer never grows past ~L, regardless of total length
        let mut w = StreamWindow::new(4);
        let mut fin = Vec::new();
        for t in 0..100_000u64 {
            w.observe(EventTime(t), upd(0, t), &mut fin);
            // At any point, live window holds only times in (W, H] ~ L+1 entries
            assert!(w.live_len() <= 5, "live window blew past bound at t={t}: {}", w.live_len());
        }
    }

    #[test]
    fn flush_finalizes_the_tail() {
        let mut w = StreamWindow::new(3);
        let mut fin = Vec::new();
        for t in 1..=5u64 {
            w.observe(EventTime(t), upd(0, t), &mut fin);
        }
        // Without flush, the last few times sit un-finalized
        let before = fin.len();
        w.flush(&mut fin);
        assert!(fin.len() > before, "flush must emit the un-finalized tail");
        assert_eq!(w.live_len(), 0, "nothing should remain after flush");
    }

    #[test]
    fn last_write_wins_at_a_coordinate() {
        let mut w = StreamWindow::new(2);
        let mut fin = Vec::new();
        w.observe(EventTime(1), upd(7, 100), &mut fin);
        w.observe(EventTime(1), upd(7, 250), &mut fin); // overwrites size at level 7 (still live)
        // Advance H so the watermark passes time 1 and finalizes it.
        w.observe(EventTime(2), upd(0, 1), &mut fin);
        w.observe(EventTime(3), upd(0, 1), &mut fin);
        w.observe(EventTime(4), upd(0, 1), &mut fin); // H=4, W=2, time 1 finalizes
        let time1 = fin
            .iter()
            .find(|(t, _)| *t == 1)
            .map(|(_, s)| s)
            .unwrap();
        assert_eq!(time1.get(&7), Some(&250));
    }
}
