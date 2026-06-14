use std::collections::{ BTreeMap, HashSet };

use crate::event::{ Event, StreamId };
use crate::window::{ LevelState, StreamWindow };

// A divergence at one moment, with the levels that disagreed
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Divergence {
    pub event_time: u64,
    pub mismatches: Vec<LevelMismatch>,
}

// One level where A and B disagreed; None means that side never reported it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelMismatch {
    pub price_level: i64,
    pub a_size: Option<u64>,
    pub b_size: Option<u64>,
}

pub struct Aligner {
    a: StreamWindow,
    b: StreamWindow,
    // moments finalized by one stream bounded by moment buffer
    pending_a: BTreeMap<u64, LevelState>,
    pending_b: BTreeMap<u64, LevelState>,
}

impl Aligner {
    pub fn new(allowed_lateness: u64) -> Self {
        Aligner {
            a: StreamWindow::new(allowed_lateness),
            b: StreamWindow::new(allowed_lateness),
            pending_a: BTreeMap::new(),
            pending_b: BTreeMap::new(),
        }
    }

    fn combined_watermark(&self) -> Option<u64> {
        match (self.a.watermark(), self.b.watermark()) {
            (Some(wa), Some(wb)) => Some(wa.min(wb)),
            _ => None,
        }
    }

    // Route one event to its window, stash anything that finalizes, then compare
    // whatever's now safe to compare
    pub fn observe(&mut self, event: Event) -> Vec<Divergence> {
        let mut finalized = Vec::new();
        match event.stream {
            StreamId::A => {
                self.a.observe(event.event_time, event.update, &mut finalized);
                for (t, state) in finalized {
                    self.pending_a.insert(t, state);
                }
            }
            StreamId::B => {
                self.b.observe(event.event_time, event.update, &mut finalized);
                for (t, state) in finalized {
                    self.pending_b.insert(t, state);
                }
            }
        }

        match self.combined_watermark() {
            Some(cw) => self.drain_and_compare(Some(cw)),
            None => Vec::new(),
        }
    }

    // End of stream:, flush both windows and compare everything left
    pub fn flush(&mut self) -> Vec<Divergence> {
        let mut fa = Vec::new();
        let mut fb = Vec::new();
        self.a.flush(&mut fa);
        self.b.flush(&mut fb);
        for (t, s) in fa {
            self.pending_a.insert(t, s);
        }
        for (t, s) in fb {
            self.pending_b.insert(t, s);
        }
        self.drain_and_compare(None)
    }

    // Compare every pending moment up to the bound , removing
    // each one. The pending maps are the record of what's left.
    fn drain_and_compare(&mut self, bound: Option<u64>) -> Vec<Divergence> {
        let mut times: HashSet<u64> = HashSet::new();
        match bound {
            Some(b) => {
                times.extend(self.pending_a.range(..=b).map(|(t, _)| *t));
                times.extend(self.pending_b.range(..=b).map(|(t, _)| *t));
            }
            None => {
                times.extend(self.pending_a.keys().copied());
                times.extend(self.pending_b.keys().copied());
            }
        }
        let mut times: Vec<u64> = times.into_iter().collect();
        times.sort_unstable();

        let mut out = Vec::new();
        for t in times {
            let a = self.pending_a.remove(&t);
            let b = self.pending_b.remove(&t);
            if let Some(d) = compare_moment(t, a.as_ref(), b.as_ref()) {
                out.push(d);
            }
        }
        out
    }
}

// Compares two finalized states
fn compare_moment(
    event_time: u64,
    a: Option<&LevelState>,
    b: Option<&LevelState>
) -> Option<Divergence> {
    let empty = LevelState::new();
    let a = a.unwrap_or(&empty);
    let b = b.unwrap_or(&empty);

    // Every level either side mentions
    let mut levels: Vec<i64> = a.keys().chain(b.keys()).copied().collect();
    levels.sort_unstable();
    levels.dedup();

    let mut mismatches = Vec::new();
    for level in levels {
        let av = a.get(&level).copied();
        let bv = b.get(&level).copied();
        if av.unwrap_or(0) != bv.unwrap_or(0) {
            mismatches.push(LevelMismatch { price_level: level, a_size: av, b_size: bv });
        }
    }

    if mismatches.is_empty() {
        None
    } else {
        Some(Divergence { event_time, mismatches })
    }
}

// Written by Claude
#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{ Event, StreamId };

    fn ev(t: u64, stream: StreamId, level: i64, size: u64) -> Event {
        Event::new(t, stream, level, size)
    }

    // compare_moment on its own

    #[test]
    fn identical_states_do_not_diverge() {
        let mut a = LevelState::new();
        a.insert(1, 100);
        a.insert(2, 200);
        let b = a.clone();
        assert_eq!(compare_moment(5, Some(&a), Some(&b)), None);
    }

    #[test]
    fn differing_size_is_a_divergence() {
        let mut a = LevelState::new();
        a.insert(1, 100);
        let mut b = LevelState::new();
        b.insert(1, 101);
        let d = compare_moment(5, Some(&a), Some(&b)).unwrap();
        assert_eq!(d.event_time, 5);
        assert_eq!(d.mismatches.len(), 1);
        assert_eq!(d.mismatches[0].a_size, Some(100));
        assert_eq!(d.mismatches[0].b_size, Some(101));
    }

    #[test]
    fn silence_versus_nonzero_is_a_divergence() {
        let mut a = LevelState::new();
        a.insert(7, 300);
        let b = LevelState::new();
        let d = compare_moment(5, Some(&a), Some(&b)).unwrap();
        assert_eq!(d.mismatches[0].a_size, Some(300));
        assert_eq!(d.mismatches[0].b_size, None);
    }

    #[test]
    fn silence_versus_zero_agree() {
        let a = LevelState::new();
        let mut b = LevelState::new();
        b.insert(7, 0);
        assert_eq!(compare_moment(5, Some(&a), Some(&b)), None);
    }

    // the whole aligner

    #[test]
    fn agreeing_streams_produce_no_divergence() {
        let mut eng = Aligner::new(2);
        let mut all = Vec::new();
        for t in 0..20u64 {
            all.extend(eng.observe(ev(t, StreamId::A, 0, t * 10)));
            all.extend(eng.observe(ev(t, StreamId::B, 0, t * 10)));
        }
        all.extend(eng.flush());
        assert!(all.is_empty(), "identical streams shouldn't diverge, got {all:?}");
    }

    #[test]
    fn one_planted_divergence_is_caught_exactly_once() {
        let mut eng = Aligner::new(2);
        let mut all = Vec::new();
        for t in 0..20u64 {
            all.extend(eng.observe(ev(t, StreamId::A, 0, t * 10)));
            let b_size = if t == 8 { 9999 } else { t * 10 };
            all.extend(eng.observe(ev(t, StreamId::B, 0, b_size)));
        }
        all.extend(eng.flush());
        assert_eq!(all.len(), 1, "expected exactly one divergence, got {all:?}");
        assert_eq!(all[0].event_time, 8);
        assert_eq!(all[0].mismatches[0].a_size, Some(80));
        assert_eq!(all[0].mismatches[0].b_size, Some(9999));
    }

    #[test]
    fn lagging_stream_does_not_cause_false_divergence() {
        // the big one: B is entirely behind A but agrees in content. a naive
        // compare would fire on every moment; the watermark suppresses all of it
        let mut eng = Aligner::new(3);
        let mut all = Vec::new();
        for t in 0..15u64 {
            all.extend(eng.observe(ev(t, StreamId::A, 0, t * 10)));
        }
        for t in 0..15u64 {
            all.extend(eng.observe(ev(t, StreamId::B, 0, t * 10)));
        }
        all.extend(eng.flush());
        assert!(all.is_empty(), "pure lag shouldn't diverge, got {all:?}");
    }

    #[test]
    fn out_of_order_arrivals_still_compare_correctly() {
        let mut eng = Aligner::new(5);
        let mut all = Vec::new();
        let order = [3u64, 0, 1, 4, 2, 7, 5, 9, 6, 8];
        for &t in &order {
            all.extend(eng.observe(ev(t, StreamId::A, 0, t * 10)));
        }
        for &t in &order {
            all.extend(eng.observe(ev(t, StreamId::B, 0, t * 10)));
        }
        all.extend(eng.flush());
        assert!(all.is_empty(), "agreeing out-of-order streams shouldn't diverge, got {all:?}");
    }

    #[test]
    fn combined_watermark_waits_for_slower_stream() {
        // A races ahead and diverges at t=50, but B has only reached t=2. since
        // the combined watermark follows B, that t=50 divergence stays invisible
        let mut eng = Aligner::new(1);
        let mut all = Vec::new();
        for t in 0..=100u64 {
            let a_size = if t == 50 { 7777 } else { t };
            all.extend(eng.observe(ev(t, StreamId::A, 0, a_size)));
        }
        all.extend(eng.observe(ev(0, StreamId::B, 0, 0)));
        all.extend(eng.observe(ev(1, StreamId::B, 0, 1)));
        all.extend(eng.observe(ev(2, StreamId::B, 0, 2)));
        assert!(all.is_empty(), "shouldn't report past the slower stream, got {all:?}");
    }
}
