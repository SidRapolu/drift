// Manufactures test input for the two streams that are *supposed* to agree,
// plus the controls to make them hard to compare

// Fault Injection

use crate::event::{ Event, StreamId };

pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    // Value 0..n
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        self.next_u64() % n
    }

    // Returns true with roughly the given percent probability (0..=100)
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

// Knobs controlling how the two streams are generated and corrupted
#[derive(Debug, Clone)]
pub struct ScenarioConfig {
    // Random seed fixes the entire scenario for reproducibility
    pub seed: u64,
    // How many event-time ticks the streams span, more ticks = longer run
    pub ticks: u64,
    // "Width" of the order book
    pub price_levels: i64,
    // Probability that a given (tick, level) actually produces an
    // event.
    pub emit_percent: u64,

    // The three knobs:
    pub lag_ticks: u64,
    pub reorder_percent: u64,
    pub planted_divergence: Option<(u64, i64, u64)>,
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        ScenarioConfig {
            seed: 0xc0ffee,
            ticks: 100,
            price_levels: 5,
            emit_percent: 100,
            lag_ticks: 0,
            reorder_percent: 0,
            planted_divergence: None,
        }
    }
}

pub struct Scenario {
    // Events in the order the engine receives them with lag and reordering applied
    pub arrivals: Vec<Event>,
    // Planted divergence event, if any
    pub planted_at: Option<(u64, i64)>,
}

pub fn generate(cfg: &ScenarioConfig) -> Scenario {
    let mut rng = Rng::new(cfg.seed);

    // Each entry: (arrival_index_hint, Event)
    // The hint models lag and reordering by modifying the positions before final sort
    let mut tagged: Vec<(i64, Event)> = Vec::new();

    for t in 0..cfg.ticks {
        for level in 0..cfg.price_levels {
            if !rng.chance(cfg.emit_percent) {
                continue;
            }

            // The "true" resting size at this point
            let true_size = 1 + rng.below(1000);

            // A tells the truth but reordered, with the same event_time values
            let a_jitter = if rng.chance(cfg.reorder_percent) {
                // arrive <= 3 slots early or late
                (rng.below(7) as i64) - 3
            } else {
                0
            };
            let a_pos = (t as i64) * cfg.price_levels + level + a_jitter;
            tagged.push((a_pos, Event::new(t, StreamId::A, level, true_size)));

            // B is truthful except for lie
            let b_size = match cfg.planted_divergence {
                Some((pt, plevel, psize)) if pt == t && plevel == level => psize,
                _ => true_size,
            };
            let b_jitter = if rng.chance(cfg.reorder_percent) {
                (rng.below(7) as i64) - 3
            } else {
                0
            };

            // Add lag to B
            let b_pos =
                (t as i64) * cfg.price_levels +
                level +
                b_jitter +
                (cfg.lag_ticks as i64) * cfg.price_levels;
            tagged.push((b_pos, Event::new(t, StreamId::B, level, b_size)));
        }
    }

    // Sort by the modified arrival position to produce the final arrival stream
    // Stable sort so equal positions keep insertion order deterministically
    tagged.sort_by_key(|(pos, _)| *pos);

    let arrivals = tagged
        .into_iter()
        .map(|(_, e)| e)
        .collect();
    let planted_at = cfg.planted_divergence.map(|(t, level, _)| (t, level));

    Scenario { arrivals, planted_at }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::StreamId;

    #[test]
    fn rng_is_deterministic() {
        // The bedrock property: same seed -> same sequence
        let mut a = Rng::new(123);
        let mut b = Rng::new(123);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn clean_scenario_has_matching_streams() {
        // A and B should report identical sizes at every (event_time, level)
        let cfg = ScenarioConfig::default();
        let s = generate(&cfg);

        // Bucket sizes by (event_time, level, stream) and check A == B everywhere.
        use std::collections::HashMap;
        let mut a: HashMap<(u64, i64), u64> = HashMap::new();
        let mut b: HashMap<(u64, i64), u64> = HashMap::new();
        for e in &s.arrivals {
            let key = (e.event_time.0, e.update.price_level);
            match e.stream {
                StreamId::A => {
                    a.insert(key, e.update.size);
                }
                StreamId::B => {
                    b.insert(key, e.update.size);
                }
            }
        }
        assert_eq!(a, b, "clean scenario must have A and B agreeing everywhere");
        assert!(s.planted_at.is_none());
    }

    #[test]
    fn planted_divergence_actually_diverges() {
        let mut cfg = ScenarioConfig::default();
        cfg.planted_divergence = Some((10, 2, 999_999)); // absurd size = unmistakable
        let s = generate(&cfg);

        let mut a_size = None;
        let mut b_size = None;
        for e in &s.arrivals {
            if e.event_time.0 == 10 && e.update.price_level == 2 {
                match e.stream {
                    StreamId::A => {
                        a_size = Some(e.update.size);
                    }
                    StreamId::B => {
                        b_size = Some(e.update.size);
                    }
                }
            }
        }
        assert_eq!(b_size, Some(999_999), "B must report the planted size");
        assert_ne!(a_size, b_size, "A and B must actually disagree at planted coord");
        assert_eq!(s.planted_at, Some((10, 2)));
    }

    #[test]
    fn lag_does_not_change_event_times() {
        // Lag delays ARRIVAL, never EVENT TIME
        let mut clean = ScenarioConfig::default();
        clean.seed = 7;
        let mut lagged = clean.clone();
        lagged.lag_ticks = 5;

        let collect_times = |s: &Scenario| {
            let mut v: Vec<u64> = s.arrivals
                .iter()
                .map(|e| e.event_time.0)
                .collect();
            v.sort_unstable();
            v
        };
        assert_eq!(
            collect_times(&generate(&clean)),
            collect_times(&generate(&lagged)),
            "lag must not alter the multiset of event times"
        );
    }

    #[test]
    fn generation_is_reproducible() {
        // Same config -> byte-identical arrival tape
        let cfg = ScenarioConfig {
            seed: 42,
            reorder_percent: 30,
            lag_ticks: 2,
            ..Default::default()
        };
        let s1 = generate(&cfg);
        let s2 = generate(&cfg);
        assert_eq!(s1.arrivals, s2.arrivals);
    }
}
