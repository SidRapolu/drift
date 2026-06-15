use crate::align::{ Aligner, Divergence };
use crate::replay::Scenario;

// Feed a whole scenario through a fresh aligner and return what it flags
pub fn run(scenario: &Scenario, allowed_lateness: u64) -> Vec<Divergence> {
    let mut eng = Aligner::new(allowed_lateness);
    let mut out = Vec::new();
    for &event in &scenario.arrivals {
        out.extend(eng.observe(event));
    }
    out.extend(eng.flush());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{ generate, ScenarioConfig };

    // A clean scenario through the real harness should flag nothing
    #[test]
    fn clean_scenario_has_no_divergence() {
        let cfg = ScenarioConfig::default();
        let scenario = generate(&cfg);
        let found = run(&scenario, 5);
        assert!(found.is_empty(), "clean scenario flagged {found:?}");
    }

    // Lag alone, with matching content, should still flag nothing as long as the
    // lateness budget covers the lag
    #[test]
    fn lag_within_budget_is_silent() {
        let cfg = ScenarioConfig { lag_ticks: 3, ..Default::default() };
        let scenario = generate(&cfg);
        // lag_ticks shifts B by 3 ticks of slots; a generous L absorbs it
        let found = run(&scenario, 20);
        assert!(found.is_empty(), "lag within budget flagged {found:?}");
    }

    // Reordering alone, with matching content, should flag nothing within budget
    #[test]
    fn reordering_within_budget_is_silent() {
        let cfg = ScenarioConfig { reorder_percent: 40, ..Default::default() };
        let scenario = generate(&cfg);
        let found = run(&scenario, 10);
        assert!(found.is_empty(), "reordering within budget flagged {found:?}");
    }

    // A planted divergence should be caught, and at the coordinate it was planted
    #[test]
    fn planted_divergence_is_caught_at_its_coordinate() {
        let cfg = ScenarioConfig {
            planted_divergence: Some((40, 2, 999_999)),
            ..Default::default()
        };
        let scenario = generate(&cfg);
        let found = run(&scenario, 5);

        let (pt, plevel) = scenario.planted_at.unwrap();
        let hit = found.iter().find(|d| d.event_time == pt);
        assert!(hit.is_some(), "planted divergence at {pt} not caught; got {found:?}");
        let hit = hit.unwrap();
        assert!(
            hit.mismatches.iter().any(|m| m.price_level == plevel),
            "divergence found but not at level {plevel}: {hit:?}"
        );
    }

    // The hard case all at once: lag + reordering as noise, with one real
    // divergence buried in it. The engine should find the needle and nothing else
    #[test]
    fn finds_real_divergence_amid_lag_and_reordering() {
        let cfg = ScenarioConfig {
            lag_ticks: 2,
            reorder_percent: 30,
            planted_divergence: Some((50, 1, 888_888)),
            ..Default::default()
        };
        let scenario = generate(&cfg);
        // L must cover both the lag and the reorder jitter to avoid false drops
        let found = run(&scenario, 15);

        // exactly the planted coordinate, nothing spurious
        assert_eq!(found.len(), 1, "expected only the planted divergence, got {found:?}");
        assert_eq!(found[0].event_time, 50);
        assert!(found[0].mismatches.iter().any(|m| m.price_level == 1));
    }
}
