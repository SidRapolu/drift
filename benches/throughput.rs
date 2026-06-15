use criterion::{ criterion_group, criterion_main, Criterion, Throughput };

use drift::replay::{ generate, ScenarioConfig };
use drift::run::run;

fn bench_throughput(c: &mut Criterion) {
    // Lag and reordering present, one divergence to catch
    let cfg = ScenarioConfig {
        ticks: 50_000,
        price_levels: 10,
        lag_ticks: 5,
        reorder_percent: 20,
        planted_divergence: Some((25_000, 3, 999_999)),
        ..Default::default()
    };
    let scenario = generate(&cfg);
    let event_count = scenario.arrivals.len() as u64;

    let mut group = c.benchmark_group("aligner");
    // Lets criterion report throughput in events/sec
    group.throughput(Throughput::Elements(event_count));
    group.bench_function("run_scenario", |b| {
        b.iter(|| run(&scenario, 30));
    });
    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
