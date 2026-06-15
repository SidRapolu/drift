use clap::Parser;

use skew::replay::{ generate, ScenarioConfig };
use skew::run::run;

// Scenario knobs, all optional with sensible defaults
#[derive(Parser)]
#[command(about = "Run the skew divergence engine over a generated scenario")]
struct Args {
    // How many event-time ticks the streams span
    #[arg(long, default_value_t = 100)]
    ticks: u64,

    // Number of price levels in the book
    #[arg(long, default_value_t = 5)]
    levels: i64,

    // Shift stream B this many ticks later in arrival order (models lag)
    #[arg(long, default_value_t = 2)]
    lag: u64,

    // Percent chance each event arrives out of event-time order
    #[arg(long, default_value_t = 30)]
    reorder: u64,

    // How long a moment stays open for stragglers
    #[arg(long, default_value_t = 15)]
    lateness: u64,

    // Random seed, so any run is reproducible
    #[arg(long, default_value_t = 0xc0ffee)]
    seed: u64,

    // Plant a real divergence at this event-time
    #[arg(long, default_value_t = 50)]
    plant_time: u64,

    // Price level for the planted divergence
    #[arg(long, default_value_t = 1)]
    plant_level: i64,

    // Size stream B falsely reports at the planted coordinate
    #[arg(long, default_value_t = 888_888)]
    plant_size: u64,

    // Skip the planted divergence entirely
    #[arg(long, default_value_t = false)]
    no_plant: bool,
}

fn main() {
    let args = Args::parse();

    let planted = if args.no_plant {
        None
    } else {
        Some((args.plant_time, args.plant_level, args.plant_size))
    };

    let cfg = ScenarioConfig {
        seed: args.seed,
        ticks: args.ticks,
        price_levels: args.levels,
        emit_percent: 100,
        lag_ticks: args.lag,
        reorder_percent: args.reorder,
        planted_divergence: planted,
    };

    let scenario = generate(&cfg);
    let total = scenario.arrivals.len();
    let found = run(&scenario, args.lateness);

    println!("skew demo");
    println!("  events fed:        {total}");
    println!("  lag_ticks:         {}", cfg.lag_ticks);
    println!("  reorder_percent:   {}", cfg.reorder_percent);
    println!("  allowed_lateness:  {}", args.lateness);
    match scenario.planted_at {
        Some((t, l)) => println!("  planted divergence at (t={t}, level={l})"),
        None => println!("  no planted divergence (streams should agree)"),
    }
    println!();

    if found.is_empty() {
        println!("no divergences found");
    } else {
        println!("{} divergence(s) found:", found.len());
        for d in &found {
            for m in &d.mismatches {
                println!(
                    "  t={:<4} level={:<3} A={:<8} B={:<8}",
                    d.event_time,
                    m.price_level,
                    fmt(m.a_size),
                    fmt(m.b_size)
                );
            }
        }
    }
}

// Render a missing side as "-" instead of leaving it blank
fn fmt(size: Option<u64>) -> String {
    match size {
        Some(s) => s.to_string(),
        None => "-".to_string(),
    }
}
