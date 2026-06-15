# Drift

## How to Run

```
cargo test            # all tests
cargo run             # demo: lag + reordering noise, one planted divergence
cargo run -- --help   # scenario knobs
cargo bench           # throughput benchmark
```

Example Scenarios:

```
cargo run -- --no-plant                       # clean: should find nothing
cargo run -- --lag 8 --reorder 60 --lateness 25
cargo run -- --lag 8 --reorder 60 --lateness 3   # L too small: false divergences
```
