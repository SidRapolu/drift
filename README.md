# Drift

Watches two streams that should agree and flags when they actually diverge—without false-alarming every time one stream is just running behind.

## Why

Comparing two streams is easy, but telling a real disagreement apart from one stream lagging is the challenge. I wanted to build that timing problem properly, in Rust for perfomance. The case I had in mind is an exchange feed vs. an order book you rebuild yourself. They should always match, and a real mismatch means your view of the market is off.

## How it works

Every event has a time it happened. Each stream tracks the highest time it's seen
and trails it by a lateness budget `L`:

```
watermark = highest_seen - L
```

- A moment is only compared once the watermark passes it — by then both streams
  have had `L` to deliver stragglers.
- Anything that shows up older than the watermark is too late, and dropped.
- Two streams, so the real watermark is `min(a, b)` — it waits for the slower one.
- Finalized moments get evicted, so memory stays flat however long it runs.

A lagging stream just holds the watermark back instead of looking like a
divergence. That's the whole trick.

## Tradeoff

`L` is the knob. Bigger catches later stragglers but buffers more and reports
slower; smaller is fast and cheap but starts dropping real stragglers as "too
late." There's no universal value — it depends how messy the feeds are.

```
cargo run -- --no-plant                          # clean, finds nothing
cargo run -- --lag 8 --reorder 60 --lateness 3   # L too small, false alarms
```

## Running it

```
cargo test
cargo run            # demo
cargo run -- --help  # knobs
cargo bench          # throughput
```

~2.7M events/sec single-threaded on an M3 Pro. Scales per-instrument across cores.

## Caveats

- Two streams only (N-way majority vote would find _which_ one is wrong).
- A stalled stream stalls the watermark on purpose; you'd alarm on that separately.
- Concrete order-book payload, not generic.
