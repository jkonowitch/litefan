# litefan benchmarks

These are informational smoke tests, not portable capacity promises. They use
temporary databases, WAL, `synchronous=NORMAL`, four connections, a maximum
batch of 500, and the 250 ms cross-process fallback interval. Filesystem,
SQLite build, payload size, durability settings, and application work all
matter. Numbers below were recorded on the development machine on July 13,
2026.

Run:

```console
cargo run --release --example benchmark
cargo run --release --example heavy_benchmark
```

## Publishing

Each case publishes 1,000 one-byte messages in batches of 500. Logical delivery
count is messages multiplied by matching consumers; lazy inboxes do not create
those rows during publish.

| Consumers | Source messages/s | Materialized rows at publish | Logical deliveries/s |
|---:|---:|---:|---:|
| 1 | 597k | 0 | 597k |
| 10 | 709k | 0 | 7.1M |
| 100 | 777k | 0 | 77.7M |
| 1,000 | 920k | 0 | 920M |

The variation is filesystem/commit noise; there is no downward trend with
consumer count. The previous materialized design fell from about 194k source
messages/s at one consumer to 681/s at 1,000 consumers while writing one
million delivery rows.

An inactive consumer accumulated 100,000 logical deliveries at 1.27M source
messages/s with zero materialized delivery rows.

## Keyed publishing

Ten thousand one-byte messages were published in 500-message batches:

| Path | Messages/s |
|---|---:|
| New keys | 105k |
| Existing-key no-ops | 745k |

The set-based ledger path replaced per-message claim/read/update statements.
The earlier implementation measured about 42k/s for new keys and 97k/s for
duplicates on the same machine.

## Claim and acknowledgement batch size

Each case drains 10,000 preloaded messages through one consumer.

| Batch size | Deliveries/s |
|---:|---:|
| 1 | 6.9k |
| 10 | 58k |
| 100 | 269k |
| 500 | 359k |

Small, irregular batches remain viable, but commits dominate at batch size one.
Batching is still the main throughput lever because SQLite permits one writer.

## Idle and unrelated pollers

One hundred, 500, and 1,000 empty long polls all completed a requested 500 ms
wait within roughly 4-26 ms. Topic-scoped in-process notifications prevented
consumers filtered to `idle` from waking when `active` was published.

Publishing 5,000 `active` messages while up to 1,000 unrelated pollers waited
remained in the approximate 376k-931k messages/s range in the measured cases.
The prior global generation counter dropped as low as roughly 5k-21k messages/s
because every commit woke every poller.

## Backlog drain and storage

Two thousand source messages were preloaded before one worker per consumer
started. Before claiming, there were zero delivery rows and the database was
about 0.1 MiB (roughly 41 bytes per source message in this tiny-payload case).

| Consumers | Logical deliveries | Drain deliveries/s | Source-equivalent messages/s |
|---:|---:|---:|---:|
| 1 | 2k | 263k | 263k |
| 10 | 20k | 136k | 13.6k |
| 50 | 100k | 163k | 3.3k |
| 100 | 200k | 135k | 1.4k |

Lazy materialization removes inactive-backlog amplification but cannot remove
the real work of delivering and acknowledging each consumer's copy.

## Live fan-out

One worker ran per consumer while publishing 2,000 messages in batches of 100;
workers claimed and acknowledged batches of 100.

| Consumers | Body | Delivery rate | Source-message rate | Body-read rate |
|---:|---:|---:|---:|---:|
| 10 | 1 byte | 137k/s | 13.7k/s | — |
| 50 | 1 byte | 138k/s | 2.8k/s | — |
| 100 | 1 byte | 135k/s | 1.3k/s | — |
| 50 | 1 KiB | 158k/s | 3.2k/s | 154 MiB/s |

With 50 consumers, 1,000 one-KiB source messages/s means 50,000 deliveries/s
and about 50 MiB/s of body reads. The measured database path has roughly 3x
delivery headroom before application handler work. At 100 consumers and 1,000
source messages/s, headroom is much narrower.

## Competing workers

Workers shared one consumer and drained 20,000 messages with 100-message polls
and acknowledgements.

| Workers | Deliveries/s |
|---:|---:|
| 1 | 262k |
| 4 | 109k |
| 16 | 90k |
| 64 | 101k |

Correctness is preserved, but additional polling workers contend for SQLite's
writer. Prefer one poll coordinator per durable consumer, distribute deliveries
to handler tasks, and aggregate completed receipts into acknowledgement batches.

## Capacity guidance

- Budget throughput in logical deliveries: source rate multiplied by matching
  durable consumers.
- Publish capacity no longer depends on inactive consumer count or backlog.
- Prefer publish batches near 500 and claim/ack batches of 100-500 when latency
  allows; smaller batches trade throughput for latency without changing
  semantics.
- Snapshot operations compute exact logical backlog and are intentionally not a
  hot-path counter API.
- `synchronous=FULL`, slower storage, multiple processes, and real handlers will
  reduce these rates. Benchmark the intended deployment.
- At a scale that exceeds one SQLite writer, shard independent consumers across
  database files rather than layering coordination onto one file.
