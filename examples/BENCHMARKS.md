# litefan benchmarks

These benchmarks are informational smoke tests, not stable performance claims.
They create temporary SQLite databases and use the library defaults: WAL,
`synchronous=NORMAL`, a four-connection pool, 500-message maximum batches, and
a 100 ms cross-process polling interval.

Filesystem, SQLite build, durability settings, payload size, hardware, and
application handler work all materially affect the result. The representative
numbers below were recorded on the development machine on July 13, 2026. Run
the commands locally when making a capacity decision.

The important unit is a durable delivery, not a source message. Publishing one
message to 100 matching consumers creates 100 delivery rows, followed by 100
claims and 100 acknowledgements.

## Basic throughput

Run:

```console
cargo run --release --example benchmark
```

The source is [`benchmark.rs`](benchmark.rs). It measures set-based publishing,
fan-out write amplification, claim/ack batch sizes, permanent idempotency keys,
and one large inactive backlog.

### Fan-out publishing

Each case publishes 1,000 one-byte messages in batches of 500.

| Consumers | Source messages/s | Delivery rows/s |
|---:|---:|---:|
| 1 | 223k | 223k |
| 10 | 80k | 798k |
| 100 | 14k | 1.40M |
| 1,000 | 1.5k | 1.51M |

The nearly stable delivery-row rate makes the amplification visible: source
message capacity is approximately the delivery-row rate divided by the number
of matching consumers.

### Claim and ack batching

Each case drains 10,000 preloaded messages through one consumer.

| Batch size | Deliveries/s |
|---:|---:|
| 1 | 8.5k |
| 10 | 66k |
| 100 | 245k |
| 500 | 325k |

Batching is the largest immediately controllable performance lever. These
numbers exclude application handler time.

### Idempotency and inactive backlog

- New keyed messages: about 44k messages/s.
- Existing-key no-ops: about 105k messages/s.
- Building a 100,000-row inactive backlog: about 820k messages/s.

The keyed path currently performs per-message ledger work inside one batch
transaction. It is correct and commit-efficient, but has not received the
set-based optimization used by unkeyed batches.

## Heavy concurrency

Run:

```console
cargo run --release --example heavy_benchmark
```

The source is [`heavy_benchmark.rs`](heavy_benchmark.rs). It measures idle long
polling, notification wake-up pressure, concurrent fan-out consumption,
payload reads, backlog storage, and competing workers sharing one consumer.

### Idle long polling

All pollers wait on empty consumers for 500 ms. With the default 100 ms fallback
interval, 1,000 idle consumers imply roughly 10,000 small indexed reads/s.

| Idle pollers | Requested timeout | Observed completion |
|---:|---:|---:|
| 100 | 500 ms | 504 ms |
| 500 | 500 ms | 509 ms |
| 1,000 | 500 ms | 515 ms |

The read-only fallback polling itself is healthy at this scale on the test
machine.

### Publishing while unrelated consumers poll

This scenario publishes 5,000 messages to one topic while consumers filtered
to another topic are long-polling. A publish commit currently changes one
process-wide notification generation, so unrelated pollers wake and recheck
SQLite.

| Unrelated pollers | Publish batch 50 | Publish batch 500 |
|---:|---:|---:|
| 0 | 176k msg/s | 674k msg/s |
| 100 | 46k msg/s | 150k msg/s |
| 500 | 10k msg/s | 40k msg/s |
| 1,000 | 5.1k msg/s | 20.6k msg/s |

This is the clearest current scaling problem. It is an in-process notification
herd, not an SQLite polling limit. Larger publish batches help by producing
fewer generations, but topic/consumer-scoped notifications or a shared polling
coordinator would avoid waking unrelated consumers.

### Backlog drain

These cases preload 2,000 one-byte messages, then start one worker for every
durable consumer. Poll and ack batches contain 100 deliveries.

| Consumers | Deliveries | Allocated database | Drain rate | Source-equivalent rate |
|---:|---:|---:|---:|---:|
| 1 | 2k | 0.1 MiB | 231k delivery/s | 231k msg/s |
| 10 | 20k | 0.8 MiB | 153k delivery/s | 15.3k msg/s |
| 50 | 100k | 3.8 MiB | 211k delivery/s | 4.2k msg/s |
| 100 | 200k | 7.5 MiB | 207k delivery/s | 2.1k msg/s |

At larger row counts, the database used roughly 39-40 bytes per delivery row,
plus one copy of each message body and its metadata. Acknowledged rows free
pages for reuse but do not automatically shrink the SQLite file.

### Live publish and consumption

One worker runs per consumer while the producer publishes in batches of 100.
Workers claim and ack batches of 100.

| Consumers | Body | Delivery rate | Source-message rate | Body-read rate |
|---:|---:|---:|---:|---:|
| 10 | 1 byte | 151k/s | 15.1k/s | — |
| 50 | 1 byte | 148k/s | 3.0k/s | — |
| 100 | 1 byte | 168k/s | 1.7k/s | — |
| 50 | 1 KiB | 157k/s | 3.1k/s | 153 MiB/s |

For a heavy but reasonable example, consider 50 durable consumers receiving
1,000 one-KiB source messages/s. That is 50,000 deliveries/s and approximately
50 MiB/s of body reads. The benchmark has about three times that delivery
headroom before accounting for application handler work.

At 100 consumers and 1,000 source messages/s, the database performs 100,000
deliveries/s. That worked with less than two times measured headroom. Around
2,000 source messages/s would exceed the observed steady-state capacity and
grow a backlog.

### Workers competing on one consumer

These cases drain 20,000 messages through one durable consumer with 100-message
poll and ack batches.

| Workers | Delivery rate |
|---:|---:|
| 1 | 233k/s |
| 4 | 103k/s |
| 16 | 98k/s |
| 64 | 104k/s |

More polling workers reduce database throughput because SQLite still has one
writer and every worker performs claim and ack mutations. Prefer one polling
coordinator per durable consumer, distribute claimed messages to application
handler tasks, and aggregate their acknowledgements into batches.

## Capacity implications

- Budget in deliveries per second: source rate multiplied by matching durable
  consumers.
- Prefer publish batches near 500 and poll/ack batches of 100-500 when latency
  permits.
- A short slowdown at 50 consumers and 1,000 messages/s creates 50,000 backlog
  rows per second, or roughly 2 MiB/s of delivery metadata plus the single-copy
  message bodies.
- One million pending deliveries occupy roughly 40 MiB in this workload. One
  hundred million pending deliveries are on the order of 4 GiB and are no
  longer a comfortable embedded-database workload.
- The prototype does not yet clean up acknowledged message bodies or the
  permanent idempotency ledger. Retention and incremental garbage collection
  are required before sustained production use.
- `synchronous=FULL`, slower storage, large payloads, multiple processes, and
  real handlers will reduce these rates. Benchmark the intended deployment.

The results support materialized inboxes for tens to low hundreds of active
consumers. If thousands of consumers commonly match every message, the
fundamental delivery and acknowledgement amplification remains even if publish
materialization becomes lazy; sharding consumers across database files may be
the more effective scale-out boundary.
