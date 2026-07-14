# litefan design

`litefan` is an embedded, durable, at-least-once fan-out log backed by SQLite.
Publishing stores each message once. Every durable consumer that matches the
message eventually receives it, and workers sharing a consumer name compete
for that consumer's copy.

The design has three durable ideas: an append-only message log, one scan cursor
per consumer, and a sparse set of materialized deliveries. Everything else is
derived from those.

## Semantics

- A consumer name is its durable identity. Reopening the name creates another
  worker for the same logical inbox.
- A new consumer starts after the current retained high-water mark. Consumer
  creation and publishing are ordered by SQLite's writer lock, so there is no
  boundary race.
- Filters are either all messages or an exact topic. Filtering is performed as
  the consumer advances through the log.
- Delivery is at least once. Polling gives a message a visibility deadline; an
  expired lease can be delivered again.
- A receipt identifies one lease generation. Ack, nack, and lease extension
  require the current generation, so a timed-out worker cannot mutate a newer
  worker's lease.
- Publish and receipt batches are atomic and bounded. Small batches remain
  valid; larger batches amortize SQLite's one-writer commit cost.
- An idempotency key suppresses another publish for a fixed configured window.
  Duplicates return the original message ID and do not extend the window.
- Draining atomically freezes a consumer's ending log position. Earlier work
  remains consumable and later publishes are ignored. Draining is irreversible.

## Storage

The message body exists once:

```sql
CREATE TABLE litefan_messages (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    topic        TEXT,
    body         BLOB NOT NULL,
    published_at INTEGER NOT NULL
) STRICT;
```

Each consumer stores its next-search boundary. `drain_cursor` is null while the
consumer is active and becomes its fixed final boundary when draining begins.

```sql
CREATE TABLE litefan_consumers (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE,
    topic_filter TEXT,
    created_at   INTEGER NOT NULL,
    draining_at  INTEGER,
    scan_cursor  INTEGER NOT NULL,
    drain_cursor INTEGER
) STRICT;
```

A delivery row exists only after work is claimed. Ack deletes it. Nack and
visibility extension update it. This table is therefore proportional to
in-flight, delayed, and expired work—not to the total historical fan-out.

```sql
CREATE TABLE litefan_deliveries (
    consumer_id    INTEGER NOT NULL,
    message_id     INTEGER NOT NULL,
    visible_at     INTEGER NOT NULL,
    generation     INTEGER NOT NULL,
    delivery_count INTEGER NOT NULL,
    PRIMARY KEY (consumer_id, message_id)
) STRICT, WITHOUT ROWID;
```

Idempotency is a separate expiring ledger. It deliberately does not reference
the message table, so retention may remove a body without weakening deduplication
during the configured window.

## Publishing

Unkeyed batches use one set-based message insert and one WAL commit. Their cost
does not depend on the number of consumers.

Keyed and mixed batches are also set based:

1. Delete expired ledger entries through the expiry index.
2. Insert all unique supplied keys with conflict-ignore.
3. Read their ledger state in one query.
4. Insert unkeyed messages and the first occurrence of each newly claimed key.
5. Fill new ledger entries with a set-based update and commit.

Duplicate keys within a batch preserve input order and point at the first
message. SQLite serializes writers, so another publisher can observe only the
completed ledger entry.

## Claiming

An idle poll first performs a read-only check. It takes SQLite's writer lock
only when a retry is visible or its cursor is behind the current log boundary.

Inside the short claim transaction:

1. A no-op consumer update obtains the writer lock and serializes workers that
   share the consumer.
2. Visible materialized deliveries are leased first.
3. Remaining batch capacity is filled with matching messages after
   `scan_cursor`, using `INSERT ... SELECT ... RETURNING`.
4. The cursor advances to the last match, or to the high-water mark when the
   scan is exhausted. This permanently skips irrelevant topic ranges.
5. Bodies are fetched in ID order, then the transaction commits.

Advancing the cursor and inserting leases occur in the same transaction. A
message cannot fall between them, and competing workers cannot claim it twice.
Retries are preferred to new log entries, preventing expired work from being
starved by a busy publisher.

## Notification and long polling

SQLite is always the source of truth. Notifications only reduce latency.

Within a process, pollers subscribe to two generation counters: one for their
filter (`All` or one exact topic) and one for their consumer identity. Publishes
wake only matching filters; nacks and draining wake only workers for that
consumer. Weak registry entries disappear when handles are dropped, so the
notification map does not become another durable catalog.

Separate processes cannot share those counters. Empty polls therefore recheck
SQLite at `cross_process_poll_interval` (250 ms by default), also waking at the
earliest known visibility deadline. Marking generations seen before querying
prevents lost wakeups.

## Inspection, draining, deletion, and retention

Snapshots are exact logical views. Outstanding work is the sum of materialized
deliveries and matching unscanned messages. This makes snapshots more expensive
than hot-path polling by design; they do not require write-time counters or
risk counter drift.

Draining records both a timestamp and the current log boundary under the writer
lock. A drained consumer has reached that boundary and has no materialized
deliveries. Safe deletion requires that state; forced deletion reports both
unseen and materialized work it discarded.

Pruning is caller-bounded. A message is eligible only when it is older than the
requested time, no materialized delivery references it, and every matching
consumer has scanned beyond it (or was already draining before it). Pruning a
message never removes its still-live idempotency entry.

## Scaling model

- Publish storage and write cost are `O(messages)`, independent of consumers.
- An inactive consumer costs one row, regardless of backlog size.
- Delivery work is still fundamental: `M` messages for `C` matching consumers
  ultimately require `M * C` claims, body reads, and acknowledgements.
- Exact-topic scans use `(topic, id)`; all-message scans use the integer primary
  key. Sparse filters do not repeatedly rescan skipped ranges.
- SQLite has one writer. One polling coordinator per durable consumer with
  batched acknowledgement is the highest-throughput application shape, though
  competing workers remain correct.
- Very large independent workloads can shard naturally by database file. The
  file is the durability and write-serialization boundary.

The result keeps the state machine inspectable in ordinary SQL while removing
the two avoidable multipliers: publish-time fan-out rows and unrelated local
long-poll wakeups.

## Code organization

The crate follows the same boundaries as the design:

- `api.rs` defines the public data model and configuration.
- `store.rs` owns publishing, store snapshots, deletion, and retention.
- `consumer.rs` owns consumer creation, leasing, receipts, and draining.
- `storage.rs` contains shared transaction helpers and SQLite bind limits.
- `schema.rs`, `signals.rs`, and `time.rs` isolate schema definition,
  best-effort wakeups, and durable time conversion respectively.
- `error.rs` is the public failure contract, while `lib.rs` is only the crate
  map and re-export surface.

Fast unit tests live beside the pure components. Public-contract integration
tests are grouped under `tests/` by publishing, delivery, lifecycle, and
configuration behavior.
