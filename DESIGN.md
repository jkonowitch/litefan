# litefan design sketch

`litefan` is an embedded, durable, at-least-once fan-out log backed by SQLite.
Publishing stores a message once. Every durable consumer that matches the
message eventually receives it, and workers sharing one consumer compete for
that consumer's copy.

This is deliberately a small first design. Delayed publishing, priorities,
dead-letter queues, and arbitrary filter expressions can be added later without
changing the core receipt or delivery model.

## Semantics worth fixing before the schema

- A consumer name is its durable identity. Opening the same name from several
  tasks or processes creates competing workers, not additional fan-out copies.
- Delivery is at least once. A claim has a visibility timeout; if it is neither
  acked nor nacked, it becomes visible again.
- A receipt identifies a particular delivery attempt. An ack or nack using a
  stale receipt is a no-op, so a timed-out worker cannot ack another worker's
  newer attempt.
- `publish_batch`, `ack_batch`, and `nack_batch` are atomic by default and have
  a configured maximum size. This gives callers one WAL commit without letting
  a huge batch monopolize SQLite's only writer.
- Idempotency keys are scoped to this message log. A duplicate returns the
  original message ID and does not create more consumer deliveries. Keys should
  be retained forever by default; an explicit retention window can trade that
  guarantee for bounded storage.
- A newly created consumer starts at `Now` by default. `Beginning` means the
  beginning of retained history, not necessarily the first message ever
  published.
- The initial filter should be either `All` or an exact topic match. Arbitrary
  Rust predicates cannot be applied transactionally during SQL fan-out, and a
  general filter language is unnecessary in v1.

## Shape A: materialized inboxes (recommended v1)

Store the body once and materialize a small delivery row for every matching
consumer:

```sql
CREATE TABLE messages (
    id           INTEGER PRIMARY KEY,
    topic        TEXT,
    body         BLOB NOT NULL,
    published_at INTEGER NOT NULL
) STRICT;

CREATE TABLE idempotency (
    key          BLOB PRIMARY KEY,
    message_id   INTEGER,
    expires_at   INTEGER
) STRICT, WITHOUT ROWID;

CREATE TABLE consumers (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    topic_filter  TEXT,
    created_at    INTEGER NOT NULL
) STRICT;

CREATE TABLE deliveries (
    consumer_id  INTEGER NOT NULL REFERENCES consumers(id) ON DELETE CASCADE,
    message_id   INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    visible_at   INTEGER NOT NULL,
    attempt      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (consumer_id, message_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX deliveries_visible
    ON deliveries(consumer_id, visible_at, message_id);
```

An ack deletes a delivery. A message is eligible for retention cleanup when it
is old enough and has no delivery referencing it. The idempotency ledger is
separate so cleanup does not accidentally make an old key reusable.

Publishing is one short transaction:

1. Claim the idempotency key, or return its existing message ID.
2. Insert the message.
3. `INSERT INTO deliveries ... SELECT` all matching consumers.
4. Commit, then signal local pollers.

`message_id` is briefly null while a transaction owns a newly inserted key.
The transaction fills it before commit; a rollback removes both rows. SQLite's
writer serialization means another publisher can only observe the completed
entry. Keeping this ledger independent of a foreign key lets message retention
delete bodies without weakening permanent deduplication.

SQLite serializes publish and consumer-creation transactions, so there is no
gap at their boundary. A `Now` consumer either commits after a publish and
misses it, or before the publish and receives it. A `Beginning` consumer
backfills retained messages in its creation transaction.

Claim up to `N` messages with an atomic update:

```sql
UPDATE deliveries
   SET visible_at = :lease_deadline,
       attempt = attempt + 1
 WHERE (consumer_id, message_id) IN (
     SELECT consumer_id, message_id
       FROM deliveries
      WHERE consumer_id = :consumer_id
        AND visible_at <= :now
      ORDER BY visible_at, message_id
      LIMIT :limit
 )
RETURNING message_id, attempt;
```

Fetch the bodies for the returned IDs on the same connection. The opaque
receipt contains `(consumer_id, message_id, attempt)`. Ack and nack include all
three values in their predicate. Nack sets a new `visible_at`; a zero delay is
an immediate retry.

Advantages:

- The state machine is small and easy to inspect with ordinary SQL.
- Claims, acks, nacks, backlog counts, and per-consumer metrics are naturally
  indexable.
- Filters are evaluated once at publish time.
- Consumers can claim concurrently without a read-then-write race.

Costs:

- Publishing `M` messages to `C` consumers writes `M * C` delivery rows.
- An inactive consumer materializes an arbitrarily large backlog.
- Adding a `Beginning` consumer backfills history in a potentially long write
  transaction. That operation may eventually need chunking and an explicit
  "initializing" state.

This is the best first implementation when the likely scale is tens or perhaps
hundreds of consumers, because the common operations remain obvious and the
performance cost is measurable rather than speculative.

## Shape B: append-only log plus sparse acknowledgements

Keep one message log and a high-water mark per consumer. Record out-of-order
acks in a sparse table until the contiguous acknowledged prefix can advance.

This makes publishing nearly constant-cost regardless of consumer count, but
individual acknowledgements complicate it substantially:

- Every ack above the consumer's contiguous watermark needs a tombstone.
- Advancing the watermark must account for ID gaps and filtered-out messages.
- A nack/lease table is still needed to prevent concurrent workers from
  claiming the same message.
- A heavily out-of-order consumer can retain a large sparse ack set.

This shape is attractive for a mostly ordered stream processor where a batch is
normally committed as one unit. It is a poor fit if arbitrary individual ack is
a primary API promise, so it should not be v1.

## Shape C: log with lazy delivery materialization

This hybrid stores `scan_cursor` on each consumer and creates delivery rows
only as polling reaches messages:

```sql
ALTER TABLE consumers ADD COLUMN scan_cursor INTEGER NOT NULL DEFAULT 0;
```

A claim transaction first retries visible delivery rows. If the batch still
has capacity, it scans matching messages after `scan_cursor`, inserts leased
delivery rows for them, and advances the cursor atomically. Ack deletes the
delivery row. It cannot be recreated because the durable cursor has passed it.

Garbage collection may delete a retained message when every consumer has
scanned past it and no live delivery references it. Exact-topic filters also
work: when a scan finds fewer than the requested batch size, it advances to the
message-log high-water mark, thereby durably skipping nonmatching messages.

Advantages:

- Publish cost is independent of consumer count.
- Inactive consumers occupy one row instead of one row per missed message.
- Ack/nack still use the simple materialized receipt model.

Costs:

- Polling is a multi-step write transaction rather than one update.
- Retention and filter-skipping rules need careful tests.
- A consumer's first poll after a long absence may scan old index ranges.
- Backlog metrics are estimates or more expensive counts.

This is the most promising v2 if materialized fan-out is measured to be the
bottleneck. It preserves the public API of Shape A, so starting with Shape A
does not paint the library into a corner.

## Rust API sketch

Keep batch operations as the primitive and build single-message and stream
conveniences on top:

```rust
let fan = LiteFan::open("events.db").await?;

let email = fan
    .consumer("send-email")
    .filter(Filter::Topic("user.created"))
    .start_at(StartAt::Now)
    .open()
    .await?;

let outcome = fan
    .publish(Publish {
        topic: Some("user.created"),
        body: br#"{"user_id": 42}"#,
        idempotency_key: Some(b"create-user:42"),
    })
    .await?;

let deliveries = email
    .poll(Poll {
        max_messages: 100,
        visibility_timeout: Duration::from_secs(30),
        wait: Duration::from_secs(20),
    })
    .await?;

email.ack_batch(deliveries.iter().map(Delivery::receipt)).await?;
// or: email.nack(delivery.receipt(), Retry::After(Duration::from_secs(5))).await?;
```

Suggested result types make idempotency and stale receipts visible rather than
surprising:

```rust
enum PublishOutcome {
    Published { id: MessageId },
    Duplicate { id: MessageId },
}

struct BatchResult {
    applied: usize,
    stale: usize,
}
```

`poll` is the performance API. `recv()` can be `poll(max_messages = 1)`, and a
`Stream<Item = Result<Delivery>>` can repeatedly poll. A delivery should own its
body (`Bytes` or `Vec<u8>`) and carry an opaque receipt; it should not hold a
database transaction open while user code handles it.

## Long polling and notification

SQLite has no cross-process notification primitive. Treat notification as a
latency optimization and the database as the source of truth.

Within one process, clones of a `LiteFan` handle share a Tokio `watch<u64>`
commit generation:

1. Mark the current generation as seen.
2. Try the indexed claim query.
3. If it is empty, wait for either a generation change, the next known
   `visible_at`, a cross-process poll tick, or the caller's deadline.
4. Retry the database query; never infer availability from the notification.

Marking the generation before querying avoids a lost wakeup: a commit either
precedes the query and is visible to it, or changes the generation that the
waiter observes. `watch` is preferable to a bare `Notify` because it preserves
a generation and works for multiple waiters. It may wake several competing
workers, but the atomic claim query gives work to only one of them. If that herd
shows up in profiles, add one consumer-local polling coordinator later.

After every committed publish or nack, increment the generation. A delayed nack
may establish an earlier wakeup time, so it signals too. Lease expiry needs no
signal, so an empty poll should also return the minimum future `visible_at` and
sleep until then.

Changes committed by another process cannot update the in-memory generation.
Use a configurable fallback poll interval with jitter (for example 100 ms by
default). `PRAGMA data_version` can detect another connection's commit but
cannot wait for one, so it does not remove polling. File-watching the WAL is not
a correctness mechanism and is not worth depending on.

## SQLite operating choices

- Enable WAL mode and a busy timeout on open.
- Use `synchronous = NORMAL` by default and document `FULL` for users who value
  power-loss durability above write latency.
- Keep write transactions short. Do not hold one while processing a message.
- Bound all public batch sizes and chunk internally below SQLite's variable
  limit.
- Use integer Unix milliseconds for visibility deadlines. Wall-clock changes
  can move a deadline; documenting this is simpler than inventing a durable
  monotonic clock.
- Use one small connection pool. More connections permit reader overlap but do
  not create more SQLite writers.
- Run retention and expired-idempotency cleanup incrementally in bounded
  batches, not as part of every publish.

## Recommended first experiment

Implement Shape A with only:

- one log, optional exact topic, and durable named consumers;
- atomic single/batch publish with permanent idempotency keys;
- `Now` consumer creation;
- claim with visibility timeout;
- receipt-checked single/batch ack and nack;
- in-process generation notification plus fallback polling.

Then benchmark publish throughput against consumer count (`1, 10, 100, 1_000`),
claim/ack batch sizes (`1, 10, 100, 1_000`), and a large inactive backlog. Those
measurements tell us whether Shape C's extra state machine is justified. Add
retention, replay, richer filters, and dead-letter policy only after the core
semantics feel right.
