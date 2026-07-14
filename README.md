# litefan

`litefan` is a durable, at-least-once fan-out message log backed by SQLite.
Each message is stored once, and every matching named consumer gets its own
delivery. Multiple workers using the same consumer name share that consumer's
work.

## Quick start

Create consumers before publishing messages they need to receive:

```rust
use std::time::Duration;
use litefan::{LiteFan, Poll, Publish, Retry};

async fn run() -> litefan::Result<()> {
    let fan = LiteFan::open("messages.db").await?;
    let email = fan.consumer("email-sender").open().await?;

    fan.publish(
        Publish::new(b"welcome@example.com")
            .with_idempotency_key(b"welcome:user-123"),
    ).await?;

    for delivery in email.poll(Poll::default()).await? {
        match send_email(&delivery.message.body).await {
            Ok(()) => {
                // Acknowledge only after the side effect succeeds.
                email.ack(delivery.receipt()).await?;
            }
            Err(_) => {
                email.nack(
                    delivery.receipt(),
                    Retry::After(Duration::from_secs(30)),
                ).await?;
            }
        }
    }

    Ok(())
}
```

`Poll::default()` returns up to 100 messages, waits up to 20 seconds when no
work is available, and leases each delivery for 30 seconds.

## Topics and fan-out

Use a unique, stable name for each logical subscriber. Give several worker
instances the same name when they should compete for one inbox.

```rust
use litefan::{Filter, Publish};

let billing = fan
    .consumer("billing-v1")
    .filter(Filter::topic("orders"))
    .open()
    .await?;

fan.publish(Publish::new(b"order-123").with_topic("orders"))
    .await?;
```

Topic filters are exact matches. A consumer's filter is durable and cannot be
changed by reopening the same name; use a new name for a new subscription.

## Archives and dead letters

Archiving is per consumer: it removes only that consumer's delivery while the
shared message remains available to every other matching consumer. Archive a
permanent failure directly, with optional diagnostic detail:

```rust
# use litefan::{ListArchives, Poll};
# async fn example(consumer: &litefan::Consumer, delivery: litefan::Delivery) -> litefan::Result<()> {
consumer
    .archive_with_detail(delivery.receipt(), "invalid payload")
    .await?;

for archived in consumer.archives(ListArchives::default()).await? {
    eprintln!("dead letter {:?}: {:?}", archived.id, archived.detail);
}
# Ok(())
# }
```

Use `redrive` to restore an archive to the same consumer. Its delivery count
resets so it begins a new attempt cycle. Redrive does not republish to other
consumers. Purge archives with `purge_archives`; their shared message bodies
become eligible for `prune_messages` once no other consumer needs them.

Retry limits are application policy, just like backoff. Check
`Delivery::delivery_count` and archive a permanent or exhausted failure instead
of nacking it again.

## Best practices

- **Open consumers first.** A new consumer starts after the current end of the
  log; it does not receive older messages.
- **Expect duplicates.** A lease can expire after processing but before `ack`.
  Make handlers idempotent, or publish with a stable idempotency key when
  duplicate publishing is the concern. Keys suppress repeats for 24 hours by
  default.
- **Set a realistic visibility timeout.** It should exceed normal processing
  time. Call `extend_visibility` before the lease expires for unusually long
  work.
- **Retry deliberately.** Use `nack` with a delay for transient failures. Do
  not acknowledge failed work; an unacknowledged delivery becomes available
  again after its visibility timeout.
- **Archive permanent failures.** Include diagnostic detail when it will help
  operators decide whether to purge or redrive the delivery.
- **Batch busy paths.** `publish_batch`, `ack_batch`, and `nack_batch` reduce
  SQLite commits. Keep batches at or below `Config::max_batch_size` (500 by
  default).
- **Treat receipts as single-attempt tokens.** A stale receipt safely returns
  `false`; it cannot acknowledge a newer delivery attempt.
- **Keep work outside database transactions.** Poll, process, then acknowledge.
  Never hold an application transaction open while doing slow network work.
- **Prune periodically.** Call `prune_messages` with an age cutoff and bounded
  batch size to remove old messages no consumer or archive still needs.

Dropping a worker handle is enough for a normal shutdown. Use
`begin_draining()` only when permanently retiring a consumer: draining is
irreversible, stops future fan-out, and leaves existing work available to
finish.

## Operational notes

The database file is the durability boundary. WAL mode and the default config
are suitable starting points for most applications. Use `snapshot()` for
health and backlog reporting, and avoid modifying `litefan_*` tables directly.
Message bodies are bytes; serialization and schema versioning belong to the
application.

See [DESIGN.md](DESIGN.md) for delivery invariants, retention behavior, and the
SQLite storage model.

Run the quality gate with:

```sh
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps
```
