# litefan

`litefan` is a small, durable, at-least-once fan-out message log backed by
SQLite. A message body is stored once, while every matching named consumer gets
an independent delivery. Workers using the same consumer name compete for that
consumer's work.

```rust
use std::time::Duration;
use litefan::{LiteFan, Poll, Publish};

async fn run() -> litefan::Result<()> {
    let fan = LiteFan::open("messages.db").await?;
    let worker = fan.consumer("email-worker").open().await?;

    fan.publish(Publish::new(b"welcome@example.com")).await?;
    for delivery in worker.poll(Poll {
        wait: Duration::ZERO,
        ..Poll::default()
    }).await? {
        // Acknowledge only after processing succeeds.
        worker.ack(delivery.receipt()).await?;
    }
    Ok(())
}
```

The important semantics are:

- Consumer names are durable identities. Reopening a name resumes its inbox.
- New consumers start after the current retained high-water mark, so create a
  consumer before publishing messages it should receive.
- Polling leases messages for a visibility timeout. Unacknowledged deliveries
  can be retried, and stale lease receipts cannot affect a newer attempt.
- Optional idempotency keys suppress duplicate publishes for a configured
  window.
- Draining permanently stops new fan-out while allowing existing work to
  finish.

See [DESIGN.md](DESIGN.md) for storage invariants, transaction boundaries,
long-poll behavior, retention rules, and the source layout.

Run the complete quality gate with:

```sh
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps
```
