//! A small, durable fan-out message log backed by SQLite.
//!
//! Each named consumer receives its own delivery of every matching message.
//! Delivery is at least once: polling leases messages for a visibility timeout,
//! and an expired lease may be delivered again. Receipts are generation-bound,
//! so stale workers cannot acknowledge a newer delivery.
//! Consumers can stop new fan-out and drain existing deliveries, while exact
//! snapshots and bounded cleanup expose the durable operational state. Failed
//! deliveries can be archived per consumer for inspection or redrive.
//!
//! # Quick start
//!
//! Create durable consumers before publishing the messages they should see:
//!
//! ```no_run
//! use std::time::Duration;
//! use litefan::{LiteFan, Poll, Publish};
//!
//! # async fn example() -> litefan::Result<()> {
//! let fan = LiteFan::open("messages.db").await?;
//! let worker = fan.consumer("email-worker").open().await?;
//!
//! fan.publish(Publish::new(b"welcome@example.com")).await?;
//! let deliveries = worker.poll(Poll {
//!     wait: Duration::ZERO,
//!     ..Poll::default()
//! }).await?;
//!
//! for delivery in deliveries {
//!     // Process before acknowledging. Failed or expired leases are retried.
//!     worker.ack(delivery.receipt()).await?;
//! }
//! # Ok(())
//! # }
//! ```

mod api;
mod consumer;
mod error;
mod schema;
mod signals;
mod storage;
mod store;
mod time;

pub use api::{
    ArchiveId, ArchivedDelivery, BatchResult, Config, ConsumerSnapshot, ConsumerState, DeleteMode,
    DeleteOutcome, Delivery, Filter, ListArchives, Message, MessageId, Poll, Prune, PruneOutcome,
    Publish, PublishOutcome, PurgeArchives, PurgeArchivesOutcome, Receipt, Retry, StoreSnapshot,
};
pub use consumer::{Consumer, ConsumerBuilder};
pub use error::{Error, Result};
pub use store::LiteFan;
