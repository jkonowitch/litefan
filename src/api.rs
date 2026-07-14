use std::time::Duration;

use sqlx::sqlite::SqliteSynchronous;

/// Connection, batching, durability, idempotency, and long-poll settings.
#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum number of SQLite connections. SQLite still permits one writer.
    pub max_connections: u32,
    /// Largest slice accepted by a batch operation.
    pub max_batch_size: usize,
    /// How long SQLite waits for a writer lock before returning `SQLITE_BUSY`.
    pub busy_timeout: Duration,
    /// WAL durability setting.
    pub synchronous: SqliteSynchronous,
    /// Ask SQLite to checkpoint after approximately this many WAL pages.
    pub wal_autocheckpoint_pages: u32,
    /// Poll cadence used to discover commits made by another process or handle.
    pub cross_process_poll_interval: Duration,
    /// How long a supplied idempotency key suppresses another publish.
    pub idempotency_window: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_connections: 4,
            max_batch_size: 500,
            busy_timeout: Duration::from_secs(5),
            synchronous: SqliteSynchronous::Normal,
            wal_autocheckpoint_pages: 1_000,
            cross_process_poll_interval: Duration::from_millis(250),
            idempotency_window: Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// An exact topic filter, or all published messages.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Filter {
    #[default]
    All,
    Topic(String),
}

impl Filter {
    pub fn topic(topic: impl Into<String>) -> Self {
        Self::Topic(topic.into())
    }

    pub(crate) fn as_database_value(&self) -> Option<&str> {
        match self {
            Self::All => None,
            Self::Topic(topic) => Some(topic),
        }
    }
}

/// A borrowed message to publish.
#[derive(Clone, Copy, Debug)]
pub struct Publish<'a> {
    pub topic: Option<&'a str>,
    pub body: &'a [u8],
    pub idempotency_key: Option<&'a [u8]>,
}

impl<'a> Publish<'a> {
    pub const fn new(body: &'a [u8]) -> Self {
        Self {
            topic: None,
            body,
            idempotency_key: None,
        }
    }

    pub const fn with_topic(mut self, topic: &'a str) -> Self {
        self.topic = Some(topic);
        self
    }

    pub const fn with_idempotency_key(mut self, key: &'a [u8]) -> Self {
        self.idempotency_key = Some(key);
        self
    }
}

/// The stable identifier assigned to a stored message.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MessageId(pub(crate) i64);

impl MessageId {
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Whether publishing inserted a message or found an existing idempotency key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishOutcome {
    Published { id: MessageId },
    Duplicate { id: MessageId },
}

impl PublishOutcome {
    pub const fn id(self) -> MessageId {
        match self {
            Self::Published { id } | Self::Duplicate { id } => id,
        }
    }

    pub const fn is_published(self) -> bool {
        matches!(self, Self::Published { .. })
    }
}

/// A stored message body and its immutable metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub id: MessageId,
    pub topic: Option<String>,
    pub body: Vec<u8>,
    pub published_at_ms: i64,
}

/// An opaque capability for acknowledging one particular lease generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Receipt {
    pub(crate) consumer_id: i64,
    pub(crate) message_id: i64,
    pub(crate) generation: i64,
}

/// A message leased to a consumer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivery {
    pub message: Message,
    /// Number of times this consumer has received the message.
    pub delivery_count: u64,
    pub(crate) receipt: Receipt,
}

impl Delivery {
    pub const fn receipt(&self) -> Receipt {
        self.receipt
    }
}

/// Options for one short or long poll.
#[derive(Clone, Copy, Debug)]
pub struct Poll {
    pub max_messages: usize,
    pub visibility_timeout: Duration,
    pub wait: Duration,
}

impl Default for Poll {
    fn default() -> Self {
        Self {
            max_messages: 100,
            visibility_timeout: Duration::from_secs(30),
            wait: Duration::from_secs(20),
        }
    }
}

/// When a nacked message should become eligible for another delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Retry {
    Immediately,
    After(Duration),
}

impl Retry {
    pub(crate) fn delay(self) -> Duration {
        match self {
            Self::Immediately => Duration::ZERO,
            Self::After(delay) => delay,
        }
    }
}

/// Result of a receipt mutation. Stale includes duplicate and wrong-consumer receipts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BatchResult {
    pub applied: usize,
    pub stale: usize,
}

/// Whether a consumer still accepts newly published messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerState {
    Active,
    Draining,
}

/// A point-in-time view of one durable consumer and its outstanding work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerSnapshot {
    pub name: String,
    pub filter: Filter,
    pub state: ConsumerState,
    pub created_at_ms: i64,
    pub draining_at_ms: Option<i64>,
    /// All unacknowledged deliveries, including leased and delayed deliveries.
    pub outstanding: u64,
    /// Deliveries whose visibility time has arrived.
    pub ready: u64,
    /// Earliest future visibility time among outstanding deliveries.
    pub next_ready_at_ms: Option<i64>,
    /// Publish time of the oldest outstanding message.
    pub oldest_outstanding_at_ms: Option<i64>,
}

impl ConsumerSnapshot {
    pub const fn is_empty(&self) -> bool {
        self.outstanding == 0
    }

    pub const fn is_drained(&self) -> bool {
        matches!(self.state, ConsumerState::Draining) && self.is_empty()
    }

    pub const fn not_ready(&self) -> u64 {
        self.outstanding.saturating_sub(self.ready)
    }
}

/// A point-in-time view of the durable state owned by litefan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreSnapshot {
    pub retained_messages: u64,
    /// Stored ledger rows, including expired rows awaiting the next keyed publish.
    pub idempotency_keys: u64,
    pub outstanding_deliveries: u64,
    pub consumers: Vec<ConsumerSnapshot>,
}

/// Safety policy for deleting a durable consumer identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteMode {
    /// Delete only after fan-out has stopped and every delivery is acknowledged.
    DrainedOnly,
    /// Delete immediately, discarding all outstanding deliveries.
    DiscardOutstanding,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteOutcome {
    pub discarded_deliveries: u64,
}

/// Bounded message-retention cleanup options.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Prune {
    /// Delete eligible messages published strictly before this Unix timestamp.
    pub before_ms: i64,
    /// Maximum messages to delete in one transaction.
    pub max_messages: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PruneOutcome {
    pub deleted_messages: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_builder_preserves_optional_metadata() {
        let publish = Publish::new(b"body")
            .with_topic("jobs")
            .with_idempotency_key(b"key");

        assert_eq!(publish.body, b"body");
        assert_eq!(publish.topic, Some("jobs"));
        assert_eq!(publish.idempotency_key, Some(b"key".as_slice()));
    }

    #[test]
    fn snapshot_helpers_describe_consumer_progress() {
        let mut snapshot = ConsumerSnapshot {
            name: "worker".into(),
            filter: Filter::All,
            state: ConsumerState::Active,
            created_at_ms: 0,
            draining_at_ms: None,
            outstanding: 2,
            ready: 1,
            next_ready_at_ms: Some(1),
            oldest_outstanding_at_ms: Some(0),
        };

        assert!(!snapshot.is_empty());
        assert!(!snapshot.is_drained());
        assert_eq!(snapshot.not_ready(), 1);

        snapshot.state = ConsumerState::Draining;
        snapshot.outstanding = 0;
        snapshot.ready = 1;
        assert!(snapshot.is_empty());
        assert!(snapshot.is_drained());
        assert_eq!(snapshot.not_ready(), 0);
    }
}
