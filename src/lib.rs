//! A small, durable fan-out message log backed by SQLite.
//!
//! Each named consumer receives its own delivery of every matching message.
//! Delivery is at least once: polling leases messages for a visibility timeout,
//! and an expired lease may be delivered again. Receipts are generation-bound,
//! so stale workers cannot acknowledge a newer delivery.
//! Consumers can stop new fan-out and drain existing deliveries, while exact
//! snapshots and bounded cleanup expose the durable operational state.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::Path,
    sync::{Arc, Mutex, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sqlx::{
    QueryBuilder, Row, Sqlite, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tokio::{sync::watch, time::Instant};

// Stay below SQLite's historical 999-variable limit. Receipt statements bind
// one consumer ID plus three values per receipt.
const MAX_SQL_VARIABLES: usize = 999;
const SCHEMA_VERSION: i64 = 1;
const RECEIPTS_PER_CHUNK: usize = (MAX_SQL_VARIABLES - 1) / 3;
const PUBLISHES_PER_CHUNK: usize = MAX_SQL_VARIABLES / 3;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS litefan_messages (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    topic        TEXT,
    body         BLOB NOT NULL,
    published_at INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS litefan_messages_topic
    ON litefan_messages(topic, id) WHERE topic IS NOT NULL;

CREATE TABLE IF NOT EXISTS litefan_idempotency (
    key        BLOB PRIMARY KEY,
    message_id INTEGER,
    expires_at INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS litefan_idempotency_expiry
    ON litefan_idempotency(expires_at);

CREATE TABLE IF NOT EXISTS litefan_consumers (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE,
    topic_filter TEXT,
    created_at   INTEGER NOT NULL,
    draining_at  INTEGER,
    scan_cursor  INTEGER NOT NULL CHECK (scan_cursor >= 0),
    drain_cursor INTEGER CHECK (drain_cursor >= scan_cursor)
) STRICT;

CREATE TABLE IF NOT EXISTS litefan_deliveries (
    consumer_id   INTEGER NOT NULL
        REFERENCES litefan_consumers(id) ON DELETE CASCADE,
    message_id    INTEGER NOT NULL
        REFERENCES litefan_messages(id) ON DELETE CASCADE,
    visible_at    INTEGER NOT NULL,
    generation    INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
    delivery_count INTEGER NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
    PRIMARY KEY (consumer_id, message_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS litefan_deliveries_visible
    ON litefan_deliveries(consumer_id, visible_at, message_id);

CREATE INDEX IF NOT EXISTS litefan_deliveries_message
    ON litefan_deliveries(message_id);
"#;

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

    fn as_database_value(&self) -> Option<&str> {
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
pub struct MessageId(i64);

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
    consumer_id: i64,
    message_id: i64,
    generation: i64,
}

/// A message leased to a consumer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivery {
    pub message: Message,
    /// Number of times this consumer has received the message.
    pub delivery_count: u64,
    receipt: Receipt,
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
    fn delay(self) -> Duration {
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

#[derive(Debug)]
pub enum Error {
    Sqlx(sqlx::Error),
    BatchTooLarge { requested: usize, maximum: usize },
    ConsumerConfigurationMismatch { name: String },
    EmptyConsumerName,
    InvalidConfig(&'static str),
    InvalidPoll(&'static str),
    InvalidVisibilityTimeout,
    IncompatibleSchema,
    UnsupportedSchemaVersion { found: i64, maximum: i64 },
    IncompleteIdempotencyEntry,
    ConsumerDeleted { name: String },
    ConsumerNotFound { name: String },
    ConsumerNotDraining { name: String },
    ConsumerNotEmpty { name: String, outstanding: u64 },
    ClockBeforeUnixEpoch,
    DurationOutOfRange,
    CounterOutOfRange,
    StorageInvariant(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlx(error) => write!(f, "SQLite error: {error}"),
            Self::BatchTooLarge { requested, maximum } => {
                write!(f, "batch contains {requested} items; maximum is {maximum}")
            }
            Self::ConsumerConfigurationMismatch { name } => {
                write!(
                    f,
                    "consumer {name:?} already exists with a different filter"
                )
            }
            Self::EmptyConsumerName => f.write_str("consumer name cannot be empty"),
            Self::InvalidConfig(message) => write!(f, "invalid configuration: {message}"),
            Self::InvalidPoll(message) => write!(f, "invalid poll: {message}"),
            Self::InvalidVisibilityTimeout => {
                f.write_str("visibility timeout must be at least one millisecond")
            }
            Self::IncompatibleSchema => {
                f.write_str("database contains an incompatible unversioned litefan schema")
            }
            Self::UnsupportedSchemaVersion { found, maximum } => write!(
                f,
                "database schema version {found} is newer than supported version {maximum}"
            ),
            Self::IncompleteIdempotencyEntry => {
                f.write_str("idempotency ledger contains an incomplete entry")
            }
            Self::ConsumerDeleted { name } => {
                write!(f, "consumer {name:?} has been deleted")
            }
            Self::ConsumerNotFound { name } => {
                write!(f, "consumer {name:?} does not exist")
            }
            Self::ConsumerNotDraining { name } => {
                write!(f, "consumer {name:?} is still active")
            }
            Self::ConsumerNotEmpty { name, outstanding } => {
                write!(
                    f,
                    "consumer {name:?} still has {outstanding} outstanding deliveries"
                )
            }
            Self::ClockBeforeUnixEpoch => f.write_str("system clock is before the Unix epoch"),
            Self::DurationOutOfRange => f.write_str("duration does not fit in SQLite milliseconds"),
            Self::CounterOutOfRange => f.write_str("SQLite counter does not fit in the Rust type"),
            Self::StorageInvariant(message) => {
                write!(f, "storage invariant violated: {message}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlx(error) => Some(error),
            _ => None,
        }
    }
}

impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        Self::Sqlx(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
struct Inner {
    pool: SqlitePool,
    max_batch_size: usize,
    cross_process_poll_interval: Duration,
    idempotency_window_ms: i64,
    signals: Signals,
}

#[derive(Debug)]
struct Signal {
    generation: watch::Sender<u64>,
}

impl Signal {
    fn new() -> Arc<Self> {
        let (generation, _) = watch::channel(0);
        Arc::new(Self { generation })
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }

    fn notify(&self) {
        self.generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

/// In-process notifications are only a latency hint; SQLite remains the source
/// of truth. Weak entries keep the registry bounded by live consumer handles.
#[derive(Debug)]
struct Signals {
    all_publishes: Arc<Signal>,
    topics: Mutex<HashMap<String, Weak<Signal>>>,
    consumers: Mutex<HashMap<i64, Weak<Signal>>>,
}

impl Signals {
    fn new() -> Self {
        Self {
            all_publishes: Signal::new(),
            topics: Mutex::new(HashMap::new()),
            consumers: Mutex::new(HashMap::new()),
        }
    }

    fn publishes_for(&self, filter: &Filter) -> Arc<Signal> {
        match filter {
            Filter::All => self.all_publishes.clone(),
            Filter::Topic(topic) => signal_for_key(&self.topics, topic.clone()),
        }
    }

    fn consumer(&self, id: i64) -> Arc<Signal> {
        signal_for_key(&self.consumers, id)
    }

    fn notify_publishes<'a>(&self, topics: impl IntoIterator<Item = Option<&'a str>>) {
        self.all_publishes.notify();

        let topics: HashSet<&str> = topics.into_iter().flatten().collect();
        let mut signals = self.topics.lock().unwrap();
        for topic in topics {
            if let Some(signal) = signals.get(topic).and_then(Weak::upgrade) {
                signal.notify();
            } else {
                signals.remove(topic);
            }
        }
    }

    fn notify_consumer(&self, id: i64) {
        let mut signals = self.consumers.lock().unwrap();
        if let Some(signal) = signals.get(&id).and_then(Weak::upgrade) {
            signal.notify();
        } else {
            signals.remove(&id);
        }
    }
}

fn signal_for_key<K>(signals: &Mutex<HashMap<K, Weak<Signal>>>, key: K) -> Arc<Signal>
where
    K: Eq + std::hash::Hash,
{
    let mut signals = signals.lock().unwrap();
    if let Some(signal) = signals.get(&key).and_then(Weak::upgrade) {
        return signal;
    }
    let signal = Signal::new();
    signals.insert(key, Arc::downgrade(&signal));
    signal
}

/// A cloneable handle to a SQLite fan-out database.
#[derive(Clone, Debug)]
pub struct LiteFan {
    inner: Arc<Inner>,
}

impl LiteFan {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(path, Config::default()).await
    }

    pub async fn open_with_config(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        validate_config(&config)?;
        let path = path.as_ref();
        let idempotency_window_ms =
            i64::try_from(config.idempotency_window.as_millis()).map_err(|_| {
                Error::InvalidConfig("idempotency_window must fit in SQLite milliseconds")
            })?;

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(config.synchronous)
            .busy_timeout(config.busy_timeout)
            .foreign_keys(true)
            .pragma(
                "wal_autocheckpoint",
                config.wal_autocheckpoint_pages.to_string(),
            );
        let pool = SqlitePoolOptions::new()
            // Separate `:memory:` connections are separate databases.
            .max_connections(if path == Path::new(":memory:") {
                1
            } else {
                config.max_connections
            })
            .connect_with(options)
            .await?;
        let schema_version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        if schema_version > SCHEMA_VERSION {
            return Err(Error::UnsupportedSchemaVersion {
                found: schema_version,
                maximum: SCHEMA_VERSION,
            });
        }
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        sqlx::query("SELECT scan_cursor, drain_cursor FROM litefan_consumers LIMIT 0")
            .execute(&pool)
            .await
            .map_err(|_| Error::IncompatibleSchema)?;
        if schema_version < SCHEMA_VERSION {
            sqlx::query("PRAGMA user_version = 1")
                .execute(&pool)
                .await?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                pool,
                max_batch_size: config.max_batch_size,
                cross_process_poll_interval: config.cross_process_poll_interval,
                idempotency_window_ms,
                signals: Signals::new(),
            }),
        })
    }

    /// Begin opening or creating a durable named consumer.
    pub fn consumer(&self, name: impl Into<String>) -> ConsumerBuilder {
        ConsumerBuilder {
            fan: self.clone(),
            name: name.into(),
            filter: Filter::All,
        }
    }

    /// Publish one message. An unexpired idempotency key is a no-op.
    pub async fn publish(&self, message: Publish<'_>) -> Result<PublishOutcome> {
        Ok(self.publish_batch(&[message]).await?.remove(0))
    }

    /// Atomically publish a batch with one WAL commit.
    pub async fn publish_batch(&self, messages: &[Publish<'_>]) -> Result<Vec<PublishOutcome>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_batch_size(messages.len())?;

        if messages
            .iter()
            .all(|message| message.idempotency_key.is_none())
        {
            return self.publish_unkeyed_batch(messages).await;
        }

        let published_at = now_ms()?;
        let expires_at = published_at
            .checked_add(self.inner.idempotency_window_ms)
            .ok_or(Error::DurationOutOfRange)?;
        let mut transaction = self.inner.pool.begin().await?;
        purge_expired_idempotency(&mut transaction, published_at).await?;
        let unique_keys: Vec<&[u8]> = messages
            .iter()
            .filter_map(|message| message.idempotency_key)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        for keys in unique_keys.chunks(MAX_SQL_VARIABLES / 2) {
            let mut query =
                QueryBuilder::<Sqlite>::new("INSERT INTO litefan_idempotency(key, expires_at) ");
            query.push_values(keys, |mut row, key| {
                row.push_bind(*key).push_bind(expires_at);
            });
            query.push(" ON CONFLICT DO NOTHING");
            query.build().execute(&mut *transaction).await?;
        }

        let mut ledger = HashMap::<Vec<u8>, Option<i64>>::with_capacity(unique_keys.len());
        for keys in unique_keys.chunks(MAX_SQL_VARIABLES) {
            let mut query = QueryBuilder::<Sqlite>::new(
                "SELECT key, message_id FROM litefan_idempotency WHERE key IN (",
            );
            let mut separated = query.separated(", ");
            for key in keys {
                separated.push_bind(*key);
            }
            separated.push_unseparated(")");
            for row in query.build().fetch_all(&mut *transaction).await? {
                ledger.insert(row.get("key"), row.get("message_id"));
            }
        }

        let mut scheduled_keys = HashSet::<Vec<u8>>::new();
        let mut publish_positions = Vec::with_capacity(messages.len());
        let mut to_publish = Vec::with_capacity(messages.len());
        for (index, message) in messages.iter().copied().enumerate() {
            let should_publish = match message.idempotency_key {
                None => true,
                Some(key) => {
                    ledger.get(key).is_some_and(Option::is_none)
                        && scheduled_keys.insert(key.to_vec())
                }
            };
            if should_publish {
                publish_positions.push(index);
                to_publish.push(message);
            }
        }

        let ids = insert_message_rows(&mut transaction, &to_publish, published_at).await?;
        let published_ids: HashMap<usize, i64> = publish_positions
            .iter()
            .copied()
            .zip(ids.iter().copied())
            .collect();
        let updates: Vec<(&[u8], i64)> = publish_positions
            .iter()
            .copied()
            .zip(ids.iter().copied())
            .filter_map(|(index, id)| messages[index].idempotency_key.map(|key| (key, id)))
            .collect();

        for updates in updates.chunks(MAX_SQL_VARIABLES / 2) {
            let mut query = QueryBuilder::<Sqlite>::new("WITH updates(key, message_id) AS (");
            query.push_values(updates, |mut row, (key, id)| {
                row.push_bind(*key).push_bind(*id);
            });
            query.push(
                ") UPDATE litefan_idempotency \
                 SET message_id = (SELECT updates.message_id FROM updates \
                                    WHERE updates.key = litefan_idempotency.key) \
                 WHERE key IN (SELECT key FROM updates)",
            );
            query.build().execute(&mut *transaction).await?;
        }
        for (key, id) in &updates {
            ledger.insert(key.to_vec(), Some(*id));
        }

        let outcomes: Vec<_> = messages
            .iter()
            .enumerate()
            .map(|(index, message)| {
                if let Some(id) = published_ids.get(&index) {
                    return Ok(PublishOutcome::Published { id: MessageId(*id) });
                }
                let id = message
                    .idempotency_key
                    .and_then(|key| ledger.get(key))
                    .copied()
                    .flatten()
                    .ok_or(Error::IncompleteIdempotencyEntry)?;
                Ok(PublishOutcome::Duplicate { id: MessageId(id) })
            })
            .collect::<Result<_>>()?;

        transaction.commit().await?;
        if !to_publish.is_empty() {
            self.inner
                .signals
                .notify_publishes(to_publish.iter().map(|message| message.topic));
        }
        Ok(outcomes)
    }

    async fn publish_unkeyed_batch(&self, messages: &[Publish<'_>]) -> Result<Vec<PublishOutcome>> {
        let published_at = now_ms()?;
        let mut transaction = self.inner.pool.begin().await?;
        let ids = insert_message_rows(&mut transaction, messages, published_at).await?;

        transaction.commit().await?;
        self.inner
            .signals
            .notify_publishes(messages.iter().map(|message| message.topic));
        Ok(ids
            .into_iter()
            .map(|id| PublishOutcome::Published { id: MessageId(id) })
            .collect())
    }

    /// Inspect all durable litefan state in one SQLite read snapshot.
    pub async fn snapshot(&self) -> Result<StoreSnapshot> {
        let now = now_ms()?;
        let mut transaction = self.inner.pool.begin().await?;
        let consumers = fetch_consumer_snapshots(&mut transaction, now, None).await?;
        let outstanding_deliveries = consumers.iter().try_fold(0_u64, |total, consumer| {
            total
                .checked_add(consumer.outstanding)
                .ok_or(Error::CounterOutOfRange)
        })?;
        let row = sqlx::query(
            "SELECT \
                 (SELECT COUNT(*) FROM litefan_messages) AS retained_messages, \
                 (SELECT COUNT(*) FROM litefan_idempotency) AS idempotency_keys",
        )
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(StoreSnapshot {
            retained_messages: count_from_row(&row, "retained_messages")?,
            idempotency_keys: count_from_row(&row, "idempotency_keys")?,
            outstanding_deliveries,
            consumers,
        })
    }

    /// Delete a durable consumer identity under an explicit safety policy.
    pub async fn delete_consumer(&self, name: &str, mode: DeleteMode) -> Result<DeleteOutcome> {
        let mut transaction = self.inner.pool.begin().await?;
        // Acquire SQLite's writer lock before inspecting the deletion guards,
        // so a concurrent publish cannot add a delivery between the count and
        // the delete.
        sqlx::query("UPDATE litefan_consumers SET draining_at = draining_at WHERE name = ?")
            .bind(name)
            .execute(&mut *transaction)
            .await?;
        let row = sqlx::query(
            "SELECT consumer.id, consumer.draining_at, \
                    (SELECT COUNT(*) FROM litefan_deliveries AS delivery \
                      WHERE delivery.consumer_id = consumer.id) + \
                    (SELECT COUNT(*) FROM litefan_messages AS message \
                      WHERE message.id > consumer.scan_cursor \
                        AND (consumer.drain_cursor IS NULL \
                             OR message.id <= consumer.drain_cursor) \
                        AND (consumer.topic_filter IS NULL \
                             OR message.topic = consumer.topic_filter)) AS outstanding \
             FROM litefan_consumers AS consumer WHERE consumer.name = ?",
        )
        .bind(name)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| Error::ConsumerNotFound {
            name: name.to_owned(),
        })?;
        let id: i64 = row.get("id");
        let draining_at: Option<i64> = row.get("draining_at");
        let outstanding = count_from_row(&row, "outstanding")?;

        if matches!(mode, DeleteMode::DrainedOnly) {
            if draining_at.is_none() {
                return Err(Error::ConsumerNotDraining {
                    name: name.to_owned(),
                });
            }
            if outstanding > 0 {
                return Err(Error::ConsumerNotEmpty {
                    name: name.to_owned(),
                    outstanding,
                });
            }
        }

        sqlx::query("DELETE FROM litefan_consumers WHERE id = ?")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        self.inner.signals.notify_consumer(id);
        Ok(DeleteOutcome {
            discarded_deliveries: outstanding,
        })
    }

    /// Delete a bounded number of old messages no consumer still references.
    pub async fn prune_messages(&self, prune: Prune) -> Result<PruneOutcome> {
        if prune.max_messages == 0 {
            return Ok(PruneOutcome::default());
        }
        self.ensure_batch_size(prune.max_messages)?;
        let limit = i64::try_from(prune.max_messages)
            .map_err(|_| Error::InvalidConfig("prune max_messages must fit in SQLite"))?;
        let result = sqlx::query(
            "DELETE FROM litefan_messages \
             WHERE id IN ( \
                 SELECT message.id FROM litefan_messages AS message \
                 WHERE message.published_at < ? \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM litefan_deliveries AS delivery \
                       WHERE delivery.message_id = message.id \
                   ) \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM litefan_consumers AS consumer \
                       WHERE message.id > consumer.scan_cursor \
                         AND (consumer.drain_cursor IS NULL \
                              OR message.id <= consumer.drain_cursor) \
                         AND (consumer.topic_filter IS NULL \
                              OR message.topic = consumer.topic_filter) \
                   ) \
                 ORDER BY message.id \
                 LIMIT ? \
             )",
        )
        .bind(prune.before_ms)
        .bind(limit)
        .execute(&self.inner.pool)
        .await?;
        Ok(PruneOutcome {
            deleted_messages: usize::try_from(result.rows_affected())
                .map_err(|_| Error::CounterOutOfRange)?,
        })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.inner.pool
    }

    pub fn max_batch_size(&self) -> usize {
        self.inner.max_batch_size
    }

    fn ensure_batch_size(&self, requested: usize) -> Result<()> {
        if requested > self.inner.max_batch_size {
            return Err(Error::BatchTooLarge {
                requested,
                maximum: self.inner.max_batch_size,
            });
        }
        Ok(())
    }
}

async fn insert_message_rows(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    messages: &[Publish<'_>],
    published_at: i64,
) -> Result<Vec<i64>> {
    let mut ids = Vec::with_capacity(messages.len());
    for messages in messages.chunks(PUBLISHES_PER_CHUNK) {
        let mut query =
            QueryBuilder::<Sqlite>::new("INSERT INTO litefan_messages(topic, body, published_at) ");
        query.push_values(messages, |mut row, message| {
            row.push_bind(message.topic)
                .push_bind(message.body)
                .push_bind(published_at);
        });
        query.push(" RETURNING id");
        ids.extend(
            query
                .build_query_scalar::<i64>()
                .fetch_all(&mut **transaction)
                .await?,
        );
    }
    // SQLite does not promise RETURNING order. With one writer and no message
    // triggers, row IDs are assigned in VALUES order.
    ids.sort_unstable();
    Ok(ids)
}

/// Builder for a durable named consumer. Creation starts at messages published after open.
#[derive(Clone, Debug)]
pub struct ConsumerBuilder {
    fan: LiteFan,
    name: String,
    filter: Filter,
}

impl ConsumerBuilder {
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = filter;
        self
    }

    pub async fn open(self) -> Result<Consumer> {
        if self.name.is_empty() {
            return Err(Error::EmptyConsumerName);
        }

        let now = now_ms()?;
        let mut transaction = self.fan.inner.pool.begin().await?;
        sqlx::query(
            "INSERT INTO litefan_consumers( \
                 name, topic_filter, created_at, scan_cursor \
             ) VALUES (?, ?, ?, (SELECT COALESCE(MAX(id), 0) FROM litefan_messages)) \
             ON CONFLICT(name) DO NOTHING",
        )
        .bind(&self.name)
        .bind(self.filter.as_database_value())
        .bind(now)
        .execute(&mut *transaction)
        .await?;

        let row = sqlx::query("SELECT id, topic_filter FROM litefan_consumers WHERE name = ?")
            .bind(&self.name)
            .fetch_one(&mut *transaction)
            .await?;
        let id: i64 = row.get("id");
        let stored_filter: Option<String> = row.get("topic_filter");
        if stored_filter.as_deref() != self.filter.as_database_value() {
            return Err(Error::ConsumerConfigurationMismatch { name: self.name });
        }
        transaction.commit().await?;

        Ok(Consumer {
            publish_signal: self.fan.inner.signals.publishes_for(&self.filter),
            consumer_signal: self.fan.inner.signals.consumer(id),
            fan: self.fan,
            id,
            name: Arc::from(self.name),
            filter: self.filter,
        })
    }
}

/// A cloneable handle to one durable consumer identity.
#[derive(Clone, Debug)]
pub struct Consumer {
    publish_signal: Arc<Signal>,
    consumer_signal: Arc<Signal>,
    fan: LiteFan,
    id: i64,
    name: Arc<str>,
    filter: Filter,
}

impl Consumer {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    /// Permanently stop future fan-out while preserving outstanding deliveries.
    ///
    /// Returns true when this call performed the active-to-draining transition.
    pub async fn begin_draining(&self) -> Result<bool> {
        let draining_at = now_ms()?;
        let changed = sqlx::query(
            "UPDATE litefan_consumers \
             SET draining_at = ?, \
                 drain_cursor = MAX( \
                     scan_cursor, \
                     (SELECT COALESCE(MAX(id), 0) FROM litefan_messages) \
                 ) \
             WHERE id = ? AND draining_at IS NULL",
        )
        .bind(draining_at)
        .bind(self.id)
        .execute(&self.fan.inner.pool)
        .await?
        .rows_affected()
            == 1;
        if changed {
            self.consumer_signal.notify();
            return Ok(true);
        }

        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM litefan_consumers WHERE id = ?)",
        )
        .bind(self.id)
        .fetch_one(&self.fan.inner.pool)
        .await?;
        if exists {
            Ok(false)
        } else {
            Err(self.deleted_error())
        }
    }

    /// Inspect this consumer's durable state and outstanding work.
    pub async fn snapshot(&self) -> Result<ConsumerSnapshot> {
        let now = now_ms()?;
        let mut transaction = self.fan.inner.pool.begin().await?;
        let snapshot = fetch_consumer_snapshots(&mut transaction, now, Some(self.id))
            .await?
            .pop()
            .ok_or_else(|| self.deleted_error())?;
        transaction.commit().await?;
        Ok(snapshot)
    }

    /// Claim immediately-visible messages, waiting up to `poll.wait` if empty.
    pub async fn poll(&self, poll: Poll) -> Result<Vec<Delivery>> {
        if poll.max_messages == 0 {
            return Ok(Vec::new());
        }
        self.fan.ensure_batch_size(poll.max_messages)?;
        let lease_ms = duration_ms(poll.visibility_timeout)?;
        if lease_ms == 0 {
            return Err(Error::InvalidVisibilityTimeout);
        }

        let deadline = Instant::now()
            .checked_add(poll.wait)
            .ok_or(Error::InvalidPoll("wait is too large"))?;
        let mut publishes = self.publish_signal.subscribe();
        let mut consumer_changes = self.consumer_signal.subscribe();

        loop {
            publishes.borrow_and_update();
            consumer_changes.borrow_and_update();
            let claim = self.claim(poll.max_messages, lease_ms).await?;
            if !claim.deliveries.is_empty() {
                return Ok(claim.deliveries);
            }
            if claim.draining && claim.next_visible_at.is_none() {
                return Ok(Vec::new());
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(Vec::new());
            }

            let mut sleep_for = remaining.min(self.fan.inner.cross_process_poll_interval);
            if let Some(next_visible_at) = claim.next_visible_at {
                let until_visible = duration_until_ms(next_visible_at)?;
                sleep_for = sleep_for.min(until_visible);
            }

            tokio::select! {
                changed = publishes.changed() => {
                    if changed.is_err() {
                        tokio::time::sleep(sleep_for).await;
                    }
                }
                changed = consumer_changes.changed() => {
                    if changed.is_err() {
                        tokio::time::sleep(sleep_for).await;
                    }
                }
                () = tokio::time::sleep(sleep_for) => {}
            }
        }
    }

    /// Acknowledge one receipt. False means it was stale or from another consumer.
    pub async fn ack(&self, receipt: Receipt) -> Result<bool> {
        Ok(self.ack_batch(&[receipt]).await?.applied == 1)
    }

    /// Atomically acknowledge receipts with one WAL commit.
    pub async fn ack_batch(&self, receipts: &[Receipt]) -> Result<BatchResult> {
        if receipts.is_empty() {
            return Ok(BatchResult::default());
        }
        self.fan.ensure_batch_size(receipts.len())?;

        let mut transaction = self.fan.inner.pool.begin().await?;
        let mut applied = 0;
        for receipts in receipts.chunks(RECEIPTS_PER_CHUNK) {
            let mut query =
                QueryBuilder::<Sqlite>::new("DELETE FROM litefan_deliveries WHERE consumer_id = ");
            query.push_bind(self.id);
            push_receipt_predicate(&mut query, receipts);
            applied += query
                .build()
                .execute(&mut *transaction)
                .await?
                .rows_affected() as usize;
        }
        transaction.commit().await?;
        Ok(BatchResult {
            applied,
            stale: receipts.len().saturating_sub(applied),
        })
    }

    /// Release one receipt for retry. False means it was stale or from another consumer.
    pub async fn nack(&self, receipt: Receipt, retry: Retry) -> Result<bool> {
        Ok(self.nack_batch(&[receipt], retry).await?.applied == 1)
    }

    /// Atomically release receipts after a shared delay with one WAL commit.
    pub async fn nack_batch(&self, receipts: &[Receipt], retry: Retry) -> Result<BatchResult> {
        if receipts.is_empty() {
            return Ok(BatchResult::default());
        }
        self.fan.ensure_batch_size(receipts.len())?;

        let visible_at = add_duration(now_ms()?, retry.delay())?;
        let mut transaction = self.fan.inner.pool.begin().await?;
        let mut applied = 0;
        for receipts in receipts.chunks(RECEIPTS_PER_CHUNK) {
            let mut query =
                QueryBuilder::<Sqlite>::new("UPDATE litefan_deliveries SET visible_at = ");
            query
                .push_bind(visible_at)
                .push(", generation = generation + 1 WHERE consumer_id = ")
                .push_bind(self.id);
            push_receipt_predicate(&mut query, receipts);
            applied += query
                .build()
                .execute(&mut *transaction)
                .await?
                .rows_affected() as usize;
        }
        transaction.commit().await?;
        if applied > 0 {
            self.consumer_signal.notify();
        }
        Ok(BatchResult {
            applied,
            stale: receipts.len().saturating_sub(applied),
        })
    }

    /// Extend one current delivery attempt's visibility deadline.
    pub async fn extend_visibility(
        &self,
        receipt: Receipt,
        visibility_timeout: Duration,
    ) -> Result<bool> {
        Ok(self
            .extend_visibility_batch(&[receipt], visibility_timeout)
            .await?
            .applied
            == 1)
    }

    /// Atomically extend current delivery attempts to one shared deadline.
    ///
    /// The receipt generation does not change, so successfully extended
    /// receipts remain valid for acknowledgement.
    pub async fn extend_visibility_batch(
        &self,
        receipts: &[Receipt],
        visibility_timeout: Duration,
    ) -> Result<BatchResult> {
        if receipts.is_empty() {
            return Ok(BatchResult::default());
        }
        self.fan.ensure_batch_size(receipts.len())?;
        if duration_ms(visibility_timeout)? == 0 {
            return Err(Error::InvalidVisibilityTimeout);
        }

        let visible_at = add_duration(now_ms()?, visibility_timeout)?;
        let mut transaction = self.fan.inner.pool.begin().await?;
        let mut applied = 0;
        for receipts in receipts.chunks(RECEIPTS_PER_CHUNK) {
            let mut query = QueryBuilder::<Sqlite>::new(
                "UPDATE litefan_deliveries SET visible_at = MAX(visible_at, ",
            );
            query
                .push_bind(visible_at)
                .push(") WHERE consumer_id = ")
                .push_bind(self.id);
            push_receipt_predicate(&mut query, receipts);
            applied += query
                .build()
                .execute(&mut *transaction)
                .await?
                .rows_affected() as usize;
        }
        transaction.commit().await?;
        Ok(BatchResult {
            applied,
            stale: receipts.len().saturating_sub(applied),
        })
    }

    async fn claim(&self, max_messages: usize, lease_ms: i64) -> Result<Claim> {
        let now = now_ms()?;
        // Avoid SQLite's single writer lock when this consumer has neither a
        // visible retry nor unseen log entries.
        let state = sqlx::query(
            "SELECT consumer.draining_at, consumer.scan_cursor, \
                    COALESCE(consumer.drain_cursor, \
                        (SELECT COALESCE(MAX(id), 0) FROM litefan_messages)) AS scan_limit, \
                    MIN(delivery.visible_at) AS earliest_visible_at \
             FROM litefan_consumers AS consumer \
             LEFT JOIN litefan_deliveries AS delivery ON delivery.consumer_id = consumer.id \
             WHERE consumer.id = ? \
             GROUP BY consumer.id",
        )
        .bind(self.id)
        .fetch_optional(&self.fan.inner.pool)
        .await?
        .ok_or_else(|| self.deleted_error())?;
        let draining = state.get::<Option<i64>, _>("draining_at").is_some();
        let scan_cursor: i64 = state.get("scan_cursor");
        let scan_limit: i64 = state.get("scan_limit");
        let earliest_visible_at: Option<i64> = state.get("earliest_visible_at");
        let retry_ready = earliest_visible_at.is_some_and(|visible_at| visible_at <= now);
        if !retry_ready && scan_cursor >= scan_limit {
            return Ok(Claim {
                deliveries: Vec::new(),
                next_visible_at: earliest_visible_at,
                draining,
            });
        }

        let lease_deadline = now.checked_add(lease_ms).ok_or(Error::DurationOutOfRange)?;
        let mut transaction = self.fan.inner.pool.begin().await?;
        // A no-op update takes the writer lock before we re-read the cursor.
        // Competing workers are consequently serialized at the one point that
        // allocates log entries to a consumer.
        let consumer = sqlx::query(
            "UPDATE litefan_consumers SET scan_cursor = scan_cursor \
             WHERE id = ? \
             RETURNING topic_filter, scan_cursor, draining_at, drain_cursor",
        )
        .bind(self.id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| self.deleted_error())?;
        let topic: Option<String> = consumer.get("topic_filter");
        let mut scan_cursor: i64 = consumer.get("scan_cursor");
        let draining = consumer.get::<Option<i64>, _>("draining_at").is_some();
        let scan_limit = match consumer.get::<Option<i64>, _>("drain_cursor") {
            Some(limit) => limit,
            None => {
                sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM litefan_messages")
                    .fetch_one(&mut *transaction)
                    .await?
            }
        };

        let rows = sqlx::query(
            r#"
            UPDATE litefan_deliveries
               SET visible_at = ?,
                   generation = generation + 1,
                   delivery_count = delivery_count + 1
             WHERE (consumer_id, message_id) IN (
                 SELECT consumer_id, message_id
                   FROM litefan_deliveries
                  WHERE consumer_id = ? AND visible_at <= ?
                  ORDER BY visible_at, message_id
                  LIMIT ?
             )
            RETURNING message_id, generation, delivery_count
            "#,
        )
        .bind(lease_deadline)
        .bind(self.id)
        .bind(now)
        .bind(
            i64::try_from(max_messages)
                .map_err(|_| Error::InvalidPoll("max_messages is too large"))?,
        )
        .fetch_all(&mut *transaction)
        .await?;

        let mut leases = Vec::with_capacity(rows.len());
        for row in rows {
            leases.push(Lease {
                message_id: row.get("message_id"),
                generation: row.get("generation"),
                delivery_count: row.get("delivery_count"),
            });
        }

        let remaining = max_messages.saturating_sub(leases.len());
        if remaining > 0 && scan_cursor < scan_limit {
            let mut query = QueryBuilder::<Sqlite>::new(
                "INSERT INTO litefan_deliveries( \
                     consumer_id, message_id, visible_at, generation, delivery_count \
                 ) SELECT ",
            );
            query
                .push_bind(self.id)
                .push(", id, ")
                .push_bind(lease_deadline)
                .push(", 1, 1 FROM litefan_messages WHERE id > ")
                .push_bind(scan_cursor)
                .push(" AND id <= ")
                .push_bind(scan_limit);
            if let Some(topic) = topic.as_deref() {
                query.push(" AND topic = ").push_bind(topic);
            }
            query
                .push(" ORDER BY id LIMIT ")
                .push_bind(
                    i64::try_from(remaining)
                        .map_err(|_| Error::InvalidPoll("max_messages is too large"))?,
                )
                .push(" RETURNING message_id");
            let mut new_ids = query
                .build_query_scalar::<i64>()
                .fetch_all(&mut *transaction)
                .await?;
            new_ids.sort_unstable();

            let found = new_ids.len();
            if found < remaining {
                scan_cursor = scan_limit;
            } else if let Some(id) = new_ids.last() {
                scan_cursor = *id;
            }

            if !new_ids.is_empty() {
                leases.extend(new_ids.into_iter().map(|message_id| Lease {
                    message_id,
                    generation: 1,
                    delivery_count: 1,
                }));
            }

            sqlx::query("UPDATE litefan_consumers SET scan_cursor = ? WHERE id = ?")
                .bind(scan_cursor)
                .bind(self.id)
                .execute(&mut *transaction)
                .await?;
        }

        leases.sort_unstable_by_key(|lease| lease.message_id);

        let messages = fetch_messages(&mut transaction, &leases).await?;
        transaction.commit().await?;

        if messages.len() != leases.len()
            || leases
                .iter()
                .zip(&messages)
                .any(|(lease, message)| lease.message_id != message.id.get())
        {
            return Err(Error::StorageInvariant(
                "a claimed delivery has no matching message",
            ));
        }

        let mut deliveries = Vec::with_capacity(leases.len());
        for (lease, message) in leases.into_iter().zip(messages) {
            deliveries.push(Delivery {
                message,
                delivery_count: u64::try_from(lease.delivery_count)
                    .map_err(|_| Error::CounterOutOfRange)?,
                receipt: Receipt {
                    consumer_id: self.id,
                    message_id: lease.message_id,
                    generation: lease.generation,
                },
            });
        }
        let (next_visible_at, draining) = if deliveries.is_empty() {
            let state = sqlx::query(
                "SELECT consumer.draining_at, consumer.scan_cursor, \
                        COALESCE(consumer.drain_cursor, \
                            (SELECT COALESCE(MAX(id), 0) FROM litefan_messages)) AS scan_limit, \
                        MIN(delivery.visible_at) AS next_visible_at \
                 FROM litefan_consumers AS consumer \
                 LEFT JOIN litefan_deliveries AS delivery \
                    ON delivery.consumer_id = consumer.id \
                 WHERE consumer.id = ? \
                 GROUP BY consumer.id",
            )
            .bind(self.id)
            .fetch_optional(&self.fan.inner.pool)
            .await?
            .ok_or_else(|| self.deleted_error())?;
            let cursor: i64 = state.get("scan_cursor");
            let limit: i64 = state.get("scan_limit");
            let next_visible_at = if cursor < limit {
                Some(now)
            } else {
                state.get("next_visible_at")
            };
            (
                next_visible_at,
                state.get::<Option<i64>, _>("draining_at").is_some(),
            )
        } else {
            (None, draining)
        };
        Ok(Claim {
            deliveries,
            next_visible_at,
            draining,
        })
    }

    fn deleted_error(&self) -> Error {
        Error::ConsumerDeleted {
            name: self.name.to_string(),
        }
    }
}

#[derive(Debug)]
struct Lease {
    message_id: i64,
    generation: i64,
    delivery_count: i64,
}

#[derive(Debug)]
struct Claim {
    deliveries: Vec<Delivery>,
    next_visible_at: Option<i64>,
    draining: bool,
}

async fn fetch_messages(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    leases: &[Lease],
) -> Result<Vec<Message>> {
    if leases.is_empty() {
        return Ok(Vec::new());
    }

    let mut messages = Vec::with_capacity(leases.len());
    for leases in leases.chunks(MAX_SQL_VARIABLES) {
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT id, topic, body, published_at FROM litefan_messages WHERE id IN (",
        );
        let mut separated = query.separated(", ");
        for lease in leases {
            separated.push_bind(lease.message_id);
        }
        separated.push_unseparated(") ORDER BY id");
        messages.extend(
            query
                .build()
                .fetch_all(&mut **transaction)
                .await?
                .into_iter()
                .map(|row| Message {
                    id: MessageId(row.get("id")),
                    topic: row.get("topic"),
                    body: row.get("body"),
                    published_at_ms: row.get("published_at"),
                }),
        );
    }
    messages.sort_unstable_by_key(|message| message.id);
    Ok(messages)
}

async fn fetch_consumer_snapshots(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    now: i64,
    consumer_id: Option<i64>,
) -> Result<Vec<ConsumerSnapshot>> {
    let mut query = QueryBuilder::<Sqlite>::new(
        "WITH high_water(id) AS ( \
             SELECT COALESCE(MAX(id), 0) FROM litefan_messages \
         ) \
         SELECT consumer.name, consumer.topic_filter, consumer.created_at, \
                consumer.draining_at, \
                (SELECT COUNT(*) FROM litefan_deliveries AS delivery \
                  WHERE delivery.consumer_id = consumer.id) + \
                (SELECT COUNT(*) FROM litefan_messages AS message, high_water \
                  WHERE message.id > consumer.scan_cursor \
                    AND message.id <= COALESCE(consumer.drain_cursor, high_water.id) \
                    AND (consumer.topic_filter IS NULL \
                         OR message.topic = consumer.topic_filter)) AS outstanding, \
                (SELECT COUNT(*) FROM litefan_deliveries AS delivery \
                  WHERE delivery.consumer_id = consumer.id \
                    AND delivery.visible_at <= ",
    );
    query
        .push_bind(now)
        .push(
            ") + \
                (SELECT COUNT(*) FROM litefan_messages AS message, high_water \
                  WHERE message.id > consumer.scan_cursor \
                    AND message.id <= COALESCE(consumer.drain_cursor, high_water.id) \
                    AND (consumer.topic_filter IS NULL \
                         OR message.topic = consumer.topic_filter)) AS ready, \
                (SELECT MIN(delivery.visible_at) \
                   FROM litefan_deliveries AS delivery \
                  WHERE delivery.consumer_id = consumer.id \
                    AND delivery.visible_at > ",
        )
        .push_bind(now)
        .push(
            ") AS next_ready_at, \
                (SELECT MIN(message.published_at) \
                   FROM litefan_messages AS message, high_water \
                  WHERE EXISTS ( \
                            SELECT 1 FROM litefan_deliveries AS delivery \
                             WHERE delivery.consumer_id = consumer.id \
                               AND delivery.message_id = message.id \
                        ) \
                     OR (message.id > consumer.scan_cursor \
                         AND message.id <= COALESCE(consumer.drain_cursor, high_water.id) \
                         AND (consumer.topic_filter IS NULL \
                              OR message.topic = consumer.topic_filter))) \
                    AS oldest_outstanding_at \
           FROM litefan_consumers AS consumer",
        );
    if let Some(consumer_id) = consumer_id {
        query.push(" WHERE consumer.id = ").push_bind(consumer_id);
    }
    query.push(" ORDER BY consumer.name");

    let rows = query.build().fetch_all(&mut **transaction).await?;
    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        let topic_filter: Option<String> = row.get("topic_filter");
        let draining_at_ms: Option<i64> = row.get("draining_at");
        snapshots.push(ConsumerSnapshot {
            name: row.get("name"),
            filter: match topic_filter {
                Some(topic) => Filter::Topic(topic),
                None => Filter::All,
            },
            state: if draining_at_ms.is_some() {
                ConsumerState::Draining
            } else {
                ConsumerState::Active
            },
            created_at_ms: row.get("created_at"),
            draining_at_ms,
            outstanding: count_from_row(&row, "outstanding")?,
            ready: count_from_row(&row, "ready")?,
            next_ready_at_ms: row.get("next_ready_at"),
            oldest_outstanding_at_ms: row.get("oldest_outstanding_at"),
        });
    }
    Ok(snapshots)
}

async fn purge_expired_idempotency(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    now: i64,
) -> Result<()> {
    sqlx::query("DELETE FROM litefan_idempotency WHERE expires_at <= ?")
        .bind(now)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

fn count_from_row(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<u64> {
    let value: i64 = row.try_get(column)?;
    u64::try_from(value).map_err(|_| Error::CounterOutOfRange)
}

fn push_receipt_predicate(query: &mut QueryBuilder<'_, Sqlite>, receipts: &[Receipt]) {
    query.push(" AND (consumer_id, message_id, generation) IN (");
    for (index, receipt) in receipts.iter().enumerate() {
        if index > 0 {
            query.push(", ");
        }
        query
            .push("(")
            .push_bind(receipt.consumer_id)
            .push(", ")
            .push_bind(receipt.message_id)
            .push(", ")
            .push_bind(receipt.generation)
            .push(")");
    }
    query.push(")");
}

fn validate_config(config: &Config) -> Result<()> {
    if config.max_connections == 0 {
        return Err(Error::InvalidConfig(
            "max_connections must be greater than zero",
        ));
    }
    if config.max_batch_size == 0 {
        return Err(Error::InvalidConfig(
            "max_batch_size must be greater than zero",
        ));
    }
    if i64::try_from(config.max_batch_size).is_err() {
        return Err(Error::InvalidConfig("max_batch_size must fit in SQLite"));
    }
    if config.cross_process_poll_interval.is_zero() {
        return Err(Error::InvalidConfig(
            "cross_process_poll_interval must be greater than zero",
        ));
    }
    if config.idempotency_window.as_millis() == 0 {
        return Err(Error::InvalidConfig(
            "idempotency_window must be at least one millisecond",
        ));
    }
    if i64::try_from(config.idempotency_window.as_millis()).is_err() {
        return Err(Error::InvalidConfig(
            "idempotency_window must fit in SQLite milliseconds",
        ));
    }
    Ok(())
}

fn now_ms() -> Result<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::ClockBeforeUnixEpoch)?
        .as_millis();
    i64::try_from(millis).map_err(|_| Error::DurationOutOfRange)
}

fn duration_ms(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_millis()).map_err(|_| Error::DurationOutOfRange)
}

fn add_duration(timestamp_ms: i64, duration: Duration) -> Result<i64> {
    timestamp_ms
        .checked_add(duration_ms(duration)?)
        .ok_or(Error::DurationOutOfRange)
}

fn duration_until_ms(timestamp_ms: i64) -> Result<Duration> {
    let remaining = timestamp_ms.saturating_sub(now_ms()?);
    Ok(Duration::from_millis(remaining as u64))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tempfile::TempDir;

    use super::*;

    async fn database() -> (TempDir, LiteFan) {
        let directory = tempfile::tempdir().unwrap();
        let fan = LiteFan::open(directory.path().join("fan.db"))
            .await
            .unwrap();
        (directory, fan)
    }

    fn immediate_poll(max_messages: usize) -> Poll {
        Poll {
            max_messages,
            visibility_timeout: Duration::from_secs(30),
            wait: Duration::ZERO,
        }
    }

    #[tokio::test]
    async fn fans_out_to_durable_consumers() {
        let (_directory, fan) = database().await;
        let first = fan.consumer("first").open().await.unwrap();
        let second = fan.consumer("second").open().await.unwrap();

        let published = fan.publish(Publish::new(b"hello")).await.unwrap();
        let first_delivery = first.poll(immediate_poll(1)).await.unwrap().remove(0);
        let second_delivery = second.poll(immediate_poll(1)).await.unwrap().remove(0);

        assert_eq!(first_delivery.message.id, published.id());
        assert_eq!(second_delivery.message.id, published.id());
        assert_eq!(first_delivery.message.body, b"hello");
        assert!(first.ack(first_delivery.receipt()).await.unwrap());
        assert!(second.ack(second_delivery.receipt()).await.unwrap());
    }

    #[tokio::test]
    async fn a_receipt_cannot_mutate_another_consumers_copy() {
        let (_directory, fan) = database().await;
        let first = fan.consumer("first").open().await.unwrap();
        let second = fan.consumer("second").open().await.unwrap();
        fan.publish(Publish::new(b"hello")).await.unwrap();

        let first_delivery = first.poll(immediate_poll(1)).await.unwrap().remove(0);
        assert!(!second.ack(first_delivery.receipt()).await.unwrap());
        assert_eq!(
            second.poll(immediate_poll(1)).await.unwrap()[0]
                .message
                .body,
            b"hello"
        );
    }

    #[tokio::test]
    async fn consumers_start_at_now_and_filter_exact_topics() {
        let (_directory, fan) = database().await;
        fan.publish(Publish::new(b"before")).await.unwrap();
        let all = fan.consumer("all").open().await.unwrap();
        let email = fan
            .consumer("email")
            .filter(Filter::topic("email"))
            .open()
            .await
            .unwrap();

        fan.publish_batch(&[
            Publish {
                topic: Some("email"),
                body: b"matching",
                idempotency_key: None,
            },
            Publish {
                topic: Some("audit"),
                body: b"other",
                idempotency_key: None,
            },
        ])
        .await
        .unwrap();

        let all_bodies: Vec<_> = all
            .poll(immediate_poll(10))
            .await
            .unwrap()
            .into_iter()
            .map(|delivery| delivery.message.body)
            .collect();
        let email_bodies: Vec<_> = email
            .poll(immediate_poll(10))
            .await
            .unwrap()
            .into_iter()
            .map(|delivery| delivery.message.body)
            .collect();
        assert_eq!(all_bodies, [b"matching".to_vec(), b"other".to_vec()]);
        assert_eq!(email_bodies, [b"matching".to_vec()]);
    }

    #[tokio::test]
    async fn idempotency_within_the_window_and_batch_order_are_preserved() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        let keyed = |body: &'static [u8], key: &'static [u8]| Publish {
            topic: None,
            body,
            idempotency_key: Some(key),
        };

        let outcomes = fan
            .publish_batch(&[
                keyed(b"first", b"key-1"),
                keyed(b"ignored", b"key-1"),
                keyed(b"second", b"key-2"),
            ])
            .await
            .unwrap();
        assert!(outcomes[0].is_published());
        assert_eq!(
            outcomes[1],
            PublishOutcome::Duplicate {
                id: outcomes[0].id()
            }
        );
        assert!(outcomes[2].is_published());

        let deliveries = consumer.poll(immediate_poll(10)).await.unwrap();
        assert_eq!(deliveries.len(), 2);
        assert_eq!(deliveries[0].message.body, b"first");
        assert_eq!(deliveries[1].message.body, b"second");
    }

    #[tokio::test]
    async fn idempotency_window_is_fixed_and_expired_entries_are_cleaned_up() {
        let (_directory, fan) = database().await;
        let keyed = |body: &'static [u8], key: &'static [u8]| Publish {
            topic: None,
            body,
            idempotency_key: Some(key),
        };

        let first = fan.publish(keyed(b"first", b"key-1")).await.unwrap();
        fan.publish(keyed(b"other", b"key-2")).await.unwrap();
        let original_expiry: i64 =
            sqlx::query_scalar("SELECT expires_at FROM litefan_idempotency WHERE key = ?")
                .bind(b"key-1".as_slice())
                .fetch_one(fan.pool())
                .await
                .unwrap();

        let duplicate = fan.publish(keyed(b"ignored", b"key-1")).await.unwrap();
        let duplicate_expiry: i64 =
            sqlx::query_scalar("SELECT expires_at FROM litefan_idempotency WHERE key = ?")
                .bind(b"key-1".as_slice())
                .fetch_one(fan.pool())
                .await
                .unwrap();
        assert_eq!(duplicate, PublishOutcome::Duplicate { id: first.id() });
        assert_eq!(duplicate_expiry, original_expiry);

        sqlx::query("UPDATE litefan_idempotency SET expires_at = 0")
            .execute(fan.pool())
            .await
            .unwrap();
        let after_expiry = fan.publish(keyed(b"new", b"key-3")).await.unwrap();
        assert!(after_expiry.is_published());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM litefan_idempotency")
                .fetch_one(fan.pool())
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn an_expired_key_can_publish_again() {
        let (_directory, fan) = database().await;
        let publish = |body: &'static [u8]| Publish {
            topic: None,
            body,
            idempotency_key: Some(b"same-key"),
        };
        let first = fan.publish(publish(b"first")).await.unwrap();
        sqlx::query("UPDATE litefan_idempotency SET expires_at = 0")
            .execute(fan.pool())
            .await
            .unwrap();

        let second = fan.publish(publish(b"second")).await.unwrap();
        assert!(second.is_published());
        assert_ne!(second.id(), first.id());
    }

    #[tokio::test]
    async fn ack_and_nack_batches_reject_stale_receipts() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        fan.publish_batch(&[Publish::new(b"a"), Publish::new(b"b")])
            .await
            .unwrap();
        let deliveries = consumer.poll(immediate_poll(10)).await.unwrap();
        let first_receipt = deliveries[0].receipt();
        let second_receipt = deliveries[1].receipt();

        assert!(
            consumer
                .nack(first_receipt, Retry::Immediately)
                .await
                .unwrap()
        );
        assert!(!consumer.ack(first_receipt).await.unwrap());
        let retried = consumer.poll(immediate_poll(1)).await.unwrap().remove(0);
        assert_eq!(retried.message.body, b"a");
        assert_eq!(retried.delivery_count, 2);

        let result = consumer
            .ack_batch(&[retried.receipt(), second_receipt, second_receipt])
            .await
            .unwrap();
        assert_eq!(
            result,
            BatchResult {
                applied: 2,
                stale: 1
            }
        );
        assert!(consumer.poll(immediate_poll(10)).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn expired_leases_are_redelivered_and_old_receipts_are_stale() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        fan.publish(Publish::new(b"work")).await.unwrap();

        let short_poll = Poll {
            max_messages: 1,
            visibility_timeout: Duration::from_millis(10),
            wait: Duration::ZERO,
        };
        let first = consumer.poll(short_poll).await.unwrap().remove(0);
        tokio::time::sleep(Duration::from_millis(15)).await;
        let second = consumer.poll(short_poll).await.unwrap().remove(0);

        assert_eq!(second.delivery_count, 2);
        assert!(!consumer.ack(first.receipt()).await.unwrap());
        assert!(consumer.ack(second.receipt()).await.unwrap());
    }

    #[tokio::test]
    async fn long_poll_wakes_after_publish() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        let waiting_consumer = consumer.clone();
        let waiter = tokio::spawn(async move {
            waiting_consumer
                .poll(Poll {
                    max_messages: 1,
                    visibility_timeout: Duration::from_secs(30),
                    wait: Duration::from_secs(2),
                })
                .await
                .unwrap()
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        fan.publish(Publish::new(b"wake up")).await.unwrap();
        let deliveries = waiter.await.unwrap();
        assert_eq!(deliveries[0].message.body, b"wake up");
    }

    #[tokio::test]
    async fn independent_handles_discover_publishes_by_fallback_polling() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fan.db");
        let receiver = LiteFan::open(&path).await.unwrap();
        let consumer = receiver.consumer("worker").open().await.unwrap();
        let publisher = LiteFan::open(&path).await.unwrap();

        let waiter = tokio::spawn(async move {
            consumer
                .poll(Poll {
                    max_messages: 1,
                    visibility_timeout: Duration::from_secs(30),
                    wait: Duration::from_secs(2),
                })
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        publisher.publish(Publish::new(b"external")).await.unwrap();

        let deliveries = waiter.await.unwrap();
        assert_eq!(deliveries[0].message.body, b"external");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_idempotent_publishers_create_one_message() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        let first_fan = fan.clone();
        let second_fan = fan.clone();
        let first = tokio::spawn(async move {
            first_fan
                .publish(Publish {
                    topic: None,
                    body: b"first",
                    idempotency_key: Some(b"same-key"),
                })
                .await
                .unwrap()
        });
        let second = tokio::spawn(async move {
            second_fan
                .publish(Publish {
                    topic: None,
                    body: b"second",
                    idempotency_key: Some(b"same-key"),
                })
                .await
                .unwrap()
        });

        let (first, second) = (first.await.unwrap(), second.await.unwrap());
        assert_eq!(first.id(), second.id());
        assert_ne!(first.is_published(), second.is_published());
        assert_eq!(consumer.poll(immediate_poll(10)).await.unwrap().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn competing_workers_do_not_claim_the_same_delivery() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        let messages: Vec<Vec<u8>> = (0_u32..100).map(u32::to_be_bytes).map(Vec::from).collect();
        let publishes: Vec<_> = messages.iter().map(|body| Publish::new(body)).collect();
        fan.publish_batch(&publishes).await.unwrap();

        let mut tasks = Vec::new();
        for _ in 0..4 {
            let consumer = consumer.clone();
            tasks.push(tokio::spawn(async move {
                consumer.poll(immediate_poll(100)).await.unwrap()
            }));
        }

        let mut ids = HashSet::new();
        for task in tasks {
            for delivery in task.await.unwrap() {
                assert!(ids.insert(delivery.message.id));
            }
        }
        assert_eq!(ids.len(), 100);
    }

    #[tokio::test]
    async fn reopening_a_consumer_validates_its_filter() {
        let (_directory, fan) = database().await;
        fan.consumer("worker")
            .filter(Filter::topic("one"))
            .open()
            .await
            .unwrap();

        let error = fan
            .consumer("worker")
            .filter(Filter::topic("two"))
            .open()
            .await
            .unwrap_err();
        assert!(matches!(error, Error::ConsumerConfigurationMismatch { .. }));
    }

    #[tokio::test]
    async fn draining_stops_new_fanout_and_finishes_existing_work() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        fan.publish(Publish::new(b"before")).await.unwrap();

        assert!(consumer.begin_draining().await.unwrap());
        assert!(!consumer.begin_draining().await.unwrap());
        fan.publish(Publish::new(b"after-unkeyed")).await.unwrap();
        fan.publish(Publish::new(b"after-keyed").with_idempotency_key(b"key"))
            .await
            .unwrap();

        let deliveries = consumer.poll(immediate_poll(10)).await.unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].message.body, b"before");
        let draining = consumer.snapshot().await.unwrap();
        assert_eq!(draining.state, ConsumerState::Draining);
        assert_eq!(draining.outstanding, 1);
        assert!(!draining.is_drained());

        assert!(consumer.ack(deliveries[0].receipt()).await.unwrap());
        assert!(consumer.snapshot().await.unwrap().is_drained());
        let empty = tokio::time::timeout(
            Duration::from_millis(50),
            consumer.poll(Poll {
                wait: Duration::from_secs(1),
                ..Poll::default()
            }),
        )
        .await
        .expect("a drained consumer should not long-poll")
        .unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn draining_long_poll_waits_for_delayed_outstanding_work() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        fan.publish(Publish::new(b"retry")).await.unwrap();
        let delivery = consumer.poll(immediate_poll(1)).await.unwrap().remove(0);
        consumer.begin_draining().await.unwrap();
        consumer
            .nack(delivery.receipt(), Retry::After(Duration::from_millis(20)))
            .await
            .unwrap();

        let retried = consumer
            .poll(Poll {
                max_messages: 1,
                visibility_timeout: Duration::from_secs(30),
                wait: Duration::from_millis(200),
            })
            .await
            .unwrap()
            .remove(0);
        assert_eq!(retried.message.body, b"retry");
        assert_eq!(retried.delivery_count, 2);
        assert!(consumer.ack(retried.receipt()).await.unwrap());
        assert!(consumer.snapshot().await.unwrap().is_drained());
    }

    #[tokio::test]
    async fn snapshots_report_ready_deferred_and_store_counts() {
        let (_directory, fan) = database().await;
        let consumer = fan
            .consumer("worker")
            .filter(Filter::topic("jobs"))
            .open()
            .await
            .unwrap();
        fan.publish_batch(&[
            Publish::new(b"a").with_topic("jobs"),
            Publish::new(b"b")
                .with_topic("jobs")
                .with_idempotency_key(b"key"),
        ])
        .await
        .unwrap();
        let leased = consumer
            .poll(Poll {
                max_messages: 1,
                visibility_timeout: Duration::from_secs(30),
                wait: Duration::ZERO,
            })
            .await
            .unwrap()
            .remove(0);

        let snapshot = consumer.snapshot().await.unwrap();
        assert_eq!(snapshot.filter, Filter::topic("jobs"));
        assert_eq!(snapshot.state, ConsumerState::Active);
        assert_eq!(snapshot.outstanding, 2);
        assert_eq!(snapshot.ready, 1);
        assert_eq!(snapshot.not_ready(), 1);
        assert!(snapshot.next_ready_at_ms.is_some());
        assert!(snapshot.oldest_outstanding_at_ms.is_some());

        let store = fan.snapshot().await.unwrap();
        assert_eq!(store.retained_messages, 2);
        assert_eq!(store.idempotency_keys, 1);
        assert_eq!(store.outstanding_deliveries, 2);
        assert_eq!(store.consumers, [snapshot]);
        assert!(consumer.ack(leased.receipt()).await.unwrap());
    }

    #[tokio::test]
    async fn safe_deletion_requires_a_drained_consumer_and_old_handles_stay_deleted() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        fan.publish(Publish::new(b"work")).await.unwrap();

        let error = fan
            .delete_consumer("worker", DeleteMode::DrainedOnly)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::ConsumerNotDraining { .. }));
        consumer.begin_draining().await.unwrap();
        let error = fan
            .delete_consumer("worker", DeleteMode::DrainedOnly)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::ConsumerNotEmpty { outstanding: 1, .. }
        ));

        let delivery = consumer.poll(immediate_poll(1)).await.unwrap().remove(0);
        consumer.ack(delivery.receipt()).await.unwrap();
        let deleted = fan
            .delete_consumer("worker", DeleteMode::DrainedOnly)
            .await
            .unwrap();
        assert_eq!(deleted.discarded_deliveries, 0);

        let replacement = fan.consumer("worker").open().await.unwrap();
        fan.publish(Publish::new(b"new identity")).await.unwrap();
        assert!(matches!(
            consumer.poll(immediate_poll(1)).await,
            Err(Error::ConsumerDeleted { .. })
        ));
        assert_eq!(
            replacement.poll(immediate_poll(1)).await.unwrap()[0]
                .message
                .body,
            b"new identity"
        );
    }

    #[tokio::test]
    async fn forced_deletion_reports_discarded_deliveries() {
        let (_directory, fan) = database().await;
        fan.consumer("worker").open().await.unwrap();
        fan.publish_batch(&[Publish::new(b"a"), Publish::new(b"b")])
            .await
            .unwrap();

        let deleted = fan
            .delete_consumer("worker", DeleteMode::DiscardOutstanding)
            .await
            .unwrap();
        assert_eq!(deleted.discarded_deliveries, 2);
        assert_eq!(fan.snapshot().await.unwrap().outstanding_deliveries, 0);
    }

    #[tokio::test]
    async fn pruning_is_bounded_preserves_live_deliveries_and_keeps_idempotency() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        let keyed = Publish::new(b"keyed").with_idempotency_key(b"key");
        let keyed_id = fan.publish(keyed).await.unwrap().id();
        fan.publish_batch(&[Publish::new(b"a"), Publish::new(b"b")])
            .await
            .unwrap();

        let prune = Prune {
            before_ms: i64::MAX,
            max_messages: 2,
        };
        assert_eq!(fan.prune_messages(prune).await.unwrap().deleted_messages, 0);
        let deliveries = consumer.poll(immediate_poll(10)).await.unwrap();
        consumer
            .ack_batch(&deliveries.iter().map(Delivery::receipt).collect::<Vec<_>>())
            .await
            .unwrap();

        assert_eq!(fan.prune_messages(prune).await.unwrap().deleted_messages, 2);
        assert_eq!(fan.snapshot().await.unwrap().retained_messages, 1);
        assert_eq!(
            fan.publish(keyed).await.unwrap(),
            PublishOutcome::Duplicate { id: keyed_id }
        );
        assert!(consumer.poll(immediate_poll(1)).await.unwrap().is_empty());
        assert_eq!(fan.prune_messages(prune).await.unwrap().deleted_messages, 1);
        assert_eq!(fan.snapshot().await.unwrap().idempotency_keys, 1);
    }

    #[tokio::test]
    async fn visibility_can_be_extended_without_replacing_the_receipt() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        fan.publish(Publish::new(b"work")).await.unwrap();
        let delivery = consumer
            .poll(Poll {
                max_messages: 1,
                visibility_timeout: Duration::from_millis(10),
                wait: Duration::ZERO,
            })
            .await
            .unwrap()
            .remove(0);

        assert!(
            consumer
                .extend_visibility(delivery.receipt(), Duration::from_millis(200))
                .await
                .unwrap()
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(consumer.poll(immediate_poll(1)).await.unwrap().is_empty());
        assert!(consumer.ack(delivery.receipt()).await.unwrap());
        assert!(
            !consumer
                .extend_visibility(delivery.receipt(), Duration::from_secs(1))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn inactive_backlogs_are_logical_until_claimed() {
        let (_directory, fan) = database().await;
        let mut consumers = Vec::new();
        for index in 0..100 {
            consumers.push(
                fan.consumer(format!("worker-{index}"))
                    .open()
                    .await
                    .unwrap(),
            );
        }
        let publishes = vec![Publish::new(b"work"); 100];
        fan.publish_batch(&publishes).await.unwrap();

        let physical: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM litefan_deliveries")
            .fetch_one(fan.pool())
            .await
            .unwrap();
        assert_eq!(physical, 0);
        assert_eq!(fan.snapshot().await.unwrap().outstanding_deliveries, 10_000);

        let claimed = consumers[0].poll(immediate_poll(10)).await.unwrap();
        assert_eq!(claimed.len(), 10);
        let physical: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM litefan_deliveries")
            .fetch_one(fan.pool())
            .await
            .unwrap();
        assert_eq!(physical, 10);
    }

    #[tokio::test]
    async fn topic_scans_skip_irrelevant_log_ranges_permanently() {
        let (_directory, fan) = database().await;
        let consumer = fan
            .consumer("jobs")
            .filter(Filter::topic("jobs"))
            .open()
            .await
            .unwrap();
        let irrelevant = vec![Publish::new(b"noise").with_topic("metrics"); 100];
        fan.publish_batch(&irrelevant).await.unwrap();
        fan.publish(Publish::new(b"work").with_topic("jobs"))
            .await
            .unwrap();

        let delivery = consumer.poll(immediate_poll(10)).await.unwrap().remove(0);
        assert_eq!(delivery.message.body, b"work");
        consumer.ack(delivery.receipt()).await.unwrap();
        assert!(consumer.poll(immediate_poll(10)).await.unwrap().is_empty());

        let (cursor, high_water): (i64, i64) = sqlx::query_as(
            "SELECT scan_cursor, (SELECT MAX(id) FROM litefan_messages) \
             FROM litefan_consumers WHERE name = 'jobs'",
        )
        .fetch_one(fan.pool())
        .await
        .unwrap();
        assert_eq!(cursor, high_water);
    }

    #[tokio::test]
    async fn draining_after_pruning_ahead_of_the_log_is_valid() {
        let (_directory, fan) = database().await;
        fan.publish(Publish::new(b"history")).await.unwrap();
        let consumer = fan.consumer("worker").open().await.unwrap();
        assert_eq!(
            fan.prune_messages(Prune {
                before_ms: i64::MAX,
                max_messages: 1,
            })
            .await
            .unwrap()
            .deleted_messages,
            1
        );
        assert!(consumer.begin_draining().await.unwrap());
        assert!(consumer.snapshot().await.unwrap().is_drained());
    }

    #[tokio::test]
    async fn in_memory_database_uses_one_coherent_connection() {
        let fan = LiteFan::open_with_config(
            ":memory:",
            Config {
                max_connections: 8,
                ..Config::default()
            },
        )
        .await
        .unwrap();
        let consumer = fan.consumer("worker").open().await.unwrap();
        fan.publish(Publish::new(b"work")).await.unwrap();
        let delivery = consumer.poll(immediate_poll(1)).await.unwrap().remove(0);
        assert_eq!(delivery.message.body, b"work");
        assert!(consumer.ack(delivery.receipt()).await.unwrap());
    }

    #[tokio::test]
    async fn obsolete_unversioned_schemas_fail_at_open() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fan.db");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE litefan_consumers ( \
                 id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE \
             )",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let error = LiteFan::open(path).await.unwrap_err();
        assert!(matches!(error, Error::IncompatibleSchema));
    }

    #[tokio::test]
    async fn sub_millisecond_durable_timeouts_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let error = LiteFan::open_with_config(
            directory.path().join("fan.db"),
            Config {
                idempotency_window: Duration::from_nanos(1),
                ..Config::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::InvalidConfig(_)));

        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        let error = consumer
            .poll(Poll {
                visibility_timeout: Duration::from_nanos(1),
                ..immediate_poll(1)
            })
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidVisibilityTimeout));
    }

    #[tokio::test]
    async fn an_unrepresentable_poll_deadline_is_rejected() {
        let (_directory, fan) = database().await;
        let consumer = fan.consumer("worker").open().await.unwrap();
        let error = consumer
            .poll(Poll {
                wait: Duration::MAX,
                ..Poll::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidPoll("wait is too large")));
    }

    #[tokio::test]
    async fn zero_idempotency_window_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let error = LiteFan::open_with_config(
            directory.path().join("fan.db"),
            Config {
                idempotency_window: Duration::ZERO,
                ..Config::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::InvalidConfig(_)));
    }
}
