//! A small, durable fan-out message log backed by SQLite.
//!
//! Each named consumer receives its own delivery of every matching message.
//! Delivery is at least once: polling leases messages for a visibility timeout,
//! and an expired lease may be delivered again. Receipts are generation-bound,
//! so stale workers cannot acknowledge a newer delivery.

use std::{
    fmt,
    path::Path,
    sync::Arc,
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
const RECEIPTS_PER_CHUNK: usize = (MAX_SQL_VARIABLES - 1) / 3;
const PUBLISHES_PER_CHUNK: usize = MAX_SQL_VARIABLES / 3;
const FANOUT_IDS_PER_CHUNK: usize = MAX_SQL_VARIABLES - 1;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS litefan_messages (
    id           INTEGER PRIMARY KEY,
    topic        TEXT,
    body         BLOB NOT NULL,
    published_at INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS litefan_idempotency (
    key        BLOB PRIMARY KEY,
    message_id INTEGER
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS litefan_consumers (
    id           INTEGER PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    topic_filter TEXT,
    created_at   INTEGER NOT NULL
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
"#;

/// Connection, batching, durability, and long-poll settings.
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_connections: 4,
            max_batch_size: 500,
            busy_timeout: Duration::from_secs(5),
            synchronous: SqliteSynchronous::Normal,
            wal_autocheckpoint_pages: 1_000,
            cross_process_poll_interval: Duration::from_millis(100),
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

#[derive(Debug)]
pub enum Error {
    Sqlx(sqlx::Error),
    BatchTooLarge { requested: usize, maximum: usize },
    ConsumerConfigurationMismatch { name: String },
    EmptyConsumerName,
    InvalidConfig(&'static str),
    InvalidPoll(&'static str),
    IncompleteIdempotencyEntry,
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
            Self::IncompleteIdempotencyEntry => {
                f.write_str("idempotency ledger contains an incomplete entry")
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
    changes: watch::Sender<u64>,
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
            .max_connections(config.max_connections)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        let (changes, _) = watch::channel(0);

        Ok(Self {
            inner: Arc::new(Inner {
                pool,
                max_batch_size: config.max_batch_size,
                cross_process_poll_interval: config.cross_process_poll_interval,
                changes,
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

    /// Publish one message. An existing idempotency key is a no-op.
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
        let mut transaction = self.inner.pool.begin().await?;
        let mut outcomes = Vec::with_capacity(messages.len());
        let mut inserted_any = false;

        for message in messages {
            if let Some(key) = message.idempotency_key {
                let claimed = sqlx::query(
                    "INSERT INTO litefan_idempotency(key, message_id) VALUES (?, NULL) \
                     ON CONFLICT DO NOTHING",
                )
                .bind(key)
                .execute(&mut *transaction)
                .await?
                .rows_affected()
                    == 1;

                if !claimed {
                    let id = sqlx::query_scalar::<_, Option<i64>>(
                        "SELECT message_id FROM litefan_idempotency WHERE key = ?",
                    )
                    .bind(key)
                    .fetch_one(&mut *transaction)
                    .await?
                    .ok_or(Error::IncompleteIdempotencyEntry)?;
                    outcomes.push(PublishOutcome::Duplicate { id: MessageId(id) });
                    continue;
                }
            }

            let result = sqlx::query(
                "INSERT INTO litefan_messages(topic, body, published_at) VALUES (?, ?, ?)",
            )
            .bind(message.topic)
            .bind(message.body)
            .bind(published_at)
            .execute(&mut *transaction)
            .await?;
            let id = result.last_insert_rowid();

            sqlx::query(
                "INSERT INTO litefan_deliveries(consumer_id, message_id, visible_at) \
                 SELECT id, ?, ? FROM litefan_consumers \
                 WHERE topic_filter IS NULL OR topic_filter = ?",
            )
            .bind(id)
            .bind(published_at)
            .bind(message.topic)
            .execute(&mut *transaction)
            .await?;

            if let Some(key) = message.idempotency_key {
                sqlx::query("UPDATE litefan_idempotency SET message_id = ? WHERE key = ?")
                    .bind(id)
                    .bind(key)
                    .execute(&mut *transaction)
                    .await?;
            }

            outcomes.push(PublishOutcome::Published { id: MessageId(id) });
            inserted_any = true;
        }

        transaction.commit().await?;
        if inserted_any {
            self.signal_change();
        }
        Ok(outcomes)
    }

    async fn publish_unkeyed_batch(&self, messages: &[Publish<'_>]) -> Result<Vec<PublishOutcome>> {
        let published_at = now_ms()?;
        let mut transaction = self.inner.pool.begin().await?;
        let mut ids = Vec::with_capacity(messages.len());

        for messages in messages.chunks(PUBLISHES_PER_CHUNK) {
            let mut query = QueryBuilder::<Sqlite>::new(
                "INSERT INTO litefan_messages(topic, body, published_at) ",
            );
            query.push_values(messages, |mut row, message| {
                row.push_bind(message.topic)
                    .push_bind(message.body)
                    .push_bind(published_at);
            });
            query.push(" RETURNING id");
            ids.extend(
                query
                    .build_query_scalar::<i64>()
                    .fetch_all(&mut *transaction)
                    .await?,
            );
        }
        // SQLite does not promise RETURNING order. With one writer and no
        // message triggers, row IDs are assigned in VALUES order.
        ids.sort_unstable();

        for ids in ids.chunks(FANOUT_IDS_PER_CHUNK) {
            let mut query = QueryBuilder::<Sqlite>::new(
                "INSERT INTO litefan_deliveries(consumer_id, message_id, visible_at) \
                 SELECT consumer.id, message.id, ",
            );
            query.push_bind(published_at).push(
                " FROM litefan_consumers AS consumer \
                        CROSS JOIN litefan_messages AS message \
                        WHERE message.id IN (",
            );
            let mut separated = query.separated(", ");
            for id in ids {
                separated.push_bind(id);
            }
            separated.push_unseparated(
                ") AND (consumer.topic_filter IS NULL \
                OR consumer.topic_filter = message.topic)",
            );
            query.build().execute(&mut *transaction).await?;
        }

        transaction.commit().await?;
        self.signal_change();
        Ok(ids
            .into_iter()
            .map(|id| PublishOutcome::Published { id: MessageId(id) })
            .collect())
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

    fn signal_change(&self) {
        self.inner
            .changes
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
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
        sqlx::query(
            "INSERT INTO litefan_consumers(name, topic_filter, created_at) VALUES (?, ?, ?) \
             ON CONFLICT(name) DO NOTHING",
        )
        .bind(&self.name)
        .bind(self.filter.as_database_value())
        .bind(now)
        .execute(&self.fan.inner.pool)
        .await?;

        let row = sqlx::query("SELECT id, topic_filter FROM litefan_consumers WHERE name = ?")
            .bind(&self.name)
            .fetch_one(&self.fan.inner.pool)
            .await?;
        let id: i64 = row.get("id");
        let stored_filter: Option<String> = row.get("topic_filter");
        if stored_filter.as_deref() != self.filter.as_database_value() {
            return Err(Error::ConsumerConfigurationMismatch { name: self.name });
        }

        Ok(Consumer {
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

    /// Claim immediately-visible messages, waiting up to `poll.wait` if empty.
    pub async fn poll(&self, poll: Poll) -> Result<Vec<Delivery>> {
        if poll.max_messages == 0 {
            return Ok(Vec::new());
        }
        self.fan.ensure_batch_size(poll.max_messages)?;
        if poll.visibility_timeout.is_zero() {
            return Err(Error::InvalidPoll(
                "visibility_timeout must be greater than zero",
            ));
        }

        let lease_ms = duration_ms(poll.visibility_timeout)?;
        let deadline = Instant::now() + poll.wait;
        let mut changes = self.fan.inner.changes.subscribe();

        loop {
            changes.borrow_and_update();
            let claim = self.claim(poll.max_messages, lease_ms).await?;
            if !claim.deliveries.is_empty() {
                return Ok(claim.deliveries);
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
                changed = changes.changed() => {
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
            self.fan.signal_change();
        }
        Ok(BatchResult {
            applied,
            stale: receipts.len().saturating_sub(applied),
        })
    }

    async fn claim(&self, max_messages: usize, lease_ms: i64) -> Result<Claim> {
        let now = now_ms()?;
        // Avoid taking SQLite's writer lock on every idle long-poll tick.
        let earliest_visible_at = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MIN(visible_at) FROM litefan_deliveries WHERE consumer_id = ?",
        )
        .bind(self.id)
        .fetch_one(&self.fan.inner.pool)
        .await?;
        if earliest_visible_at.is_none_or(|visible_at| visible_at > now) {
            return Ok(Claim {
                deliveries: Vec::new(),
                next_visible_at: earliest_visible_at,
            });
        }

        let lease_deadline = now.checked_add(lease_ms).ok_or(Error::DurationOutOfRange)?;
        let mut transaction = self.fan.inner.pool.begin().await?;
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
        let next_visible_at = if deliveries.is_empty() {
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT MIN(visible_at) FROM litefan_deliveries WHERE consumer_id = ?",
            )
            .bind(self.id)
            .fetch_one(&self.fan.inner.pool)
            .await?
        } else {
            None
        };
        Ok(Claim {
            deliveries,
            next_visible_at,
        })
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
    async fn idempotency_is_permanent_and_batch_order_is_preserved() {
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
}
