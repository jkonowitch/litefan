//! Store-wide publishing, inspection, deletion, and retention operations.

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::Duration,
};

use sqlx::{
    QueryBuilder, Row, Sqlite, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};

use crate::{
    Config, ConsumerBuilder, DeleteMode, DeleteOutcome, Error, MessageId, Prune, PruneOutcome,
    Publish, PublishOutcome, Result, StoreSnapshot,
    schema::{SQL as SCHEMA, VERSION as SCHEMA_VERSION},
    signals::Signals,
    storage::{
        MAX_SQL_VARIABLES, count_from_row, fetch_consumer_snapshots, insert_message_rows,
        purge_expired_idempotency,
    },
    time::{now_ms, validate_config},
};

#[derive(Debug)]
pub(crate) struct Inner {
    pub(crate) pool: SqlitePool,
    pub(crate) max_batch_size: usize,
    pub(crate) cross_process_poll_interval: Duration,
    pub(crate) idempotency_window_ms: i64,
    pub(crate) signals: Signals,
}

/// A cloneable handle to a SQLite fan-out database.
#[derive(Clone, Debug)]
pub struct LiteFan {
    pub(crate) inner: Arc<Inner>,
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
        sqlx::query(
            "SELECT consumer_id, message_id, detail \
             FROM litefan_archived_deliveries LIMIT 0",
        )
        .execute(&pool)
        .await
        .map_err(|_| Error::IncompatibleSchema)?;
        if schema_version < SCHEMA_VERSION {
            sqlx::query("PRAGMA user_version = 2")
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
        ConsumerBuilder::new(self.clone(), name.into())
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
        let archived_deliveries = consumers.iter().try_fold(0_u64, |total, consumer| {
            total
                .checked_add(consumer.archived)
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
            archived_deliveries,
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
                             OR message.topic = consumer.topic_filter)) AS outstanding, \
                    (SELECT COUNT(*) FROM litefan_archived_deliveries AS archive \
                      WHERE archive.consumer_id = consumer.id) AS archived \
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
        let archived = count_from_row(&row, "archived")?;

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
        if !matches!(mode, DeleteMode::DiscardAll) && archived > 0 {
            return Err(Error::ConsumerHasArchives {
                name: name.to_owned(),
                archived,
            });
        }

        if matches!(mode, DeleteMode::DiscardAll) {
            sqlx::query("DELETE FROM litefan_archived_deliveries WHERE consumer_id = ?")
                .bind(id)
                .execute(&mut *transaction)
                .await?;
        }

        sqlx::query("DELETE FROM litefan_consumers WHERE id = ?")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        self.inner.signals.notify_consumer(id);
        Ok(DeleteOutcome {
            discarded_deliveries: outstanding,
            discarded_archives: archived,
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
                       SELECT 1 FROM litefan_archived_deliveries AS archive \
                       WHERE archive.message_id = message.id \
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

    pub(crate) fn ensure_batch_size(&self, requested: usize) -> Result<()> {
        if requested > self.inner.max_batch_size {
            return Err(Error::BatchTooLarge {
                requested,
                maximum: self.inner.max_batch_size,
            });
        }
        Ok(())
    }
}
