//! Durable consumer creation, leasing, acknowledgement, and draining.

use std::{sync::Arc, time::Duration};

use sqlx::{QueryBuilder, Row, Sqlite};
use tokio::time::Instant;

use crate::{
    BatchResult, ConsumerSnapshot, Delivery, Error, Filter, LiteFan, Message, MessageId, Poll,
    Receipt, Result, Retry,
    signals::Signal,
    storage::{
        Lease, MAX_SQL_VARIABLES, RECEIPTS_PER_CHUNK, fetch_consumer_snapshots,
        push_receipt_predicate,
    },
    time::{add_duration, duration_ms, duration_until_ms, now_ms},
};

/// Builder for a durable named consumer. Creation starts at messages published after open.
#[derive(Clone, Debug)]
pub struct ConsumerBuilder {
    fan: LiteFan,
    name: String,
    filter: Filter,
}

impl ConsumerBuilder {
    pub(crate) fn new(fan: LiteFan, name: String) -> Self {
        Self {
            fan,
            name,
            filter: Filter::All,
        }
    }

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
