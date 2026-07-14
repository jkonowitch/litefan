//! Shared SQL limits, row mapping, and transactional query helpers.

use sqlx::{QueryBuilder, Row, Sqlite};

use crate::{ConsumerSnapshot, ConsumerState, Error, Filter, Publish, Receipt, Result};

// Stay below SQLite's historical 999-variable limit. Receipt statements bind
// one consumer ID plus three values per receipt.
pub(crate) const MAX_SQL_VARIABLES: usize = 999;
pub(crate) const RECEIPTS_PER_CHUNK: usize = (MAX_SQL_VARIABLES - 1) / 3;
const PUBLISHES_PER_CHUNK: usize = MAX_SQL_VARIABLES / 3;

#[derive(Debug)]
pub(crate) struct Lease {
    pub(crate) message_id: i64,
    pub(crate) generation: i64,
    pub(crate) delivery_count: i64,
}

pub(crate) async fn insert_message_rows(
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

pub(crate) async fn fetch_consumer_snapshots(
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
                    AS oldest_outstanding_at, \
                (SELECT COUNT(*) FROM litefan_archived_deliveries AS archive \
                  WHERE archive.consumer_id = consumer.id) AS archived \
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
            archived: count_from_row(&row, "archived")?,
        });
    }
    Ok(snapshots)
}

pub(crate) async fn purge_expired_idempotency(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    now: i64,
) -> Result<()> {
    sqlx::query("DELETE FROM litefan_idempotency WHERE expires_at <= ?")
        .bind(now)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

pub(crate) fn count_from_row(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<u64> {
    let value: i64 = row.try_get(column)?;
    u64::try_from(value).map_err(|_| Error::CounterOutOfRange)
}

pub(crate) fn push_receipt_predicate(query: &mut QueryBuilder<Sqlite>, receipts: &[Receipt]) {
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
