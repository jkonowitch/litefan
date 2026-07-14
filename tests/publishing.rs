mod common;

use common::{database, immediate_poll};
use litefan::*;

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

#[tokio::test]
async fn maximum_keyed_batch_crosses_sql_variable_chunks_safely() {
    let (_directory, fan) = database().await;
    let consumer = fan.consumer("worker").open().await.unwrap();
    let bodies: Vec<_> = (0_u32..500).map(u32::to_be_bytes).collect();
    let keys: Vec<_> = (0_u32..500).map(u32::to_le_bytes).map(Vec::from).collect();
    let publishes: Vec<_> = bodies
        .iter()
        .zip(&keys)
        .map(|(body, key)| Publish::new(body).with_idempotency_key(key))
        .collect();

    let published = fan.publish_batch(&publishes).await.unwrap();
    assert_eq!(published.len(), 500);
    assert!(published.iter().all(|outcome| outcome.is_published()));
    assert!(published.windows(2).all(|pair| pair[0].id() < pair[1].id()));

    let duplicates = fan.publish_batch(&publishes).await.unwrap();
    assert_eq!(
        duplicates
            .iter()
            .map(|outcome| outcome.id())
            .collect::<Vec<_>>(),
        published
            .iter()
            .map(|outcome| outcome.id())
            .collect::<Vec<_>>()
    );
    assert!(duplicates.iter().all(|outcome| !outcome.is_published()));

    let deliveries = consumer.poll(immediate_poll(500)).await.unwrap();
    assert_eq!(deliveries.len(), 500);
    let receipts: Vec<_> = deliveries.iter().map(Delivery::receipt).collect();
    assert_eq!(
        consumer.ack_batch(&receipts).await.unwrap(),
        BatchResult {
            applied: 500,
            stale: 0,
        }
    );
    assert!(consumer.snapshot().await.unwrap().is_empty());
}
