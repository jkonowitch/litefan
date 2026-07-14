mod common;

use std::time::Duration;

use common::{database, immediate_poll};
use litefan::*;

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
    assert_eq!(deleted.discarded_archives, 0);

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
    assert_eq!(deleted.discarded_archives, 0);
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
