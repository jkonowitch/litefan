mod common;

use common::{database, immediate_poll};
use litefan::*;

#[tokio::test]
async fn manual_archive_is_per_consumer_and_preserves_diagnostics() {
    let (_directory, fan) = database().await;
    let first = fan.consumer("first").open().await.unwrap();
    let second = fan.consumer("second").open().await.unwrap();
    fan.publish(Publish::new(b"poison").with_topic("jobs"))
        .await
        .unwrap();

    let delivery = first.poll(immediate_poll(1)).await.unwrap().remove(0);
    assert!(
        first
            .archive_with_detail(delivery.receipt(), "invalid payload")
            .await
            .unwrap()
    );
    assert!(!first.archive(delivery.receipt()).await.unwrap());
    assert!(!first.ack(delivery.receipt()).await.unwrap());
    assert!(first.poll(immediate_poll(1)).await.unwrap().is_empty());

    let archives = first.archives(ListArchives::default()).await.unwrap();
    assert_eq!(archives.len(), 1);
    assert_eq!(archives[0].message.body, b"poison");
    assert_eq!(archives[0].message.topic.as_deref(), Some("jobs"));
    assert_eq!(archives[0].delivery_count, 1);
    assert_eq!(archives[0].detail.as_deref(), Some("invalid payload"));

    let other = second.poll(immediate_poll(1)).await.unwrap().remove(0);
    assert_eq!(other.message.id, archives[0].message.id);
    assert!(second.ack(other.receipt()).await.unwrap());

    let snapshot = first.snapshot().await.unwrap();
    assert_eq!(snapshot.outstanding, 0);
    assert_eq!(snapshot.archived, 1);
    let store = fan.snapshot().await.unwrap();
    assert_eq!(store.outstanding_deliveries, 0);
    assert_eq!(store.archived_deliveries, 1);
}

#[tokio::test]
async fn stale_receipt_cannot_archive_a_newer_attempt() {
    let (_directory, fan) = database().await;
    let consumer = fan.consumer("worker").open().await.unwrap();
    fan.publish(Publish::new(b"retry")).await.unwrap();

    let first = consumer.poll(immediate_poll(1)).await.unwrap().remove(0);
    assert!(
        consumer
            .nack(first.receipt(), Retry::Immediately)
            .await
            .unwrap()
    );
    let second = consumer.poll(immediate_poll(1)).await.unwrap().remove(0);
    assert!(!consumer.archive(first.receipt()).await.unwrap());
    assert!(consumer.archive(second.receipt()).await.unwrap());
    assert_eq!(
        consumer.archives(ListArchives::default()).await.unwrap()[0].delivery_count,
        2
    );
}

#[tokio::test]
async fn archives_are_paginated_redriven_and_purged_without_republishing() {
    let (_directory, fan) = database().await;
    let consumer = fan.consumer("worker").open().await.unwrap();
    let observer = fan.consumer("observer").open().await.unwrap();
    fan.publish_batch(&[Publish::new(b"a"), Publish::new(b"b")])
        .await
        .unwrap();
    let deliveries = consumer.poll(immediate_poll(2)).await.unwrap();
    let receipts: Vec<_> = deliveries.iter().map(Delivery::receipt).collect();
    assert_eq!(
        consumer.archive_batch(&receipts).await.unwrap(),
        BatchResult {
            applied: 2,
            stale: 0,
        }
    );

    let first_page = consumer
        .archives(ListArchives {
            after: None,
            max_archives: 1,
        })
        .await
        .unwrap();
    let second_page = consumer
        .archives(ListArchives {
            after: Some(first_page[0].id),
            max_archives: 1,
        })
        .await
        .unwrap();
    assert_eq!(first_page[0].message.body, b"a");
    assert_eq!(second_page[0].message.body, b"b");

    assert!(
        consumer
            .redrive(first_page[0].id, Retry::Immediately)
            .await
            .unwrap()
    );
    assert!(
        !consumer
            .redrive(first_page[0].id, Retry::Immediately)
            .await
            .unwrap()
    );
    let redriven = consumer.poll(immediate_poll(1)).await.unwrap().remove(0);
    assert_eq!(redriven.message.body, b"a");
    assert_eq!(redriven.delivery_count, 1);
    assert!(consumer.ack(redriven.receipt()).await.unwrap());

    // Redrive restores only this consumer's copy; it does not publish again.
    assert_eq!(observer.poll(immediate_poll(10)).await.unwrap().len(), 2);
    assert!(observer.poll(immediate_poll(1)).await.unwrap().is_empty());

    assert_eq!(
        consumer
            .purge_archives(PurgeArchives {
                before_ms: i64::MAX,
                max_archives: 1,
            })
            .await
            .unwrap(),
        PurgeArchivesOutcome {
            deleted_archives: 1,
        }
    );
    assert!(
        consumer
            .archives(ListArchives::default())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn archives_retain_messages_until_the_archive_is_purged() {
    let (_directory, fan) = database().await;
    let consumer = fan.consumer("worker").open().await.unwrap();
    fan.publish(Publish::new(b"retained")).await.unwrap();
    let delivery = consumer.poll(immediate_poll(1)).await.unwrap().remove(0);
    consumer.archive(delivery.receipt()).await.unwrap();

    let prune = Prune {
        before_ms: i64::MAX,
        max_messages: 1,
    };
    assert_eq!(fan.prune_messages(prune).await.unwrap().deleted_messages, 0);
    consumer
        .purge_archives(PurgeArchives {
            before_ms: i64::MAX,
            max_archives: 1,
        })
        .await
        .unwrap();
    assert_eq!(fan.prune_messages(prune).await.unwrap().deleted_messages, 1);
}

#[tokio::test]
async fn consumer_deletion_requires_explicit_archive_discard() {
    let (_directory, fan) = database().await;
    let consumer = fan.consumer("worker").open().await.unwrap();
    fan.publish(Publish::new(b"archive")).await.unwrap();
    let delivery = consumer.poll(immediate_poll(1)).await.unwrap().remove(0);
    consumer.archive(delivery.receipt()).await.unwrap();
    consumer.begin_draining().await.unwrap();
    assert!(consumer.snapshot().await.unwrap().is_drained());

    assert!(matches!(
        fan.delete_consumer("worker", DeleteMode::DrainedOnly).await,
        Err(Error::ConsumerHasArchives { archived: 1, .. })
    ));
    let deleted = fan
        .delete_consumer("worker", DeleteMode::DiscardAll)
        .await
        .unwrap();
    assert_eq!(deleted.discarded_deliveries, 0);
    assert_eq!(deleted.discarded_archives, 1);
}
