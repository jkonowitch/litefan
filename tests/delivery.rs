mod common;

use std::{collections::HashSet, time::Duration};

use common::{database, immediate_poll};
use litefan::*;

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
