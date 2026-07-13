//! Informational performance smoke test for the lazy-inbox design.
//!
//! Run with: `cargo run --release --example benchmark`
//! Results and interpretation live beside this file in `BENCHMARKS.md`.

use std::time::{Duration, Instant};

use litefan::{Consumer, LiteFan, Poll, Publish};

fn rate(count: usize, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64()
}

async fn database() -> (tempfile::TempDir, LiteFan) {
    let directory = tempfile::tempdir().unwrap();
    let fan = LiteFan::open(directory.path().join("fan.db"))
        .await
        .unwrap();
    (directory, fan)
}

async fn publish_messages(fan: &LiteFan, count: usize) -> Duration {
    let payloads = vec![Publish::new(b"x"); fan.max_batch_size()];
    let started = Instant::now();
    for offset in (0..count).step_by(payloads.len()) {
        let length = (count - offset).min(payloads.len());
        fan.publish_batch(&payloads[..length]).await.unwrap();
    }
    started.elapsed()
}

async fn fanout_publish(consumer_count: usize, message_count: usize) {
    let (_directory, fan) = database().await;
    for index in 0..consumer_count {
        fan.consumer(format!("consumer-{index}"))
            .open()
            .await
            .unwrap();
    }

    let elapsed = publish_messages(&fan, message_count).await;
    let deliveries = consumer_count * message_count;
    println!(
        "publish consumers={consumer_count:4}: {message_count:5} messages / \
         {deliveries:7} logical deliveries in {elapsed:?} ({:.0} msg/s, {:.0} logical delivery/s)",
        rate(message_count, elapsed),
        rate(deliveries, elapsed),
    );
}

async fn drain(consumer: &Consumer, message_count: usize, batch_size: usize) -> Duration {
    let started = Instant::now();
    let mut drained = 0;
    while drained < message_count {
        let deliveries = consumer
            .poll(Poll {
                max_messages: batch_size,
                visibility_timeout: Duration::from_secs(60),
                wait: Duration::ZERO,
            })
            .await
            .unwrap();
        assert!(!deliveries.is_empty());
        let receipts: Vec<_> = deliveries
            .iter()
            .map(|delivery| delivery.receipt())
            .collect();
        consumer.ack_batch(&receipts).await.unwrap();
        drained += deliveries.len();
    }
    started.elapsed()
}

async fn claim_and_ack(batch_size: usize, message_count: usize) {
    let (_directory, fan) = database().await;
    let consumer = fan.consumer("worker").open().await.unwrap();
    publish_messages(&fan, message_count).await;

    let elapsed = drain(&consumer, message_count, batch_size).await;
    println!(
        "drain   batch={batch_size:4}: {message_count:5} messages in {elapsed:?} ({:.0} msg/s)",
        rate(message_count, elapsed),
    );
}

async fn idempotent_publish(message_count: usize) {
    let (_directory, fan) = database().await;
    fan.consumer("worker").open().await.unwrap();
    let keys: Vec<_> = (0..message_count)
        .map(|index| format!("message-{index}"))
        .collect();
    let messages: Vec<_> = keys
        .iter()
        .map(|key| Publish {
            topic: None,
            body: b"x",
            idempotency_key: Some(key.as_bytes()),
        })
        .collect();

    let started = Instant::now();
    for messages in messages.chunks(fan.max_batch_size()) {
        fan.publish_batch(messages).await.unwrap();
    }
    let inserted = started.elapsed();

    let started = Instant::now();
    for messages in messages.chunks(fan.max_batch_size()) {
        fan.publish_batch(messages).await.unwrap();
    }
    let duplicates = started.elapsed();

    println!(
        "publish keyed insert: {message_count:5} in {inserted:?} ({:.0} msg/s)",
        rate(message_count, inserted),
    );
    println!(
        "publish keyed no-op : {message_count:5} in {duplicates:?} ({:.0} msg/s)",
        rate(message_count, duplicates),
    );
}

#[tokio::main]
async fn main() {
    for consumer_count in [1, 10, 100, 1_000] {
        fanout_publish(consumer_count, 1_000).await;
    }

    for batch_size in [1, 10, 100, 500] {
        claim_and_ack(batch_size, 10_000).await;
    }

    idempotent_publish(10_000).await;

    let (_directory, fan) = database().await;
    let inactive = fan.consumer("inactive").open().await.unwrap();
    let elapsed = publish_messages(&fan, 100_000).await;
    let backlog = inactive.snapshot().await.unwrap().outstanding;
    let materialized: i64 = sqlx::query_scalar("SELECT count(*) FROM litefan_deliveries")
        .fetch_one(fan.pool())
        .await
        .unwrap();
    assert_eq!(backlog, 100_000);
    assert_eq!(materialized, 0);
    println!(
        "backlog inactive: {backlog} logical / {materialized} materialized deliveries \
         published in {elapsed:?} ({:.0} msg/s)",
        rate(backlog as usize, elapsed),
    );
    drop(inactive);
}
