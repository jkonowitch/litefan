//! Heavier concurrency scenarios for the lazy-inbox design.
//!
//! Run with: `cargo run --release --example heavy_benchmark`
//! Results and interpretation live beside this file in `BENCHMARKS.md`.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use litefan::{Consumer, Filter, LiteFan, Poll, Publish};
use tokio::{sync::Barrier, task::JoinHandle};

const VISIBILITY: Duration = Duration::from_secs(60);

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

async fn create_consumers(fan: &LiteFan, count: usize, filter: Filter) -> Vec<Consumer> {
    let mut consumers = Vec::with_capacity(count);
    for index in 0..count {
        consumers.push(
            fan.consumer(format!("consumer-{index}"))
                .filter(filter.clone())
                .open()
                .await
                .unwrap(),
        );
    }
    consumers
}

async fn publish_batches(
    fan: &LiteFan,
    message_count: usize,
    batch_size: usize,
    topic: Option<&str>,
    body: &[u8],
) {
    let messages = vec![Publish::new(body); batch_size];
    for offset in (0..message_count).step_by(batch_size) {
        let length = (message_count - offset).min(batch_size);
        let mut batch = messages[..length].to_vec();
        for message in &mut batch {
            message.topic = topic;
        }
        fan.publish_batch(&batch).await.unwrap();
    }
}

async fn drain_exact(consumer: Consumer, expected: usize, batch_size: usize) -> usize {
    let mut drained = 0;
    while drained < expected {
        let deliveries = consumer
            .poll(Poll {
                max_messages: batch_size,
                visibility_timeout: VISIBILITY,
                wait: Duration::from_secs(2),
            })
            .await
            .unwrap();
        assert!(!deliveries.is_empty(), "consumer timed out before draining");
        let receipts: Vec<_> = deliveries
            .iter()
            .map(|delivery| delivery.receipt())
            .collect();
        let result = consumer.ack_batch(&receipts).await.unwrap();
        assert_eq!(result.applied, deliveries.len());
        drained += result.applied;
    }
    drained
}

async fn backlog_fanout(consumer_count: usize, messages: usize) {
    let (_directory, fan) = database().await;
    let consumers = create_consumers(&fan, consumer_count, Filter::All).await;
    publish_batches(&fan, messages, 500, None, b"x").await;
    let materialized: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM litefan_deliveries")
        .fetch_one(fan.pool())
        .await
        .unwrap();
    assert_eq!(materialized, 0);
    let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(fan.pool())
        .await
        .unwrap();
    let page_count: i64 = sqlx::query_scalar("PRAGMA page_count")
        .fetch_one(fan.pool())
        .await
        .unwrap();
    let allocated_bytes = page_size * page_count;

    let started = Instant::now();
    let tasks: Vec<_> = consumers
        .into_iter()
        .map(|consumer| tokio::spawn(drain_exact(consumer, messages, 100)))
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap(), messages);
    }
    let elapsed = started.elapsed();
    let deliveries = consumer_count * messages;
    println!(
        "backlog fanout consumers={consumer_count:3}: {deliveries:7} logical deliveries, \
         {materialized} materialized before claim, {:.1} MiB ({:.1} B/source message), \
         drained in {elapsed:?} \
         ({:.0} delivery/s, {:.0} source msg/s)",
        allocated_bytes as f64 / (1024.0 * 1024.0),
        allocated_bytes as f64 / messages as f64,
        rate(deliveries, elapsed),
        rate(messages, elapsed),
    );
}

async fn live_fanout(consumer_count: usize, messages: usize, body_size: usize) {
    let (_directory, fan) = database().await;
    let consumers = create_consumers(&fan, consumer_count, Filter::All).await;
    let ready = Arc::new(Barrier::new(consumer_count + 1));
    let mut tasks = Vec::with_capacity(consumer_count);
    for consumer in consumers {
        let ready = ready.clone();
        tasks.push(tokio::spawn(async move {
            ready.wait().await;
            drain_exact(consumer, messages, 100).await
        }));
    }

    let body = vec![b'x'; body_size];
    ready.wait().await;
    let started = Instant::now();
    publish_batches(&fan, messages, 100, None, &body).await;
    for task in tasks {
        assert_eq!(task.await.unwrap(), messages);
    }
    let elapsed = started.elapsed();
    let deliveries = consumer_count * messages;
    let transferred = deliveries * body_size;
    println!(
        "live    fanout consumers={consumer_count:3} body={body_size:4} B: \
         {deliveries:7} deliveries in {elapsed:?} ({:.0} delivery/s, {:.0} source msg/s, {:.1} MiB/s)",
        rate(deliveries, elapsed),
        rate(messages, elapsed),
        rate(transferred, elapsed) / (1024.0 * 1024.0),
    );
}

async fn competing_workers(worker_count: usize, messages: usize) {
    let (_directory, fan) = database().await;
    let consumer = fan.consumer("shared").open().await.unwrap();
    publish_batches(&fan, messages, 500, None, b"x").await;
    let acknowledged = Arc::new(AtomicUsize::new(0));

    let started = Instant::now();
    let mut tasks = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let consumer = consumer.clone();
        let acknowledged = acknowledged.clone();
        tasks.push(tokio::spawn(async move {
            while acknowledged.load(Ordering::Acquire) < messages {
                let deliveries = consumer
                    .poll(Poll {
                        max_messages: 100,
                        visibility_timeout: VISIBILITY,
                        wait: Duration::from_millis(100),
                    })
                    .await
                    .unwrap();
                if deliveries.is_empty() {
                    continue;
                }
                let receipts: Vec<_> = deliveries
                    .iter()
                    .map(|delivery| delivery.receipt())
                    .collect();
                let result = consumer.ack_batch(&receipts).await.unwrap();
                acknowledged.fetch_add(result.applied, Ordering::AcqRel);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let elapsed = started.elapsed();
    assert_eq!(acknowledged.load(Ordering::Acquire), messages);
    println!(
        "compete workers={worker_count:3}: {messages:6} messages in {elapsed:?} ({:.0} msg/s)",
        rate(messages, elapsed),
    );
}

async fn idle_timeout(poller_count: usize) {
    let (_directory, fan) = database().await;
    let consumers = create_consumers(&fan, poller_count, Filter::All).await;
    let ready = Arc::new(Barrier::new(poller_count + 1));
    let mut tasks = Vec::with_capacity(poller_count);
    for consumer in consumers {
        let ready = ready.clone();
        tasks.push(tokio::spawn(async move {
            ready.wait().await;
            consumer
                .poll(Poll {
                    max_messages: 1,
                    visibility_timeout: VISIBILITY,
                    wait: Duration::from_millis(500),
                })
                .await
                .unwrap()
        }));
    }

    ready.wait().await;
    let started = Instant::now();
    for task in tasks {
        assert!(task.await.unwrap().is_empty());
    }
    let elapsed = started.elapsed();
    println!(
        "idle long-pollers={poller_count:4}: 500 ms timeout completed in {elapsed:?} \
         ({:.1} ms excess)",
        elapsed
            .saturating_sub(Duration::from_millis(500))
            .as_secs_f64()
            * 1_000.0,
    );
}

async fn publish_with_idle_pollers(poller_count: usize, messages: usize, batch_size: usize) {
    let (_directory, fan) = database().await;
    fan.consumer("active")
        .filter(Filter::topic("active"))
        .open()
        .await
        .unwrap();
    let consumers = create_consumers(&fan, poller_count, Filter::topic("idle")).await;
    let mut pollers: Vec<JoinHandle<_>> = consumers
        .into_iter()
        .map(|consumer| {
            tokio::spawn(async move {
                consumer
                    .poll(Poll {
                        max_messages: 1,
                        visibility_timeout: VISIBILITY,
                        wait: Duration::from_secs(10),
                    })
                    .await
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let started = Instant::now();
    // Smaller batches deliberately create more notification generations.
    publish_batches(&fan, messages, batch_size, Some("active"), b"x").await;
    let elapsed = started.elapsed();
    for poller in &pollers {
        poller.abort();
    }
    for poller in pollers.drain(..) {
        let _ = poller.await;
    }
    println!(
        "publish idle pollers={poller_count:4} batch={batch_size:3}: {messages:5} messages \
         in {elapsed:?} ({:.0} msg/s)",
        rate(messages, elapsed),
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    println!("idle polling (default 4-connection pool, 250 ms fallback):");
    for pollers in [100, 500, 1_000] {
        idle_timeout(pollers).await;
    }

    println!("\npublish while filtered consumers are idle:");
    for batch_size in [50, 500] {
        for pollers in [0, 100, 500, 1_000] {
            publish_with_idle_pollers(pollers, 5_000, batch_size).await;
        }
    }

    println!("\npreloaded backlog, one worker per durable consumer:");
    for consumers in [1, 10, 50, 100] {
        backlog_fanout(consumers, 2_000).await;
    }

    println!("\nlive publish and consume, one worker per durable consumer:");
    for consumers in [10, 50, 100] {
        live_fanout(consumers, 2_000, 1).await;
    }
    live_fanout(50, 2_000, 1_024).await;

    println!("\ncompeting workers sharing one durable consumer:");
    for workers in [1, 4, 16, 64] {
        competing_workers(workers, 20_000).await;
    }
}
