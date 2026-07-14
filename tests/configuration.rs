mod common;

use std::time::Duration;

use common::{database, immediate_poll};
use litefan::*;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

#[tokio::test]
async fn in_memory_database_uses_one_coherent_connection() {
    let fan = LiteFan::open_with_config(
        ":memory:",
        Config {
            max_connections: 8,
            ..Config::default()
        },
    )
    .await
    .unwrap();
    let consumer = fan.consumer("worker").open().await.unwrap();
    fan.publish(Publish::new(b"work")).await.unwrap();
    let delivery = consumer.poll(immediate_poll(1)).await.unwrap().remove(0);
    assert_eq!(delivery.message.body, b"work");
    assert!(consumer.ack(delivery.receipt()).await.unwrap());
}

#[tokio::test]
async fn obsolete_unversioned_schemas_fail_at_open() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fan.db");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true),
        )
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE litefan_consumers ( \
             id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE \
         )",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;

    let error = LiteFan::open(path).await.unwrap_err();
    assert!(matches!(error, Error::IncompatibleSchema));
}

#[tokio::test]
async fn sub_millisecond_durable_timeouts_are_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let error = LiteFan::open_with_config(
        directory.path().join("fan.db"),
        Config {
            idempotency_window: Duration::from_nanos(1),
            ..Config::default()
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(error, Error::InvalidConfig(_)));

    let (_directory, fan) = database().await;
    let consumer = fan.consumer("worker").open().await.unwrap();
    let error = consumer
        .poll(Poll {
            visibility_timeout: Duration::from_nanos(1),
            ..immediate_poll(1)
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidVisibilityTimeout));
}

#[tokio::test]
async fn an_unrepresentable_poll_deadline_is_rejected() {
    let (_directory, fan) = database().await;
    let consumer = fan.consumer("worker").open().await.unwrap();
    let error = consumer
        .poll(Poll {
            wait: Duration::MAX,
            ..Poll::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidPoll("wait is too large")));
}

#[tokio::test]
async fn zero_idempotency_window_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let error = LiteFan::open_with_config(
        directory.path().join("fan.db"),
        Config {
            idempotency_window: Duration::ZERO,
            ..Config::default()
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(error, Error::InvalidConfig(_)));
}

#[tokio::test]
async fn configured_batch_limit_is_enforced_consistently() {
    let directory = tempfile::tempdir().unwrap();
    let fan = LiteFan::open_with_config(
        directory.path().join("fan.db"),
        Config {
            max_batch_size: 2,
            ..Config::default()
        },
    )
    .await
    .unwrap();
    let consumer = fan.consumer("worker").open().await.unwrap();
    let too_many = [Publish::new(b"a"), Publish::new(b"b"), Publish::new(b"c")];

    let publish_error = fan.publish_batch(&too_many).await.unwrap_err();
    assert!(matches!(
        publish_error,
        Error::BatchTooLarge {
            requested: 3,
            maximum: 2,
        }
    ));

    fan.publish_batch(&too_many[..2]).await.unwrap();
    let poll_error = consumer.poll(immediate_poll(3)).await.unwrap_err();
    assert!(matches!(poll_error, Error::BatchTooLarge { .. }));
    let deliveries = consumer.poll(immediate_poll(2)).await.unwrap();
    let receipt = deliveries[0].receipt();
    let receipt_error = consumer
        .ack_batch(&[receipt, receipt, receipt])
        .await
        .unwrap_err();
    assert!(matches!(receipt_error, Error::BatchTooLarge { .. }));
    let archive_error = consumer
        .archive_batch(&[receipt, receipt, receipt])
        .await
        .unwrap_err();
    assert!(matches!(archive_error, Error::BatchTooLarge { .. }));
    let list_error = consumer
        .archives(ListArchives {
            after: None,
            max_archives: 3,
        })
        .await
        .unwrap_err();
    assert!(matches!(list_error, Error::BatchTooLarge { .. }));

    let prune_error = fan
        .prune_messages(Prune {
            before_ms: i64::MAX,
            max_messages: 3,
        })
        .await
        .unwrap_err();
    assert!(matches!(prune_error, Error::BatchTooLarge { .. }));
}

#[tokio::test]
async fn empty_operations_are_explicit_no_ops() {
    let (_directory, fan) = database().await;
    let consumer = fan.consumer("worker").open().await.unwrap();

    assert!(fan.publish_batch(&[]).await.unwrap().is_empty());
    assert_eq!(
        consumer.ack_batch(&[]).await.unwrap(),
        BatchResult::default()
    );
    assert_eq!(
        consumer.nack_batch(&[], Retry::Immediately).await.unwrap(),
        BatchResult::default()
    );
    assert_eq!(
        consumer.archive_batch(&[]).await.unwrap(),
        BatchResult::default()
    );
    assert_eq!(
        consumer
            .redrive_batch(&[], Retry::Immediately)
            .await
            .unwrap(),
        BatchResult::default()
    );
    assert!(
        consumer
            .archives(ListArchives {
                after: None,
                max_archives: 0,
            })
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        consumer
            .purge_archives(PurgeArchives {
                before_ms: i64::MAX,
                max_archives: 0,
            })
            .await
            .unwrap(),
        PurgeArchivesOutcome::default()
    );
    assert_eq!(
        consumer
            .extend_visibility_batch(&[], Duration::ZERO)
            .await
            .unwrap(),
        BatchResult::default()
    );
    assert!(
        consumer
            .poll(Poll {
                max_messages: 0,
                visibility_timeout: Duration::ZERO,
                wait: Duration::MAX,
            })
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fan.prune_messages(Prune {
            before_ms: i64::MAX,
            max_messages: 0,
        })
        .await
        .unwrap(),
        PruneOutcome::default()
    );
}

#[tokio::test]
async fn invalid_consumer_and_schema_errors_preserve_context() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fan.db");
    let fan = LiteFan::open(&path).await.unwrap();

    assert!(matches!(
        fan.consumer("").open().await,
        Err(Error::EmptyConsumerName)
    ));
    assert!(matches!(
        fan.delete_consumer("missing", DeleteMode::DrainedOnly)
            .await,
        Err(Error::ConsumerNotFound { name }) if name == "missing"
    ));
    drop(fan);

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(SqliteConnectOptions::new().filename(&path))
        .await
        .unwrap();
    sqlx::query("PRAGMA user_version = 3")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    assert!(matches!(
        LiteFan::open(&path).await,
        Err(Error::UnsupportedSchemaVersion {
            found: 3,
            maximum: 2,
        })
    ));
}
