use std::time::Duration;

use litefan::{LiteFan, Poll};
use tempfile::TempDir;

pub(crate) async fn database() -> (TempDir, LiteFan) {
    let directory = tempfile::tempdir().unwrap();
    let fan = LiteFan::open(directory.path().join("fan.db"))
        .await
        .unwrap();
    (directory, fan)
}

pub(crate) fn immediate_poll(max_messages: usize) -> Poll {
    Poll {
        max_messages,
        visibility_timeout: Duration::from_secs(30),
        wait: Duration::ZERO,
    }
}
