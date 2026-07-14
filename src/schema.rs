pub(crate) const VERSION: i64 = 1;

pub(crate) const SQL: &str = r#"
CREATE TABLE IF NOT EXISTS litefan_messages (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    topic        TEXT,
    body         BLOB NOT NULL,
    published_at INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS litefan_messages_topic
    ON litefan_messages(topic, id) WHERE topic IS NOT NULL;

CREATE TABLE IF NOT EXISTS litefan_idempotency (
    key        BLOB PRIMARY KEY,
    message_id INTEGER,
    expires_at INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS litefan_idempotency_expiry
    ON litefan_idempotency(expires_at);

CREATE TABLE IF NOT EXISTS litefan_consumers (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE,
    topic_filter TEXT,
    created_at   INTEGER NOT NULL,
    draining_at  INTEGER,
    scan_cursor  INTEGER NOT NULL CHECK (scan_cursor >= 0),
    drain_cursor INTEGER CHECK (drain_cursor >= scan_cursor)
) STRICT;

CREATE TABLE IF NOT EXISTS litefan_deliveries (
    consumer_id   INTEGER NOT NULL
        REFERENCES litefan_consumers(id) ON DELETE CASCADE,
    message_id    INTEGER NOT NULL
        REFERENCES litefan_messages(id) ON DELETE CASCADE,
    visible_at    INTEGER NOT NULL,
    generation    INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
    delivery_count INTEGER NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
    PRIMARY KEY (consumer_id, message_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS litefan_deliveries_visible
    ON litefan_deliveries(consumer_id, visible_at, message_id);

CREATE INDEX IF NOT EXISTS litefan_deliveries_message
    ON litefan_deliveries(message_id);
"#;
