use std::fmt;

#[derive(Debug)]
pub enum Error {
    Sqlx(sqlx::Error),
    BatchTooLarge { requested: usize, maximum: usize },
    ConsumerConfigurationMismatch { name: String },
    EmptyConsumerName,
    InvalidConfig(&'static str),
    InvalidPoll(&'static str),
    InvalidVisibilityTimeout,
    IncompatibleSchema,
    UnsupportedSchemaVersion { found: i64, maximum: i64 },
    IncompleteIdempotencyEntry,
    ConsumerDeleted { name: String },
    ConsumerNotFound { name: String },
    ConsumerNotDraining { name: String },
    ConsumerNotEmpty { name: String, outstanding: u64 },
    ClockBeforeUnixEpoch,
    DurationOutOfRange,
    CounterOutOfRange,
    StorageInvariant(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlx(error) => write!(f, "SQLite error: {error}"),
            Self::BatchTooLarge { requested, maximum } => {
                write!(f, "batch contains {requested} items; maximum is {maximum}")
            }
            Self::ConsumerConfigurationMismatch { name } => write!(
                f,
                "consumer {name:?} already exists with a different filter"
            ),
            Self::EmptyConsumerName => f.write_str("consumer name cannot be empty"),
            Self::InvalidConfig(message) => write!(f, "invalid configuration: {message}"),
            Self::InvalidPoll(message) => write!(f, "invalid poll: {message}"),
            Self::InvalidVisibilityTimeout => {
                f.write_str("visibility timeout must be at least one millisecond")
            }
            Self::IncompatibleSchema => {
                f.write_str("database contains an incompatible unversioned litefan schema")
            }
            Self::UnsupportedSchemaVersion { found, maximum } => write!(
                f,
                "database schema version {found} is newer than supported version {maximum}"
            ),
            Self::IncompleteIdempotencyEntry => {
                f.write_str("idempotency ledger contains an incomplete entry")
            }
            Self::ConsumerDeleted { name } => write!(f, "consumer {name:?} has been deleted"),
            Self::ConsumerNotFound { name } => write!(f, "consumer {name:?} does not exist"),
            Self::ConsumerNotDraining { name } => write!(f, "consumer {name:?} is still active"),
            Self::ConsumerNotEmpty { name, outstanding } => write!(
                f,
                "consumer {name:?} still has {outstanding} outstanding deliveries"
            ),
            Self::ClockBeforeUnixEpoch => f.write_str("system clock is before the Unix epoch"),
            Self::DurationOutOfRange => f.write_str("duration does not fit in SQLite milliseconds"),
            Self::CounterOutOfRange => f.write_str("SQLite counter does not fit in the Rust type"),
            Self::StorageInvariant(message) => {
                write!(f, "storage invariant violated: {message}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlx(error) => Some(error),
            _ => None,
        }
    }
}

impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        Self::Sqlx(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_have_actionable_messages() {
        assert_eq!(
            Error::BatchTooLarge {
                requested: 10,
                maximum: 5,
            }
            .to_string(),
            "batch contains 10 items; maximum is 5"
        );
        assert_eq!(
            Error::ConsumerNotEmpty {
                name: "worker".into(),
                outstanding: 2,
            }
            .to_string(),
            "consumer \"worker\" still has 2 outstanding deliveries"
        );
    }
}
