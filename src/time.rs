use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{Config, Error, Result};

pub(crate) fn validate_config(config: &Config) -> Result<()> {
    if config.max_connections == 0 {
        return Err(Error::InvalidConfig(
            "max_connections must be greater than zero",
        ));
    }
    if config.max_batch_size == 0 {
        return Err(Error::InvalidConfig(
            "max_batch_size must be greater than zero",
        ));
    }
    if i64::try_from(config.max_batch_size).is_err() {
        return Err(Error::InvalidConfig("max_batch_size must fit in SQLite"));
    }
    if config.cross_process_poll_interval.is_zero() {
        return Err(Error::InvalidConfig(
            "cross_process_poll_interval must be greater than zero",
        ));
    }
    if config.idempotency_window.as_millis() == 0 {
        return Err(Error::InvalidConfig(
            "idempotency_window must be at least one millisecond",
        ));
    }
    if i64::try_from(config.idempotency_window.as_millis()).is_err() {
        return Err(Error::InvalidConfig(
            "idempotency_window must fit in SQLite milliseconds",
        ));
    }
    Ok(())
}

pub(crate) fn now_ms() -> Result<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::ClockBeforeUnixEpoch)?
        .as_millis();
    i64::try_from(millis).map_err(|_| Error::DurationOutOfRange)
}

pub(crate) fn duration_ms(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_millis()).map_err(|_| Error::DurationOutOfRange)
}

pub(crate) fn add_duration(timestamp_ms: i64, duration: Duration) -> Result<i64> {
    timestamp_ms
        .checked_add(duration_ms(duration)?)
        .ok_or(Error::DurationOutOfRange)
}

pub(crate) fn duration_until_ms(timestamp_ms: i64) -> Result<Duration> {
    let remaining = timestamp_ms.saturating_sub(now_ms()?);
    Ok(Duration::from_millis(remaining as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_conversion_truncates_sub_millisecond_precision() {
        assert_eq!(duration_ms(Duration::from_micros(1_999)).unwrap(), 1);
    }

    #[test]
    fn timestamp_addition_checks_overflow() {
        assert_eq!(add_duration(10, Duration::from_millis(5)).unwrap(), 15);
        assert!(matches!(
            add_duration(i64::MAX, Duration::from_millis(1)),
            Err(Error::DurationOutOfRange)
        ));
    }

    #[test]
    fn config_validation_covers_independent_limits() {
        let invalid = [
            Config {
                max_connections: 0,
                ..Config::default()
            },
            Config {
                max_batch_size: 0,
                ..Config::default()
            },
            Config {
                cross_process_poll_interval: Duration::ZERO,
                ..Config::default()
            },
            Config {
                idempotency_window: Duration::ZERO,
                ..Config::default()
            },
        ];

        for config in invalid {
            assert!(matches!(
                validate_config(&config),
                Err(Error::InvalidConfig(_))
            ));
        }
        assert!(validate_config(&Config::default()).is_ok());
    }
}
