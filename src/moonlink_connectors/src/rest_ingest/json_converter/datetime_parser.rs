use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use moonlink::row::RowValue;
use super::error::JsonToMoonlinkRowError;

pub const ARROW_EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

pub fn parse_date(field_name: &str, date_str: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").map_err(|e| {
        JsonToMoonlinkRowError::InvalidDateFormat(field_name.to_string(), e.to_string())
    })?;

    let days = date.signed_duration_since(ARROW_EPOCH).num_days() as i32;
    Ok(RowValue::Int32(days))
}

pub fn parse_time(field_name: &str, time_str: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(time_str, "%H:%M:%S"))
        .map_err(|e| {
            JsonToMoonlinkRowError::InvalidTimeFormat(field_name.to_string(), e.to_string())
        })?;

    let seconds = time.num_seconds_from_midnight() as i64;
    let nanos = time.nanosecond() as i64;
    let microseconds = seconds * 1_000_000 + nanos / 1_000;
    Ok(RowValue::Int64(microseconds))
}

pub fn parse_timestamp(field_name: &str, timestamp_str: &str) -> Result<RowValue, JsonToMoonlinkRowError> {
    TimestampParser::parse(timestamp_str)
        .map(RowValue::Int64)
        .ok_or_else(|| JsonToMoonlinkRowError::InvalidTimestampFormat(
            field_name.to_string(),
            format!("unable to parse timestamp: {timestamp_str}"),
        ))
}

pub struct TimestampParser;

impl TimestampParser {
    pub fn parse(timestamp_str: &str) -> Option<i64> {
        Self::parse_rfc3339(timestamp_str)
            .or_else(|| Self::parse_iso8601(timestamp_str))
            .or_else(|| Self::parse_traditional(timestamp_str))
    }

    fn parse_rfc3339(s: &str) -> Option<i64> {
        DateTime::<FixedOffset>::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc).timestamp_micros())
    }

    fn parse_iso8601(s: &str) -> Option<i64> {
        const ISO_FORMATS: &[&str] = &[
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
        ];

        for format in ISO_FORMATS {
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, format) {
                return Some(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp_micros());
            }
        }
        None
    }

    fn parse_traditional(s: &str) -> Option<i64> {
        const TRADITIONAL_FORMATS_WITH_TZ: &[&str] = &[
            "%Y-%m-%d %H:%M:%S%.f%#z",
            "%Y-%m-%d %H:%M:%S%.f%:z",
        ];

        const TRADITIONAL_FORMATS_WITHOUT_TZ: &[&str] = &[
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
        ];

        for format in TRADITIONAL_FORMATS_WITH_TZ {
            if let Ok(dt) = DateTime::<FixedOffset>::parse_from_str(s, format) {
                return Some(dt.with_timezone(&Utc).timestamp_micros());
            }
        }

        for format in TRADITIONAL_FORMATS_WITHOUT_TZ {
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, format) {
                return Some(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp_micros());
            }
        }

        None
    }
}