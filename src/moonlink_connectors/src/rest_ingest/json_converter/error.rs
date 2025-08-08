use thiserror::Error;

#[derive(Debug, Error)]
pub enum JsonToMoonlinkRowError {
    #[error("missing field: {0}")]
    MissingField(String),
    #[error("type mismatch for field: {0}")]
    TypeMismatch(String),
    #[error("invalid value for field: {0}")]
    InvalidValue(String),
    #[error("invalid date format for field {0}: {1}")]
    InvalidDateFormat(String, String),
    #[error("invalid time format for field {0}: {1}")]
    InvalidTimeFormat(String, String),
    #[error("invalid timestamp format for field {0}: {1}")]
    InvalidTimestampFormat(String, String),
    #[error("serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
}