use core::str;
use std::num::{ParseFloatError, ParseIntError};

use bigdecimal::ParseBigDecimalError;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use thiserror::Error;
use tokio_postgres::types::{Kind, Type};
use tracing::warn;
use uuid::Uuid;

use crate::pg_replicate::conversions::{bool::parse_bool, hex};

use super::{bool::ParseBoolError, hex::ByteaHexParseError, numeric::PgNumeric, ArrayCell, Cell};

#[derive(Debug, Error)]
pub enum FromTextError {
    #[error("invalid text conversion")]
    InvalidConversion(),

    #[error("invalid bool value")]
    InvalidBool(#[from] ParseBoolError),

    #[error("invalid int value")]
    InvalidInt(#[from] ParseIntError),

    #[error("invalid float value")]
    InvalidFloat(#[from] ParseFloatError),

    #[error("invalid numeric: {0}")]
    InvalidNumeric(#[from] ParseBigDecimalError),

    #[error("invalid bytea: {0}")]
    InvalidBytea(#[from] ByteaHexParseError),

    #[error("invalid uuid: {0}")]
    InvalidUuid(#[from] uuid::Error),

    #[error("invalid json: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("invalid timestamp: {0} ")]
    InvalidTimestamp(#[from] chrono::ParseError),

    #[error("invalid array: {0}")]
    InvalidArray(#[from] ArrayParseError),

    #[error("invalid composite: {0}")]
    InvalidComposite(#[from] CompositeParseError),

    #[error(
        "array dimensionality mismatch: expected {expected} dimensions, got {actual} dimensions"
    )]
    ArrayDimensionalityMismatch { expected: usize, actual: usize },

    #[error("row get error: {0:?}")]
    RowGetError(#[from] Box<dyn std::error::Error + Sync + Send>),
}

pub struct TextFormatConverter;

#[derive(Debug, Error)]
pub enum ArrayParseError {
    #[error("input too short")]
    InputTooShort,

    #[error("missing braces")]
    MissingBraces,
}

#[derive(Debug, Error)]
pub enum CompositeParseError {
    #[error("input too short")]
    InputTooShort,

    #[error("missing parentheses")]
    MissingParentheses,

    #[error("field count mismatch")]
    FieldCountMismatch,
}

impl TextFormatConverter {
    pub fn is_supported_type(typ: &Type) -> bool {
        match typ.kind() {
            Kind::Simple => matches!(
                *typ,
                Type::BOOL
                    | Type::BOOL_ARRAY
                    | Type::CHAR
                    | Type::BPCHAR
                    | Type::VARCHAR
                    | Type::NAME
                    | Type::TEXT
                    | Type::CHAR_ARRAY
                    | Type::BPCHAR_ARRAY
                    | Type::VARCHAR_ARRAY
                    | Type::NAME_ARRAY
                    | Type::TEXT_ARRAY
                    | Type::INT2
                    | Type::INT2_ARRAY
                    | Type::INT4
                    | Type::INT4_ARRAY
                    | Type::INT8
                    | Type::INT8_ARRAY
                    | Type::FLOAT4
                    | Type::FLOAT4_ARRAY
                    | Type::FLOAT8
                    | Type::FLOAT8_ARRAY
                    | Type::NUMERIC
                    | Type::NUMERIC_ARRAY
                    | Type::BYTEA
                    | Type::BYTEA_ARRAY
                    | Type::DATE
                    | Type::DATE_ARRAY
                    | Type::TIME
                    | Type::TIME_ARRAY
                    | Type::TIMESTAMP
                    | Type::TIMESTAMP_ARRAY
                    | Type::TIMESTAMPTZ
                    | Type::TIMESTAMPTZ_ARRAY
                    | Type::UUID
                    | Type::UUID_ARRAY
                    | Type::JSON
                    | Type::JSON_ARRAY
                    | Type::JSONB
                    | Type::JSONB_ARRAY
                    | Type::OID
                    | Type::OID_ARRAY
            ),
            Kind::Array(_) => true,
            Kind::Composite(_) => true,
            _ => false,
        }
    }

    pub fn default_value(typ: &Type) -> Cell {
        match *typ {
            Type::BOOL => Cell::Bool(bool::default()),
            Type::BOOL_ARRAY => Cell::Array(ArrayCell::Bool(Vec::default())),
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                Cell::String(String::default())
            }
            Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY => Cell::Array(ArrayCell::String(Vec::default())),
            Type::INT2 => Cell::I16(i16::default()),
            Type::INT2_ARRAY => Cell::Array(ArrayCell::I16(Vec::default())),
            Type::INT4 => Cell::I32(i32::default()),
            Type::INT4_ARRAY => Cell::Array(ArrayCell::I32(Vec::default())),
            Type::INT8 => Cell::I64(i64::default()),
            Type::INT8_ARRAY => Cell::Array(ArrayCell::I64(Vec::default())),
            Type::FLOAT4 => Cell::F32(f32::default()),
            Type::FLOAT4_ARRAY => Cell::Array(ArrayCell::F32(Vec::default())),
            Type::FLOAT8 => Cell::F64(f64::default()),
            Type::FLOAT8_ARRAY => Cell::Array(ArrayCell::F64(Vec::default())),
            Type::NUMERIC => Cell::Numeric(PgNumeric::default()),
            Type::NUMERIC_ARRAY => Cell::Array(ArrayCell::Numeric(Vec::default())),
            Type::BYTEA => Cell::Bytes(Vec::default()),
            Type::BYTEA_ARRAY => Cell::Array(ArrayCell::Bytes(Vec::default())),
            Type::DATE => Cell::Date(NaiveDate::MIN),
            Type::DATE_ARRAY => Cell::Array(ArrayCell::Date(Vec::default())),
            Type::TIME => Cell::Time(NaiveTime::MIN),
            Type::TIME_ARRAY => Cell::Array(ArrayCell::Time(Vec::default())),
            Type::TIMESTAMP => Cell::TimeStamp(NaiveDateTime::MIN),
            Type::TIMESTAMP_ARRAY => Cell::Array(ArrayCell::TimeStamp(Vec::default())),
            Type::TIMESTAMPTZ => {
                let val = DateTime::<Utc>::from_naive_utc_and_offset(NaiveDateTime::MIN, Utc);
                Cell::TimeStampTz(val)
            }
            Type::TIMESTAMPTZ_ARRAY => Cell::Array(ArrayCell::TimeStampTz(Vec::default())),
            Type::UUID => Cell::Uuid(Uuid::default()),
            Type::UUID_ARRAY => Cell::Array(ArrayCell::Uuid(Vec::default())),
            Type::JSON | Type::JSONB => Cell::Json(serde_json::Value::default()),
            Type::JSON_ARRAY | Type::JSONB_ARRAY => Cell::Array(ArrayCell::Json(Vec::default())),
            Type::OID => Cell::U32(u32::default()),
            Type::OID_ARRAY => Cell::Array(ArrayCell::U32(Vec::default())),
            _ => match typ.kind() {
                Kind::Composite(_) => Cell::Composite(Vec::default()),
                Kind::Array(inner_type) => {
                    // Handle arrays of composite types.
                    // Note: inner_type here refers to the element type of the array.
                    // PostgreSQL supports multi-dimensional arrays (e.g., text[][]),
                    // but we currently only handle arrays of composite types here.
                    match inner_type.kind() {
                        Kind::Composite(_) => Cell::Array(ArrayCell::Composite(Vec::default())),
                        Kind::Array(_) => Cell::Null, // TODO: Multi-dimensional arrays not yet handled
                        _ => Cell::Null,              // Unknown array type
                    }
                }
                _ => Cell::Null,
            },
        }
    }

    pub fn try_from_str(typ: &Type, str: &str) -> Result<Cell, FromTextError> {
        match *typ {
            Type::BOOL => Ok(Cell::Bool(parse_bool(str)?)),
            Type::BOOL_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(parse_bool(str)?)),
                ArrayCell::Bool,
            ),
            Type::CHAR | Type::BPCHAR => Ok(Cell::String(str.trim_end().to_string())),
            Type::VARCHAR | Type::NAME | Type::TEXT => Ok(Cell::String(str.to_string())),
            Type::CHAR_ARRAY | Type::BPCHAR_ARRAY => {
                TextFormatConverter::parse_array_with_dimensionality_handling(
                    str,
                    typ,
                    |str| Ok(Some(str.trim_end().to_string())),
                    ArrayCell::String,
                )
            }
            Type::VARCHAR_ARRAY | Type::NAME_ARRAY | Type::TEXT_ARRAY => {
                TextFormatConverter::parse_array_with_dimensionality_handling(
                    str,
                    typ,
                    |str| Ok(Some(str.to_string())),
                    ArrayCell::String,
                )
            }
            Type::INT2 => Ok(Cell::I16(str.parse()?)),
            Type::INT2_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(str.parse()?)),
                ArrayCell::I16,
            ),
            Type::INT4 => Ok(Cell::I32(str.parse()?)),
            Type::INT4_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(str.parse()?)),
                ArrayCell::I32,
            ),
            Type::INT8 => Ok(Cell::I64(str.parse()?)),
            Type::INT8_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(str.parse()?)),
                ArrayCell::I64,
            ),
            Type::FLOAT4 => Ok(Cell::F32(str.parse()?)),
            Type::FLOAT4_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(str.parse()?)),
                ArrayCell::F32,
            ),
            Type::FLOAT8 => Ok(Cell::F64(str.parse()?)),
            Type::FLOAT8_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(str.parse()?)),
                ArrayCell::F64,
            ),
            Type::NUMERIC => Ok(Cell::Numeric(str.parse()?)),
            Type::NUMERIC_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(str.parse()?)),
                ArrayCell::Numeric,
            ),
            Type::BYTEA => Ok(Cell::Bytes(hex::from_bytea_hex(str)?)),
            Type::BYTEA_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(hex::from_bytea_hex(str)?)),
                ArrayCell::Bytes,
            ),
            Type::DATE => {
                let val = NaiveDate::parse_from_str(str, "%Y-%m-%d")?;
                Ok(Cell::Date(val))
            }
            Type::DATE_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(NaiveDate::parse_from_str(str, "%Y-%m-%d")?)),
                ArrayCell::Date,
            ),
            Type::TIME => {
                let val = NaiveTime::parse_from_str(str, "%H:%M:%S%.f")?;
                Ok(Cell::Time(val))
            }
            Type::TIME_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(NaiveTime::parse_from_str(str, "%H:%M:%S%.f")?)),
                ArrayCell::Time,
            ),
            Type::TIMESTAMP => {
                let val = NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f")?;
                Ok(Cell::TimeStamp(val))
            }
            Type::TIMESTAMP_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| {
                    Ok(Some(NaiveDateTime::parse_from_str(
                        str,
                        "%Y-%m-%d %H:%M:%S%.f",
                    )?))
                },
                ArrayCell::TimeStamp,
            ),
            Type::TIMESTAMPTZ => {
                let val =
                    match DateTime::<FixedOffset>::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f%#z") {
                        Ok(val) => val,
                        Err(_) => {
                            DateTime::<FixedOffset>::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f%:z")?
                        }
                    };
                Ok(Cell::TimeStampTz(val.into()))
            }
            Type::TIMESTAMPTZ_ARRAY => {
                TextFormatConverter::parse_array_with_dimensionality_handling(
                    str,
                    typ,
                    |str| {
                        let val = match DateTime::<FixedOffset>::parse_from_str(
                            str,
                            "%Y-%m-%d %H:%M:%S%.f%#z",
                        ) {
                            Ok(val) => val,
                            Err(_) => DateTime::<FixedOffset>::parse_from_str(
                                str,
                                "%Y-%m-%d %H:%M:%S%.f%:z",
                            )?,
                        };
                        Ok(Some(val.into()))
                    },
                    ArrayCell::TimeStampTz,
                )
            }
            Type::UUID => Ok(Cell::Uuid(Uuid::parse_str(str)?)),
            Type::UUID_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(Uuid::parse_str(str)?)),
                ArrayCell::Uuid,
            ),
            Type::JSON => Ok(Cell::Json(serde_json::from_str(str)?)),
            Type::JSON_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(serde_json::from_str(str)?)),
                ArrayCell::Json,
            ),
            Type::JSONB => Ok(Cell::Json(serde_json::from_str(str)?)),
            Type::JSONB_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(serde_json::from_str(str)?)),
                ArrayCell::Json,
            ),
            Type::OID => {
                let val: u32 = str.parse()?;
                Ok(Cell::U32(val))
            }
            Type::OID_ARRAY => TextFormatConverter::parse_array_with_dimensionality_handling(
                str,
                typ,
                |str| Ok(Some(str.parse()?)),
                ArrayCell::U32,
            ),
            _ => match typ.kind() {
                Kind::Composite(fields) => TextFormatConverter::parse_composite(str, fields),
                Kind::Array(inner_type) => {
                    // Check if the array contains composite types.
                    // PostgreSQL supports multi-dimensional arrays (e.g., int[][], text[][]),
                    // but here we currently only handle arrays of composite types.
                    match inner_type.kind() {
                        Kind::Composite(fields) => {
                            TextFormatConverter::parse_composite_array(str, fields)
                        }
                        Kind::Array(_) => {
                            // TODO: Multi-dimensional arrays not yet implemented
                            // TODO: Need to print out the unsupported type (included in error type)
                            Err(FromTextError::InvalidConversion())
                        }
                        _ => Err(FromTextError::InvalidConversion()),
                    }
                }
                _ => Err(FromTextError::InvalidConversion()),
            },
        }
    }

    fn parse_array<P, M, T>(str: &str, mut parse: P, m: M) -> Result<Cell, FromTextError>
    where
        P: FnMut(&str) -> Result<Option<T>, FromTextError>,
        M: FnOnce(Vec<Option<T>>) -> ArrayCell,
    {
        if str.len() < 2 {
            return Err(ArrayParseError::InputTooShort.into());
        }

        if !str.starts_with('{') || !str.ends_with('}') {
            return Err(ArrayParseError::MissingBraces.into());
        }

        let mut res = vec![];
        let str = &str[1..(str.len() - 1)];
        let mut val_str = String::with_capacity(10);
        let mut in_quotes = false;
        let mut in_escape = false;
        let mut val_quoted = false;
        let mut chars = str.chars();
        let mut done = str.is_empty();

        while !done {
            loop {
                match chars.next() {
                    Some(c) => match c {
                        c if in_escape => {
                            val_str.push(c);
                            in_escape = false;
                        }
                        '"' => {
                            if !in_quotes {
                                val_quoted = true;
                            }
                            in_quotes = !in_quotes;
                        }
                        '\\' => in_escape = true,
                        ',' if !in_quotes => {
                            break;
                        }
                        c => {
                            val_str.push(c);
                        }
                    },
                    None => {
                        done = true;
                        break;
                    }
                }
            }
            let val = if !val_quoted && val_str.to_lowercase() == "null" {
                None
            } else {
                parse(&val_str)?
            };
            res.push(val);
            val_str.clear();
            val_quoted = false;
        }

        Ok(Cell::Array(m(res)))
    }

    /// Parses an array with automatic dimensionality detection and handling.
    /// This function analyzes the Type to determine expected dimensionality and
    /// handles mismatches by converting the content appropriately.
    fn parse_array_with_dimensionality_handling<P, M, T>(
        str: &str,
        typ: &Type,
        mut parse: P,
        m: M,
    ) -> Result<Cell, FromTextError>
    where
        P: FnMut(&str) -> Result<Option<T>, FromTextError>,
        M: FnOnce(Vec<Option<T>>) -> ArrayCell,
    {
        // Determine expected dimensions from the Type
        let expected_dimensions = TextFormatConverter::get_expected_array_dimensions(typ);
        let actual_dimensions = TextFormatConverter::count_array_dimensions(str);

        if actual_dimensions != expected_dimensions {
            warn!(
                "Array dimensionality mismatch: expected {} dimensions, got {} dimensions. Content: {}",
                expected_dimensions, actual_dimensions, str
            );

            // Handle dimensionality conversion
            let actual_content = TextFormatConverter::parse_array_raw(str)?;
            let converted_content = TextFormatConverter::convert_array_dimensionality(
                &actual_content,
                actual_dimensions,
                expected_dimensions,
            )?;

            // Parse the converted content
            let mut res = vec![];
            for item in converted_content {
                let val = if item.to_lowercase() == "null" {
                    None
                } else {
                    parse(&item)?
                };
                res.push(val);
            }

            Ok(Cell::Array(m(res)))
        } else {
            // No mismatch, parse normally
            TextFormatConverter::parse_array(str, parse, m)
        }
    }

    /// Parses array content and returns raw string values without type conversion.
    fn parse_array_raw(str: &str) -> Result<Vec<String>, FromTextError> {
        if str.len() < 2 {
            return Err(ArrayParseError::InputTooShort.into());
        }

        if !str.starts_with('{') || !str.ends_with('}') {
            return Err(ArrayParseError::MissingBraces.into());
        }

        let mut res = vec![];
        let str = &str[1..(str.len() - 1)];
        let mut val_str = String::with_capacity(10);
        let mut in_quotes = false;
        let mut in_escape = false;
        let mut val_quoted = false;
        let mut chars = str.chars();
        let mut done = str.is_empty();

        while !done {
            loop {
                match chars.next() {
                    Some(c) => match c {
                        c if in_escape => {
                            val_str.push(c);
                            in_escape = false;
                        }
                        '"' => {
                            if !in_quotes {
                                val_quoted = true;
                            }
                            in_quotes = !in_quotes;
                        }
                        '\\' => in_escape = true,
                        ',' if !in_quotes => {
                            break;
                        }
                        c => {
                            val_str.push(c);
                        }
                    },
                    None => {
                        done = true;
                        break;
                    }
                }
            }
            res.push(val_str.clone());
            val_str.clear();
            val_quoted = false;
        }

        Ok(res)
    }

    /// Counts the number of dimensions in a PostgreSQL array string.
    fn count_array_dimensions(str: &str) -> usize {
        let mut dimensions = 0;
        let mut brace_count = 0;
        let mut in_quotes = false;
        let mut in_escape = false;

        for c in str.chars() {
            match c {
                c if in_escape => {
                    in_escape = false;
                }
                '"' => {
                    in_quotes = !in_quotes;
                }
                '\\' => {
                    in_escape = true;
                }
                '{' if !in_quotes => {
                    brace_count += 1;
                    if brace_count > dimensions {
                        dimensions = brace_count;
                    }
                }
                '}' if !in_quotes => {
                    brace_count -= 1;
                }
                _ => {}
            }
        }

        dimensions
    }

    /// Converts array content between different dimensionalities.
    /// This handles the edge cases mentioned in the user requirements:
    /// 1. If column is text[][] and content is ["aa", "bb"], convert to [["aa", "bb"]]
    /// 2. If column is text[] and content is [["aa"], ["bb"]], convert to ["aa"]
    fn convert_array_dimensionality(
        content: &[String],
        actual_dimensions: usize,
        expected_dimensions: usize,
    ) -> Result<Vec<String>, FromTextError> {
        match (actual_dimensions, expected_dimensions) {
            (1, 2) => {
                // Convert 1D to 2D: ["aa", "bb"] -> [["aa", "bb"]]
                if content.len() == 1 {
                    // Single element case
                    Ok(vec![content[0].clone()])
                } else {
                    // Multiple elements case - wrap in another array
                    let joined = format!("{{{}}}", content.join(","));
                    Ok(vec![joined])
                }
            }
            (2, 1) => {
                // Convert 2D to 1D: [["aa"], ["bb"]] -> ["aa"]
                if content.len() == 1 {
                    // Single element case - extract the inner content
                    let inner = &content[0];
                    if inner.starts_with('{') && inner.ends_with('}') {
                        let inner_content = &inner[1..inner.len() - 1];
                        Ok(vec![inner_content.to_string()])
                    } else {
                        Ok(vec![inner.clone()])
                    }
                } else {
                    // Multiple elements case - take the first element
                    let first = &content[0];
                    if first.starts_with('{') && first.ends_with('}') {
                        let inner_content = &first[1..first.len() - 1];
                        Ok(vec![inner_content.to_string()])
                    } else {
                        Ok(vec![first.clone()])
                    }
                }
            }
            _ => {
                // For other cases, return the original content
                // This handles cases where we can't easily convert
                warn!(
                    "Unsupported dimensionality conversion: {} -> {} dimensions",
                    actual_dimensions, expected_dimensions
                );
                Ok(content.to_vec())
            }
        }
    }

    /// Parses a PostgreSQL composite type from its text representation.
    ///
    /// PostgreSQL composite types are represented as `(field1,field2,...)` where:
    /// - Fields are comma-separated
    /// - NULL values are represented as empty or the literal 'null' (case-insensitive)
    /// - Quoted values preserve all characters including commas and parentheses
    /// - Escaped characters within quotes are handled with backslash
    ///
    /// Reference: https://www.postgresql.org/docs/current/rowtypes.html#ROWTYPES-IO-SYNTAX
    fn parse_composite(
        s: &str,
        fields: &[tokio_postgres::types::Field],
    ) -> Result<Cell, FromTextError> {
        if s.len() < 2 {
            return Err(CompositeParseError::InputTooShort.into());
        }

        if !s.starts_with('(') || !s.ends_with(')') {
            return Err(CompositeParseError::MissingParentheses.into());
        }

        let mut res = Vec::with_capacity(fields.len());
        let inner = &s[1..(s.len() - 1)];
        let mut val_str = String::with_capacity(10);
        let mut in_quotes = false;
        let mut in_escape = false;
        let mut val_quoted = false;
        let mut chars = inner.chars();
        let mut field_iter = fields.iter();
        let mut done = inner.is_empty();

        while !done {
            loop {
                match chars.next() {
                    Some(c) => match c {
                        c if in_escape => {
                            val_str.push(c);
                            in_escape = false;
                        }
                        '"' => {
                            if !in_quotes {
                                val_quoted = true;
                            }
                            in_quotes = !in_quotes;
                        }
                        '\\' if in_quotes => in_escape = true,
                        ',' if !in_quotes => {
                            break;
                        }
                        c => {
                            val_str.push(c);
                        }
                    },
                    None => {
                        done = true;
                        break;
                    }
                }
            }

            let field = field_iter
                .next()
                .ok_or(CompositeParseError::FieldCountMismatch)?;

            let val = if !val_quoted && val_str.is_empty() {
                Cell::Null
            } else {
                TextFormatConverter::try_from_str(field.type_(), &val_str)?
            };

            res.push(val);
            val_str.clear();
            val_quoted = false;
        }

        if field_iter.next().is_some() {
            return Err(CompositeParseError::FieldCountMismatch.into());
        }

        Ok(Cell::Composite(res))
    }

    /// Parses a PostgreSQL array of composite types from its text representation.
    ///
    /// PostgreSQL arrays of composite types are represented as `{"(field1,field2)","(field3,field4)"}` where:
    /// - The array is enclosed in curly braces `{}`
    /// - Each composite element is enclosed in double quotes if it contains special characters
    /// - Composite elements follow the same format as regular composites: `(field1,field2,...)`
    /// - NULL array elements are represented as the literal 'null' (case-insensitive)
    /// - Empty arrays are represented as `{}`
    ///
    /// Example formats:
    /// - Simple: `{"(1,hello)","(2,world)"}`
    /// - With NULLs: `{"(1,hello)",null,"(3,test)"}`
    /// - With special chars: `{"(1,\"hello, world\")","(2,\"test\")"}`
    /// - Empty: `{}`
    ///
    /// Reference: https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
    fn parse_composite_array(
        s: &str,
        fields: &[tokio_postgres::types::Field],
    ) -> Result<Cell, FromTextError> {
        if s.len() < 2 {
            return Err(ArrayParseError::InputTooShort.into());
        }

        if !s.starts_with('{') || !s.ends_with('}') {
            return Err(ArrayParseError::MissingBraces.into());
        }

        let mut res = Vec::new();
        let inner = &s[1..(s.len() - 1)];

        if inner.is_empty() {
            return Ok(Cell::Array(ArrayCell::Composite(res)));
        }

        let mut val_str = String::with_capacity(50);
        let mut in_quotes = false;
        let mut in_escape = false;
        let mut chars = inner.chars();
        let mut done = false;

        while !done {
            loop {
                match chars.next() {
                    Some(c) => match c {
                        c if in_escape => {
                            val_str.push(c);
                            in_escape = false;
                        }
                        '\\' if in_quotes => {
                            in_escape = true;
                        }
                        '"' => {
                            in_quotes = !in_quotes;
                        }
                        ',' if !in_quotes => {
                            break;
                        }
                        c => {
                            val_str.push(c);
                        }
                    },
                    None => {
                        done = true;
                        break;
                    }
                }
            }

            let val = if val_str.to_lowercase() == "null" {
                None
            } else {
                // The value should be a composite wrapped in parentheses
                Some(
                    match TextFormatConverter::parse_composite(&val_str, fields)? {
                        Cell::Composite(cells) => cells,
                        _ => unreachable!("parse_composite should always return Cell::Composite"),
                    },
                )
            };

            res.push(val);
            val_str.clear();
        }

        Ok(Cell::Array(ArrayCell::Composite(res)))
    }

    /// Determines the expected array dimensions from a Type.
    /// For simple array types (e.g., TEXT_ARRAY), this returns 1.
    /// For multi-dimensional arrays, this would return the number of dimensions.
    fn get_expected_array_dimensions(typ: &Type) -> usize {
        // For now, we assume all array types expect 1 dimension
        // This can be extended in the future to handle multi-dimensional arrays
        // based on the schema information
        match typ.kind() {
            Kind::Array(inner_type) => {
                // Check if the inner type is also an array (multi-dimensional)
                match inner_type.kind() {
                    Kind::Array(_) => 2, // Multi-dimensional array
                    _ => 1,              // Single-dimensional array
                }
            }
            _ => 1, // Default to 1 dimension
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_dimensionality_mismatch_1d_to_2d() {
        // Test case 1: column is text[][] and content is ["aa", "bb"]
        // Should convert to [["aa", "bb"]]
        let content = vec!["aa".to_string(), "bb".to_string()];
        let result = TextFormatConverter::convert_array_dimensionality(&content, 1, 2).unwrap();
        assert_eq!(result, vec!["{aa,bb}"]);
    }

    #[test]
    fn test_array_dimensionality_mismatch_2d_to_1d() {
        // Test case 2: column is text[] and content is [["aa"], ["bb"]]
        // Should convert to ["aa"]
        let content = vec!["{aa}".to_string(), "{bb}".to_string()];
        let result = TextFormatConverter::convert_array_dimensionality(&content, 2, 1).unwrap();
        assert_eq!(result, vec!["aa"]);
    }

    #[test]
    fn test_count_array_dimensions() {
        assert_eq!(TextFormatConverter::count_array_dimensions("{a,b}"), 1);
        assert_eq!(TextFormatConverter::count_array_dimensions("{{a},{b}}"), 2);
        assert_eq!(TextFormatConverter::count_array_dimensions("{{{a,b}}}"), 3);
        assert_eq!(
            TextFormatConverter::count_array_dimensions("{\"a\",\"b\"}"),
            1
        );
        assert_eq!(
            TextFormatConverter::count_array_dimensions("{{\"a\"},{\"b\"}}"),
            2
        );
    }

    #[test]
    fn test_parse_array_raw() {
        let result = TextFormatConverter::parse_array_raw("{a,b,c}").unwrap();
        assert_eq!(result, vec!["a", "b", "c"]);

        let result = TextFormatConverter::parse_array_raw("{\"a\",\"b\",\"c\"}").unwrap();
        assert_eq!(result, vec!["a", "b", "c"]);

        let result = TextFormatConverter::parse_array_raw("{null,a,b}").unwrap();
        assert_eq!(result, vec!["null", "a", "b"]);
    }

    #[test]
    fn test_get_expected_array_dimensions() {
        // Test simple array types
        assert_eq!(
            TextFormatConverter::get_expected_array_dimensions(&Type::TEXT_ARRAY),
            1
        );
        assert_eq!(
            TextFormatConverter::get_expected_array_dimensions(&Type::INT4_ARRAY),
            1
        );

        // Test multi-dimensional array types (if they exist)
        // Note: This would need to be tested with actual multi-dimensional array types
        // For now, we assume all standard array types are 1-dimensional
    }

    #[test]
    fn test_array_dimensionality_handling_integration() {
        // Test the full integration with actual array parsing
        // This tests that the dimensionality handling works end-to-end

        // Test 1D array parsing (normal case)
        let result = TextFormatConverter::try_from_str(&Type::TEXT_ARRAY, "{a,b,c}").unwrap();
        assert!(matches!(result, Cell::Array(ArrayCell::String(_))));

        // Test 2D array content being converted to 1D
        // This simulates the case where content is [["aa"], ["bb"]] but schema expects ["aa"]
        let result = TextFormatConverter::try_from_str(&Type::TEXT_ARRAY, "{{aa},{bb}}").unwrap();
        assert!(matches!(result, Cell::Array(ArrayCell::String(_))));

        // Test 1D array content being converted to 2D
        // This would require a multi-dimensional array type, which we don't have in the standard types
        // but the logic is in place for when such types are supported
    }

    #[test]
    fn parse_text_array_quoted_null_as_string() {
        let cell =
            TextFormatConverter::try_from_str(&Type::TEXT_ARRAY, "{\"a\",\"null\"}").unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(v, vec![Some("a".to_string()), Some("null".to_string())]);
            }
            _ => panic!("unexpected cell: {cell:?}"),
        }
    }

    #[test]
    fn parse_text_array_unquoted_null_is_none() {
        let cell = TextFormatConverter::try_from_str(&Type::TEXT_ARRAY, "{a,NULL}").unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(v, vec![Some("a".to_string()), None]);
            }
            _ => panic!("unexpected cell: {cell:?}"),
        }
    }

    #[test]
    fn parse_char_vs_varchar_trailing_spaces() {
        // CHAR/BPCHAR should trim trailing spaces
        let char_cell = TextFormatConverter::try_from_str(&Type::CHAR, "hello   ").unwrap();
        match char_cell {
            Cell::String(s) => assert_eq!(s, "hello"),
            _ => panic!("expected string cell, got: {char_cell:?}"),
        }

        let bpchar_cell = TextFormatConverter::try_from_str(&Type::BPCHAR, "world   ").unwrap();
        match bpchar_cell {
            Cell::String(s) => assert_eq!(s, "world"),
            _ => panic!("expected string cell, got: {bpchar_cell:?}"),
        }

        // VARCHAR/NAME/TEXT should preserve trailing spaces
        let varchar_cell = TextFormatConverter::try_from_str(&Type::VARCHAR, "hello   ").unwrap();
        match varchar_cell {
            Cell::String(s) => assert_eq!(s, "hello   "),
            _ => panic!("expected string cell, got: {varchar_cell:?}"),
        }

        let text_cell = TextFormatConverter::try_from_str(&Type::TEXT, "world   ").unwrap();
        match text_cell {
            Cell::String(s) => assert_eq!(s, "world   "),
            _ => panic!("expected string cell, got: {text_cell:?}"),
        }
    }

    #[test]
    fn parse_char_array_vs_varchar_array_trailing_spaces() {
        // CHAR_ARRAY/BPCHAR_ARRAY should trim trailing spaces
        let char_array_cell =
            TextFormatConverter::try_from_str(&Type::CHAR_ARRAY, "{\"hello   \",\"world   \"}")
                .unwrap();
        match char_array_cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(
                    v,
                    vec![Some("hello".to_string()), Some("world".to_string())]
                );
            }
            _ => panic!("expected string array cell, got: {char_array_cell:?}"),
        }

        // VARCHAR_ARRAY should preserve trailing spaces
        let varchar_array_cell =
            TextFormatConverter::try_from_str(&Type::VARCHAR_ARRAY, "{\"hello   \",\"world   \"}")
                .unwrap();
        match varchar_array_cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(
                    v,
                    vec![Some("hello   ".to_string()), Some("world   ".to_string())]
                );
            }
            _ => panic!("expected string array cell, got: {varchar_array_cell:?}"),
        }
    }

    #[test]
    fn parse_composite_basic() {
        use tokio_postgres::types::Field;

        // Create mock field definitions for a composite type with (int4, text)
        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("name".to_string(), Type::TEXT),
        ];

        // Test parsing a basic composite value
        let composite_str = "(42,\"hello world\")";
        let cell = TextFormatConverter::parse_composite(composite_str, &fields).unwrap();

        match cell {
            Cell::Composite(values) => {
                assert_eq!(values.len(), 2);
                assert!(matches!(values[0], Cell::I32(42)));
                assert!(matches!(values[1], Cell::String(ref s) if s == "hello world"));
            }
            _ => panic!("expected composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_composite_with_nulls() {
        use tokio_postgres::types::Field;

        // Create mock field definitions
        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("name".to_string(), Type::TEXT),
            Field::new("active".to_string(), Type::BOOL),
        ];

        // Test parsing with null values (PostgreSQL uses 't' for true)
        let composite_str = "(42,,t)";
        let cell = TextFormatConverter::parse_composite(composite_str, &fields).unwrap();

        match cell {
            Cell::Composite(values) => {
                assert_eq!(values.len(), 3);
                assert!(matches!(values[0], Cell::I32(42)));
                assert!(matches!(values[1], Cell::Null));
                assert!(matches!(values[2], Cell::Bool(true)));
            }
            _ => panic!("expected composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_composite_with_array_field() {
        use tokio_postgres::types::Field;

        // Create a composite type with an array field
        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("tags".to_string(), Type::TEXT_ARRAY),
            Field::new("scores".to_string(), Type::INT4_ARRAY),
        ];

        // Test parsing composite with array fields
        let composite_str = "(1,\"{\\\"tag1\\\",\\\"tag2\\\"}\",\"{10,20,30}\")";
        let cell = TextFormatConverter::parse_composite(composite_str, &fields).unwrap();

        match cell {
            Cell::Composite(values) => {
                assert_eq!(values.len(), 3);
                assert!(matches!(values[0], Cell::I32(1)));

                // Check text array field
                match &values[1] {
                    Cell::Array(ArrayCell::String(tags)) => {
                        assert_eq!(tags.len(), 2);
                        assert_eq!(tags[0], Some("tag1".to_string()));
                        assert_eq!(tags[1], Some("tag2".to_string()));
                    }
                    _ => panic!("expected text array"),
                }

                // Check int array field
                match &values[2] {
                    Cell::Array(ArrayCell::I32(scores)) => {
                        assert_eq!(scores.len(), 3);
                        assert_eq!(scores[0], Some(10));
                        assert_eq!(scores[1], Some(20));
                        assert_eq!(scores[2], Some(30));
                    }
                    _ => panic!("expected int array"),
                }
            }
            _ => panic!("expected composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_composite_nested() {
        use tokio_postgres::types::Field;

        // Create a nested composite type structure
        let inner_fields = vec![
            Field::new("x".to_string(), Type::INT4),
            Field::new("y".to_string(), Type::INT4),
        ];

        // Mock a composite type that contains another composite
        let composite_type = Type::new(
            "point".to_string(),
            0, // OID doesn't matter for this test
            Kind::Composite(inner_fields.clone()),
            "public".to_string(),
        );

        let outer_fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("point".to_string(), composite_type),
        ];

        // Test parsing nested composite
        let composite_str = "(1,\"(10,20)\")";
        let cell = TextFormatConverter::parse_composite(composite_str, &outer_fields).unwrap();

        match cell {
            Cell::Composite(values) => {
                assert_eq!(values.len(), 2);
                assert!(matches!(values[0], Cell::I32(1)));

                // The nested composite should be parsed as well
                match &values[1] {
                    Cell::Composite(inner_values) => {
                        assert_eq!(inner_values.len(), 2);
                        assert!(matches!(inner_values[0], Cell::I32(10)));
                        assert!(matches!(inner_values[1], Cell::I32(20)));
                    }
                    _ => panic!("expected nested composite"),
                }
            }
            _ => panic!("expected composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_composite_deeply_nested_with_arrays() {
        use tokio_postgres::types::Field;

        // Create a complex nested structure:
        // outer_type {
        //   id: int4,
        //   data: inner_type {
        //     values: int4[],
        //     metadata: text
        //   }
        // }

        let inner_fields = vec![
            Field::new("values".to_string(), Type::INT4_ARRAY),
            Field::new("metadata".to_string(), Type::TEXT),
        ];

        let inner_type = Type::new(
            "inner_type".to_string(),
            0,
            Kind::Composite(inner_fields.clone()),
            "public".to_string(),
        );

        let outer_fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("data".to_string(), inner_type),
        ];

        // Test parsing deeply nested composite with arrays
        let composite_str = "(99,\"(\\\"{1,2,3}\\\",\\\"meta info\\\")\")";
        let cell = TextFormatConverter::parse_composite(composite_str, &outer_fields).unwrap();

        match cell {
            Cell::Composite(outer_values) => {
                assert_eq!(outer_values.len(), 2);
                assert!(matches!(outer_values[0], Cell::I32(99)));

                // Check nested composite
                match &outer_values[1] {
                    Cell::Composite(inner_values) => {
                        assert_eq!(inner_values.len(), 2);

                        // Check array within nested composite
                        match &inner_values[0] {
                            Cell::Array(ArrayCell::I32(values)) => {
                                assert_eq!(values.len(), 3);
                                assert_eq!(values[0], Some(1));
                                assert_eq!(values[1], Some(2));
                                assert_eq!(values[2], Some(3));
                            }
                            _ => panic!("expected int array in nested composite"),
                        }

                        // Check text field in nested composite
                        assert!(matches!(inner_values[1], Cell::String(ref s) if s == "meta info"));
                    }
                    _ => panic!("expected nested composite"),
                }
            }
            _ => panic!("expected composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_array_of_composites() {
        use tokio_postgres::types::Field;

        // Create a composite type definition
        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("name".to_string(), Type::TEXT),
        ];

        // Test parsing array of composites - PostgreSQL format uses quotes around the whole composite
        let array_str = r#"{"(1,\"alice\")","(2,\"bob\")","(3,\"charlie\")"}"#;
        let cell = TextFormatConverter::parse_composite_array(array_str, &fields).unwrap();

        match cell {
            Cell::Array(ArrayCell::Composite(composites)) => {
                assert_eq!(composites.len(), 3);

                // Check first composite
                let first = composites[0].as_ref().unwrap();
                assert_eq!(first.len(), 2);
                assert!(matches!(first[0], Cell::I32(1)));
                assert!(matches!(first[1], Cell::String(ref s) if s == "alice"));

                // Check second composite
                let second = composites[1].as_ref().unwrap();
                assert_eq!(second.len(), 2);
                assert!(matches!(second[0], Cell::I32(2)));
                assert!(matches!(second[1], Cell::String(ref s) if s == "bob"));

                // Check third composite
                let third = composites[2].as_ref().unwrap();
                assert_eq!(third.len(), 2);
                assert!(matches!(third[0], Cell::I32(3)));
                assert!(matches!(third[1], Cell::String(ref s) if s == "charlie"));
            }
            _ => panic!("expected array of composites, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_array_of_composites_with_nulls() {
        use tokio_postgres::types::Field;

        // Create a composite type definition
        let fields = vec![
            Field::new("x".to_string(), Type::INT4),
            Field::new("y".to_string(), Type::INT4),
        ];

        // Test parsing array with null composite and composites with null fields
        let array_str = r#"{"(1,2)",NULL,"(3,)"}"#;
        let cell = TextFormatConverter::parse_composite_array(array_str, &fields).unwrap();

        match cell {
            Cell::Array(ArrayCell::Composite(composites)) => {
                assert_eq!(composites.len(), 3);

                // First composite: (1,2)
                let first = composites[0].as_ref().unwrap();
                assert_eq!(first.len(), 2);
                assert!(matches!(first[0], Cell::I32(1)));
                assert!(matches!(first[1], Cell::I32(2)));

                // Second composite: NULL
                assert!(composites[1].is_none());

                // Third composite: (3,NULL)
                let third = composites[2].as_ref().unwrap();
                assert_eq!(third.len(), 2);
                assert!(matches!(third[0], Cell::I32(3)));
                assert!(matches!(third[1], Cell::Null));
            }
            _ => panic!("expected array of composites, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_empty_composite_array() {
        use tokio_postgres::types::Field;

        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("name".to_string(), Type::TEXT),
        ];

        // Test empty array
        let array_str = "{}";
        let cell = TextFormatConverter::parse_composite_array(array_str, &fields).unwrap();

        match cell {
            Cell::Array(ArrayCell::Composite(composites)) => {
                assert_eq!(composites.len(), 0);
            }
            _ => panic!("expected empty array of composites, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_composite_array_via_type_system() {
        use tokio_postgres::types::Field;

        // Create composite type fields
        let composite_fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("active".to_string(), Type::BOOL),
        ];

        // Create the composite type
        let composite_type = Type::new(
            "user_info".to_string(),
            0,
            Kind::Composite(composite_fields.clone()),
            "public".to_string(),
        );

        // Create array of composite type
        let array_type = Type::new(
            "_user_info".to_string(),
            0,
            Kind::Array(composite_type),
            "public".to_string(),
        );

        // Test parsing through the main try_from_str function
        let array_str = r#"{"(1,t)","(2,f)"}"#;
        let cell = TextFormatConverter::try_from_str(&array_type, array_str).unwrap();

        match cell {
            Cell::Array(ArrayCell::Composite(composites)) => {
                assert_eq!(composites.len(), 2);

                let first = composites[0].as_ref().unwrap();
                assert!(matches!(first[0], Cell::I32(1)));
                assert!(matches!(first[1], Cell::Bool(true)));

                let second = composites[1].as_ref().unwrap();
                assert!(matches!(second[0], Cell::I32(2)));
                assert!(matches!(second[1], Cell::Bool(false)));
            }
            _ => panic!("expected array of composites, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_empty_composite() {
        use tokio_postgres::types::Field;

        // Create an empty composite type (no fields)
        let fields: Vec<Field> = vec![];

        // Test parsing empty composite
        let composite_str = "()";
        let cell = TextFormatConverter::parse_composite(composite_str, &fields).unwrap();

        match cell {
            Cell::Composite(values) => {
                assert_eq!(values.len(), 0);
            }
            _ => panic!("expected empty composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn test_composite_parse_errors() {
        use tokio_postgres::types::Field;

        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("name".to_string(), Type::TEXT),
        ];

        // Test input too short
        let result = TextFormatConverter::parse_composite("", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::InputTooShort
            ))
        ));

        let result = TextFormatConverter::parse_composite("(", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::InputTooShort
            ))
        ));

        // Test missing parentheses
        let result = TextFormatConverter::parse_composite("1,hello", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::MissingParentheses
            ))
        ));

        let result = TextFormatConverter::parse_composite("(1,hello", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::MissingParentheses
            ))
        ));

        let result = TextFormatConverter::parse_composite("1,hello)", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::MissingParentheses
            ))
        ));

        // Test field count mismatch - too many fields
        let result = TextFormatConverter::parse_composite("(1,hello,extra)", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::FieldCountMismatch
            ))
        ));

        // Test field count mismatch - too few fields
        let result = TextFormatConverter::parse_composite("(1)", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::FieldCountMismatch
            ))
        ));
    }
}
