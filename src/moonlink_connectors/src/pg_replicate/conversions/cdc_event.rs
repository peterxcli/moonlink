use core::str;
use std::{collections::HashMap, str::Utf8Error};

use postgres_replication::protocol::{
    BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, PrimaryKeepAliveBody,
    RelationBody, ReplicationMessage, StreamAbortBody, StreamCommitBody, StreamStartBody,
    StreamStopBody, TupleData, TypeBody, UpdateBody,
};
use thiserror::Error;

use crate::pg_replicate::table::{ColumnSchema, SrcTableId, TableSchema};

use super::{
    table_row::TableRow,
    text::{FromTextError, TextFormatConverter},
    Cell,
};

#[derive(Debug, Error)]
pub enum CdcEventConversionError {
    #[error("message not supported")]
    MessageNotSupported,

    #[error("unknown replication message")]
    UnknownReplicationMessage,

    #[error("binary format not yet supported")]
    BinaryFormatNotSupported,

    #[error("unsupported type: {0}")]
    UnsupportedType(String),

    #[error("missing tuple in delete body")]
    MissingTupleInDeleteBody,

    #[error("schema missing for table id {0}")]
    MissingSchema(SrcTableId),

    #[error("from bytes error: {0}")]
    FromBytes(#[from] FromTextError),

    #[error("invalid string value")]
    InvalidStr(#[from] Utf8Error),
}

pub struct CdcEventConverter;

impl CdcEventConverter {
    fn extract_ddl_event(
        column_schemas: &[ColumnSchema],
        tuple_data: &[TupleData],
    ) -> Result<DdlEvent, CdcEventConversionError> {
        let mut query = String::new();
        let mut tags = Vec::new();
        let mut timestamp = None;

        for (i, column_schema) in column_schemas.iter().enumerate() {
            match column_schema.name.as_str() {
                "query" => {
                    if let TupleData::Text(bytes) = &tuple_data[i] {
                        query = str::from_utf8(&bytes[..])?.to_string();
                    }
                }
                "tags" => {
                    if let TupleData::Text(bytes) = &tuple_data[i] {
                        let tags_str = str::from_utf8(&bytes[..])?;
                        // Parse PostgreSQL array format {tag1,tag2,...}
                        if tags_str.starts_with('{') && tags_str.ends_with('}') {
                            let inner = &tags_str[1..tags_str.len() - 1];
                            tags = inner.split(',').map(|s| s.trim().to_string()).collect();
                        }
                    }
                }
                "created_at" => {
                    if let TupleData::Text(bytes) = &tuple_data[i] {
                        // Try to parse timestamp
                        // For now, just store as a placeholder
                        timestamp = Some(0);
                    }
                }
                _ => {}
            }
        }

        if query.is_empty() {
            return Err(CdcEventConversionError::MessageNotSupported);
        }

        Ok(DdlEvent {
            query,
            tags,
            table_id: None,
            timestamp,
        })
    }

    fn try_from_tuple_data_slice(
        column_schemas: &[ColumnSchema],
        tuple_data: &[TupleData],
    ) -> Result<TableRow, CdcEventConversionError> {
        let mut values = Vec::with_capacity(column_schemas.len());

        for (i, column_schema) in column_schemas.iter().enumerate() {
            let cell = match &tuple_data[i] {
                TupleData::Null => Cell::Null,
                TupleData::UnchangedToast => TextFormatConverter::default_value(&column_schema.typ),
                TupleData::Text(bytes) => {
                    let str = str::from_utf8(&bytes[..])?;
                    TextFormatConverter::try_from_str(&column_schema.typ, str)?
                }
            };
            values.push(cell);
        }

        Ok(TableRow { values })
    }

    fn try_from_insert_body(
        src_table_id: SrcTableId,
        column_schemas: &[ColumnSchema],
        insert_body: InsertBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let row =
            Self::try_from_tuple_data_slice(column_schemas, insert_body.tuple().tuple_data())?;

        Ok(CdcEvent::Insert((src_table_id, row, insert_body.xid())))
    }

    // TODO: handle when identity columns are changed
    fn try_from_update_body(
        src_table_id: SrcTableId,
        column_schemas: &[ColumnSchema],
        update_body: UpdateBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let old_row = update_body
            .old_tuple()
            .map(|tuple| Self::try_from_tuple_data_slice(column_schemas, tuple.tuple_data()))
            .transpose()?;
        let new_row =
            Self::try_from_tuple_data_slice(column_schemas, update_body.new_tuple().tuple_data())?;

        Ok(CdcEvent::Update((
            src_table_id,
            old_row,
            new_row,
            update_body.xid(),
        )))
    }

    fn try_from_delete_body(
        src_table_id: SrcTableId,
        column_schemas: &[ColumnSchema],
        delete_body: DeleteBody,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        let tuple = delete_body
            .key_tuple()
            .or(delete_body.old_tuple())
            .ok_or(CdcEventConversionError::MissingTupleInDeleteBody)?;

        let row = Self::try_from_tuple_data_slice(column_schemas, tuple.tuple_data())?;

        Ok(CdcEvent::Delete((src_table_id, row, delete_body.xid())))
    }

    pub fn try_from(
        value: ReplicationMessage<LogicalReplicationMessage>,
        table_schemas: &HashMap<SrcTableId, TableSchema>,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        match value {
            ReplicationMessage::XLogData(xlog_data) => match xlog_data.into_data() {
                LogicalReplicationMessage::Begin(begin_body) => Ok(CdcEvent::Begin(begin_body)),
                LogicalReplicationMessage::Commit(commit_body) => Ok(CdcEvent::Commit(commit_body)),
                LogicalReplicationMessage::Origin(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                LogicalReplicationMessage::Relation(relation_body) => {
                    Ok(CdcEvent::Relation(relation_body))
                }
                LogicalReplicationMessage::Type(type_body) => Ok(CdcEvent::Type(type_body)),
                LogicalReplicationMessage::Insert(insert_body) => {
                    let table_id = insert_body.rel_id();

                    // Check if this is a DDL event from mooncake.ddl_logs table
                    if let Some(schema) = table_schemas.get(&table_id) {
                        if (schema.table_name.schema == "mooncake"
                            && schema.table_name.name == "ddl_logs")
                            || (schema.table_name.name == "ddl_logs"
                                && schema.column_schemas.iter().any(|c| c.name == "query"))
                        {
                            // This is a DDL event, extract the DDL information
                            if let Ok(ddl_event) = Self::extract_ddl_event(
                                &schema.column_schemas,
                                insert_body.tuple().tuple_data(),
                            ) {
                                return Ok(CdcEvent::Ddl(ddl_event));
                            }
                        }
                    }

                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(CdcEventConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    Ok(Self::try_from_insert_body(
                        table_id,
                        column_schemas,
                        insert_body,
                    )?)
                }
                LogicalReplicationMessage::Update(update_body) => {
                    let table_id = update_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(CdcEventConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    Ok(Self::try_from_update_body(
                        table_id,
                        column_schemas,
                        update_body,
                    )?)
                }
                LogicalReplicationMessage::Delete(delete_body) => {
                    let table_id = delete_body.rel_id();
                    let column_schemas = &table_schemas
                        .get(&table_id)
                        .ok_or(CdcEventConversionError::MissingSchema(table_id))?
                        .column_schemas;
                    Ok(Self::try_from_delete_body(
                        table_id,
                        column_schemas,
                        delete_body,
                    )?)
                }
                LogicalReplicationMessage::Truncate(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                LogicalReplicationMessage::StreamStart(stream_start_body) => {
                    Ok(CdcEvent::StreamStart(stream_start_body))
                }
                LogicalReplicationMessage::StreamStop(stream_stop_body) => {
                    Ok(CdcEvent::StreamStop(stream_stop_body))
                }
                LogicalReplicationMessage::StreamCommit(stream_commit_body) => {
                    Ok(CdcEvent::StreamCommit(stream_commit_body))
                }
                LogicalReplicationMessage::StreamAbort(stream_abort_body) => {
                    Ok(CdcEvent::StreamAbort(stream_abort_body))
                }
                _ => Err(CdcEventConversionError::UnknownReplicationMessage),
            },
            ReplicationMessage::PrimaryKeepAlive(primary_keepalive_body) => {
                Ok(CdcEvent::PrimaryKeepAlive(primary_keepalive_body))
            }
            _ => Err(CdcEventConversionError::UnknownReplicationMessage),
        }
    }
}

#[derive(Debug)]
pub enum CdcEvent {
    Begin(BeginBody),
    Commit(CommitBody),
    Insert((SrcTableId, TableRow, Option<u32>)),
    Update((SrcTableId, Option<TableRow>, TableRow, Option<u32>)),
    Delete((SrcTableId, TableRow, Option<u32>)),
    Relation(RelationBody),
    Type(TypeBody),
    PrimaryKeepAlive(PrimaryKeepAliveBody),
    StreamStart(StreamStartBody),
    StreamStop(StreamStopBody),
    StreamCommit(StreamCommitBody),
    StreamAbort(StreamAbortBody),
    Ddl(DdlEvent),
}

#[derive(Debug, Clone)]
pub struct DdlEvent {
    pub query: String,
    pub tags: Vec<String>,
    pub table_id: Option<SrcTableId>,
    pub timestamp: Option<i64>,
}
