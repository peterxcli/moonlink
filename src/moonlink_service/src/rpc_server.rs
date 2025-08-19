use crate::{error::Error, Result};
use arrow_ipc::writer::StreamWriter;
use moonlink_backend::{EventOperation, EventRequest, MoonlinkBackend, REST_API_URI};
use moonlink_error::{ErrorStatus, ErrorStruct};
use moonlink_rpc::{read, write, FieldSchema, Request, Table};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::io::ErrorKind::{BrokenPipe, ConnectionReset, UnexpectedEof};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UnixListener};
use tracing::info;

/// Start the Unix socket RPC server and serve requests until the task is aborted.
pub async fn start_unix_server(
    backend: Arc<MoonlinkBackend>,
    socket_path: std::path::PathBuf,
) -> Result<()> {
    if fs::metadata(&socket_path).await.is_ok() {
        fs::remove_file(&socket_path).await?;
    }
    let listener = UnixListener::bind(&socket_path)?;
    info!(
        "Moonlink RPC server listening on Unix socket: {:?}",
        socket_path
    );

    loop {
        let (stream, _addr) = listener.accept().await?;
        let backend = Arc::clone(&backend);
        tokio::spawn(async move {
            match handle_stream(backend, stream).await {
                Err(Error::Rpc(error_struct))
                    if error_struct
                        .source()
                        .and_then(|src| src.downcast_ref::<std::io::Error>())
                        .map(|io_err| {
                            matches!(io_err.kind(), BrokenPipe | ConnectionReset | UnexpectedEof)
                        })
                        .unwrap_or(false) => {}
                Err(e) => panic!("Unexpected Unix RPC server error: {e}"),
                Ok(()) => {}
            }
        });
    }
}

/// Start the TCP socket RPC server and serve requests until the task is aborted.
pub async fn start_tcp_server(backend: Arc<MoonlinkBackend>, addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Moonlink RPC server listening on TCP: {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let backend = Arc::clone(&backend);
        tokio::spawn(async move {
            match handle_stream(backend, stream).await {
                Err(Error::Rpc(error_struct))
                    if error_struct
                        .source()
                        .and_then(|src| src.downcast_ref::<std::io::Error>())
                        .map(|io_err| {
                            matches!(io_err.kind(), BrokenPipe | ConnectionReset | UnexpectedEof)
                        })
                        .unwrap_or(false) => {}
                Err(e) => panic!("Unexpected TCP RPC server error: {e}"),
                Ok(()) => {}
            }
        });
    }
}

async fn handle_stream<S>(backend: Arc<MoonlinkBackend>, mut stream: S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut map = HashMap::new();
    loop {
        let request = read(&mut stream).await?;
        match request {
            Request::CreateSnapshot {
                database,
                table,
                lsn,
            } => {
                backend.create_snapshot(database, table, lsn).await.unwrap();
                write(&mut stream, &()).await?;
            }
            Request::CreateTable {
                database,
                table,
                src,
                src_uri,
                table_config,
            } => {
                // Use default mooncake config, and local filesystem for storage layer.
                backend
                    .create_table(
                        database,
                        table,
                        src,
                        src_uri,
                        table_config,
                        None, /* input_database */
                    )
                    .await
                    .unwrap();
                write(&mut stream, &()).await?;
            }
            Request::DropTable { database, table } => {
                backend.drop_table(database, table).await;
                write(&mut stream, &()).await?;
            }
            Request::GetTableSchema { database, table } => {
                let schema = backend.get_table_schema(database, table).await?;
                let writer = StreamWriter::try_new(vec![], &schema)?;
                let data = writer.into_inner()?;
                write(&mut stream, &data).await?;
            }
            Request::ListTables {} => {
                let tables = backend.list_tables().await?;
                let tables: Vec<Table> = tables
                    .into_iter()
                    .map(|table| Table {
                        database: table.database,
                        table: table.table,
                        commit_lsn: table.commit_lsn,
                        flush_lsn: table.flush_lsn,
                        iceberg_warehouse_location: table.iceberg_warehouse_location,
                    })
                    .collect();
                write(&mut stream, &tables).await?;
            }
            Request::OptimizeTable {
                database,
                table,
                mode,
            } => {
                backend
                    .optimize_table(database, table, &mode)
                    .await
                    .unwrap();
                write(&mut stream, &()).await?;
            }
            Request::ScanTableBegin {
                database,
                table,
                lsn,
            } => {
                let state = backend
                    .scan_table(database.to_string(), table.to_string(), Some(lsn))
                    .await
                    .unwrap();
                write(&mut stream, &state.data).await?;
                assert!(map.insert((database, table), state).is_none());
            }
            Request::ScanTableEnd { database, table } => {
                assert!(map.remove(&(database, table)).is_some());
                write(&mut stream, &()).await?;
            }
            Request::CreateEventTable {
                database,
                table,
                src_table,
                schema_fields,
            } => {
                use arrow_schema::{DataType, Field, Schema};
                use std::collections::HashMap;

                // Convert FieldSchema to Arrow Schema mirroring REST behavior
                let mut field_id: i32 = 0;
                let fields: crate::Result<Vec<Field>> = schema_fields
                    .into_iter()
                    .map(|field: FieldSchema| {
                        let data_type = match field.data_type.as_str() {
                            "int32" => DataType::Int32,
                            "int64" => DataType::Int64,
                            "string" | "text" => DataType::Utf8,
                            "boolean" | "bool" => DataType::Boolean,
                            "float32" => DataType::Float32,
                            "float64" => DataType::Float64,
                            other => {
                                return Err(crate::error::Error::InvalidArgument(ErrorStruct::new(
                                    format!("Unsupported data type: {other}"),
                                    ErrorStatus::Permanent,
                                )))
                            }
                        };

                        let mut metadata = HashMap::new();
                        metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
                        field_id += 1;
                        Ok(Field::new(&field.name, data_type, field.nullable)
                            .with_metadata(metadata))
                    })
                    .collect();

                let fields = fields?;
                let arrow_schema = Schema::new(fields);
                backend
                    .create_table(
                        database,
                        table,
                        src_table,
                        REST_API_URI.to_string(),
                        /*table_config=*/"{}".to_string(),
                        Some(arrow_schema),
                    )
                    .await?;
                write(&mut stream, &()).await?;
            }
            Request::WriteEvent {
                src_table,
                operation,
                payload,
            } => {
                use std::time::SystemTime;
                let op = match operation.as_str() {
                    "insert" => EventOperation::Insert,
                    "update" => EventOperation::Update,
                    "delete" => EventOperation::Delete,
                    _ => {
                        return Err(crate::error::Error::InvalidArgument(ErrorStruct::new(
                            format!(
                                "Invalid operation '{operation}'. Must be 'insert', 'update', or 'delete'"
                            ),
                            ErrorStatus::Permanent,
                        )))
                    }
                };

                let request = EventRequest {
                    src_table_name: src_table,
                    operation: op,
                    payload,
                    timestamp: SystemTime::now(),
                };
                backend.send_event_request(request).await?;
                write(&mut stream, &()).await?;
            }
        }
    }
}
