use crate::pg_replicate::ReplicationClient;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_postgres::{connect, NoTls};

const DEFAULT_DB_URL: &str = "postgresql://postgres:postgres@postgres:5432/postgres";

pub fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB_URL.to_string())
}

pub async fn setup_connection() -> tokio_postgres::Client {
    let database_url = database_url();
    let (client, connection) = connect(&database_url, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {e}");
        }
    });
    client
}

pub async fn create_replication_client() -> ReplicationClient {
    let url = database_url();
    let (mut replication_client, connection) =
        ReplicationClient::connect_no_tls(&url, true).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Replication connection error: {e}");
        }
    });
    replication_client
}

async fn drop_publication(client: &tokio_postgres::Client, publication: &str) {
    let _ = client
        .simple_query(&format!(
            "DROP PUBLICATION IF EXISTS {publication} CASCADE;"
        ))
        .await
        .unwrap();
}

async fn drop_table(client: &tokio_postgres::Client, table_name: &str) {
    let _ = client
        .simple_query(&format!("DROP TABLE IF EXISTS {table_name} CASCADE;"))
        .await
        .unwrap();
}

async fn drop_type(client: &tokio_postgres::Client, type_name: &str) {
    let _ = client
        .simple_query(&format!("DROP TYPE IF EXISTS {type_name} CASCADE;"))
        .await
        .unwrap();
}

async fn drop_replication_slot(client: &tokio_postgres::Client, slot_name: &str) {
    let _ = client
        .simple_query(&format!(
            "SELECT pg_drop_replication_slot('{slot}');",
            slot = slot_name
        ))
        .await
        .unwrap();
}

pub struct TestResources {
    client: Arc<tokio_postgres::Client>,
    tables: Vec<String>,
    publications: Vec<String>,
    slots: Vec<String>,
    types: Vec<String>,
    sql_tx: Option<mpsc::UnboundedSender<String>>,
}

impl TestResources {
    pub fn new(client: tokio_postgres::Client) -> Self {
        Self {
            client: Arc::new(client),
            tables: Vec::new(),
            publications: Vec::new(),
            slots: Vec::new(),
            types: Vec::new(),
            sql_tx: None,
        }
    }

    pub fn client(&self) -> &tokio_postgres::Client {
        &self.client
    }

    pub fn add_table(&mut self, name: impl Into<String>) {
        self.tables.push(name.into());
    }

    pub fn add_publication(&mut self, name: impl Into<String>) {
        self.publications.push(name.into());
    }

    pub fn add_slot(&mut self, name: impl Into<String>) {
        self.slots.push(name.into());
    }

    pub fn add_type(&mut self, name: impl Into<String>) {
        self.types.push(name.into());
    }

    pub fn set_sql_tx(&mut self, tx: mpsc::UnboundedSender<String>) {
        self.sql_tx = Some(tx);
    }

    pub async fn cleanup(&self) {
        for slot in &self.slots {
            drop_replication_slot(&self.client, slot).await;
        }
        for publication in &self.publications {
            drop_publication(&self.client, publication).await;
        }
        for table in &self.tables {
            drop_table(&self.client, table).await;
        }
        for type_name in &self.types {
            drop_type(&self.client, type_name).await;
        }
    }
}

impl Drop for TestResources {
    fn drop(&mut self) {
        // Drop the SQL sender to stop background executor if still running
        let _ = self.sql_tx.take();

        // Use Arc to share the client without cloning
        let client = Arc::clone(&self.client);
        let tables = self.tables.clone();
        let publications = self.publications.clone();
        let slots = self.slots.clone();
        let types = self.types.clone();
        tokio::spawn(async move {
            for slot in &slots {
                drop_replication_slot(&client, slot).await;
            }
            for publication in &publications {
                drop_publication(&client, publication).await;
            }
            for table in &tables {
                drop_table(&client, table).await;
            }
            for type_name in &types {
                drop_type(&client, type_name).await;
            }
        });
    }
}
