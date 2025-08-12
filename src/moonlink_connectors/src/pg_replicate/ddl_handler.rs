use crate::pg_replicate::conversions::cdc_event::DdlEvent;
use crate::pg_replicate::moonlink_sink::Sink;
use crate::pg_replicate::table::SrcTableId;
use crate::Result;
use std::collections::HashMap;
use tokio_postgres::Client;
use tracing::{debug, error, info, warn};

pub struct DdlHandler {
    client: Client,
    sink: Sink,
    table_mapping: HashMap<String, SrcTableId>,
}

impl DdlHandler {
    pub fn new(client: Client, sink: Sink) -> Self {
        Self {
            client,
            sink,
            table_mapping: HashMap::new(),
        }
    }

    pub async fn handle_ddl_event(&mut self, event: &DdlEvent) -> Result<()> {
        info!("Processing DDL event: {:?}", event.tags);
        debug!("DDL query: {}", event.query);

        // Parse the DDL command type from tags
        let command_type = event.tags.first().map(|s| s.as_str()).unwrap_or("");

        match command_type {
            "CREATE TABLE" => self.handle_create_table(&event.query).await?,
            "ALTER TABLE" => self.handle_alter_table(&event.query).await?,
            "DROP TABLE" => self.handle_drop_table(&event.query).await?,
            "CREATE INDEX" => self.handle_create_index(&event.query).await?,
            "DROP INDEX" => self.handle_drop_index(&event.query).await?,
            "CREATE VIEW" => self.handle_create_view(&event.query).await?,
            "DROP VIEW" => self.handle_drop_view(&event.query).await?,
            _ => {
                debug!(
                    "DDL command type '{}' not explicitly handled, applying directly",
                    command_type
                );
                self.apply_ddl_to_sink(&event.query).await?;
            }
        }

        Ok(())
    }

    async fn handle_create_table(&mut self, query: &str) -> Result<()> {
        info!("Handling CREATE TABLE DDL");

        // Extract table name from query (simplified parsing)
        let table_name = self.extract_table_name_from_create(query);

        // Apply DDL to sink
        self.apply_ddl_to_sink(query).await?;

        // Update table mapping if needed
        if let Some(name) = table_name {
            info!("Created table: {}", name);
            // Could register the table for CDC here if needed
        }

        Ok(())
    }

    async fn handle_alter_table(&mut self, query: &str) -> Result<()> {
        info!("Handling ALTER TABLE DDL");

        // Apply DDL to sink
        self.apply_ddl_to_sink(query).await?;

        // Check if we need to refresh schema information
        let table_name = self.extract_table_name_from_alter(query);
        if let Some(name) = table_name {
            info!("Altered table: {}", name);
            // Could trigger schema refresh here
        }

        Ok(())
    }

    async fn handle_drop_table(&mut self, query: &str) -> Result<()> {
        info!("Handling DROP TABLE DDL");

        let table_name = self.extract_table_name_from_drop(query);

        // Apply DDL to sink
        self.apply_ddl_to_sink(query).await?;

        // Remove from table mapping if present
        if let Some(name) = table_name {
            info!("Dropped table: {}", name);
            self.table_mapping.remove(&name);
        }

        Ok(())
    }

    async fn handle_create_index(&mut self, query: &str) -> Result<()> {
        info!("Handling CREATE INDEX DDL");
        self.apply_ddl_to_sink(query).await
    }

    async fn handle_drop_index(&mut self, query: &str) -> Result<()> {
        info!("Handling DROP INDEX DDL");
        self.apply_ddl_to_sink(query).await
    }

    async fn handle_create_view(&mut self, query: &str) -> Result<()> {
        info!("Handling CREATE VIEW DDL");
        self.apply_ddl_to_sink(query).await
    }

    async fn handle_drop_view(&mut self, query: &str) -> Result<()> {
        info!("Handling DROP VIEW DDL");
        self.apply_ddl_to_sink(query).await
    }

    async fn apply_ddl_to_sink(&mut self, query: &str) -> Result<()> {
        // Skip temporary table DDLs
        if query.to_uppercase().contains("TEMP") || query.to_uppercase().contains("TEMPORARY") {
            debug!("Skipping temporary table DDL");
            return Ok(());
        }

        // Skip mooncake internal DDLs
        if query.contains("mooncake.ddl_logs") || query.contains("mooncake.ddl_replication_state") {
            debug!("Skipping mooncake internal DDL");
            return Ok(());
        }

        info!("Applying DDL to sink: {}", query);

        // The sink should implement DDL execution
        // For now, we'll just log it
        warn!("DDL execution in sink not yet implemented: {}", query);

        Ok(())
    }

    // Helper methods to extract table names from DDL queries
    fn extract_table_name_from_create(&self, query: &str) -> Option<String> {
        let upper = query.to_uppercase();
        if let Some(pos) = upper.find("CREATE TABLE") {
            let after_create = &query[pos + 12..].trim();
            if let Some(end) = after_create.find(|c: char| c == '(' || c.is_whitespace()) {
                return Some(after_create[..end].trim().to_string());
            }
        }
        None
    }

    fn extract_table_name_from_alter(&self, query: &str) -> Option<String> {
        let upper = query.to_uppercase();
        if let Some(pos) = upper.find("ALTER TABLE") {
            let after_alter = &query[pos + 11..].trim();
            if let Some(end) = after_alter.find(|c: char| c.is_whitespace()) {
                return Some(after_alter[..end].trim().to_string());
            }
        }
        None
    }

    fn extract_table_name_from_drop(&self, query: &str) -> Option<String> {
        let upper = query.to_uppercase();
        if let Some(pos) = upper.find("DROP TABLE") {
            let after_drop = &query[pos + 10..].trim();
            // Handle IF EXISTS
            let cleaned = if after_drop.to_uppercase().starts_with("IF EXISTS") {
                &after_drop[9..].trim()
            } else {
                after_drop
            };

            if let Some(end) = cleaned.find(|c: char| c == ';' || c.is_whitespace()) {
                return Some(cleaned[..end].trim().to_string());
            } else if !cleaned.is_empty() {
                return Some(cleaned.to_string());
            }
        }
        None
    }

    pub fn register_table(&mut self, table_name: String, table_id: SrcTableId) {
        self.table_mapping.insert(table_name, table_id);
    }
}
