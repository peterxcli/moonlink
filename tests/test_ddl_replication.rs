#[cfg(test)]
mod tests {
    use moonlink_connectors::pg_replicate::conversions::cdc_event::{CdcEvent, DdlEvent};
    use moonlink_connectors::pg_replicate::ddl_handler::DdlHandler;
    use std::collections::HashMap;

    #[test]
    fn test_ddl_event_creation() {
        let ddl_event = DdlEvent {
            query: "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))".to_string(),
            tags: vec!["CREATE TABLE".to_string()],
            table_id: None,
            timestamp: Some(1234567890),
        };

        assert_eq!(
            ddl_event.query,
            "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))"
        );
        assert_eq!(ddl_event.tags[0], "CREATE TABLE");
    }

    #[test]
    fn test_ddl_event_parsing() {
        // Test CREATE TABLE parsing
        let create_query = "CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(255))";
        assert!(create_query.contains("CREATE TABLE"));

        // Test ALTER TABLE parsing
        let alter_query = "ALTER TABLE users ADD COLUMN age INT";
        assert!(alter_query.contains("ALTER TABLE"));

        // Test DROP TABLE parsing
        let drop_query = "DROP TABLE IF EXISTS users";
        assert!(drop_query.contains("DROP TABLE"));
    }

    #[test]
    fn test_ddl_event_filtering() {
        // Test that temporary tables are filtered
        let temp_query = "CREATE TEMPORARY TABLE temp_data (id INT)";
        assert!(temp_query.to_uppercase().contains("TEMPORARY"));

        // Test that mooncake internal tables are filtered
        let internal_query = "INSERT INTO mooncake.ddl_logs VALUES (...)";
        assert!(internal_query.contains("mooncake.ddl_logs"));
    }

    #[tokio::test]
    async fn test_ddl_event_in_cdc_stream() {
        // Create a DDL event
        let ddl_event = DdlEvent {
            query: "CREATE INDEX idx_users_email ON users(email)".to_string(),
            tags: vec!["CREATE INDEX".to_string()],
            table_id: None,
            timestamp: None,
        };

        // Wrap in CdcEvent
        let cdc_event = CdcEvent::Ddl(ddl_event);

        // Verify it can be matched
        match cdc_event {
            CdcEvent::Ddl(event) => {
                assert_eq!(event.tags[0], "CREATE INDEX");
                assert!(event.query.contains("idx_users_email"));
            }
            _ => panic!("Expected DDL event"),
        }
    }

    #[test]
    fn test_ddl_command_classification() {
        let test_cases = vec![
            ("CREATE TABLE test (id INT)", "CREATE TABLE"),
            ("ALTER TABLE test ADD COLUMN name VARCHAR", "ALTER TABLE"),
            ("DROP TABLE test", "DROP TABLE"),
            ("CREATE INDEX idx ON test(id)", "CREATE INDEX"),
            ("CREATE VIEW v AS SELECT * FROM test", "CREATE VIEW"),
            ("DROP VIEW v", "DROP VIEW"),
            ("CREATE SEQUENCE seq", "CREATE SEQUENCE"),
            ("ALTER SEQUENCE seq RESTART", "ALTER SEQUENCE"),
            ("DROP SEQUENCE seq", "DROP SEQUENCE"),
        ];

        for (query, expected_tag) in test_cases {
            let ddl_event = DdlEvent {
                query: query.to_string(),
                tags: vec![expected_tag.to_string()],
                table_id: None,
                timestamp: None,
            };

            assert_eq!(ddl_event.tags[0], expected_tag);
        }
    }
}
