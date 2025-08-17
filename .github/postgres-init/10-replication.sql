ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_wal_senders = 16;
ALTER SYSTEM SET max_replication_slots = 16;