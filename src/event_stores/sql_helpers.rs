pub const CREATE_EVENTS: &str = r#"
    CREATE TABLE IF NOT EXISTS events (
        aggregate_id TEXT NOT NULL,
        aggregate_type TEXT NOT NULL,
        version INT NOT NULL,
        action JSONB NOT NULL,
        PRIMARY KEY (aggregate_id, aggregate_type, version)
    )
"#;

pub const CREATE_SNAPSHOTS: &str = r#"
    CREATE TABLE IF NOT EXISTS snapshots (
        aggregate_id TEXT NOT NULL,
        aggregate_type TEXT NOT NULL,
        key TEXT NOT NULL,
        version INT NOT NULL,
        snapshot JSONB NOT NULL,
        PRIMARY KEY (aggregate_id, aggregate_type, key)
    )
"#;
