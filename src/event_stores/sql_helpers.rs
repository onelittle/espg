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

pub const CREATE_TRIGGER: &str = r#"
    CREATE OR REPLACE FUNCTION events_notification() RETURNS TRIGGER AS $$
        BEGIN
        PERFORM pg_notify(MD5('events'), '-');
        PERFORM pg_notify(MD5('events:' || NEW.aggregate_type), '' || NEW.aggregate_id);
        PERFORM pg_notify(MD5('events:' || NEW.aggregate_type || ':' || NEW.aggregate_id), '' || NEW.version);
        RETURN NULL;
        END;
    $$ LANGUAGE plpgsql;

    CREATE OR REPLACE TRIGGER events_notification_trigger
    AFTER INSERT
    ON events
    FOR EACH ROW
    EXECUTE PROCEDURE events_notification();
"#;
