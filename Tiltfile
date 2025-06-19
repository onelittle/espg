local_resource(
  name="pgmanager",
  serve_cmd="rm tmp/test_manager.sock; cargo run --release",
  serve_dir="pgmanager",
  serve_env={
    "DATABASE_URL": "postgres:///postgres",
    "DATABASE_PREFIX": "espg_test",
    "DATABASE_COUNT": "16",
  },
  labels=["test"],
)

local_resource(
  name="test",
  cmd="cargo nextest run --all-features",
  env={
    "PGMANAGER_SOCKET": "pgmanager/tmp/test_manager.sock",
  },
  labels=["test"],
  resource_deps=["pgmanager"],
)
