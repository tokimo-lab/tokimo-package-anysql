# tokimo-package-anysql

Multi-database session manager for Rust — unified connection, schema browsing, and SQL execution across PostgreSQL, MySQL, SQLite, SQL Server (MSSQL), MongoDB, Oracle, ClickHouse, and Elasticsearch.

## Features

- **Session management** — create / keep-alive / expire database sessions with configurable TTL
- **Unified API** — one `SessionManager` and `DbConnectionConfig` covers all supported drivers
- **Schema browsing** — list databases, tables, columns, indexes, foreign keys
- **SQL execution** — typed result sets serialized as `serde_json::Value`
- **ts-rs bindings** — TypeScript types auto-generated for DTO structs
- **Optional Oracle** — gated behind `oracle` feature flag (requires native OCI libraries)

## Supported Databases

| Driver | Crate |
|---|---|
| PostgreSQL | `sqlx` |
| MySQL / MariaDB | `sqlx` |
| SQLite | `sqlx` |
| SQL Server (MSSQL) | `tiberius` |
| MongoDB | `mongodb` |
| Oracle | `oracle` (optional feature) |
| ClickHouse | `reqwest` (HTTP API) |
| Elasticsearch | `reqwest` (HTTP API) |

## Usage

```rust
use rust_anysql::{SessionManager, DbConnectionConfig, DbDriver};
use std::time::Duration;

let manager = SessionManager::new(Duration::from_hours(1));

// Test a connection
let config = DbConnectionConfig {
    driver: DbDriver::Postgres,
    host: "localhost".into(),
    port: 5432,
    database: "mydb".into(),
    username: "user".into(),
    password: "pass".into(),
    ..Default::default()
};
SessionManager::test_connection(&config).await?;

// Open a session
let session_id = manager.open(config).await?;

// Execute SQL
let rows = manager.execute(&session_id, "SELECT * FROM users LIMIT 10", &[]).await?;
```

## Crate name

The crate lib name is `rust_anysql` for backward compatibility (`use rust_anysql::...`).
In Cargo.toml, reference it with the `package` key:

```toml
rust-anysql = { git = "https://github.com/tokimo-lab/tokimo-package-anysql", package = "tokimo-package-anysql" }
```

## License

MIT
