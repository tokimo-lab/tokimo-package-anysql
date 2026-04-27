//! 集成测试 — 连接远端 Docker 上的 PostgreSQL / MySQL + 本地 SQLite，
//! 验证 SessionManager 全部 API。
//!
//! 运行: cargo test --test integration -- --nocapture

use std::time::Duration;
use tokimo_package_anysql::{DbConnectionConfig, DbDriver, SessionManager};

fn pg_config() -> DbConnectionConfig {
    DbConnectionConfig {
        driver: DbDriver::Postgres,
        name: "test-pg".into(),
        host: "10.0.4.7".into(),
        port: Some(25432),
        username: Some("postgres".into()),
        password: Some("testpass123".into()),
        database: Some("testdb".into()),
        params: None,
    }
}

fn mysql_config() -> DbConnectionConfig {
    DbConnectionConfig {
        driver: DbDriver::Mysql,
        name: "test-mysql".into(),
        host: "10.0.4.7".into(),
        port: Some(23306),
        username: Some("root".into()),
        password: Some("testpass123".into()),
        database: Some("testdb".into()),
        params: None,
    }
}

fn sqlite_config() -> DbConnectionConfig {
    DbConnectionConfig {
        driver: DbDriver::Sqlite,
        name: "test-sqlite".into(),
        host: "/tmp/anysql_test.db".into(),
        port: None,
        username: None,
        password: None,
        database: None,
        params: None,
    }
}

// ── Helper ──────────────────────────────────────────────────────────────────

macro_rules! test_driver {
    ($name:ident, $config_fn:ident $(, #[$extra:meta])*) => {
        mod $name {
            use super::*;

            #[tokio::test]
            $(#[$extra])*
            async fn test_connection() {
                let overview = SessionManager::test_connection(&$config_fn()).await;
                assert!(overview.is_ok(), "test_connection failed: {:?}", overview.err());
                let ov = overview.unwrap();
                println!("[{}] version: {}", stringify!($name), ov.server_version);
                println!("[{}] current_db: {}", stringify!($name), ov.current_database);
                println!("[{}] active_connections: {}", stringify!($name), ov.active_connections);
            }

            #[tokio::test]
            $(#[$extra])*
            async fn session_lifecycle() {
                let mgr = SessionManager::new(Duration::from_mins(10));

                // connect
                let session = mgr.connect($config_fn()).await;
                assert!(session.is_ok(), "connect failed: {:?}", session.err());
                let session = session.unwrap();
                let sid = session.id.clone();
                println!("[{}] session: {}", stringify!($name), sid);

                // list sessions
                let sessions = mgr.list_sessions().await;
                assert!(!sessions.is_empty());
                println!("[{}] active sessions: {}", stringify!($name), sessions.len());

                // get session
                let s = mgr.get_session(&sid).await;
                assert!(s.is_ok());

                // overview
                let overview = mgr.overview(&sid).await;
                assert!(overview.is_ok(), "overview failed: {:?}", overview.err());
                let ov = overview.unwrap();
                println!("[{}] overview: version={}, db={}, user={}", stringify!($name), ov.server_version, ov.current_database, ov.current_user);

                // disconnect
                let disc = mgr.disconnect(&sid).await;
                assert!(disc.is_ok());

                // session should be gone
                let gone = mgr.get_session(&sid).await;
                assert!(gone.is_err());
            }

            #[tokio::test]
            $(#[$extra])*
            async fn list_databases() {
                let mgr = SessionManager::new(Duration::from_mins(10));
                let session = mgr.connect($config_fn()).await.unwrap();
                let sid = session.id;

                let dbs = mgr.list_databases(&sid).await;
                assert!(dbs.is_ok(), "list_databases failed: {:?}", dbs.err());
                let dbs = dbs.unwrap();
                println!("[{}] databases:", stringify!($name));
                for db in &dbs {
                    println!("  - {} (size={:?}, encoding={:?})", db.name, db.size_bytes, db.encoding);
                }
                assert!(!dbs.is_empty());
            }

            #[tokio::test]
            $(#[$extra])*
            async fn list_schemas() {
                let mgr = SessionManager::new(Duration::from_mins(10));
                let session = mgr.connect($config_fn()).await.unwrap();
                let sid = session.id;

                let schemas = mgr.list_schemas(&sid).await;
                assert!(schemas.is_ok(), "list_schemas failed: {:?}", schemas.err());
                let schemas = schemas.unwrap();
                println!("[{}] schemas:", stringify!($name));
                for s in &schemas {
                    println!("  - {} (owner={:?})", s.name, s.owner);
                }
                assert!(!schemas.is_empty());
            }

            #[tokio::test]
            $(#[$extra])*
            async fn list_tables() {
                let mgr = SessionManager::new(Duration::from_mins(10));
                let session = mgr.connect($config_fn()).await.unwrap();
                let sid = session.id;

                let tables = mgr.list_tables(&sid, None).await;
                assert!(tables.is_ok(), "list_tables failed: {:?}", tables.err());
                let tables = tables.unwrap();
                println!("[{}] tables:", stringify!($name));
                for t in &tables {
                    println!("  - {} ({:?}) rows={:?} size={:?}", t.name, t.kind, t.estimated_rows, t.size_bytes);
                }
                // PG and MySQL have users + orders tables
                if !matches!($config_fn().driver, DbDriver::Sqlite) {
                    assert!(tables.len() >= 2, "expected at least 2 tables, got {}", tables.len());
                }
            }

            #[tokio::test]
            $(#[$extra])*
            async fn describe_table() {
                let mgr = SessionManager::new(Duration::from_mins(10));
                let session = mgr.connect($config_fn()).await.unwrap();
                let sid = session.id;

                // For SQLite, create test table first
                if matches!($config_fn().driver, DbDriver::Sqlite) {
                    mgr.execute_sql(&sid, "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE, age INTEGER)", 0).await.unwrap();
                    mgr.execute_sql(&sid, "INSERT OR IGNORE INTO users VALUES (1, 'Alice', 'alice@test.com', 30)", 0).await.unwrap();
                }

                let detail = mgr.describe_table(&sid, "users", None).await;
                assert!(detail.is_ok(), "describe_table failed: {:?}", detail.err());
                let detail = detail.unwrap();
                println!("[{}] table detail: {}", stringify!($name), detail.name);
                println!("  columns:");
                for c in &detail.columns {
                    println!("    - {} {} nullable={} pk={} default={:?}", c.name, c.data_type, c.is_nullable, c.is_primary_key, c.default_value);
                }
                println!("  indexes:");
                for i in &detail.indexes {
                    println!("    - {} cols={:?} unique={} primary={}", i.name, i.columns, i.is_unique, i.is_primary);
                }
                println!("  foreign_keys:");
                for fk in &detail.foreign_keys {
                    println!("    - {} {} -> {}.{:?}", fk.name, fk.columns.join(","), fk.referenced_table, fk.referenced_columns);
                }
                if let Some(ddl) = &detail.create_sql {
                    println!("  DDL: {}", &ddl[..ddl.len().min(200)]);
                }

                assert!(!detail.columns.is_empty());
            }

            #[tokio::test]
            $(#[$extra])*
            async fn execute_sql_select() {
                let mgr = SessionManager::new(Duration::from_mins(10));
                let session = mgr.connect($config_fn()).await.unwrap();
                let sid = session.id;

                // For SQLite, create test table first
                if matches!($config_fn().driver, DbDriver::Sqlite) {
                    mgr.execute_sql(&sid, "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)", 0).await.unwrap();
                    mgr.execute_sql(&sid, "INSERT OR IGNORE INTO users VALUES (1, 'Alice', 'alice@test.com', 30), (2, 'Bob', 'bob@test.com', 25)", 0).await.unwrap();
                }

                let result = mgr.execute_sql(&sid, "SELECT * FROM users", 100).await;
                assert!(result.is_ok(), "execute_sql failed: {:?}", result.err());
                let result = result.unwrap();
                println!("[{}] query result:", stringify!($name));
                println!("  columns: {:?}", result.columns.iter().map(|c| &c.name).collect::<Vec<_>>());
                println!("  rows: {} (truncated={})", result.rows.len(), result.truncated);
                println!("  elapsed: {}ms", result.elapsed_ms);
                for row in &result.rows {
                    println!("  row: {:?}", row);
                }
                assert!(!result.columns.is_empty());
                assert!(!result.rows.is_empty());
            }

            #[tokio::test]
            $(#[$extra])*
            async fn execute_sql_dml() {
                let mgr = SessionManager::new(Duration::from_mins(10));
                let session = mgr.connect($config_fn()).await.unwrap();
                let sid = session.id;

                // Create a temp table for DML testing
                let create = if matches!($config_fn().driver, DbDriver::Sqlite) {
                    "CREATE TABLE IF NOT EXISTS dml_test (id INTEGER PRIMARY KEY, val TEXT)"
                } else if matches!($config_fn().driver, DbDriver::Mysql) {
                    "CREATE TABLE IF NOT EXISTS dml_test (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(100))"
                } else {
                    "CREATE TABLE IF NOT EXISTS dml_test (id SERIAL PRIMARY KEY, val VARCHAR(100))"
                };
                let r = mgr.execute_sql(&sid, create, 0).await;
                assert!(r.is_ok(), "CREATE TABLE failed: {:?}", r.err());

                // INSERT
                let r = mgr.execute_sql(&sid, "INSERT INTO dml_test (val) VALUES ('hello')", 0).await;
                assert!(r.is_ok(), "INSERT failed: {:?}", r.err());
                let r = r.unwrap();
                println!("[{}] INSERT rows_affected: {}", stringify!($name), r.rows_affected);
                assert!(r.rows_affected >= 1);

                // UPDATE
                let r = mgr.execute_sql(&sid, "UPDATE dml_test SET val = 'world' WHERE val = 'hello'", 0).await;
                assert!(r.is_ok(), "UPDATE failed: {:?}", r.err());
                let r = r.unwrap();
                println!("[{}] UPDATE rows_affected: {}", stringify!($name), r.rows_affected);

                // DELETE
                let r = mgr.execute_sql(&sid, "DELETE FROM dml_test", 0).await;
                assert!(r.is_ok(), "DELETE failed: {:?}", r.err());
                println!("[{}] DELETE rows_affected: {}", stringify!($name), r.unwrap().rows_affected);

                // DROP
                let r = mgr.execute_sql(&sid, "DROP TABLE dml_test", 0).await;
                assert!(r.is_ok(), "DROP TABLE failed: {:?}", r.err());
            }

            #[tokio::test]
            $(#[$extra])*
            async fn list_variables() {
                let mgr = SessionManager::new(Duration::from_mins(10));
                let session = mgr.connect($config_fn()).await.unwrap();
                let sid = session.id;

                let vars = mgr.list_variables(&sid, None).await;
                assert!(vars.is_ok(), "list_variables failed: {:?}", vars.err());
                let vars = vars.unwrap();
                println!("[{}] variables (first 5):", stringify!($name));
                for v in vars.iter().take(5) {
                    println!("  - {} = {} ({:?})", v.name, &v.value[..v.value.len().min(60)], v.description.as_deref().map(|d| &d[..d.len().min(40)]));
                }
                assert!(!vars.is_empty());
            }

            #[tokio::test]
            $(#[$extra])*
            async fn list_active_queries() {
                let mgr = SessionManager::new(Duration::from_mins(10));
                let session = mgr.connect($config_fn()).await.unwrap();
                let sid = session.id;

                let queries = mgr.list_active_queries(&sid).await;
                // SQLite returns empty which is fine
                if matches!($config_fn().driver, DbDriver::Sqlite) {
                    assert!(queries.is_ok());
                    println!("[{}] active queries: (empty — expected for SQLite)", stringify!($name));
                } else {
                    assert!(queries.is_ok(), "list_active_queries failed: {:?}", queries.err());
                    let queries = queries.unwrap();
                    println!("[{}] active queries: {}", stringify!($name), queries.len());
                    for q in queries.iter().take(3) {
                        println!("  - pid={} user={:?} state={:?} query={:?}", q.pid, q.username, q.state, q.query.as_deref().map(|s| &s[..s.len().min(60)]));
                    }
                }
            }

            #[tokio::test]
            $(#[$extra])*
            async fn list_routines_and_triggers() {
                let mgr = SessionManager::new(Duration::from_mins(10));
                let session = mgr.connect($config_fn()).await.unwrap();
                let sid = session.id;

                let routines = mgr.list_routines(&sid, None).await;
                assert!(routines.is_ok(), "list_routines failed: {:?}", routines.err());
                println!("[{}] routines: {}", stringify!($name), routines.unwrap().len());

                let triggers = mgr.list_triggers(&sid, None).await;
                assert!(triggers.is_ok(), "list_triggers failed: {:?}", triggers.err());
                println!("[{}] triggers: {}", stringify!($name), triggers.unwrap().len());
            }

            #[tokio::test]
            $(#[$extra])*
            async fn max_rows_truncation() {
                let mgr = SessionManager::new(Duration::from_mins(10));
                let session = mgr.connect($config_fn()).await.unwrap();
                let sid = session.id;

                if matches!($config_fn().driver, DbDriver::Sqlite) {
                    mgr.execute_sql(&sid, "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)", 0).await.unwrap();
                    mgr.execute_sql(&sid, "INSERT OR IGNORE INTO users VALUES (1, 'A', 'a@t', 1), (2, 'B', 'b@t', 2), (3, 'C', 'c@t', 3)", 0).await.unwrap();
                }

                // max_rows = 1 should truncate
                let r = mgr.execute_sql(&sid, "SELECT * FROM users", 1).await.unwrap();
                println!("[{}] truncation test: rows={} truncated={}", stringify!($name), r.rows.len(), r.truncated);
                assert_eq!(r.rows.len(), 1);
                assert!(r.truncated);
            }
        }
    };
}

test_driver!(postgres, pg_config, #[ignore = "requires live PostgreSQL at configured host; run with --ignored"]);
test_driver!(mysql, mysql_config, #[ignore = "requires live MySQL at configured host; run with --ignored"]);
test_driver!(sqlite, sqlite_config);
