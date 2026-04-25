use std::time::Instant;

use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::{Column, Row, TypeInfo};

use crate::connector::DatabaseConnector;
use crate::error::AnySqlError;
use crate::types::*;

pub struct SqliteConnector {
    pool: SqlitePool,
}

impl SqliteConnector {
    pub async fn connect(config: &DbConnectionConfig) -> Result<Self, AnySqlError> {
        let url = config.to_url();
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .connect(&url)
            .await
            .map_err(|e| AnySqlError::Connection(e.to_string()))?;
        Ok(Self { pool })
    }
}

fn sqlite_column_to_json(row: &SqliteRow, idx: usize) -> serde_json::Value {
    macro_rules! try_get {
        ($t:ty) => {
            if let Ok(v) = row.try_get::<Option<$t>, _>(idx) {
                return match v {
                    Some(v) => serde_json::to_value(v).unwrap_or(serde_json::Value::Null),
                    None => serde_json::Value::Null,
                };
            }
        };
    }

    try_get!(bool);
    try_get!(i32);
    try_get!(i64);
    try_get!(f64);
    try_get!(String);

    if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
        return match v {
            Some(s) => serde_json::Value::String(s),
            None => serde_json::Value::Null,
        };
    }

    serde_json::Value::Null
}

#[async_trait::async_trait]
impl DatabaseConnector for SqliteConnector {
    fn driver(&self) -> DbDriver {
        DbDriver::Sqlite
    }

    async fn ping(&self) -> Result<(), AnySqlError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(AnySqlError::from)?;
        Ok(())
    }

    async fn execute_sql(&self, sql: &str, max_rows: usize) -> Result<QueryResult, AnySqlError> {
        let start = Instant::now();
        let trimmed = sql.trim_start().to_uppercase();
        let is_query = trimmed.starts_with("SELECT")
            || trimmed.starts_with("WITH")
            || trimmed.starts_with("PRAGMA")
            || trimmed.starts_with("EXPLAIN")
            || trimmed.starts_with("VALUES");

        if is_query {
            let rows: Vec<SqliteRow> = sqlx::query(sql)
                .fetch_all(&self.pool)
                .await
                .map_err(AnySqlError::from)?;

            let elapsed_ms = start.elapsed().as_millis() as u64;

            if rows.is_empty() {
                return Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: 0,
                    elapsed_ms,
                    truncated: false,
                });
            }

            let columns: Vec<ColumnInfo> = rows[0]
                .columns()
                .iter()
                .enumerate()
                .map(|(i, c)| ColumnInfo {
                    name: c.name().to_string(),
                    data_type: c.type_info().name().to_string(),
                    ordinal: i,
                })
                .collect();

            let truncated = rows.len() > max_rows;
            let take = rows.len().min(max_rows);

            let data_rows: Vec<serde_json::Map<String, serde_json::Value>> = rows
                .iter()
                .take(take)
                .map(|row| {
                    let mut map = serde_json::Map::new();
                    for (i, col) in columns.iter().enumerate() {
                        map.insert(col.name.clone(), sqlite_column_to_json(row, i));
                    }
                    map
                })
                .collect();

            Ok(QueryResult {
                columns,
                rows: data_rows,
                rows_affected: 0,
                elapsed_ms,
                truncated,
            })
        } else {
            let result = sqlx::query(sql).execute(&self.pool).await.map_err(AnySqlError::from)?;
            let elapsed_ms = start.elapsed().as_millis() as u64;

            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: result.rows_affected(),
                elapsed_ms,
                truncated: false,
            })
        }
    }

    async fn overview(&self) -> Result<DatabaseOverview, AnySqlError> {
        let version: (String,) = sqlx::query_as("SELECT sqlite_version()")
            .fetch_one(&self.pool)
            .await
            .map_err(AnySqlError::from)?;

        // SQLite 数据库大小
        let page_count: (i64,) = sqlx::query_as("PRAGMA page_count")
            .fetch_one(&self.pool)
            .await
            .unwrap_or((0,));
        let page_size: (i64,) = sqlx::query_as("PRAGMA page_size")
            .fetch_one(&self.pool)
            .await
            .unwrap_or((4096,));
        let db_size = page_count.0 * page_size.0;

        Ok(DatabaseOverview {
            server_version: format!("SQLite {}", version.0),
            uptime_seconds: None,
            current_database: "main".to_string(),
            current_user: String::new(),
            database_size_bytes: Some(db_size),
            active_connections: 1,
            max_connections: 1,
        })
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseEntry>, AnySqlError> {
        let rows = sqlx::query_as::<_, (i32, String, String)>("PRAGMA database_list")
            .fetch_all(&self.pool)
            .await
            .map_err(AnySqlError::from)?;

        Ok(rows
            .into_iter()
            .map(|(_seq, name, _file)| DatabaseEntry {
                name,
                size_bytes: None,
                encoding: None,
            })
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaEntry>, AnySqlError> {
        // SQLite 没有 schema 概念，返回 main
        Ok(vec![SchemaEntry {
            name: "main".to_string(),
            owner: None,
        }])
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableEntry>, AnySqlError> {
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        let mut entries = Vec::with_capacity(rows.len());
        for (name, kind_str) in rows {
            let kind = if kind_str == "view" {
                TableKind::View
            } else {
                TableKind::Table
            };

            // 尝试获取行数
            let count: (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM \"{}\"", name.replace('"', "\"\"")))
                .fetch_one(&self.pool)
                .await
                .unwrap_or((0,));

            entries.push(TableEntry {
                name,
                schema: Some("main".to_string()),
                kind,
                estimated_rows: Some(count.0),
                size_bytes: None,
                comment: None,
            });
        }
        Ok(entries)
    }

    async fn describe_table(&self, table: &str, _schema: Option<&str>) -> Result<TableDetail, AnySqlError> {
        // table_info
        let col_rows = sqlx::query_as::<_, (i32, String, String, i32, Option<String>, i32)>(&format!(
            "PRAGMA table_info(\"{}\")",
            table.replace('"', "\"\"")
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        let columns: Vec<ColumnDetail> = col_rows
            .into_iter()
            .map(|(cid, name, dtype, notnull, default, pk)| ColumnDetail {
                ordinal: cid as usize,
                name,
                data_type: dtype,
                is_nullable: notnull == 0,
                is_primary_key: pk > 0,
                default_value: default,
                comment: None,
                max_length: None,
            })
            .collect();

        // 索引
        let idx_rows =
            sqlx::query_as::<_, (String, i32)>(&format!("PRAGMA index_list(\"{}\")", table.replace('"', "\"\"")))
                .fetch_all(&self.pool)
                .await
                .unwrap_or_default();

        let mut indexes = Vec::new();
        for (idx_name, unique) in &idx_rows {
            let idx_cols = sqlx::query_as::<_, (i32, i32, Option<String>)>(&format!(
                "PRAGMA index_info(\"{}\")",
                idx_name.replace('"', "\"\"")
            ))
            .fetch_all(&self.pool)
            .await
            .unwrap_or_default();

            indexes.push(IndexEntry {
                name: idx_name.clone(),
                columns: idx_cols.into_iter().filter_map(|(_, _, name)| name).collect(),
                is_unique: *unique != 0,
                is_primary: false,
                index_type: Some("btree".to_string()),
            });
        }

        // 外键
        let fk_rows = sqlx::query_as::<_, (i32, i32, String, String, String, String, String)>(&format!(
            "PRAGMA foreign_key_list(\"{}\")",
            table.replace('"', "\"\"")
        ))
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        let mut fk_map: std::collections::HashMap<i32, ForeignKeyEntry> = std::collections::HashMap::new();
        for (id, _seq, ref_table, from, to, on_update, on_delete) in fk_rows {
            let entry = fk_map.entry(id).or_insert_with(|| ForeignKeyEntry {
                name: format!("fk_{table}_{id}"),
                columns: vec![],
                referenced_table: ref_table,
                referenced_schema: None,
                referenced_columns: vec![],
                on_delete: Some(on_delete),
                on_update: Some(on_update),
            });
            entry.columns.push(from);
            entry.referenced_columns.push(to);
        }
        let foreign_keys: Vec<ForeignKeyEntry> = fk_map.into_values().collect();

        // DDL
        let ddl: Option<String> = sqlx::query_as::<_, (String,)>(
            "SELECT sql FROM sqlite_master WHERE name = ? AND type IN ('table', 'view')",
        )
        .bind(table)
        .fetch_one(&self.pool)
        .await
        .ok()
        .map(|(s,)| s);

        // 行数
        let count: (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM \"{}\"", table.replace('"', "\"\"")))
            .fetch_one(&self.pool)
            .await
            .unwrap_or((0,));

        Ok(TableDetail {
            name: table.to_string(),
            schema: Some("main".to_string()),
            kind: TableKind::Table,
            columns,
            indexes,
            foreign_keys,
            create_sql: ddl,
            comment: None,
            estimated_rows: Some(count.0),
            size_bytes: None,
        })
    }

    async fn list_routines(&self, _schema: Option<&str>) -> Result<Vec<RoutineEntry>, AnySqlError> {
        // SQLite 不支持存储过程
        Ok(vec![])
    }

    async fn list_triggers(&self, _schema: Option<&str>) -> Result<Vec<TriggerEntry>, AnySqlError> {
        let rows = sqlx::query_as::<_, (String, String, Option<String>)>(
            "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY tbl_name, name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        Ok(rows
            .into_iter()
            .map(|(name, table, sql)| TriggerEntry {
                name,
                table_name: table,
                schema: Some("main".to_string()),
                event: String::new(),
                timing: String::new(),
                definition: sql,
            })
            .collect())
    }

    async fn list_active_queries(&self) -> Result<Vec<ActiveQuery>, AnySqlError> {
        // SQLite 是嵌入式引擎，没有进程列表
        Ok(vec![])
    }

    async fn kill_query(&self, _pid: &str) -> Result<(), AnySqlError> {
        Err(AnySqlError::Unsupported(
            "SQLite does not support killing queries".into(),
        ))
    }

    async fn list_variables(&self, _filter: Option<&str>) -> Result<Vec<ServerVariable>, AnySqlError> {
        // 返回常用 PRAGMA 值
        let pragmas = [
            "journal_mode",
            "synchronous",
            "cache_size",
            "page_size",
            "temp_store",
            "wal_autocheckpoint",
            "foreign_keys",
            "auto_vacuum",
            "busy_timeout",
            "encoding",
        ];

        let mut vars = Vec::new();
        for name in pragmas {
            let result: Option<(String,)> = sqlx::query_as(&format!("PRAGMA {name}"))
                .fetch_one(&self.pool)
                .await
                .ok();
            if let Some((value,)) = result {
                vars.push(ServerVariable {
                    name: name.to_string(),
                    value,
                    description: None,
                });
            }
        }
        Ok(vars)
    }

    async fn switch_database(&self, _database: &str) -> Result<(), AnySqlError> {
        Err(AnySqlError::Unsupported(
            "SQLite does not support switching databases".into(),
        ))
    }
}
