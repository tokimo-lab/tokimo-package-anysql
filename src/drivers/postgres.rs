use std::fmt::Write as _;
use std::time::Instant;

use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Column, Row, TypeInfo};

use crate::connector::DatabaseConnector;
use crate::error::AnySqlError;
use crate::types::*;

pub struct PgConnector {
    pool: PgPool,
}

impl PgConnector {
    pub async fn connect(config: &DbConnectionConfig) -> Result<Self, AnySqlError> {
        let url = config.to_url();
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .connect(&url)
            .await
            .map_err(|e| AnySqlError::Connection(e.to_string()))?;
        Ok(Self { pool })
    }
}

/// 把 `PgRow` 的一列转成 `serde_json::Value`
fn pg_column_to_json(row: &PgRow, idx: usize) -> serde_json::Value {
    // 尝试常见类型，失败则 fallback 到字符串
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

    // 基本类型
    try_get!(bool);
    try_get!(i16);
    try_get!(i32);
    try_get!(i64);
    try_get!(f32);
    try_get!(f64);
    try_get!(String);

    // UUID
    try_get!(uuid::Uuid);

    // 日期时间
    try_get!(chrono::NaiveDateTime);
    try_get!(chrono::NaiveDate);
    try_get!(chrono::NaiveTime);
    try_get!(chrono::DateTime<chrono::Utc>);

    // JSON / JSONB
    try_get!(serde_json::Value);

    // 字节数组 (bytea) — 显示为十六进制
    if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
        return match v {
            Some(bytes) => {
                let hex: String = bytes.iter().fold(String::new(), |mut s, b| {
                    write!(s, "{b:02x}").unwrap();
                    s
                });
                serde_json::Value::String(format!("\\x{hex}"))
            }
            None => serde_json::Value::Null,
        };
    }

    // 数组类型 (text[], int4[], uuid[], etc.)
    try_get!(Vec<bool>);
    try_get!(Vec<i16>);
    try_get!(Vec<i32>);
    try_get!(Vec<i64>);
    try_get!(Vec<f32>);
    try_get!(Vec<f64>);
    try_get!(Vec<String>);
    try_get!(Vec<uuid::Uuid>);
    try_get!(Vec<chrono::NaiveDateTime>);
    try_get!(Vec<chrono::DateTime<chrono::Utc>>);
    try_get!(Vec<serde_json::Value>);

    // fallback: 尝试当 string 拿
    if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
        return match v {
            Some(s) => serde_json::Value::String(s),
            None => serde_json::Value::Null,
        };
    }

    serde_json::Value::Null
}

#[allow(clippy::too_many_lines)]
#[async_trait::async_trait]
impl DatabaseConnector for PgConnector {
    fn driver(&self) -> DbDriver {
        DbDriver::Postgres
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

        // 判断是否为 SELECT 类语句（需要返回行）
        let trimmed = sql.trim_start().to_uppercase();
        let is_query = trimmed.starts_with("SELECT")
            || trimmed.starts_with("WITH")
            || trimmed.starts_with("TABLE")
            || trimmed.starts_with("VALUES")
            || trimmed.starts_with("SHOW")
            || trimmed.starts_with("EXPLAIN");

        if is_query {
            let rows: Vec<PgRow> = sqlx::query(sql)
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
                        map.insert(col.name.clone(), pg_column_to_json(row, i));
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
        let row = sqlx::query_as::<_, (String, String, String)>(
            "SELECT version(),
                    current_database(),
                    current_user",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        let (version, database, user) = row;

        // 活跃连接数
        let active: (i64,) = sqlx::query_as("SELECT count(*) FROM pg_stat_activity WHERE state = 'active'")
            .fetch_one(&self.pool)
            .await
            .map_err(AnySqlError::from)?;

        // 最大连接数
        let max_conn: (String,) = sqlx::query_as("SHOW max_connections")
            .fetch_one(&self.pool)
            .await
            .map_err(AnySqlError::from)?;

        // 数据库大小
        let db_size: (Option<i64>,) = sqlx::query_as("SELECT pg_database_size(current_database())")
            .fetch_one(&self.pool)
            .await
            .map_err(AnySqlError::from)?;

        // uptime
        let uptime: (Option<String>,) =
            sqlx::query_as("SELECT extract(epoch from now() - pg_postmaster_start_time())::text")
                .fetch_one(&self.pool)
                .await
                .unwrap_or((None,));

        Ok(DatabaseOverview {
            server_version: version,
            uptime_seconds: uptime.0,
            current_database: database,
            current_user: user,
            database_size_bytes: db_size.0,
            active_connections: active.0,
            max_connections: max_conn.0.parse().unwrap_or(100),
        })
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseEntry>, AnySqlError> {
        let rows = sqlx::query_as::<_, (String, Option<i64>, Option<String>)>(
            "SELECT d.datname,
                    pg_database_size(d.datname),
                    pg_encoding_to_char(d.encoding)
             FROM pg_database d
             WHERE d.datistemplate = false
             ORDER BY d.datname",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        Ok(rows
            .into_iter()
            .map(|(name, size, enc)| DatabaseEntry {
                name,
                size_bytes: size,
                encoding: enc,
            })
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaEntry>, AnySqlError> {
        let rows = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT schema_name, schema_owner
             FROM information_schema.schemata
             ORDER BY schema_name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        Ok(rows
            .into_iter()
            .map(|(name, owner)| SchemaEntry { name, owner })
            .collect())
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableEntry>, AnySqlError> {
        let schema = schema.unwrap_or("public");

        let rows = sqlx::query_as::<_, (String, String, Option<i64>, Option<i64>, Option<String>)>(
            "SELECT c.relname,
                    CASE c.relkind
                        WHEN 'r' THEN 'table'
                        WHEN 'v' THEN 'view'
                        WHEN 'm' THEN 'materialized_view'
                        WHEN 'f' THEN 'foreign_table'
                        WHEN 'S' THEN 'sequence'
                        ELSE 'table'
                    END,
                    c.reltuples::bigint,
                    pg_total_relation_size(c.oid),
                    obj_description(c.oid, 'pg_class')
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1
               AND c.relkind IN ('r','v','m','f','S')
             ORDER BY c.relname",
        )
        .bind(schema)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        Ok(rows
            .into_iter()
            .map(|(name, kind_str, rows_est, size, comment)| {
                let kind = match kind_str.as_str() {
                    "view" => TableKind::View,
                    "materialized_view" => TableKind::MaterializedView,
                    "foreign_table" => TableKind::ForeignTable,
                    "sequence" => TableKind::Sequence,
                    _ => TableKind::Table,
                };
                TableEntry {
                    name,
                    schema: Some(schema.to_string()),
                    kind,
                    estimated_rows: rows_est,
                    size_bytes: size,
                    comment,
                }
            })
            .collect())
    }

    async fn describe_table(&self, table: &str, schema: Option<&str>) -> Result<TableDetail, AnySqlError> {
        let schema = schema.unwrap_or("public");

        // 列信息
        let col_rows = sqlx::query_as::<_, (String, String, String, Option<String>, Option<i32>, i32)>(
            "SELECT c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    c.ordinal_position
             FROM information_schema.columns c
             WHERE c.table_schema = $1 AND c.table_name = $2
             ORDER BY c.ordinal_position",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        // 主键列
        let pk_rows = sqlx::query_as::<_, (String,)>(
            "SELECT a.attname
             FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
             JOIN pg_class c ON c.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        let pk_names: Vec<String> = pk_rows.into_iter().map(|r| r.0).collect();

        // 列注释
        let comment_rows = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT a.attname, col_description(c.oid, a.attnum)
             FROM pg_attribute a
             JOIN pg_class c ON c.oid = a.attrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        let comment_map: std::collections::HashMap<String, Option<String>> = comment_rows.into_iter().collect();

        let columns: Vec<ColumnDetail> = col_rows
            .into_iter()
            .map(|(name, dtype, nullable, default, max_len, ordinal)| {
                let comment = comment_map.get(&name).cloned().flatten();
                ColumnDetail {
                    is_primary_key: pk_names.contains(&name),
                    name,
                    data_type: dtype,
                    is_nullable: nullable == "YES",
                    default_value: default,
                    comment,
                    max_length: max_len,
                    ordinal: ordinal as usize,
                }
            })
            .collect();

        // 索引
        let idx_rows = sqlx::query_as::<_, (String, bool, bool, Option<String>)>(
            "SELECT i.relname,
                    ix.indisunique,
                    ix.indisprimary,
                    am.amname
             FROM pg_index ix
             JOIN pg_class t ON t.oid = ix.indrelid
             JOIN pg_class i ON i.oid = ix.indexrelid
             JOIN pg_namespace n ON n.oid = t.relnamespace
             LEFT JOIN pg_am am ON am.oid = i.relam
             WHERE n.nspname = $1 AND t.relname = $2",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        let mut indexes = Vec::new();
        for (idx_name, is_unique, is_primary, idx_type) in idx_rows {
            let idx_cols = sqlx::query_as::<_, (String,)>(
                "SELECT a.attname
                 FROM pg_index ix
                 JOIN pg_class i ON i.oid = ix.indexrelid
                 JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = ANY(ix.indkey)
                 WHERE i.relname = $1
                 ORDER BY array_position(ix.indkey, a.attnum)",
            )
            .bind(&idx_name)
            .fetch_all(&self.pool)
            .await
            .unwrap_or_default();

            indexes.push(IndexEntry {
                name: idx_name,
                columns: idx_cols.into_iter().map(|r| r.0).collect(),
                is_unique,
                is_primary,
                index_type: idx_type,
            });
        }

        // 外键
        let fk_rows = sqlx::query_as::<_, (String, String, String, String, Option<String>, Option<String>)>(
            "SELECT tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS referenced_table,
                    ccu.column_name AS referenced_column,
                    rc.delete_rule,
                    rc.update_rule
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
             JOIN information_schema.constraint_column_usage ccu
               ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
             JOIN information_schema.referential_constraints rc
               ON rc.constraint_name = tc.constraint_name AND rc.constraint_schema = tc.table_schema
             WHERE tc.constraint_type = 'FOREIGN KEY'
               AND tc.table_schema = $1
               AND tc.table_name = $2
             ORDER BY tc.constraint_name, kcu.ordinal_position",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        // 把同名 FK 的列聚合
        let mut fk_map: std::collections::HashMap<String, ForeignKeyEntry> = std::collections::HashMap::new();
        for (name, col, ref_table, ref_col, on_del, on_upd) in fk_rows {
            let entry = fk_map.entry(name.clone()).or_insert_with(|| ForeignKeyEntry {
                name,
                columns: vec![],
                referenced_table: ref_table,
                referenced_schema: Some(schema.to_string()),
                referenced_columns: vec![],
                on_delete: on_del,
                on_update: on_upd,
            });
            entry.columns.push(col);
            entry.referenced_columns.push(ref_col);
        }
        let foreign_keys: Vec<ForeignKeyEntry> = fk_map.into_values().collect();

        // 表 DDL（使用 pg_get_tabledef 或 fallback）
        let create_sql = None; // PostgreSQL 没有原生 SHOW CREATE TABLE

        // 表注释
        let table_comment: (Option<String>,) = sqlx::query_as(
            "SELECT obj_description(c.oid, 'pg_class')
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
        )
        .bind(schema)
        .bind(table)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((None,));

        // 行数 & 大小
        let stats: (Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT c.reltuples::bigint, pg_total_relation_size(c.oid)
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
        )
        .bind(schema)
        .bind(table)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((None, None));

        // 表类型
        let kind_row: (String,) = sqlx::query_as(
            "SELECT CASE c.relkind
                WHEN 'r' THEN 'table'
                WHEN 'v' THEN 'view'
                WHEN 'm' THEN 'materialized_view'
                WHEN 'f' THEN 'foreign_table'
                WHEN 'S' THEN 'sequence'
                ELSE 'table'
             END
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
        )
        .bind(schema)
        .bind(table)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(("table".to_string(),));

        let kind = match kind_row.0.as_str() {
            "view" => TableKind::View,
            "materialized_view" => TableKind::MaterializedView,
            "foreign_table" => TableKind::ForeignTable,
            "sequence" => TableKind::Sequence,
            _ => TableKind::Table,
        };

        Ok(TableDetail {
            name: table.to_string(),
            schema: Some(schema.to_string()),
            kind,
            columns,
            indexes,
            foreign_keys,
            create_sql,
            comment: table_comment.0,
            estimated_rows: stats.0,
            size_bytes: stats.1,
        })
    }

    async fn list_routines(&self, schema: Option<&str>) -> Result<Vec<RoutineEntry>, AnySqlError> {
        let schema = schema.unwrap_or("public");

        let rows = sqlx::query_as::<_, (String, String, Option<String>, Option<String>, Option<String>)>(
            "SELECT p.proname,
                    CASE p.prokind WHEN 'f' THEN 'function' WHEN 'p' THEN 'procedure' WHEN 'a' THEN 'aggregate' WHEN 'w' THEN 'window' ELSE 'function' END,
                    pg_get_function_result(p.oid),
                    l.lanname,
                    pg_get_functiondef(p.oid)
             FROM pg_proc p
             JOIN pg_namespace n ON n.oid = p.pronamespace
             LEFT JOIN pg_language l ON l.oid = p.prolang
             WHERE n.nspname = $1
             ORDER BY p.proname",
        )
        .bind(schema)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        Ok(rows
            .into_iter()
            .map(|(name, kind, ret, lang, def)| RoutineEntry {
                name,
                schema: Some(schema.to_string()),
                kind,
                return_type: ret,
                language: lang,
                definition: def,
            })
            .collect())
    }

    async fn list_triggers(&self, schema: Option<&str>) -> Result<Vec<TriggerEntry>, AnySqlError> {
        let schema = schema.unwrap_or("public");

        let rows = sqlx::query_as::<_, (String, String, String, String, Option<String>)>(
            "SELECT t.trigger_name,
                    t.event_object_table,
                    t.event_manipulation,
                    t.action_timing,
                    t.action_statement
             FROM information_schema.triggers t
             WHERE t.trigger_schema = $1
             ORDER BY t.event_object_table, t.trigger_name",
        )
        .bind(schema)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        Ok(rows
            .into_iter()
            .map(|(name, table, event, timing, def)| TriggerEntry {
                name,
                table_name: table,
                schema: Some(schema.to_string()),
                event,
                timing,
                definition: def,
            })
            .collect())
    }

    async fn list_active_queries(&self) -> Result<Vec<ActiveQuery>, AnySqlError> {
        let rows = sqlx::query_as::<
            _,
            (
                i32,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
            ),
        >(
            "SELECT pid,
                    usename,
                    datname,
                    query,
                    state,
                    query_start::text,
                    (now() - query_start)::text,
                    client_addr::text
             FROM pg_stat_activity
             WHERE state IS NOT NULL
             ORDER BY query_start DESC NULLS LAST",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        Ok(rows
            .into_iter()
            .map(|(pid, user, db, query, state, started, dur, addr)| ActiveQuery {
                pid: pid.to_string(),
                username: user,
                database: db,
                query,
                state,
                started_at: started,
                duration: dur,
                client_addr: addr,
            })
            .collect())
    }

    async fn kill_query(&self, pid: &str) -> Result<(), AnySqlError> {
        let pid: i32 = pid.parse().map_err(|_| AnySqlError::BadInput("invalid pid".into()))?;
        sqlx::query("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .execute(&self.pool)
            .await
            .map_err(AnySqlError::from)?;
        Ok(())
    }

    async fn list_variables(&self, filter: Option<&str>) -> Result<Vec<ServerVariable>, AnySqlError> {
        let query = if let Some(pattern) = filter {
            format!(
                "SELECT name, setting, short_desc FROM pg_settings WHERE name ILIKE '%{pat}%' OR short_desc ILIKE '%{pat}%' ORDER BY name",
                pat = pattern.replace('\'', "''")
            )
        } else {
            "SELECT name, setting, short_desc FROM pg_settings ORDER BY name".to_string()
        };

        let rows = sqlx::query_as::<_, (String, String, Option<String>)>(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(AnySqlError::from)?;

        Ok(rows
            .into_iter()
            .map(|(name, value, desc)| ServerVariable {
                name,
                value,
                description: desc,
            })
            .collect())
    }

    async fn switch_database(&self, _database: &str) -> Result<(), AnySqlError> {
        // PostgreSQL 不支持运行时切换数据库（需要新建连接）
        // 这个由 SessionManager 层面处理：断开旧连接，建新连接
        Err(AnySqlError::Unsupported(
            "PostgreSQL does not support USE DATABASE; reconnect with the target database instead".into(),
        ))
    }
}
