use std::time::Instant;

use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::{Column, Row, TypeInfo};

use crate::connector::DatabaseConnector;
use crate::error::AnySqlError;
use crate::types::*;

pub struct MysqlConnector {
    pool: MySqlPool,
}

impl MysqlConnector {
    pub async fn connect(config: &DbConnectionConfig) -> Result<Self, AnySqlError> {
        let url = config.to_url();
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .connect(&url)
            .await
            .map_err(|e| AnySqlError::Connection(e.to_string()))?;
        Ok(Self { pool })
    }
}

// ── Row 安全提取工具 ─────────────────────────────────────────────────────────

/// 从 `MySqlRow` 安全提取 string（兼容 VARCHAR / VARBINARY / BLOB）
fn get_str(row: &MySqlRow, idx: usize) -> String {
    row.try_get::<String, _>(idx)
        .or_else(|_| {
            row.try_get::<Vec<u8>, _>(idx)
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        })
        .unwrap_or_default()
}

fn get_opt_str(row: &MySqlRow, idx: usize) -> Option<String> {
    row.try_get::<Option<String>, _>(idx).ok().flatten().or_else(|| {
        row.try_get::<Option<Vec<u8>>, _>(idx)
            .ok()
            .flatten()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    })
}

fn get_opt_i64(row: &MySqlRow, idx: usize) -> Option<i64> {
    row.try_get::<Option<i64>, _>(idx)
        .ok()
        .flatten()
        .or_else(|| row.try_get::<Option<u64>, _>(idx).ok().flatten().map(|v| v as i64))
        .or_else(|| row.try_get::<Option<i32>, _>(idx).ok().flatten().map(i64::from))
}

fn get_i64(row: &MySqlRow, idx: usize) -> i64 {
    get_opt_i64(row, idx).unwrap_or(0)
}

fn get_opt_i32(row: &MySqlRow, idx: usize) -> Option<i32> {
    row.try_get::<Option<i32>, _>(idx)
        .ok()
        .flatten()
        .or_else(|| row.try_get::<Option<u32>, _>(idx).ok().flatten().map(|v| v as i32))
        .or_else(|| row.try_get::<Option<i64>, _>(idx).ok().flatten().map(|v| v as i32))
}

// ── execute_sql 的通用 JSON 提取 ─────────────────────────────────────────────

fn mysql_column_to_json(row: &MySqlRow, idx: usize) -> serde_json::Value {
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
    try_get!(i16);
    try_get!(i32);
    try_get!(i64);
    try_get!(u64);
    try_get!(u32);
    try_get!(f32);
    try_get!(f64);
    try_get!(String);

    // 日期时间
    try_get!(chrono::NaiveDateTime);
    try_get!(chrono::NaiveDate);
    try_get!(chrono::NaiveTime);

    // JSON
    try_get!(serde_json::Value);

    // 兜底：VARBINARY / BLOB → String
    if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
        return match v {
            Some(b) => serde_json::Value::String(String::from_utf8_lossy(&b).into_owned()),
            None => serde_json::Value::Null,
        };
    }

    serde_json::Value::Null
}

#[async_trait::async_trait]
impl DatabaseConnector for MysqlConnector {
    fn driver(&self) -> DbDriver {
        DbDriver::Mysql
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
            || trimmed.starts_with("SHOW")
            || trimmed.starts_with("DESCRIBE")
            || trimmed.starts_with("DESC")
            || trimmed.starts_with("EXPLAIN");

        if is_query {
            let rows: Vec<MySqlRow> = sqlx::query(sql)
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
                        map.insert(col.name.clone(), mysql_column_to_json(row, i));
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
        let version: (String,) = sqlx::query_as("SELECT version()")
            .fetch_one(&self.pool)
            .await
            .map_err(AnySqlError::from)?;

        let db: (Option<String>,) = sqlx::query_as("SELECT database()")
            .fetch_one(&self.pool)
            .await
            .map_err(AnySqlError::from)?;

        let user: (String,) = sqlx::query_as("SELECT current_user()")
            .fetch_one(&self.pool)
            .await
            .map_err(AnySqlError::from)?;

        // uptime
        let uptime: (Option<String>,) = sqlx::query_as(
            "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime'",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or((None,));

        // 活跃连接
        let active: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM information_schema.processlist WHERE COMMAND != 'Sleep'")
                .fetch_one(&self.pool)
                .await
                .unwrap_or((0,));

        // 最大连接
        let max_conn: (String,) = sqlx::query_as(
            "SELECT VARIABLE_VALUE FROM performance_schema.global_variables WHERE VARIABLE_NAME = 'max_connections'",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(("100".to_string(),));

        // 数据库大小
        let current_db = db.0.clone().unwrap_or_default();
        let db_size: (Option<i64>,) = if current_db.is_empty() {
            (None,)
        } else {
            sqlx::query_as(
                "SELECT SUM(data_length + index_length) FROM information_schema.tables WHERE table_schema = ?",
            )
            .bind(&current_db)
            .fetch_one(&self.pool)
            .await
            .unwrap_or((None,))
        };

        Ok(DatabaseOverview {
            server_version: version.0,
            uptime_seconds: uptime.0,
            current_database: current_db,
            current_user: user.0,
            database_size_bytes: db_size.0,
            active_connections: active.0,
            max_connections: max_conn.0.parse().unwrap_or(100),
        })
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseEntry>, AnySqlError> {
        // SHOW DATABASES returns VARBINARY in MySQL — use Row extraction
        let rows: Vec<MySqlRow> = sqlx::query("SHOW DATABASES")
            .fetch_all(&self.pool)
            .await
            .map_err(AnySqlError::from)?;

        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            let name = get_str(row, 0);

            let size: (Option<i64>,) = sqlx::query_as(
                "SELECT SUM(data_length + index_length) FROM information_schema.tables WHERE table_schema = ?",
            )
            .bind(&name)
            .fetch_one(&self.pool)
            .await
            .unwrap_or((None,));

            let charset_row: Vec<MySqlRow> =
                sqlx::query("SELECT default_character_set_name FROM information_schema.schemata WHERE schema_name = ?")
                    .bind(&name)
                    .fetch_all(&self.pool)
                    .await
                    .unwrap_or_default();

            let charset = charset_row.first().map(|r| get_str(r, 0));

            result.push(DatabaseEntry {
                name,
                size_bytes: size.0,
                encoding: charset,
            });
        }
        Ok(result)
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaEntry>, AnySqlError> {
        // MySQL 中 schema = database
        let dbs = self.list_databases().await?;
        Ok(dbs
            .into_iter()
            .map(|d| SchemaEntry {
                name: d.name,
                owner: None,
            })
            .collect())
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableEntry>, AnySqlError> {
        let db = if let Some(s) = schema {
            s.to_string()
        } else {
            let row: (Option<String>,) = sqlx::query_as("SELECT database()")
                .fetch_one(&self.pool)
                .await
                .map_err(AnySqlError::from)?;
            row.0.unwrap_or_default()
        };

        let rows: Vec<MySqlRow> = sqlx::query(
            "SELECT TABLE_NAME,
                    TABLE_TYPE,
                    TABLE_ROWS,
                    DATA_LENGTH + INDEX_LENGTH,
                    TABLE_COMMENT
             FROM information_schema.tables
             WHERE TABLE_SCHEMA = ?
             ORDER BY TABLE_NAME",
        )
        .bind(&db)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        Ok(rows
            .iter()
            .map(|row| {
                let name = get_str(row, 0);
                let kind_str = get_str(row, 1);
                let rows_est = get_opt_i64(row, 2);
                let size = get_opt_i64(row, 3);
                let comment = get_opt_str(row, 4).filter(|c| !c.is_empty());

                let kind = if kind_str.contains("VIEW") {
                    TableKind::View
                } else {
                    TableKind::Table
                };
                TableEntry {
                    name,
                    schema: Some(db.clone()),
                    kind,
                    estimated_rows: rows_est,
                    size_bytes: size,
                    comment,
                }
            })
            .collect())
    }

    async fn describe_table(&self, table: &str, schema: Option<&str>) -> Result<TableDetail, AnySqlError> {
        let db = if let Some(s) = schema {
            s.to_string()
        } else {
            let row: (Option<String>,) = sqlx::query_as("SELECT database()")
                .fetch_one(&self.pool)
                .await
                .map_err(AnySqlError::from)?;
            row.0.unwrap_or_default()
        };

        // 列
        let col_rows: Vec<MySqlRow> = sqlx::query(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH, ORDINAL_POSITION
             FROM information_schema.columns
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
             ORDER BY ORDINAL_POSITION",
        )
        .bind(&db)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        let columns: Vec<ColumnDetail> = col_rows
            .iter()
            .map(|row| {
                let name = get_str(row, 0);
                let dtype = get_str(row, 1);
                let nullable = get_str(row, 2);
                let key = get_str(row, 3);
                let default = get_opt_str(row, 4);
                let max_len = get_opt_i32(row, 5);
                let ord = get_i64(row, 6) as usize;

                ColumnDetail {
                    is_primary_key: key == "PRI",
                    name,
                    data_type: dtype,
                    is_nullable: nullable == "YES",
                    default_value: default,
                    comment: None,
                    max_length: max_len,
                    ordinal: ord,
                }
            })
            .collect();

        // 索引
        let idx_rows: Vec<MySqlRow> = sqlx::query(
            "SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME, INDEX_TYPE
             FROM information_schema.statistics
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
             ORDER BY INDEX_NAME, SEQ_IN_INDEX",
        )
        .bind(&db)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        let mut idx_map: std::collections::HashMap<String, IndexEntry> = std::collections::HashMap::new();
        for row in &idx_rows {
            let name = get_str(row, 0);
            let non_unique = get_i64(row, 1);
            let col = get_str(row, 2);
            let idx_type = get_opt_str(row, 3);

            let entry = idx_map.entry(name.clone()).or_insert_with(|| IndexEntry {
                is_primary: name == "PRIMARY",
                is_unique: non_unique == 0,
                name,
                columns: vec![],
                index_type: idx_type,
            });
            entry.columns.push(col);
        }
        let indexes: Vec<IndexEntry> = idx_map.into_values().collect();

        // 外键
        let fk_rows: Vec<MySqlRow> = sqlx::query(
            "SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME,
                    kcu.REFERENCED_COLUMN_NAME, rc.DELETE_RULE, rc.UPDATE_RULE
             FROM information_schema.key_column_usage kcu
             JOIN information_schema.referential_constraints rc
               ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
             WHERE kcu.TABLE_SCHEMA = ? AND kcu.TABLE_NAME = ? AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
             ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
        )
        .bind(&db)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        let mut fk_map: std::collections::HashMap<String, ForeignKeyEntry> = std::collections::HashMap::new();
        for row in &fk_rows {
            let name = get_str(row, 0);
            let col = get_str(row, 1);
            let ref_table = get_str(row, 2);
            let ref_col = get_str(row, 3);
            let on_del = get_opt_str(row, 4);
            let on_upd = get_opt_str(row, 5);

            let entry = fk_map.entry(name.clone()).or_insert_with(|| ForeignKeyEntry {
                name,
                columns: vec![],
                referenced_table: ref_table,
                referenced_schema: Some(db.clone()),
                referenced_columns: vec![],
                on_delete: on_del,
                on_update: on_upd,
            });
            entry.columns.push(col);
            entry.referenced_columns.push(ref_col);
        }
        let foreign_keys: Vec<ForeignKeyEntry> = fk_map.into_values().collect();

        // SHOW CREATE TABLE
        let create_row: Option<MySqlRow> = sqlx::query(&format!(
            "SHOW CREATE TABLE `{}`.`{}`",
            db.replace('`', "``"),
            table.replace('`', "``")
        ))
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        let create_sql = create_row.map(|r| get_str(&r, 1));

        // 表信息
        let info_rows: Vec<MySqlRow> = sqlx::query(
            "SELECT TABLE_ROWS, DATA_LENGTH + INDEX_LENGTH, TABLE_COMMENT
             FROM information_schema.tables
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        )
        .bind(&db)
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        let (estimated_rows, size_bytes, comment) = if let Some(row) = info_rows.first() {
            (
                get_opt_i64(row, 0),
                get_opt_i64(row, 1),
                get_opt_str(row, 2).filter(|c| !c.is_empty()),
            )
        } else {
            (None, None, None)
        };

        Ok(TableDetail {
            name: table.to_string(),
            schema: Some(db),
            kind: TableKind::Table,
            columns,
            indexes,
            foreign_keys,
            create_sql,
            comment,
            estimated_rows,
            size_bytes,
        })
    }

    async fn list_routines(&self, schema: Option<&str>) -> Result<Vec<RoutineEntry>, AnySqlError> {
        let db = if let Some(s) = schema {
            s.to_string()
        } else {
            let row: (Option<String>,) = sqlx::query_as("SELECT database()")
                .fetch_one(&self.pool)
                .await
                .map_err(AnySqlError::from)?;
            row.0.unwrap_or_default()
        };

        let rows: Vec<MySqlRow> = sqlx::query(
            "SELECT ROUTINE_NAME, ROUTINE_TYPE, DTD_IDENTIFIER, EXTERNAL_LANGUAGE, ROUTINE_DEFINITION
             FROM information_schema.routines
             WHERE ROUTINE_SCHEMA = ?
             ORDER BY ROUTINE_NAME",
        )
        .bind(&db)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        Ok(rows
            .iter()
            .map(|row| RoutineEntry {
                name: get_str(row, 0),
                schema: Some(db.clone()),
                kind: get_str(row, 1).to_lowercase(),
                return_type: get_opt_str(row, 2),
                language: get_opt_str(row, 3),
                definition: get_opt_str(row, 4),
            })
            .collect())
    }

    async fn list_triggers(&self, schema: Option<&str>) -> Result<Vec<TriggerEntry>, AnySqlError> {
        let db = if let Some(s) = schema {
            s.to_string()
        } else {
            let row: (Option<String>,) = sqlx::query_as("SELECT database()")
                .fetch_one(&self.pool)
                .await
                .map_err(AnySqlError::from)?;
            row.0.unwrap_or_default()
        };

        let rows: Vec<MySqlRow> = sqlx::query(
            "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION,
                    ACTION_TIMING, ACTION_STATEMENT
             FROM information_schema.triggers
             WHERE TRIGGER_SCHEMA = ?
             ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME",
        )
        .bind(&db)
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        Ok(rows
            .iter()
            .map(|row| TriggerEntry {
                name: get_str(row, 0),
                table_name: get_str(row, 1),
                schema: Some(db.clone()),
                event: get_str(row, 2),
                timing: get_str(row, 3),
                definition: get_opt_str(row, 4),
            })
            .collect())
    }

    async fn list_active_queries(&self) -> Result<Vec<ActiveQuery>, AnySqlError> {
        let rows: Vec<MySqlRow> = sqlx::query(
            "SELECT ID, USER, DB, INFO, COMMAND, TIME, HOST
             FROM information_schema.processlist
             ORDER BY TIME DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(AnySqlError::from)?;

        Ok(rows
            .iter()
            .map(|row| {
                let id = row
                    .try_get::<u64, _>(0)
                    .map(|v| v.to_string())
                    .or_else(|_| row.try_get::<i64, _>(0).map(|v| v.to_string()))
                    .unwrap_or_default();
                ActiveQuery {
                    pid: id,
                    username: get_opt_str(row, 1),
                    database: get_opt_str(row, 2),
                    query: get_opt_str(row, 3),
                    state: get_opt_str(row, 4),
                    started_at: None,
                    duration: get_opt_i64(row, 5).map(|d| format!("{d}s")),
                    client_addr: get_opt_str(row, 6),
                }
            })
            .collect())
    }

    async fn kill_query(&self, pid: &str) -> Result<(), AnySqlError> {
        let pid: u64 = pid.parse().map_err(|_| AnySqlError::BadInput("invalid pid".into()))?;
        sqlx::query(&format!("KILL {pid}"))
            .execute(&self.pool)
            .await
            .map_err(AnySqlError::from)?;
        Ok(())
    }

    async fn list_variables(&self, filter: Option<&str>) -> Result<Vec<ServerVariable>, AnySqlError> {
        let query = if let Some(pattern) = filter {
            format!("SHOW VARIABLES LIKE '%{pat}%'", pat = pattern.replace('\'', "\\'"))
        } else {
            "SHOW VARIABLES".to_string()
        };

        let rows: Vec<MySqlRow> = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(AnySqlError::from)?;

        Ok(rows
            .iter()
            .map(|row| ServerVariable {
                name: get_str(row, 0),
                value: get_str(row, 1),
                description: None,
            })
            .collect())
    }

    async fn switch_database(&self, database: &str) -> Result<(), AnySqlError> {
        sqlx::query(&format!("USE `{}`", database.replace('`', "``")))
            .execute(&self.pool)
            .await
            .map_err(AnySqlError::from)?;
        Ok(())
    }
}
