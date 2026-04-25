use std::fmt::Write as _;
use std::time::Instant;

use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::connector::DatabaseConnector;
use crate::error::AnySqlError;
use crate::types::*;

pub struct MssqlConnector {
    client: Mutex<Client<Compat<TcpStream>>>,
}

impl MssqlConnector {
    pub async fn connect(config: &DbConnectionConfig) -> Result<Self, AnySqlError> {
        let mut tib_config = Config::new();
        tib_config.host(&config.host);
        tib_config.port(config.port.unwrap_or(1433));
        tib_config.authentication(AuthMethod::sql_server(
            config.username.as_deref().unwrap_or("sa"),
            config.password.as_deref().unwrap_or(""),
        ));
        if let Some(db) = &config.database {
            tib_config.database(db);
        }

        // 默认不加密（开发环境常见），通过 params 可设置 encrypt=true
        let mut encrypt = false;
        let mut trust_cert = false;
        if let Some(params) = &config.params {
            for part in params.split(';') {
                let part = part.trim();
                if let Some((k, v)) = part.split_once('=') {
                    match k.trim().to_lowercase().as_str() {
                        "encrypt" => encrypt = v.trim().eq_ignore_ascii_case("true"),
                        "trustservercertificate" | "trust_cert" => {
                            trust_cert = v.trim().eq_ignore_ascii_case("true");
                        }
                        _ => {}
                    }
                }
            }
        }
        if encrypt {
            tib_config.encryption(EncryptionLevel::Required);
        } else {
            tib_config.encryption(EncryptionLevel::NotSupported);
        }
        if trust_cert {
            tib_config.trust_cert();
        }

        let tcp = TcpStream::connect(tib_config.get_addr())
            .await
            .map_err(|e| AnySqlError::Connection(format!("TCP connect failed: {e}")))?;
        tcp.set_nodelay(true)
            .map_err(|e| AnySqlError::Connection(format!("set_nodelay failed: {e}")))?;

        let client = Client::connect(tib_config, tcp.compat_write())
            .await
            .map_err(|e| AnySqlError::Connection(e.to_string()))?;

        Ok(Self {
            client: Mutex::new(client),
        })
    }
}

/// 从 tiberius Row 中安全提取 JSON 值
fn mssql_column_to_json(row: &tiberius::Row, idx: usize) -> serde_json::Value {
    macro_rules! try_get {
        ($t:ty) => {
            if let Ok(v) = row.try_get::<$t, _>(idx) {
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
    try_get!(f32);
    try_get!(f64);
    try_get!(&str);

    // UUID
    if let Ok(v) = row.try_get::<tiberius::Uuid, _>(idx) {
        return match v {
            Some(v) => serde_json::Value::String(v.to_string()),
            None => serde_json::Value::Null,
        };
    }

    // chrono 日期时间
    if let Ok(v) = row.try_get::<chrono::NaiveDateTime, _>(idx) {
        return match v {
            Some(v) => serde_json::Value::String(v.to_string()),
            None => serde_json::Value::Null,
        };
    }
    if let Ok(v) = row.try_get::<chrono::NaiveDate, _>(idx) {
        return match v {
            Some(v) => serde_json::Value::String(v.to_string()),
            None => serde_json::Value::Null,
        };
    }
    if let Ok(v) = row.try_get::<chrono::NaiveTime, _>(idx) {
        return match v {
            Some(v) => serde_json::Value::String(v.to_string()),
            None => serde_json::Value::Null,
        };
    }

    // 兜底：字节数组
    if let Ok(v) = row.try_get::<&[u8], _>(idx) {
        return match v {
            Some(bytes) => {
                let hex: String = bytes.iter().fold(String::new(), |mut s, b| {
                    write!(s, "{b:02x}").unwrap();
                    s
                });
                serde_json::Value::String(format!("0x{hex}"))
            }
            None => serde_json::Value::Null,
        };
    }

    serde_json::Value::Null
}

#[async_trait::async_trait]
impl DatabaseConnector for MssqlConnector {
    fn driver(&self) -> DbDriver {
        DbDriver::Mssql
    }

    async fn ping(&self) -> Result<(), AnySqlError> {
        let mut client = self.client.lock().await;
        client
            .simple_query("SELECT 1")
            .await
            .map_err(|e| AnySqlError::Connection(e.to_string()))?
            .into_row()
            .await
            .map_err(|e| AnySqlError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn execute_sql(&self, sql: &str, max_rows: usize) -> Result<QueryResult, AnySqlError> {
        let start = Instant::now();
        let mut client = self.client.lock().await;

        let trimmed = sql.trim_start().to_uppercase();
        let is_query = trimmed.starts_with("SELECT")
            || trimmed.starts_with("WITH")
            || trimmed.starts_with("EXEC")
            || trimmed.starts_with("SP_")
            || trimmed.starts_with("EXPLAIN");

        if is_query {
            let stream = client
                .simple_query(sql)
                .await
                .map_err(|e| AnySqlError::Query(e.to_string()))?;

            let result = stream
                .into_first_result()
                .await
                .map_err(|e| AnySqlError::Query(e.to_string()))?;

            let elapsed_ms = start.elapsed().as_millis() as u64;

            if result.is_empty() {
                return Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: 0,
                    elapsed_ms,
                    truncated: false,
                });
            }

            let columns: Vec<ColumnInfo> = result[0]
                .columns()
                .iter()
                .enumerate()
                .map(|(i, c)| ColumnInfo {
                    name: c.name().to_string(),
                    data_type: format!("{:?}", c.column_type()),
                    ordinal: i,
                })
                .collect();

            let truncated = result.len() > max_rows;
            let take = result.len().min(max_rows);

            let data_rows: Vec<serde_json::Map<String, serde_json::Value>> = result
                .iter()
                .take(take)
                .map(|row| {
                    let mut map = serde_json::Map::new();
                    for (i, col) in columns.iter().enumerate() {
                        map.insert(col.name.clone(), mssql_column_to_json(row, i));
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
            let result = client
                .execute(sql, &[])
                .await
                .map_err(|e| AnySqlError::Query(e.to_string()))?;

            let elapsed_ms = start.elapsed().as_millis() as u64;
            let total = result.total();
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: total,
                elapsed_ms,
                truncated: false,
            })
        }
    }

    async fn overview(&self) -> Result<DatabaseOverview, AnySqlError> {
        let mut client = self.client.lock().await;

        let stream = client
            .simple_query("SELECT @@VERSION, DB_NAME(), SUSER_SNAME()")
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let row = stream
            .into_row()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?
            .ok_or_else(|| AnySqlError::Query("no result".into()))?;

        let version: String = row.get::<&str, _>(0).unwrap_or("unknown").to_string();
        let database: String = row.get::<&str, _>(1).unwrap_or("unknown").to_string();
        let user: String = row.get::<&str, _>(2).unwrap_or("unknown").to_string();

        // Uptime
        let stream = client
            .simple_query(
                "SELECT DATEDIFF(SECOND, sqlserver_start_time, GETDATE()) \
                 FROM sys.dm_os_sys_info",
            )
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let uptime_row = stream.into_row().await.map_err(|e| AnySqlError::Query(e.to_string()))?;
        let uptime_seconds: Option<String> = uptime_row.and_then(|r| r.get::<i32, _>(0).map(|v| v.to_string()));

        // Active connections
        let stream = client
            .simple_query("SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process = 1")
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let active_row = stream.into_row().await.map_err(|e| AnySqlError::Query(e.to_string()))?;
        let active: i64 = active_row.and_then(|r| r.get::<i32, _>(0).map(i64::from)).unwrap_or(0);

        // Max connections
        let stream = client
            .simple_query("SELECT @@MAX_CONNECTIONS")
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let max_row = stream.into_row().await.map_err(|e| AnySqlError::Query(e.to_string()))?;
        let max_connections: i64 = max_row.and_then(|r| r.get::<i32, _>(0).map(i64::from)).unwrap_or(32767);

        // DB size
        let stream = client
            .simple_query("SELECT SUM(size) * 8 * 1024 FROM sys.database_files WHERE type_desc = 'ROWS'")
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let size_row = stream.into_row().await.map_err(|e| AnySqlError::Query(e.to_string()))?;
        let db_size: Option<i64> = size_row.and_then(|r| r.get::<i32, _>(0).map(i64::from));

        Ok(DatabaseOverview {
            server_version: version,
            uptime_seconds,
            current_database: database,
            current_user: user,
            database_size_bytes: db_size,
            active_connections: active,
            max_connections,
        })
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseEntry>, AnySqlError> {
        let mut client = self.client.lock().await;
        let stream = client
            .simple_query(
                "SELECT d.name, \
                 (SELECT SUM(mf.size) * 8 * 1024 FROM sys.master_files mf \
                  WHERE mf.database_id = d.database_id AND mf.type_desc = 'ROWS'), \
                 d.collation_name \
                 FROM sys.databases d ORDER BY d.name",
            )
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        Ok(rows
            .iter()
            .map(|row| DatabaseEntry {
                name: row.get::<&str, _>(0).unwrap_or("").to_string(),
                size_bytes: row.get::<i32, _>(1).map(i64::from),
                encoding: row.get::<&str, _>(2).map(std::string::ToString::to_string),
            })
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaEntry>, AnySqlError> {
        let mut client = self.client.lock().await;
        let stream = client
            .simple_query(
                "SELECT s.name, p.name AS owner \
                 FROM sys.schemas s \
                 LEFT JOIN sys.database_principals p ON s.principal_id = p.principal_id \
                 ORDER BY s.name",
            )
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        Ok(rows
            .iter()
            .map(|row| SchemaEntry {
                name: row.get::<&str, _>(0).unwrap_or("").to_string(),
                owner: row.get::<&str, _>(1).map(std::string::ToString::to_string),
            })
            .collect())
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableEntry>, AnySqlError> {
        let schema = schema.unwrap_or("dbo");
        let mut client = self.client.lock().await;

        let sql = format!(
            "SELECT t.name, t.type_desc, \
             p.rows, \
             (SELECT SUM(a.total_pages) * 8 * 1024 \
              FROM sys.indexes i \
              JOIN sys.partitions pa ON i.object_id = pa.object_id AND i.index_id = pa.index_id \
              JOIN sys.allocation_units a ON pa.partition_id = a.container_id \
              WHERE i.object_id = t.object_id) AS size_bytes, \
             CAST(ep.value AS NVARCHAR(MAX)) AS comment \
             FROM sys.objects t \
             LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1) \
             LEFT JOIN sys.extended_properties ep \
               ON ep.major_id = t.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description' \
             WHERE t.schema_id = SCHEMA_ID('{schema}') \
             AND t.type IN ('U', 'V') \
             ORDER BY t.name",
            schema = schema.replace('\'', "''")
        );

        let stream = client
            .simple_query(&sql)
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        Ok(rows
            .iter()
            .map(|row| {
                let name = row.get::<&str, _>(0).unwrap_or("").to_string();
                let type_desc = row.get::<&str, _>(1).unwrap_or("").to_string();
                let rows_est: Option<i64> = row.get::<i32, _>(2).map(i64::from);
                let size: Option<i64> = row.get::<i32, _>(3).map(i64::from);
                let comment: Option<String> = row.get::<&str, _>(4).map(std::string::ToString::to_string);

                let kind = if type_desc.contains("VIEW") {
                    TableKind::View
                } else {
                    TableKind::Table
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
        let schema = schema.unwrap_or("dbo");
        let table_esc = table.replace('\'', "''");
        let schema_esc = schema.replace('\'', "''");
        let mut client = self.client.lock().await;

        // Columns
        let col_sql = format!(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.COLUMN_DEFAULT, \
             c.CHARACTER_MAXIMUM_LENGTH, c.ORDINAL_POSITION, \
             CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk, \
             CAST(ep.value AS NVARCHAR(MAX)) AS comment \
             FROM INFORMATION_SCHEMA.COLUMNS c \
             LEFT JOIN ( \
               SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME \
               FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
               JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku \
                 ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA \
               WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
             ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA AND c.TABLE_NAME = pk.TABLE_NAME \
               AND c.COLUMN_NAME = pk.COLUMN_NAME \
             LEFT JOIN sys.columns sc \
               ON sc.object_id = OBJECT_ID('{schema_esc}.{table_esc}') AND sc.name = c.COLUMN_NAME \
             LEFT JOIN sys.extended_properties ep \
               ON ep.major_id = sc.object_id AND ep.minor_id = sc.column_id \
               AND ep.name = 'MS_Description' \
             WHERE c.TABLE_SCHEMA = '{schema_esc}' AND c.TABLE_NAME = '{table_esc}' \
             ORDER BY c.ORDINAL_POSITION"
        );

        let stream = client
            .simple_query(&col_sql)
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let col_rows = stream
            .into_first_result()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let columns: Vec<ColumnDetail> = col_rows
            .iter()
            .map(|row| {
                let name = row.get::<&str, _>(0).unwrap_or("").to_string();
                let data_type = row.get::<&str, _>(1).unwrap_or("").to_string();
                let nullable = row.get::<&str, _>(2).unwrap_or("NO");
                let default = row.get::<&str, _>(3).map(std::string::ToString::to_string);
                let max_len: Option<i32> = row.get::<i32, _>(4);
                let ordinal = row.get::<i32, _>(5).unwrap_or(0) as usize;
                let is_pk = row.get::<i32, _>(6).unwrap_or(0) != 0;
                let comment = row.get::<&str, _>(7).map(std::string::ToString::to_string);

                ColumnDetail {
                    name,
                    data_type,
                    is_nullable: nullable == "YES",
                    is_primary_key: is_pk,
                    default_value: default,
                    comment,
                    max_length: max_len,
                    ordinal,
                }
            })
            .collect();

        // Indexes
        let idx_sql = format!(
            "SELECT i.name, i.is_unique, i.is_primary_key, c.name, i.type_desc \
             FROM sys.indexes i \
             JOIN sys.index_columns ic \
               ON i.object_id = ic.object_id AND i.index_id = ic.index_id \
             JOIN sys.columns c \
               ON ic.object_id = c.object_id AND ic.column_id = c.column_id \
             WHERE i.object_id = OBJECT_ID('{schema_esc}.{table_esc}') AND i.name IS NOT NULL \
             ORDER BY i.name, ic.key_ordinal"
        );
        let stream = client
            .simple_query(&idx_sql)
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let idx_rows = stream
            .into_first_result()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let mut idx_map: std::collections::HashMap<String, IndexEntry> = std::collections::HashMap::new();
        for row in &idx_rows {
            let name = row.get::<&str, _>(0).unwrap_or("").to_string();
            let is_unique = row.get::<bool, _>(1).unwrap_or(false);
            let is_primary = row.get::<bool, _>(2).unwrap_or(false);
            let col = row.get::<&str, _>(3).unwrap_or("").to_string();
            let idx_type = row.get::<&str, _>(4).map(std::string::ToString::to_string);

            let entry = idx_map.entry(name.clone()).or_insert_with(|| IndexEntry {
                name,
                columns: vec![],
                is_unique,
                is_primary,
                index_type: idx_type,
            });
            entry.columns.push(col);
        }
        let indexes: Vec<IndexEntry> = idx_map.into_values().collect();

        // Foreign keys
        let fk_sql = format!(
            "SELECT fk.name, COL_NAME(fkc.parent_object_id, fkc.parent_column_id), \
             OBJECT_NAME(fkc.referenced_object_id), \
             COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id), \
             fk.delete_referential_action_desc, fk.update_referential_action_desc \
             FROM sys.foreign_keys fk \
             JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id \
             WHERE fk.parent_object_id = OBJECT_ID('{schema_esc}.{table_esc}') \
             ORDER BY fk.name, fkc.constraint_column_id"
        );
        let stream = client
            .simple_query(&fk_sql)
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let fk_rows = stream
            .into_first_result()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let mut fk_map: std::collections::HashMap<String, ForeignKeyEntry> = std::collections::HashMap::new();
        for row in &fk_rows {
            let name = row.get::<&str, _>(0).unwrap_or("").to_string();
            let col = row.get::<&str, _>(1).unwrap_or("").to_string();
            let ref_table = row.get::<&str, _>(2).unwrap_or("").to_string();
            let ref_col = row.get::<&str, _>(3).unwrap_or("").to_string();
            let on_del = row.get::<&str, _>(4).map(std::string::ToString::to_string);
            let on_upd = row.get::<&str, _>(5).map(std::string::ToString::to_string);

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

        // Table info (rows + size)
        let info_sql = format!(
            "SELECT p.rows, \
             (SELECT SUM(a.total_pages) * 8 * 1024 FROM sys.indexes i \
              JOIN sys.partitions pa ON i.object_id = pa.object_id AND i.index_id = pa.index_id \
              JOIN sys.allocation_units a ON pa.partition_id = a.container_id \
              WHERE i.object_id = OBJECT_ID('{schema_esc}.{table_esc}')) \
             FROM sys.partitions p \
             WHERE p.object_id = OBJECT_ID('{schema_esc}.{table_esc}') AND p.index_id IN (0, 1)"
        );
        let stream = client
            .simple_query(&info_sql)
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let info_row = stream.into_row().await.map_err(|e| AnySqlError::Query(e.to_string()))?;
        let (estimated_rows, size_bytes) = if let Some(row) = info_row {
            (row.get::<i32, _>(0).map(i64::from), row.get::<i32, _>(1).map(i64::from))
        } else {
            (None, None)
        };

        Ok(TableDetail {
            name: table.to_string(),
            schema: Some(schema.to_string()),
            kind: TableKind::Table,
            columns,
            indexes,
            foreign_keys,
            create_sql: None,
            comment: None,
            estimated_rows,
            size_bytes,
        })
    }

    async fn list_routines(&self, schema: Option<&str>) -> Result<Vec<RoutineEntry>, AnySqlError> {
        let schema = schema.unwrap_or("dbo");
        let mut client = self.client.lock().await;

        let sql = format!(
            "SELECT r.ROUTINE_NAME, r.ROUTINE_TYPE, r.DATA_TYPE, r.EXTERNAL_LANGUAGE, \
             m.definition \
             FROM INFORMATION_SCHEMA.ROUTINES r \
             LEFT JOIN sys.sql_modules m \
               ON m.object_id = OBJECT_ID(r.ROUTINE_SCHEMA + '.' + r.ROUTINE_NAME) \
             WHERE r.ROUTINE_SCHEMA = '{schema}' \
             ORDER BY r.ROUTINE_NAME",
            schema = schema.replace('\'', "''")
        );

        let stream = client
            .simple_query(&sql)
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        Ok(rows
            .iter()
            .map(|row| RoutineEntry {
                name: row.get::<&str, _>(0).unwrap_or("").to_string(),
                schema: Some(schema.to_string()),
                kind: row.get::<&str, _>(1).unwrap_or("procedure").to_lowercase(),
                return_type: row.get::<&str, _>(2).map(std::string::ToString::to_string),
                language: row.get::<&str, _>(3).map(std::string::ToString::to_string),
                definition: row.get::<&str, _>(4).map(std::string::ToString::to_string),
            })
            .collect())
    }

    async fn list_triggers(&self, schema: Option<&str>) -> Result<Vec<TriggerEntry>, AnySqlError> {
        let schema = schema.unwrap_or("dbo");
        let mut client = self.client.lock().await;

        let sql = format!(
            "SELECT tr.name, OBJECT_NAME(tr.parent_id), te.type_desc, \
             CASE WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF' ELSE 'AFTER' END, \
             m.definition \
             FROM sys.triggers tr \
             JOIN sys.trigger_events te ON tr.object_id = te.object_id \
             LEFT JOIN sys.sql_modules m ON m.object_id = tr.object_id \
             WHERE OBJECT_SCHEMA_NAME(tr.parent_id) = '{schema}' \
             ORDER BY OBJECT_NAME(tr.parent_id), tr.name",
            schema = schema.replace('\'', "''")
        );

        let stream = client
            .simple_query(&sql)
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        Ok(rows
            .iter()
            .map(|row| TriggerEntry {
                name: row.get::<&str, _>(0).unwrap_or("").to_string(),
                table_name: row.get::<&str, _>(1).unwrap_or("").to_string(),
                schema: Some(schema.to_string()),
                event: row.get::<&str, _>(2).unwrap_or("").to_string(),
                timing: row.get::<&str, _>(3).unwrap_or("").to_string(),
                definition: row.get::<&str, _>(4).map(std::string::ToString::to_string),
            })
            .collect())
    }

    async fn list_active_queries(&self) -> Result<Vec<ActiveQuery>, AnySqlError> {
        let mut client = self.client.lock().await;

        let stream = client
            .simple_query(
                "SELECT r.session_id, s.login_name, DB_NAME(r.database_id), \
                 t.text, r.status, r.start_time, r.total_elapsed_time, \
                 s.host_name \
                 FROM sys.dm_exec_requests r \
                 JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id \
                 CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t \
                 WHERE s.is_user_process = 1 \
                 ORDER BY r.total_elapsed_time DESC",
            )
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        Ok(rows
            .iter()
            .map(|row| ActiveQuery {
                pid: row.get::<i16, _>(0).map(|v| v.to_string()).unwrap_or_default(),
                username: row.get::<&str, _>(1).map(std::string::ToString::to_string),
                database: row.get::<&str, _>(2).map(std::string::ToString::to_string),
                query: row.get::<&str, _>(3).map(std::string::ToString::to_string),
                state: row.get::<&str, _>(4).map(std::string::ToString::to_string),
                started_at: None,
                duration: row.get::<i32, _>(6).map(|v| format!("{v}ms")),
                client_addr: row.get::<&str, _>(7).map(std::string::ToString::to_string),
            })
            .collect())
    }

    async fn kill_query(&self, pid: &str) -> Result<(), AnySqlError> {
        let pid: i32 = pid.parse().map_err(|_| AnySqlError::BadInput("invalid pid".into()))?;
        let mut client = self.client.lock().await;
        let sql = format!("KILL {pid}");
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        Ok(())
    }

    async fn list_variables(&self, filter: Option<&str>) -> Result<Vec<ServerVariable>, AnySqlError> {
        let mut client = self.client.lock().await;

        let sql = if let Some(pattern) = filter {
            format!(
                "SELECT name, CAST(value AS NVARCHAR(MAX)), description \
                 FROM sys.configurations \
                 WHERE name LIKE '%{pat}%' \
                 ORDER BY name",
                pat = pattern.replace('\'', "''")
            )
        } else {
            "SELECT name, CAST(value AS NVARCHAR(MAX)), description \
             FROM sys.configurations \
             ORDER BY name"
                .to_string()
        };

        let stream = client
            .simple_query(&sql)
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        Ok(rows
            .iter()
            .map(|row| ServerVariable {
                name: row.get::<&str, _>(0).unwrap_or("").to_string(),
                value: row.get::<&str, _>(1).unwrap_or("").to_string(),
                description: row.get::<&str, _>(2).map(std::string::ToString::to_string),
            })
            .collect())
    }

    async fn switch_database(&self, database: &str) -> Result<(), AnySqlError> {
        let mut client = self.client.lock().await;
        let sql = format!("USE [{}]", database.replace(']', "]]"));
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;
        Ok(())
    }
}
