use std::time::Instant;

use oracle::Connection;

use crate::connector::DatabaseConnector;
use crate::error::AnySqlError;
use crate::types::*;

pub struct OracleConnector {
    conn: Connection,
    database: String,
    username: String,
}

impl OracleConnector {
    pub async fn connect(config: &DbConnectionConfig) -> Result<Self, AnySqlError> {
        let host = config.host.clone();
        let port = config.port.unwrap_or(1521);
        let service = config.database.clone().unwrap_or_else(|| "ORCL".into());
        let username = config.username.clone().unwrap_or_else(|| "system".into());
        let password = config.password.clone().unwrap_or_default();

        let connect_string = format!("//{}:{}/{}", host, port, service);
        let user = username.clone();
        let pass = password.clone();

        let conn = tokio::task::spawn_blocking(move || Connection::connect(&user, &pass, &connect_string))
            .await
            .map_err(|e| AnySqlError::Internal(format!("spawn_blocking error: {e}")))?
            .map_err(|e| AnySqlError::Connection(format!("Oracle connect failed: {e}")))?;

        Ok(Self {
            conn,
            database: service,
            username,
        })
    }

    fn blocking_query(
        &self,
        sql: &str,
        max_rows: usize,
    ) -> Result<(Vec<ColumnInfo>, Vec<serde_json::Map<String, serde_json::Value>>, bool), AnySqlError> {
        let stmt = self
            .conn
            .statement(sql)
            .build()
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let rows = stmt.query(&[]).map_err(|e| AnySqlError::Query(e.to_string()))?;

        let col_info = rows.column_info();
        let columns: Vec<ColumnInfo> = col_info
            .iter()
            .enumerate()
            .map(|(i, ci)| ColumnInfo {
                name: ci.name().to_string(),
                data_type: format!("{:?}", ci.oracle_type()),
                ordinal: i,
            })
            .collect();

        let mut result_rows = Vec::new();
        let mut count = 0;
        let mut truncated = false;
        for row_result in rows {
            if count >= max_rows {
                truncated = true;
                break;
            }
            let row = row_result.map_err(|e| AnySqlError::Query(e.to_string()))?;
            let mut map = serde_json::Map::new();
            for (i, ci) in col_info.iter().enumerate() {
                let val: serde_json::Value = match row.get::<usize, Option<String>>(i) {
                    Ok(Some(s)) => serde_json::Value::from(s),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                };
                map.insert(ci.name().to_string(), val);
            }
            result_rows.push(map);
            count += 1;
        }

        Ok((columns, result_rows, truncated))
    }
}

// oracle::Connection is !Send, so we need to use unsafe impl
// This is safe because we only access it within spawn_blocking
unsafe impl Send for OracleConnector {}
unsafe impl Sync for OracleConnector {}

#[async_trait::async_trait]
impl DatabaseConnector for OracleConnector {
    fn driver(&self) -> DbDriver {
        DbDriver::Oracle
    }

    async fn ping(&self) -> Result<(), AnySqlError> {
        // Oracle connection is already established; just do a quick query
        let conn_ptr = &self.conn as *const Connection;
        tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };
            conn.query("SELECT 1 FROM DUAL", &[])
                .map_err(|e| AnySqlError::Connection(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))?
    }

    async fn execute_sql(&self, sql: &str, max_rows: usize) -> Result<QueryResult, AnySqlError> {
        let start = Instant::now();

        let conn_ptr = &self.conn as *const Connection;
        let sql_owned = sql.to_string();
        let (columns, rows, truncated) = tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };
            let trimmed = sql_owned.trim_start().to_uppercase();
            let is_query = trimmed.starts_with("SELECT")
                || trimmed.starts_with("WITH")
                || trimmed.starts_with("SHOW")
                || trimmed.starts_with("EXPLAIN");

            if is_query {
                let stmt = conn
                    .statement(&sql_owned)
                    .build()
                    .map_err(|e| AnySqlError::Query(e.to_string()))?;

                let result_rows = stmt.query(&[]).map_err(|e| AnySqlError::Query(e.to_string()))?;

                let col_info = result_rows.column_info();
                let columns: Vec<ColumnInfo> = col_info
                    .iter()
                    .enumerate()
                    .map(|(i, ci)| ColumnInfo {
                        name: ci.name().to_string(),
                        data_type: format!("{:?}", ci.oracle_type()),
                        ordinal: i,
                    })
                    .collect();

                let mut rows = Vec::new();
                let mut count = 0;
                let mut truncated = false;
                for row_result in result_rows {
                    if count >= max_rows {
                        truncated = true;
                        break;
                    }
                    let row = row_result.map_err(|e| AnySqlError::Query(e.to_string()))?;
                    let mut map = serde_json::Map::new();
                    for (i, ci) in col_info.iter().enumerate() {
                        let val: serde_json::Value = match row.get::<usize, Option<String>>(i) {
                            Ok(Some(s)) => serde_json::Value::from(s),
                            Ok(None) => serde_json::Value::Null,
                            Err(_) => serde_json::Value::Null,
                        };
                        map.insert(ci.name().to_string(), val);
                    }
                    rows.push(map);
                    count += 1;
                }

                Ok((columns, rows, truncated))
            } else {
                conn.execute(&sql_owned, &[])
                    .map_err(|e| AnySqlError::Query(e.to_string()))?;
                conn.commit().map_err(|e| AnySqlError::Query(e.to_string()))?;
                Ok((vec![], vec![], false))
            }
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))??;

        let elapsed_ms = start.elapsed().as_millis() as u64;

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
            elapsed_ms,
            truncated,
        })
    }

    async fn overview(&self) -> Result<DatabaseOverview, AnySqlError> {
        let conn_ptr = &self.conn as *const Connection;
        let db = self.database.clone();
        let user = self.username.clone();

        tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };

            let version: String = conn
                .query_row_as::<String>("SELECT banner FROM v$version WHERE ROWNUM = 1", &[])
                .unwrap_or_else(|_| "Oracle".into());

            let uptime: Option<String> = conn
                .query_row_as::<String>("SELECT TO_CHAR(SYSDATE - startup_time) FROM v$instance", &[])
                .ok();

            let active: i64 = conn
                .query_row_as::<i64>("SELECT COUNT(*) FROM v$session WHERE status = 'ACTIVE'", &[])
                .unwrap_or(0);

            let max_conn: i64 = conn
                .query_row_as::<i64>("SELECT TO_NUMBER(value) FROM v$parameter WHERE name = 'sessions'", &[])
                .unwrap_or(0);

            let db_size: Option<i64> = conn
                .query_row_as::<i64>("SELECT SUM(bytes) FROM dba_data_files", &[])
                .ok();

            Ok(DatabaseOverview {
                server_version: version,
                uptime_seconds: uptime,
                current_database: db,
                current_user: user,
                database_size_bytes: db_size,
                active_connections: active,
                max_connections: max_conn,
            })
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))?
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseEntry>, AnySqlError> {
        let conn_ptr = &self.conn as *const Connection;

        tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };
            let mut entries = Vec::new();

            let rows = conn
                .query("SELECT name FROM v$database", &[])
                .map_err(|e| AnySqlError::Query(e.to_string()))?;

            for row in rows {
                let row = row.map_err(|e| AnySqlError::Query(e.to_string()))?;
                let name: String = row.get(0).unwrap_or_default();
                entries.push(DatabaseEntry {
                    name,
                    size_bytes: None,
                    encoding: None,
                });
            }

            Ok(entries)
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))?
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaEntry>, AnySqlError> {
        let conn_ptr = &self.conn as *const Connection;

        tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };
            let mut entries = Vec::new();

            let rows = conn
                .query("SELECT username FROM all_users ORDER BY username", &[])
                .map_err(|e| AnySqlError::Query(e.to_string()))?;

            for row in rows {
                let row = row.map_err(|e| AnySqlError::Query(e.to_string()))?;
                let name: String = row.get(0).unwrap_or_default();
                entries.push(SchemaEntry { name, owner: None });
            }

            Ok(entries)
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))?
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableEntry>, AnySqlError> {
        let conn_ptr = &self.conn as *const Connection;
        let schema_owned = schema.map(|s| s.to_string());
        let user = self.username.clone();

        tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };
            let owner = schema_owned.as_deref().unwrap_or(&user);
            let mut entries = Vec::new();

            // Tables
            let sql = format!(
                "SELECT table_name, num_rows FROM all_tables WHERE owner = UPPER('{}') ORDER BY table_name",
                owner.replace('\'', "''")
            );
            let rows = conn.query(&sql, &[]).map_err(|e| AnySqlError::Query(e.to_string()))?;

            for row in rows {
                let row = row.map_err(|e| AnySqlError::Query(e.to_string()))?;
                let name: String = row.get(0).unwrap_or_default();
                let num_rows: Option<i64> = row.get(1).ok();
                entries.push(TableEntry {
                    name,
                    schema: Some(owner.to_uppercase()),
                    kind: TableKind::Table,
                    estimated_rows: num_rows,
                    size_bytes: None,
                    comment: None,
                });
            }

            // Views
            let view_sql = format!(
                "SELECT view_name FROM all_views WHERE owner = UPPER('{}') ORDER BY view_name",
                owner.replace('\'', "''")
            );
            let view_rows = conn
                .query(&view_sql, &[])
                .map_err(|e| AnySqlError::Query(e.to_string()))?;

            for row in view_rows {
                let row = row.map_err(|e| AnySqlError::Query(e.to_string()))?;
                let name: String = row.get(0).unwrap_or_default();
                entries.push(TableEntry {
                    name,
                    schema: Some(owner.to_uppercase()),
                    kind: TableKind::View,
                    estimated_rows: None,
                    size_bytes: None,
                    comment: None,
                });
            }

            Ok(entries)
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))?
    }

    async fn describe_table(&self, table: &str, schema: Option<&str>) -> Result<TableDetail, AnySqlError> {
        let conn_ptr = &self.conn as *const Connection;
        let table_owned = table.to_string();
        let schema_owned = schema.map(|s| s.to_string());
        let user = self.username.clone();

        tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };
            let owner = schema_owned.as_deref().unwrap_or(&user);
            let tbl = table_owned.replace('\'', "''");
            let own_upper = owner.replace('\'', "''").to_uppercase();

            // Columns
            let col_sql = format!(
                "SELECT column_name, data_type, nullable, data_default, column_id, data_length \
                 FROM all_tab_columns \
                 WHERE owner = '{own_upper}' AND table_name = UPPER('{tbl}') \
                 ORDER BY column_id"
            );
            let col_rows = conn
                .query(&col_sql, &[])
                .map_err(|e| AnySqlError::Query(e.to_string()))?;

            let mut columns = Vec::new();
            for row in col_rows {
                let row = row.map_err(|e| AnySqlError::Query(e.to_string()))?;
                let name: String = row.get(0).unwrap_or_default();
                let dtype: String = row.get(1).unwrap_or_default();
                let nullable: String = row.get(2).unwrap_or_default();
                let default: Option<String> = row.get(3).ok();
                let ordinal: i32 = row.get(4).unwrap_or(0);
                let max_len: Option<i32> = row.get(5).ok();

                columns.push(ColumnDetail {
                    name,
                    data_type: dtype,
                    is_nullable: nullable == "Y",
                    is_primary_key: false,
                    default_value: default,
                    comment: None,
                    max_length: max_len,
                    ordinal: ordinal as usize,
                });
            }

            // Primary key columns
            let pk_sql = format!(
                "SELECT cols.column_name \
                 FROM all_constraints cons \
                 JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name \
                   AND cons.owner = cols.owner \
                 WHERE cons.constraint_type = 'P' \
                   AND cons.owner = '{own_upper}' \
                   AND cons.table_name = UPPER('{tbl}')"
            );
            if let Ok(pk_rows) = conn.query(&pk_sql, &[]) {
                let pk_cols: Vec<String> = pk_rows
                    .filter_map(|r| r.ok())
                    .filter_map(|r| r.get::<usize, String>(0).ok())
                    .collect();

                for col in &mut columns {
                    if pk_cols.contains(&col.name) {
                        col.is_primary_key = true;
                    }
                }
            }

            // Indexes
            let idx_sql = format!(
                "SELECT i.index_name, i.uniqueness, ic.column_name, i.index_type \
                 FROM all_indexes i \
                 JOIN all_ind_columns ic ON i.index_name = ic.index_name AND i.owner = ic.index_owner \
                 WHERE i.owner = '{own_upper}' AND i.table_name = UPPER('{tbl}') \
                 ORDER BY i.index_name, ic.column_position"
            );
            let mut idx_map = std::collections::HashMap::<String, (Vec<String>, bool, Option<String>)>::new();
            if let Ok(idx_rows) = conn.query(&idx_sql, &[]) {
                for row in idx_rows {
                    if let Ok(row) = row {
                        let name: String = row.get(0).unwrap_or_default();
                        let unique: String = row.get(1).unwrap_or_default();
                        let col: String = row.get(2).unwrap_or_default();
                        let itype: Option<String> = row.get(3).ok();
                        let entry = idx_map
                            .entry(name)
                            .or_insert_with(|| (vec![], unique == "UNIQUE", itype));
                        entry.0.push(col);
                    }
                }
            }

            let indexes: Vec<IndexEntry> = idx_map
                .into_iter()
                .map(|(name, (cols, is_unique, itype))| IndexEntry {
                    name,
                    columns: cols,
                    is_unique,
                    is_primary: false,
                    index_type: itype,
                })
                .collect();

            Ok(TableDetail {
                name: table_owned,
                schema: Some(own_upper),
                kind: TableKind::Table,
                columns,
                indexes,
                foreign_keys: vec![],
                create_sql: None,
                comment: None,
                estimated_rows: None,
                size_bytes: None,
            })
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))?
    }

    async fn list_routines(&self, schema: Option<&str>) -> Result<Vec<RoutineEntry>, AnySqlError> {
        let conn_ptr = &self.conn as *const Connection;
        let schema_owned = schema.map(|s| s.to_string());
        let user = self.username.clone();

        tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };
            let owner = schema_owned.as_deref().unwrap_or(&user);
            let own_upper = owner.replace('\'', "''").to_uppercase();

            let sql = format!(
                "SELECT object_name, object_type FROM all_objects \
                 WHERE owner = '{own_upper}' \
                 AND object_type IN ('PROCEDURE', 'FUNCTION', 'PACKAGE') \
                 ORDER BY object_name"
            );
            let rows = conn.query(&sql, &[]).map_err(|e| AnySqlError::Query(e.to_string()))?;

            let mut entries = Vec::new();
            for row in rows {
                let row = row.map_err(|e| AnySqlError::Query(e.to_string()))?;
                let name: String = row.get(0).unwrap_or_default();
                let kind: String = row.get(1).unwrap_or_default();
                entries.push(RoutineEntry {
                    name,
                    schema: Some(own_upper.clone()),
                    kind: kind.to_lowercase(),
                    return_type: None,
                    language: Some("PL/SQL".into()),
                    definition: None,
                });
            }
            Ok(entries)
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))?
    }

    async fn list_triggers(&self, schema: Option<&str>) -> Result<Vec<TriggerEntry>, AnySqlError> {
        let conn_ptr = &self.conn as *const Connection;
        let schema_owned = schema.map(|s| s.to_string());
        let user = self.username.clone();

        tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };
            let owner = schema_owned.as_deref().unwrap_or(&user);
            let own_upper = owner.replace('\'', "''").to_uppercase();

            let sql = format!(
                "SELECT trigger_name, table_name, triggering_event, trigger_type \
                 FROM all_triggers \
                 WHERE owner = '{own_upper}' \
                 ORDER BY trigger_name"
            );
            let rows = conn.query(&sql, &[]).map_err(|e| AnySqlError::Query(e.to_string()))?;

            let mut entries = Vec::new();
            for row in rows {
                let row = row.map_err(|e| AnySqlError::Query(e.to_string()))?;
                let name: String = row.get(0).unwrap_or_default();
                let table: String = row.get(1).unwrap_or_default();
                let event: String = row.get(2).unwrap_or_default();
                let timing: String = row.get(3).unwrap_or_default();
                entries.push(TriggerEntry {
                    name,
                    table_name: table,
                    schema: Some(own_upper.clone()),
                    event,
                    timing,
                    definition: None,
                });
            }
            Ok(entries)
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))?
    }

    async fn list_active_queries(&self) -> Result<Vec<ActiveQuery>, AnySqlError> {
        let conn_ptr = &self.conn as *const Connection;

        tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };
            let sql = "SELECT s.sid || ',' || s.serial# AS pid, \
                              s.username, s.schemaname, q.sql_text, s.status, \
                              TO_CHAR(s.logon_time, 'YYYY-MM-DD HH24:MI:SS') AS logon_time, \
                              s.last_call_et, s.machine \
                       FROM v$session s \
                       LEFT JOIN v$sql q ON s.sql_id = q.sql_id \
                       WHERE s.type = 'USER' AND s.status = 'ACTIVE' \
                       ORDER BY s.last_call_et DESC";

            let rows = conn.query(sql, &[]).map_err(|e| AnySqlError::Query(e.to_string()))?;

            let mut entries = Vec::new();
            for row in rows {
                let row = row.map_err(|e| AnySqlError::Query(e.to_string()))?;
                entries.push(ActiveQuery {
                    pid: row.get::<_, String>(0).unwrap_or_default(),
                    username: row.get(1).ok(),
                    database: row.get(2).ok(),
                    query: row.get(3).ok(),
                    state: row.get(4).ok(),
                    started_at: row.get(5).ok(),
                    duration: row.get::<_, i64>(6).ok().map(|s| format!("{s}s")),
                    client_addr: row.get(7).ok(),
                });
            }
            Ok(entries)
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))?
    }

    async fn kill_query(&self, pid: &str) -> Result<(), AnySqlError> {
        let conn_ptr = &self.conn as *const Connection;
        let pid_owned = pid.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };
            // pid is "sid,serial#"
            let sql = format!("ALTER SYSTEM KILL SESSION '{}'", pid_owned.replace('\'', "''"));
            conn.execute(&sql, &[]).map_err(|e| AnySqlError::Query(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))?
    }

    async fn list_variables(&self, filter: Option<&str>) -> Result<Vec<ServerVariable>, AnySqlError> {
        let conn_ptr = &self.conn as *const Connection;
        let filter_owned = filter.map(|f| f.to_string());

        tokio::task::spawn_blocking(move || {
            let conn = unsafe { &*conn_ptr };
            let sql = if let Some(ref f) = filter_owned {
                format!(
                    "SELECT name, value, description FROM v$parameter \
                     WHERE LOWER(name) LIKE '%{}%' ORDER BY name",
                    f.to_lowercase().replace('\'', "''")
                )
            } else {
                "SELECT name, value, description FROM v$parameter ORDER BY name".to_string()
            };

            let rows = conn.query(&sql, &[]).map_err(|e| AnySqlError::Query(e.to_string()))?;

            let mut vars = Vec::new();
            for row in rows {
                let row = row.map_err(|e| AnySqlError::Query(e.to_string()))?;
                vars.push(ServerVariable {
                    name: row.get(0).unwrap_or_default(),
                    value: row.get(1).unwrap_or_default(),
                    description: row.get(2).ok(),
                });
            }
            Ok(vars)
        })
        .await
        .map_err(|e| AnySqlError::Internal(e.to_string()))?
    }

    async fn switch_database(&self, _database: &str) -> Result<(), AnySqlError> {
        Err(AnySqlError::Unsupported(
            "Oracle does not support switching databases at runtime".into(),
        ))
    }
}
