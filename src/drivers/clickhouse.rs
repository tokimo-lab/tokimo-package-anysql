use std::time::Instant;

use reqwest::Client;

use crate::connector::DatabaseConnector;
use crate::error::AnySqlError;
use crate::types::*;

pub struct ClickHouseConnector {
    client: Client,
    base_url: String,
    user: String,
    password: String,
    database: String,
}

impl ClickHouseConnector {
    pub async fn connect(config: &DbConnectionConfig) -> Result<Self, AnySqlError> {
        let port = config.port.unwrap_or(8123);
        let base_url = format!("http://{}:{port}", config.host);
        let user = config.username.clone().unwrap_or_else(|| "default".into());
        let password = config.password.clone().unwrap_or_default();
        let database = config.database.clone().unwrap_or_else(|| "default".into());

        let client = Client::new();

        // 测试连通性
        let resp = client
            .get(format!("{base_url}/ping"))
            .send()
            .await
            .map_err(|e| AnySqlError::Connection(format!("ClickHouse connect failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(AnySqlError::Connection(format!(
                "ClickHouse ping failed: HTTP {}",
                resp.status()
            )));
        }

        Ok(Self {
            client,
            base_url,
            user,
            password,
            database,
        })
    }

    /// 执行查询，返回 JSON 格式结果
    async fn query_json(&self, sql: &str) -> Result<serde_json::Value, AnySqlError> {
        let url = format!("{}/", self.base_url);
        let full_sql = format!("{sql} FORMAT JSON");

        let mut req = self
            .client
            .post(&url)
            .query(&[("database", &self.database)])
            .header("X-ClickHouse-User", &self.user)
            .body(full_sql);

        if !self.password.is_empty() {
            req = req.header("X-ClickHouse-Key", &self.password);
        }

        let resp = req.send().await.map_err(|e| AnySqlError::Query(e.to_string()))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AnySqlError::Query(body));
        }

        resp.json::<serde_json::Value>()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))
    }

    /// 执行非查询语句
    async fn exec(&self, sql: &str) -> Result<String, AnySqlError> {
        let url = format!("{}/", self.base_url);

        let mut req = self
            .client
            .post(&url)
            .query(&[("database", &self.database)])
            .header("X-ClickHouse-User", &self.user)
            .body(sql.to_string());

        if !self.password.is_empty() {
            req = req.header("X-ClickHouse-Key", &self.password);
        }

        let resp = req.send().await.map_err(|e| AnySqlError::Query(e.to_string()))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AnySqlError::Query(body));
        }

        resp.text().await.map_err(|e| AnySqlError::Query(e.to_string()))
    }
}

#[async_trait::async_trait]
impl DatabaseConnector for ClickHouseConnector {
    fn driver(&self) -> DbDriver {
        DbDriver::Clickhouse
    }

    async fn ping(&self) -> Result<(), AnySqlError> {
        let resp = self
            .client
            .get(format!("{}/ping", self.base_url))
            .send()
            .await
            .map_err(|e| AnySqlError::Connection(e.to_string()))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(AnySqlError::Connection(format!("ping failed: HTTP {}", resp.status())))
        }
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
            let json = self.query_json(sql).await?;
            let elapsed_ms = start.elapsed().as_millis() as u64;

            let meta = json["meta"].as_array();
            let data = json["data"].as_array();

            let columns: Vec<ColumnInfo> = meta
                .map(|arr| {
                    arr.iter()
                        .enumerate()
                        .map(|(i, m)| ColumnInfo {
                            name: m["name"].as_str().unwrap_or("").to_string(),
                            data_type: m["type"].as_str().unwrap_or("").to_string(),
                            ordinal: i,
                        })
                        .collect()
                })
                .unwrap_or_default();

            let all_rows = data.map_or(0, std::vec::Vec::len);
            let truncated = all_rows > max_rows;

            let rows: Vec<serde_json::Map<String, serde_json::Value>> = data
                .into_iter()
                .flat_map(|arr| arr.iter())
                .take(max_rows)
                .filter_map(|v| v.as_object().cloned())
                .collect();

            Ok(QueryResult {
                columns,
                rows,
                rows_affected: 0,
                elapsed_ms,
                truncated,
            })
        } else {
            self.exec(sql).await?;
            let elapsed_ms = start.elapsed().as_millis() as u64;

            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                elapsed_ms,
                truncated: false,
            })
        }
    }

    async fn overview(&self) -> Result<DatabaseOverview, AnySqlError> {
        // Version
        let version = self.exec("SELECT version()").await?.trim().to_string();

        // Uptime
        let uptime = self.exec("SELECT uptime()").await.ok().map(|s| s.trim().to_string());

        // Current database
        let current_db = self.exec("SELECT currentDatabase()").await?.trim().to_string();

        // Current user
        let current_user = self.exec("SELECT currentUser()").await?.trim().to_string();

        // DB size
        let size_sql = format!(
            "SELECT sum(bytes) FROM system.parts WHERE database = '{}'",
            self.database.replace('\'', "\\'")
        );
        let db_size: Option<i64> = self.exec(&size_sql).await.ok().and_then(|s| s.trim().parse().ok());

        // 活跃查询数
        let active: i64 = self
            .exec("SELECT count() FROM system.processes WHERE is_cancelled = 0")
            .await
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);

        // Max concurrent queries
        let max_conn: i64 = self
            .exec("SELECT value FROM system.settings WHERE name = 'max_concurrent_queries'")
            .await
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(100);

        Ok(DatabaseOverview {
            server_version: format!("ClickHouse {version}"),
            uptime_seconds: uptime,
            current_database: current_db,
            current_user,
            database_size_bytes: db_size,
            active_connections: active,
            max_connections: max_conn,
        })
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseEntry>, AnySqlError> {
        let json = self
            .query_json("SELECT name FROM system.databases ORDER BY name")
            .await?;
        let data = json["data"].as_array();

        Ok(data
            .into_iter()
            .flat_map(|arr| arr.iter())
            .map(|row| DatabaseEntry {
                name: row["name"].as_str().unwrap_or("").to_string(),
                size_bytes: None,
                encoding: None,
            })
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaEntry>, AnySqlError> {
        // ClickHouse 没有 schema 概念，用 database 代替
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
        let db = schema.unwrap_or(&self.database);
        let sql = format!(
            "SELECT name, engine, total_rows, total_bytes, comment \
             FROM system.tables \
             WHERE database = '{}' \
             ORDER BY name",
            db.replace('\'', "\\'")
        );
        let json = self.query_json(&sql).await?;
        let data = json["data"].as_array();

        Ok(data
            .into_iter()
            .flat_map(|arr| arr.iter())
            .map(|row| {
                let engine = row["engine"].as_str().unwrap_or("");
                let kind = if engine.contains("View") {
                    TableKind::View
                } else if engine.contains("MaterializedView") {
                    TableKind::MaterializedView
                } else {
                    TableKind::Table
                };

                TableEntry {
                    name: row["name"].as_str().unwrap_or("").to_string(),
                    schema: Some(db.to_string()),
                    kind,
                    estimated_rows: row["total_rows"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .or_else(|| row["total_rows"].as_u64().map(|v| v as i64)),
                    size_bytes: row["total_bytes"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .or_else(|| row["total_bytes"].as_u64().map(|v| v as i64)),
                    comment: row["comment"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .map(std::string::ToString::to_string),
                }
            })
            .collect())
    }

    async fn describe_table(&self, table: &str, schema: Option<&str>) -> Result<TableDetail, AnySqlError> {
        let db = schema.unwrap_or(&self.database);
        let table_esc = table.replace('\'', "\\'");
        let db_esc = db.replace('\'', "\\'");

        // Columns
        let col_sql = format!(
            "SELECT name, type, default_kind, default_expression, comment, position \
             FROM system.columns \
             WHERE database = '{db_esc}' AND table = '{table_esc}' \
             ORDER BY position"
        );
        let json = self.query_json(&col_sql).await?;
        let col_data = json["data"].as_array();

        let columns: Vec<ColumnDetail> = col_data
            .into_iter()
            .flat_map(|arr| arr.iter())
            .map(|row| {
                let dtype = row["type"].as_str().unwrap_or("").to_string();
                let is_nullable = dtype.starts_with("Nullable");
                let default_kind = row["default_kind"].as_str().unwrap_or("");
                let default_expr = row["default_expression"].as_str().unwrap_or("");
                let default_value = if default_kind.is_empty() {
                    None
                } else {
                    Some(format!("{default_kind} {default_expr}"))
                };
                let ordinal = row["position"]
                    .as_str()
                    .and_then(|s| s.parse::<usize>().ok())
                    .or_else(|| row["position"].as_u64().map(|v| v as usize))
                    .unwrap_or(0);

                ColumnDetail {
                    name: row["name"].as_str().unwrap_or("").to_string(),
                    data_type: dtype,
                    is_nullable,
                    is_primary_key: false,
                    default_value,
                    comment: row["comment"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .map(std::string::ToString::to_string),
                    max_length: None,
                    ordinal,
                }
            })
            .collect();

        // DDL
        let create_sql = self
            .exec(&format!("SHOW CREATE TABLE `{db_esc}`.`{table_esc}`"))
            .await
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        // Row count and size from system.tables
        let info_sql = format!(
            "SELECT total_rows, total_bytes, comment FROM system.tables \
             WHERE database = '{db_esc}' AND name = '{table_esc}'"
        );
        let info_json = self.query_json(&info_sql).await.ok();
        let info_row = info_json
            .as_ref()
            .and_then(|j| j["data"].as_array())
            .and_then(|arr| arr.first());

        let (estimated_rows, size_bytes, comment) = if let Some(row) = info_row {
            (
                row["total_rows"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| row["total_rows"].as_u64().map(|v| v as i64)),
                row["total_bytes"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| row["total_bytes"].as_u64().map(|v| v as i64)),
                row["comment"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .map(std::string::ToString::to_string),
            )
        } else {
            (None, None, None)
        };

        Ok(TableDetail {
            name: table.to_string(),
            schema: Some(db.to_string()),
            kind: TableKind::Table,
            columns,
            indexes: vec![],
            foreign_keys: vec![],
            create_sql,
            comment,
            estimated_rows,
            size_bytes,
        })
    }

    async fn list_routines(&self, _schema: Option<&str>) -> Result<Vec<RoutineEntry>, AnySqlError> {
        Ok(vec![])
    }

    async fn list_triggers(&self, _schema: Option<&str>) -> Result<Vec<TriggerEntry>, AnySqlError> {
        Ok(vec![])
    }

    async fn list_active_queries(&self) -> Result<Vec<ActiveQuery>, AnySqlError> {
        let json = self
            .query_json(
                "SELECT query_id, user, initial_database AS database, query, \
                 type, elapsed, address \
                 FROM system.processes \
                 ORDER BY elapsed DESC",
            )
            .await?;

        let data = json["data"].as_array();
        Ok(data
            .into_iter()
            .flat_map(|arr| arr.iter())
            .map(|row| ActiveQuery {
                pid: row["query_id"].as_str().unwrap_or("").to_string(),
                username: row["user"].as_str().map(std::string::ToString::to_string),
                database: row["database"].as_str().map(std::string::ToString::to_string),
                query: row["query"].as_str().map(std::string::ToString::to_string),
                state: row["type"].as_str().map(std::string::ToString::to_string),
                started_at: None,
                duration: row["elapsed"]
                    .as_str()
                    .or_else(|| row["elapsed"].as_f64().map(|_| ""))
                    .map(|_| {
                        let v = row["elapsed"].as_f64().unwrap_or(0.0);
                        format!("{v:.1}s")
                    }),
                client_addr: row["address"].as_str().map(std::string::ToString::to_string),
            })
            .collect())
    }

    async fn kill_query(&self, pid: &str) -> Result<(), AnySqlError> {
        self.exec(&format!("KILL QUERY WHERE query_id = '{}'", pid.replace('\'', "\\'")))
            .await?;
        Ok(())
    }

    async fn list_variables(&self, filter: Option<&str>) -> Result<Vec<ServerVariable>, AnySqlError> {
        let sql = if let Some(pattern) = filter {
            format!(
                "SELECT name, value, description FROM system.settings \
                 WHERE name LIKE '%{pat}%' ORDER BY name",
                pat = pattern.replace('\'', "\\'")
            )
        } else {
            "SELECT name, value, description FROM system.settings ORDER BY name".to_string()
        };

        let json = self.query_json(&sql).await?;
        let data = json["data"].as_array();

        Ok(data
            .into_iter()
            .flat_map(|arr| arr.iter())
            .map(|row| ServerVariable {
                name: row["name"].as_str().unwrap_or("").to_string(),
                value: row["value"].as_str().unwrap_or("").to_string(),
                description: row["description"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .map(std::string::ToString::to_string),
            })
            .collect())
    }

    async fn switch_database(&self, _database: &str) -> Result<(), AnySqlError> {
        // ClickHouse HTTP 接口每次请求独立指定数据库，无需切换
        Err(AnySqlError::Unsupported(
            "ClickHouse uses per-query database specification".into(),
        ))
    }
}
