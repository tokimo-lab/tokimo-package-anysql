use std::time::Instant;

use reqwest::Client;
use serde_json::Value;

use crate::connector::DatabaseConnector;
use crate::error::AnySqlError;
use crate::types::*;

pub struct ElasticsearchConnector {
    client: Client,
    base_url: String,
    username: Option<String>,
    password: Option<String>,
}

impl ElasticsearchConnector {
    pub async fn connect(config: &DbConnectionConfig) -> Result<Self, AnySqlError> {
        let port = config.port.unwrap_or(9200);
        let scheme = if config.params.as_deref().is_some_and(|p| p.contains("ssl=true")) {
            "https"
        } else {
            "http"
        };
        let base_url = format!("{scheme}://{}:{port}", config.host);

        let client = Client::new();

        // Test connectivity
        let mut req = client.get(&base_url);
        if let (Some(u), Some(p)) = (&config.username, &config.password) {
            req = req.basic_auth(u, Some(p));
        }

        let resp = req
            .send()
            .await
            .map_err(|e| AnySqlError::Connection(format!("Elasticsearch connect failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(AnySqlError::Connection(format!(
                "Elasticsearch returned HTTP {}",
                resp.status()
            )));
        }

        Ok(Self {
            client,
            base_url,
            username: config.username.clone(),
            password: config.password.clone(),
        })
    }

    fn build_request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.request(method, url);
        if let (Some(u), Some(p)) = (&self.username, &self.password) {
            req = req.basic_auth(u, Some(p));
        }
        req
    }

    async fn get_json(&self, path: &str) -> Result<Value, AnySqlError> {
        let resp = self
            .build_request(reqwest::Method::GET, path)
            .send()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AnySqlError::Query(body));
        }

        resp.json::<Value>()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))
    }

    async fn post_json(&self, path: &str, body: &Value) -> Result<Value, AnySqlError> {
        let resp = self
            .build_request(reqwest::Method::POST, path)
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AnySqlError::Query(body));
        }

        resp.json::<Value>()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))
    }
}

#[async_trait::async_trait]
impl DatabaseConnector for ElasticsearchConnector {
    fn driver(&self) -> DbDriver {
        DbDriver::Elasticsearch
    }

    async fn ping(&self) -> Result<(), AnySqlError> {
        let resp = self
            .build_request(reqwest::Method::GET, "/")
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
        let trimmed = sql.trim();

        // If the input looks like JSON, treat it as an Elasticsearch query DSL
        if trimmed.starts_with('{') {
            let body: Value =
                serde_json::from_str(trimmed).map_err(|e| AnySqlError::Query(format!("Invalid JSON: {e}")))?;

            // Extract index from _index field, or use _all
            let index = body.get("_index").and_then(|v| v.as_str()).unwrap_or("_all");

            let mut query = body.clone();
            // Remove our meta field
            if let Some(obj) = query.as_object_mut() {
                obj.remove("_index");
                // Ensure size limit
                if !obj.contains_key("size") {
                    obj.insert("size".into(), Value::from(max_rows));
                }
            }

            let result = self.post_json(&format!("/{index}/_search"), &query).await?;

            return Ok(Self::parse_search_result(&result, start, max_rows));
        }

        // Otherwise, try Elasticsearch SQL API
        let body = serde_json::json!({
            "query": trimmed,
            "fetch_size": max_rows
        });

        let result = self.post_json("/_sql?format=json", &body).await?;
        let elapsed_ms = start.elapsed().as_millis() as u64;

        // Parse SQL API response: { columns: [{name, type}], rows: [[val, ...]] }
        let columns: Vec<ColumnInfo> = result["columns"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .enumerate()
                    .map(|(i, col)| ColumnInfo {
                        name: col["name"].as_str().unwrap_or("").to_string(),
                        data_type: col["type"].as_str().unwrap_or("").to_string(),
                        ordinal: i,
                    })
                    .collect()
            })
            .unwrap_or_default();

        let rows: Vec<serde_json::Map<String, Value>> = result["rows"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .take(max_rows)
                    .map(|row| {
                        let mut map = serde_json::Map::new();
                        if let Some(vals) = row.as_array() {
                            for (i, val) in vals.iter().enumerate() {
                                let col_name = columns.get(i).map_or_else(|| format!("col_{i}"), |c| c.name.clone());
                                map.insert(col_name, val.clone());
                            }
                        }
                        map
                    })
                    .collect()
            })
            .unwrap_or_default();

        let total = result["rows"].as_array().map_or(0, std::vec::Vec::len);
        let truncated = total > max_rows;

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
            elapsed_ms,
            truncated,
        })
    }

    async fn overview(&self) -> Result<DatabaseOverview, AnySqlError> {
        let info = self.get_json("/").await?;
        let version = info["version"]["number"].as_str().unwrap_or("unknown").to_string();
        let cluster = info["cluster_name"].as_str().unwrap_or("unknown").to_string();

        // Cluster health
        let health = self.get_json("/_cluster/health").await.ok();
        let active = health
            .as_ref()
            .and_then(|h| h["active_primary_shards"].as_i64())
            .unwrap_or(0);

        // Cluster stats
        let stats = self.get_json("/_cluster/stats").await.ok();
        let size = stats
            .as_ref()
            .and_then(|s| s["indices"]["store"]["size_in_bytes"].as_i64());

        let nodes = stats
            .as_ref()
            .and_then(|s| s["nodes"]["count"]["total"].as_i64())
            .unwrap_or(1);

        Ok(DatabaseOverview {
            server_version: format!("Elasticsearch {version}"),
            uptime_seconds: None,
            current_database: cluster,
            current_user: self.username.clone().unwrap_or_else(|| "anonymous".into()),
            database_size_bytes: size,
            active_connections: active,
            max_connections: nodes * 1000, // Approximate
        })
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseEntry>, AnySqlError> {
        // Elasticsearch doesn't have databases; return cluster as a single entry
        let info = self.get_json("/").await?;
        let cluster = info["cluster_name"].as_str().unwrap_or("default").to_string();

        Ok(vec![DatabaseEntry {
            name: cluster,
            size_bytes: None,
            encoding: None,
        }])
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaEntry>, AnySqlError> {
        Ok(vec![])
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableEntry>, AnySqlError> {
        // List all indices
        let json = self.get_json("/_cat/indices?format=json").await?;

        let indices = json.as_array().map(|arr| {
            arr.iter()
                .map(|idx| {
                    let name = idx["index"].as_str().unwrap_or("").to_string();
                    let docs = idx["docs.count"].as_str().and_then(|s| s.parse::<i64>().ok());
                    let size = idx["store.size"].as_str().and_then(parse_es_size);

                    TableEntry {
                        name,
                        schema: None,
                        kind: TableKind::Table,
                        estimated_rows: docs,
                        size_bytes: size,
                        comment: idx["status"].as_str().map(std::string::ToString::to_string),
                    }
                })
                .collect::<Vec<_>>()
        });

        let mut tables = indices.unwrap_or_default();
        tables.sort_by(|a, b| a.name.cmp(&b.name));

        // Also list aliases as views
        if let Ok(aliases_json) = self.get_json("/_cat/aliases?format=json").await
            && let Some(arr) = aliases_json.as_array()
        {
            for alias in arr {
                let name = alias["alias"].as_str().unwrap_or("").to_string();
                if !name.starts_with('.') {
                    tables.push(TableEntry {
                        name,
                        schema: None,
                        kind: TableKind::View,
                        estimated_rows: None,
                        size_bytes: None,
                        comment: Some("alias".into()),
                    });
                }
            }
        }

        Ok(tables)
    }

    async fn describe_table(&self, table: &str, _schema: Option<&str>) -> Result<TableDetail, AnySqlError> {
        // Get mapping for the index
        let mapping = self.get_json(&format!("/{table}/_mapping")).await?;

        let properties = mapping
            .get(table)
            .and_then(|v| v.get("mappings"))
            .and_then(|v| v.get("properties"))
            .and_then(|v| v.as_object());

        let columns: Vec<ColumnDetail> = properties
            .map(|props| {
                props
                    .iter()
                    .enumerate()
                    .map(|(i, (name, info))| {
                        let data_type = info["type"].as_str().unwrap_or("object").to_string();
                        ColumnDetail {
                            name: name.clone(),
                            data_type,
                            is_nullable: true,
                            is_primary_key: name == "_id",
                            default_value: None,
                            comment: None,
                            max_length: None,
                            ordinal: i,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Get index stats
        let stats = self.get_json(&format!("/_cat/indices/{table}?format=json")).await.ok();
        let first = stats.as_ref().and_then(|v| v.as_array()).and_then(|a| a.first());

        let count = first
            .and_then(|r| r["docs.count"].as_str())
            .and_then(|s| s.parse::<i64>().ok());
        let size = first.and_then(|r| r["store.size"].as_str()).and_then(parse_es_size);

        // Get settings for DDL-like info
        let settings = self
            .get_json(&format!("/{table}/_settings"))
            .await
            .ok()
            .map(|v| serde_json::to_string_pretty(&v).unwrap_or_default());

        Ok(TableDetail {
            name: table.to_string(),
            schema: None,
            kind: TableKind::Table,
            columns,
            indexes: vec![],
            foreign_keys: vec![],
            create_sql: settings,
            comment: None,
            estimated_rows: count,
            size_bytes: size,
        })
    }

    async fn list_routines(&self, _schema: Option<&str>) -> Result<Vec<RoutineEntry>, AnySqlError> {
        // List ingest pipelines as "routines"
        let pipelines = self.get_json("/_ingest/pipeline").await.ok();

        Ok(pipelines
            .and_then(|v| v.as_object().cloned())
            .map(|map| {
                map.keys()
                    .map(|name| RoutineEntry {
                        name: name.clone(),
                        schema: None,
                        kind: "pipeline".into(),
                        return_type: None,
                        language: Some("painless".into()),
                        definition: map.get(name).map(std::string::ToString::to_string),
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn list_triggers(&self, _schema: Option<&str>) -> Result<Vec<TriggerEntry>, AnySqlError> {
        Ok(vec![])
    }

    async fn list_active_queries(&self) -> Result<Vec<ActiveQuery>, AnySqlError> {
        let tasks = self.get_json("/_tasks?detailed=true&actions=*search*").await?;

        let mut queries = Vec::new();
        if let Some(nodes) = tasks["nodes"].as_object() {
            for (_node_id, node) in nodes {
                if let Some(tasks_map) = node["tasks"].as_object() {
                    for (task_id, task) in tasks_map {
                        queries.push(ActiveQuery {
                            pid: task_id.clone(),
                            username: None,
                            database: task["headers"]["X-Opaque-Id"]
                                .as_str()
                                .map(std::string::ToString::to_string),
                            query: task["description"].as_str().map(std::string::ToString::to_string),
                            state: task["action"].as_str().map(std::string::ToString::to_string),
                            started_at: None,
                            duration: task["running_time_in_nanos"]
                                .as_i64()
                                .map(|ns| format!("{:.1}s", ns as f64 / 1_000_000_000.0)),
                            client_addr: node["host"].as_str().map(std::string::ToString::to_string),
                        });
                    }
                }
            }
        }

        Ok(queries)
    }

    async fn kill_query(&self, pid: &str) -> Result<(), AnySqlError> {
        let resp = self
            .build_request(reqwest::Method::POST, &format!("/_tasks/{pid}/_cancel"))
            .send()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AnySqlError::Query(body));
        }

        Ok(())
    }

    async fn list_variables(&self, filter: Option<&str>) -> Result<Vec<ServerVariable>, AnySqlError> {
        let settings = self.get_json("/_cluster/settings?include_defaults=true").await?;

        let mut vars = Vec::new();
        // Flatten defaults
        if let Some(defaults) = settings["defaults"].as_object() {
            flatten_settings("", defaults, &mut vars);
        }
        // Overlay persistent
        if let Some(persistent) = settings["persistent"].as_object() {
            flatten_settings("", persistent, &mut vars);
        }

        if let Some(f) = filter {
            let f_lower = f.to_lowercase();
            vars.retain(|v| v.name.to_lowercase().contains(&f_lower));
        }

        vars.sort_by(|a, b| a.name.cmp(&b.name));
        vars.truncate(500); // Limit output
        Ok(vars)
    }

    async fn switch_database(&self, _database: &str) -> Result<(), AnySqlError> {
        Err(AnySqlError::Unsupported(
            "Elasticsearch does not have a database concept".into(),
        ))
    }
}

impl ElasticsearchConnector {
    fn parse_search_result(result: &Value, start: Instant, max_rows: usize) -> QueryResult {
        let elapsed_ms = start.elapsed().as_millis() as u64;

        let hits = result["hits"]["hits"].as_array();
        let total = result["hits"]["total"]["value"].as_u64().unwrap_or(0) as usize;

        let mut columns_map = indexmap::IndexMap::<String, ()>::new();
        let mut rows = Vec::new();

        if let Some(hits) = hits {
            for (i, hit) in hits.iter().enumerate() {
                if i >= max_rows {
                    break;
                }
                let mut row = serde_json::Map::new();
                // Include _id
                if let Some(id) = hit["_id"].as_str() {
                    columns_map.entry("_id".into()).or_insert(());
                    row.insert("_id".into(), Value::from(id));
                }
                // Include _source fields
                if let Some(source) = hit["_source"].as_object() {
                    for (k, v) in source {
                        columns_map.entry(k.clone()).or_insert(());
                        row.insert(k.clone(), v.clone());
                    }
                }
                rows.push(row);
            }
        }

        let columns = columns_map
            .keys()
            .enumerate()
            .map(|(i, name)| ColumnInfo {
                name: name.clone(),
                data_type: "json".into(),
                ordinal: i,
            })
            .collect();

        QueryResult {
            columns,
            rows,
            rows_affected: 0,
            elapsed_ms,
            truncated: total > max_rows,
        }
    }
}

/// Recursively flatten ES settings into key-value pairs
fn flatten_settings(prefix: &str, obj: &serde_json::Map<String, Value>, out: &mut Vec<ServerVariable>) {
    for (key, val) in obj {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match val {
            Value::Object(inner) => flatten_settings(&full_key, inner, out),
            _ => out.push(ServerVariable {
                name: full_key,
                value: match val {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                },
                description: None,
            }),
        }
    }
}

/// Parse ES human-readable size to bytes (e.g., "100kb" → 102400)
fn parse_es_size(s: &str) -> Option<i64> {
    let s = s.trim().to_lowercase();
    if let Some(num) = s.strip_suffix("tb") {
        num.trim()
            .parse::<f64>()
            .ok()
            .map(|n| (n * 1024.0 * 1024.0 * 1024.0 * 1024.0) as i64)
    } else if let Some(num) = s.strip_suffix("gb") {
        num.trim()
            .parse::<f64>()
            .ok()
            .map(|n| (n * 1024.0 * 1024.0 * 1024.0) as i64)
    } else if let Some(num) = s.strip_suffix("mb") {
        num.trim().parse::<f64>().ok().map(|n| (n * 1024.0 * 1024.0) as i64)
    } else if let Some(num) = s.strip_suffix("kb") {
        num.trim().parse::<f64>().ok().map(|n| (n * 1024.0) as i64)
    } else if let Some(num) = s.strip_suffix('b') {
        num.trim().parse::<i64>().ok()
    } else {
        s.parse::<i64>().ok()
    }
}
