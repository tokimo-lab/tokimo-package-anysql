use std::time::Instant;

use futures_util::TryStreamExt;
use mongodb::Client;
use mongodb::bson::{self, Document, doc};
use mongodb::options::ClientOptions;

use crate::connector::DatabaseConnector;
use crate::error::AnySqlError;
use crate::types::*;

pub struct MongoConnector {
    client: Client,
    database: String,
    config: DbConnectionConfig,
}

impl MongoConnector {
    pub async fn connect(config: &DbConnectionConfig) -> Result<Self, AnySqlError> {
        let url = config.to_url();
        let database = config.database.clone().unwrap_or_else(|| "admin".into());

        let opts = ClientOptions::parse(&url)
            .await
            .map_err(|e| AnySqlError::Connection(format!("MongoDB parse error: {e}")))?;

        let client =
            Client::with_options(opts).map_err(|e| AnySqlError::Connection(format!("MongoDB client error: {e}")))?;

        // Test connectivity
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|e| AnySqlError::Connection(format!("MongoDB ping failed: {e}")))?;

        Ok(Self {
            client,
            database,
            config: config.clone(),
        })
    }

    fn db(&self) -> mongodb::Database {
        self.client.database(&self.database)
    }
}

#[async_trait::async_trait]
impl DatabaseConnector for MongoConnector {
    fn driver(&self) -> DbDriver {
        DbDriver::Mongodb
    }

    async fn ping(&self) -> Result<(), AnySqlError> {
        self.client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|e| AnySqlError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn execute_sql(&self, sql: &str, max_rows: usize) -> Result<QueryResult, AnySqlError> {
        let start = Instant::now();
        let trimmed = sql.trim();

        // Accept JSON document as a MongoDB runCommand
        let cmd: Document = serde_json::from_str(trimmed)
            .map_err(|e| AnySqlError::Query(format!("Expected JSON document for MongoDB command. Error: {e}")))?;

        let result = self
            .db()
            .run_command(cmd)
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let elapsed_ms = start.elapsed().as_millis() as u64;

        // Try to extract cursor results (find, aggregate, etc.)
        if let Ok(cursor_doc) = result.get_document("cursor")
            && let Some(batch) = cursor_doc
                .get_array("firstBatch")
                .ok()
                .or_else(|| cursor_doc.get_array("nextBatch").ok())
        {
            let mut columns_map = indexmap::IndexMap::<String, ()>::new();
            let mut rows = Vec::new();

            for (i, val) in batch.iter().enumerate() {
                if i >= max_rows {
                    break;
                }
                if let Some(doc) = val.as_document() {
                    let mut row = serde_json::Map::new();
                    for (k, v) in doc {
                        columns_map.entry(k.clone()).or_insert(());
                        let json_val = bson_to_json(v);
                        row.insert(k.clone(), json_val);
                    }
                    rows.push(row);
                }
            }

            let truncated = batch.len() > max_rows;
            let columns = columns_map
                .keys()
                .enumerate()
                .map(|(i, name)| ColumnInfo {
                    name: name.clone(),
                    data_type: "bson".into(),
                    ordinal: i,
                })
                .collect();

            return Ok(QueryResult {
                columns,
                rows,
                rows_affected: 0,
                elapsed_ms,
                truncated,
            });
        }

        // Fallback: return the whole result as a single-row JSON
        let json_val: serde_json::Value = serde_json::to_value(&result).unwrap_or(serde_json::Value::Null);
        let mut row = serde_json::Map::new();
        row.insert("result".into(), json_val);

        Ok(QueryResult {
            columns: vec![ColumnInfo {
                name: "result".into(),
                data_type: "document".into(),
                ordinal: 0,
            }],
            rows: vec![row],
            rows_affected: 0,
            elapsed_ms,
            truncated: false,
        })
    }

    async fn overview(&self) -> Result<DatabaseOverview, AnySqlError> {
        let build_info = self
            .client
            .database("admin")
            .run_command(doc! { "buildInfo": 1 })
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let version = build_info.get_str("version").unwrap_or("unknown").to_string();

        let server_status = self
            .client
            .database("admin")
            .run_command(doc! { "serverStatus": 1 })
            .await
            .ok();

        let uptime = server_status
            .as_ref()
            .and_then(|s| s.get_f64("uptime").ok())
            .map(|u| format!("{}", u as u64));

        let current_user = self.config.username.clone().unwrap_or_else(|| "anonymous".into());

        let connections = i64::from(
            server_status
                .as_ref()
                .and_then(|s| s.get_document("connections").ok())
                .and_then(|c| c.get_i32("current").ok())
                .unwrap_or(0),
        );

        let max_conn = i64::from(
            server_status
                .as_ref()
                .and_then(|s| s.get_document("connections").ok())
                .and_then(|c| c.get_i32("available").ok())
                .unwrap_or(0),
        ) + connections;

        // DB stats
        let db_stats = self.db().run_command(doc! { "dbStats": 1 }).await.ok();
        let db_size = db_stats.as_ref().and_then(|s| {
            s.get_i64("dataSize")
                .ok()
                .or_else(|| s.get_f64("dataSize").ok().map(|v| v as i64))
        });

        Ok(DatabaseOverview {
            server_version: format!("MongoDB {version}"),
            uptime_seconds: uptime,
            current_database: self.database.clone(),
            current_user,
            database_size_bytes: db_size,
            active_connections: connections,
            max_connections: max_conn,
        })
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseEntry>, AnySqlError> {
        let result = self
            .client
            .database("admin")
            .run_command(doc! { "listDatabases": 1 })
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let databases = result
            .get_array("databases")
            .map_err(|e| AnySqlError::Query(format!("Failed to parse listDatabases: {e}")))?;

        Ok(databases
            .iter()
            .filter_map(|v| v.as_document())
            .map(|doc| {
                let name = doc.get_str("name").unwrap_or("").to_string();
                let size = doc
                    .get_i64("sizeOnDisk")
                    .ok()
                    .or_else(|| doc.get_f64("sizeOnDisk").ok().map(|v| v as i64));
                DatabaseEntry {
                    name,
                    size_bytes: size,
                    encoding: None,
                }
            })
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaEntry>, AnySqlError> {
        // MongoDB has no schema concept
        Ok(vec![])
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableEntry>, AnySqlError> {
        let mut collections: Vec<TableEntry> = Vec::new();

        let mut cursor = self
            .db()
            .list_collections()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        while let Some(spec) = cursor.try_next().await.map_err(|e| AnySqlError::Query(e.to_string()))? {
            let kind = match spec.collection_type {
                mongodb::results::CollectionType::View => TableKind::View,
                _ => TableKind::Table,
            };

            // Get collection stats
            let stats = self.db().run_command(doc! { "collStats": &spec.name }).await.ok();

            let count = stats.as_ref().and_then(|s| {
                s.get_i64("count")
                    .ok()
                    .or_else(|| s.get_i32("count").ok().map(i64::from))
            });
            let size = stats
                .as_ref()
                .and_then(|s| s.get_i64("size").ok().or_else(|| s.get_i32("size").ok().map(i64::from)));

            collections.push(TableEntry {
                name: spec.name,
                schema: None,
                kind,
                estimated_rows: count,
                size_bytes: size,
                comment: None,
            });
        }

        collections.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(collections)
    }

    async fn describe_table(&self, table: &str, _schema: Option<&str>) -> Result<TableDetail, AnySqlError> {
        // Sample documents to infer field schema
        let collection = self.db().collection::<Document>(table);

        let mut cursor = collection
            .find(doc! {})
            .limit(100)
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let mut field_map = indexmap::IndexMap::<String, String>::new();
        while let Some(doc) = cursor.try_next().await.map_err(|e| AnySqlError::Query(e.to_string()))? {
            for (key, val) in &doc {
                field_map.entry(key.clone()).or_insert_with(|| bson_type_name(val));
            }
        }

        let columns: Vec<ColumnDetail> = field_map
            .iter()
            .enumerate()
            .map(|(i, (name, dtype))| ColumnDetail {
                name: name.clone(),
                data_type: dtype.clone(),
                is_nullable: true,
                is_primary_key: name == "_id",
                default_value: None,
                comment: None,
                max_length: None,
                ordinal: i,
            })
            .collect();

        // Get indexes
        let mut idx_cursor = collection
            .list_indexes()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let mut indexes = Vec::new();
        while let Some(idx) = idx_cursor
            .try_next()
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?
        {
            let name = idx.options.as_ref().and_then(|o| o.name.clone()).unwrap_or_default();
            let cols: Vec<String> = idx.keys.iter().map(|(k, _)| k.clone()).collect();
            let is_unique = idx.options.as_ref().and_then(|o| o.unique).unwrap_or(false);
            let is_primary = name == "_id_";

            indexes.push(IndexEntry {
                name,
                columns: cols,
                is_unique: is_unique || is_primary,
                is_primary,
                index_type: Some("btree".into()),
            });
        }

        // Collection stats
        let stats = self.db().run_command(doc! { "collStats": table }).await.ok();
        let count = stats.as_ref().and_then(|s| {
            s.get_i64("count")
                .ok()
                .or_else(|| s.get_i32("count").ok().map(i64::from))
        });
        let size = stats
            .as_ref()
            .and_then(|s| s.get_i64("size").ok().or_else(|| s.get_i32("size").ok().map(i64::from)));

        Ok(TableDetail {
            name: table.to_string(),
            schema: None,
            kind: TableKind::Table,
            columns,
            indexes,
            foreign_keys: vec![],
            create_sql: None,
            comment: None,
            estimated_rows: count,
            size_bytes: size,
        })
    }

    async fn list_routines(&self, _schema: Option<&str>) -> Result<Vec<RoutineEntry>, AnySqlError> {
        Ok(vec![])
    }

    async fn list_triggers(&self, _schema: Option<&str>) -> Result<Vec<TriggerEntry>, AnySqlError> {
        Ok(vec![])
    }

    async fn list_active_queries(&self) -> Result<Vec<ActiveQuery>, AnySqlError> {
        let result = self
            .db()
            .run_command(doc! { "currentOp": 1, "active": true })
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let empty = vec![];
        let ops = result.get_array("inprog").unwrap_or(&empty);

        Ok(ops
            .iter()
            .filter_map(|v| v.as_document())
            .map(|op| {
                let pid = op
                    .get_i64("opid")
                    .map(|v| v.to_string())
                    .or_else(|_| op.get_str("opid").map(std::string::ToString::to_string))
                    .unwrap_or_default();

                ActiveQuery {
                    pid,
                    username: op.get_str("client").ok().map(std::string::ToString::to_string),
                    database: op.get_str("ns").ok().map(std::string::ToString::to_string),
                    query: op.get_document("command").ok().map(|d| format!("{d}")),
                    state: op.get_str("op").ok().map(std::string::ToString::to_string),
                    started_at: None,
                    duration: op
                        .get_i64("microsecs_running")
                        .ok()
                        .map(|us| format!("{:.1}s", us as f64 / 1_000_000.0)),
                    client_addr: op.get_str("client_s").ok().map(std::string::ToString::to_string),
                }
            })
            .collect())
    }

    async fn kill_query(&self, pid: &str) -> Result<(), AnySqlError> {
        let opid: i64 = pid
            .parse()
            .map_err(|_| AnySqlError::BadInput("Invalid MongoDB opid".into()))?;

        self.client
            .database("admin")
            .run_command(doc! { "killOp": 1, "op": opid })
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        Ok(())
    }

    async fn list_variables(&self, filter: Option<&str>) -> Result<Vec<ServerVariable>, AnySqlError> {
        let result = self
            .client
            .database("admin")
            .run_command(doc! { "getParameter": "*" })
            .await
            .map_err(|e| AnySqlError::Query(e.to_string()))?;

        let filter_lower = filter.map(str::to_lowercase);
        let mut vars: Vec<ServerVariable> = result
            .iter()
            .filter(|(k, _)| k != &"ok" && k != &"$clusterTime" && k != &"operationTime")
            .filter(|(k, _)| filter_lower.as_ref().is_none_or(|f| k.to_lowercase().contains(f)))
            .map(|(k, v)| ServerVariable {
                name: k.clone(),
                value: format!("{v}"),
                description: None,
            })
            .collect();

        vars.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(vars)
    }

    async fn switch_database(&self, _database: &str) -> Result<(), AnySqlError> {
        // MongoDB client doesn't persist a "current" database in the same way,
        // but we can't change self.database since &self is immutable.
        // The session manager handles reconnection.
        Err(AnySqlError::Unsupported(
            "Use reconnect with a different database for MongoDB".into(),
        ))
    }
}

/// Convert BSON value to JSON value
fn bson_to_json(val: &bson::Bson) -> serde_json::Value {
    match val {
        bson::Bson::Double(v) => serde_json::Value::from(*v),
        bson::Bson::String(s) => serde_json::Value::from(s.as_str()),
        bson::Bson::Boolean(b) => serde_json::Value::from(*b),
        bson::Bson::Null => serde_json::Value::Null,
        bson::Bson::Int32(v) => serde_json::Value::from(*v),
        bson::Bson::Int64(v) => serde_json::Value::from(*v),
        bson::Bson::ObjectId(oid) => serde_json::Value::from(oid.to_hex()),
        bson::Bson::DateTime(dt) => serde_json::Value::from(dt.try_to_rfc3339_string().unwrap_or_default()),
        bson::Bson::Array(arr) => serde_json::Value::Array(arr.iter().map(bson_to_json).collect()),
        bson::Bson::Document(doc) => {
            let map: serde_json::Map<String, serde_json::Value> =
                doc.iter().map(|(k, v)| (k.clone(), bson_to_json(v))).collect();
            serde_json::Value::Object(map)
        }
        other => serde_json::Value::from(format!("{other}")),
    }
}

/// Get a human-readable BSON type name
fn bson_type_name(val: &bson::Bson) -> String {
    match val {
        bson::Bson::Double(_) => "double".into(),
        bson::Bson::String(_) => "string".into(),
        bson::Bson::Array(_) => "array".into(),
        bson::Bson::Document(_) => "object".into(),
        bson::Bson::Boolean(_) => "bool".into(),
        bson::Bson::Null => "null".into(),
        bson::Bson::Int32(_) => "int32".into(),
        bson::Bson::Int64(_) => "int64".into(),
        bson::Bson::ObjectId(_) => "objectId".into(),
        bson::Bson::DateTime(_) => "date".into(),
        bson::Bson::Binary(_) => "binData".into(),
        bson::Bson::RegularExpression(_) => "regex".into(),
        bson::Bson::Timestamp(_) => "timestamp".into(),
        bson::Bson::Decimal128(_) => "decimal128".into(),
        _ => "unknown".into(),
    }
}
