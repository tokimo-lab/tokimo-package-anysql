use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

// ── 数据库驱动类型 ───────────────────────────────────────────────────────────

/// 支持的数据库驱动
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(rename_all = "lowercase")]
pub enum DbDriver {
    Postgres,
    Mysql,
    Sqlite,
    Mariadb,
    Mssql,
    Cockroachdb,
    Tidb,
    Clickhouse,
    Oracle,
    Mongodb,
    Elasticsearch,
}

impl std::fmt::Display for DbDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres => write!(f, "postgres"),
            Self::Mysql => write!(f, "mysql"),
            Self::Sqlite => write!(f, "sqlite"),
            Self::Mariadb => write!(f, "mariadb"),
            Self::Mssql => write!(f, "mssql"),
            Self::Cockroachdb => write!(f, "cockroachdb"),
            Self::Tidb => write!(f, "tidb"),
            Self::Clickhouse => write!(f, "clickhouse"),
            Self::Oracle => write!(f, "oracle"),
            Self::Mongodb => write!(f, "mongodb"),
            Self::Elasticsearch => write!(f, "elasticsearch"),
        }
    }
}

// ── 连接配置 ─────────────────────────────────────────────────────────────────

/// 数据库连接参数
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct DbConnectionConfig {
    pub driver: DbDriver,
    /// 显示名称
    pub name: String,
    /// 主机地址（SQLite 时为文件路径）
    pub host: String,
    /// 端口（SQLite 时忽略）
    #[ts(type = "number | null")]
    pub port: Option<u16>,
    /// 用户名
    pub username: Option<String>,
    /// 密码
    pub password: Option<String>,
    /// 默认数据库名
    pub database: Option<String>,
    /// 额外连接参数，如 sslmode=require
    pub params: Option<String>,
}

impl DbConnectionConfig {
    /// 构造 sqlx 连接 URL
    pub fn to_url(&self) -> String {
        match self.driver {
            DbDriver::Sqlite => {
                let base = format!("sqlite:{}", self.host);
                if let Some(params) = &self.params {
                    format!("{base}?{params}")
                } else {
                    format!("{base}?mode=rwc")
                }
            }
            DbDriver::Postgres | DbDriver::Cockroachdb => {
                let user = self.username.as_deref().unwrap_or("postgres");
                let pass = self.password.as_deref().unwrap_or("");
                let port = self.port.unwrap_or(match self.driver {
                    DbDriver::Cockroachdb => 26257,
                    _ => 5432,
                });
                let db = self.database.as_deref().unwrap_or("postgres");
                let base = format!("postgres://{user}:{pass}@{}:{port}/{db}", self.host);
                if let Some(params) = &self.params {
                    format!("{base}?{params}")
                } else {
                    base
                }
            }
            DbDriver::Mysql | DbDriver::Mariadb | DbDriver::Tidb => {
                let user = self.username.as_deref().unwrap_or("root");
                let pass = self.password.as_deref().unwrap_or("");
                let port = self.port.unwrap_or(match self.driver {
                    DbDriver::Tidb => 4000,
                    _ => 3306,
                });
                let db = self.database.as_deref().unwrap_or("");
                let base = format!("mysql://{user}:{pass}@{}:{port}/{db}", self.host);
                if let Some(params) = &self.params {
                    format!("{base}?{params}")
                } else {
                    base
                }
            }
            DbDriver::Mssql => {
                // MSSQL 使用 tiberius，此处返回 ADO.NET 风格连接字符串供参考
                let user = self.username.as_deref().unwrap_or("sa");
                let pass = self.password.as_deref().unwrap_or("");
                let port = self.port.unwrap_or(1433);
                let db = self.database.as_deref().unwrap_or("master");
                let mut base = format!(
                    "server=tcp:{},{port};user={user};password={pass};database={db}",
                    self.host
                );
                if let Some(params) = &self.params {
                    base.push(';');
                    base.push_str(params);
                }
                base
            }
            DbDriver::Oracle => {
                let user = self.username.as_deref().unwrap_or("system");
                let pass = self.password.as_deref().unwrap_or("");
                let port = self.port.unwrap_or(1521);
                let service = self.database.as_deref().unwrap_or("ORCL");
                format!("oracle://{user}:{pass}@{}:{port}/{service}", self.host)
            }
            DbDriver::Mongodb => {
                let user = self.username.as_deref().unwrap_or("");
                let pass = self.password.as_deref().unwrap_or("");
                let port = self.port.unwrap_or(27017);
                let db = self.database.as_deref().unwrap_or("admin");
                let auth = if user.is_empty() {
                    String::new()
                } else if pass.is_empty() {
                    format!("{user}@")
                } else {
                    format!("{user}:{pass}@")
                };
                let mut base = format!("mongodb://{auth}{}:{port}/{db}", self.host);
                if let Some(params) = &self.params {
                    base.push('?');
                    base.push_str(params);
                }
                base
            }
            DbDriver::Elasticsearch => {
                let port = self.port.unwrap_or(9200);
                let scheme = if self.params.as_deref().is_some_and(|p| p.contains("ssl=true")) {
                    "https"
                } else {
                    "http"
                };
                format!("{scheme}://{}:{port}", self.host)
            }
            DbDriver::Clickhouse => {
                let user = self.username.as_deref().unwrap_or("default");
                let pass = self.password.as_deref().unwrap_or("");
                let port = self.port.unwrap_or(8123);
                let db = self.database.as_deref().unwrap_or("default");
                let mut base = format!("http://{}:{port}/?database={db}&user={user}", self.host);
                if !pass.is_empty() {
                    write!(base, "&password={pass}").unwrap();
                }
                if let Some(params) = &self.params {
                    base.push('&');
                    base.push_str(params);
                }
                base
            }
        }
    }
}

// ── 查询结果 ─────────────────────────────────────────────────────────────────

/// 单个列的元信息
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    /// 列序号（0-based）
    #[ts(type = "number")]
    pub ordinal: usize,
}

/// 查询结果集
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult {
    /// 列元信息
    pub columns: Vec<ColumnInfo>,
    /// 行数据，每行是 `column_name` → JSON value 的映射
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    /// 影响行数（INSERT/UPDATE/DELETE 等）
    #[ts(type = "number")]
    pub rows_affected: u64,
    /// 执行耗时（毫秒）
    #[ts(type = "number")]
    pub elapsed_ms: u64,
    /// 是否已截断（超出 max rows 限制）
    pub truncated: bool,
}

// ── Session DTO ──────────────────────────────────────────────────────────────

/// Session 信息（返回给前端）
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct DbSessionDto {
    pub id: String,
    pub config: DbConnectionConfig,
    pub current_database: Option<String>,
    pub server_version: Option<String>,
    pub created_at: String,
    pub last_active_at: String,
}

// ── Schema 浏览 ──────────────────────────────────────────────────────────────

/// 数据库列表项
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseEntry {
    pub name: String,
    /// 数据库大小（字节，可能不支持所有数据库）
    #[ts(type = "number | null")]
    pub size_bytes: Option<i64>,
    pub encoding: Option<String>,
}

/// Schema 列表项
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct SchemaEntry {
    pub name: String,
    pub owner: Option<String>,
}

/// 表/视图的类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(rename_all = "snake_case")]
pub enum TableKind {
    Table,
    View,
    MaterializedView,
    ForeignTable,
    Sequence,
}

/// 表列表项
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct TableEntry {
    pub name: String,
    pub schema: Option<String>,
    pub kind: TableKind,
    /// 估计行数（不一定精确）
    #[ts(type = "number | null")]
    pub estimated_rows: Option<i64>,
    /// 表大小（字节）
    #[ts(type = "number | null")]
    pub size_bytes: Option<i64>,
    pub comment: Option<String>,
}

/// 列详情
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct ColumnDetail {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<String>,
    pub comment: Option<String>,
    #[ts(type = "number | null")]
    pub max_length: Option<i32>,
    #[ts(type = "number")]
    pub ordinal: usize,
}

/// 索引信息
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct IndexEntry {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    pub index_type: Option<String>,
}

/// 外键信息
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct ForeignKeyEntry {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_schema: Option<String>,
    pub referenced_columns: Vec<String>,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

/// 表完整详情（结构 + 索引 + 外键）
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct TableDetail {
    pub name: String,
    pub schema: Option<String>,
    pub kind: TableKind,
    pub columns: Vec<ColumnDetail>,
    pub indexes: Vec<IndexEntry>,
    pub foreign_keys: Vec<ForeignKeyEntry>,
    pub create_sql: Option<String>,
    pub comment: Option<String>,
    #[ts(type = "number | null")]
    pub estimated_rows: Option<i64>,
    #[ts(type = "number | null")]
    pub size_bytes: Option<i64>,
}

/// 存储过程 / 函数信息
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct RoutineEntry {
    pub name: String,
    pub schema: Option<String>,
    pub kind: String,
    pub return_type: Option<String>,
    pub language: Option<String>,
    pub definition: Option<String>,
}

/// 触发器信息
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct TriggerEntry {
    pub name: String,
    pub table_name: String,
    pub schema: Option<String>,
    pub event: String,
    pub timing: String,
    pub definition: Option<String>,
}

// ── 数据库统计 ───────────────────────────────────────────────────────────────

/// 数据库概览
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseOverview {
    pub server_version: String,
    pub uptime_seconds: Option<String>,
    pub current_database: String,
    pub current_user: String,
    #[ts(type = "number | null")]
    pub database_size_bytes: Option<i64>,
    #[ts(type = "number")]
    pub active_connections: i64,
    #[ts(type = "number")]
    pub max_connections: i64,
}

/// 当前活跃查询
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct ActiveQuery {
    pub pid: String,
    pub username: Option<String>,
    pub database: Option<String>,
    pub query: Option<String>,
    pub state: Option<String>,
    pub started_at: Option<String>,
    pub duration: Option<String>,
    pub client_addr: Option<String>,
}

/// 变量 / 配置项
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export)]
#[serde(rename_all = "camelCase")]
pub struct ServerVariable {
    pub name: String,
    pub value: String,
    pub description: Option<String>,
}

// ── 内部工具 ─────────────────────────────────────────────────────────────────

/// 内部 trait：DateTime 转 API 字符串
pub(crate) trait ApiDateTime {
    fn to_api_string(&self) -> String;
}

impl ApiDateTime for DateTime<Utc> {
    fn to_api_string(&self) -> String {
        self.to_rfc3339()
    }
}
