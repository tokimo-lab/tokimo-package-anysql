use crate::error::AnySqlError;
use crate::types::*;

/// 数据库连接器统一接口。
///
/// 每个驱动（Postgres、MySQL、SQLite）各实现一份。
#[async_trait::async_trait]
pub trait DatabaseConnector: Send + Sync {
    /// 返回驱动类型
    fn driver(&self) -> DbDriver;

    /// 测试连接是否存活
    async fn ping(&self) -> Result<(), AnySqlError>;

    /// 执行任意 SQL（SELECT / DML / DDL 均可）
    async fn execute_sql(&self, sql: &str, max_rows: usize) -> Result<QueryResult, AnySqlError>;

    /// 获取数据库概览
    async fn overview(&self) -> Result<DatabaseOverview, AnySqlError>;

    // ── Schema 浏览 ─────────────────────────────────────────────────────

    /// 列出所有数据库
    async fn list_databases(&self) -> Result<Vec<DatabaseEntry>, AnySqlError>;

    /// 列出所有 schema（Postgres 支持，MySQL/SQLite 返回空或等效概念）
    async fn list_schemas(&self) -> Result<Vec<SchemaEntry>, AnySqlError>;

    /// 列出表 / 视图
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableEntry>, AnySqlError>;

    /// 获取表的完整结构（列 + 索引 + 外键 + DDL）
    async fn describe_table(&self, table: &str, schema: Option<&str>) -> Result<TableDetail, AnySqlError>;

    /// 列出存储过程 / 函数
    async fn list_routines(&self, schema: Option<&str>) -> Result<Vec<RoutineEntry>, AnySqlError>;

    /// 列出触发器
    async fn list_triggers(&self, schema: Option<&str>) -> Result<Vec<TriggerEntry>, AnySqlError>;

    // ── 运维 ────────────────────────────────────────────────────────────

    /// 查看当前活跃查询
    async fn list_active_queries(&self) -> Result<Vec<ActiveQuery>, AnySqlError>;

    /// 终止指定查询
    async fn kill_query(&self, pid: &str) -> Result<(), AnySqlError>;

    /// 列出服务器变量 / 配置参数
    async fn list_variables(&self, filter: Option<&str>) -> Result<Vec<ServerVariable>, AnySqlError>;

    /// 切换当前数据库
    async fn switch_database(&self, database: &str) -> Result<(), AnySqlError>;
}
