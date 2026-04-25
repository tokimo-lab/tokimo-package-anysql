//! Schema 浏览相关的 SQL 生成辅助。
//!
//! 各驱动在自己的模块中实现具体 SQL，这里放公共工具。

/// 默认 schema 名
pub fn default_schema(driver: crate::types::DbDriver) -> &'static str {
    match driver {
        crate::types::DbDriver::Postgres | crate::types::DbDriver::Cockroachdb => "public",
        crate::types::DbDriver::Mysql
        | crate::types::DbDriver::Mariadb
        | crate::types::DbDriver::Tidb
        | crate::types::DbDriver::Clickhouse
        | crate::types::DbDriver::Mongodb
        | crate::types::DbDriver::Elasticsearch => "",
        crate::types::DbDriver::Sqlite => "main",
        crate::types::DbDriver::Mssql => "dbo",
        crate::types::DbDriver::Oracle => "SYS",
    }
}
