pub mod clickhouse;
pub mod elasticsearch;
pub mod mongo;
pub mod mssql;
pub mod mysql;
#[cfg(feature = "oracle")]
pub mod oracle_db;
pub mod postgres;
pub mod sqlite;

use crate::connector::DatabaseConnector;
use crate::error::AnySqlError;
use crate::types::{DbConnectionConfig, DbDriver};

/// 根据配置创建对应驱动的 connector
pub async fn create_connector(config: &DbConnectionConfig) -> Result<Box<dyn DatabaseConnector>, AnySqlError> {
    match config.driver {
        DbDriver::Postgres | DbDriver::Cockroachdb => {
            let c = postgres::PgConnector::connect(config).await?;
            Ok(Box::new(c))
        }
        DbDriver::Mysql | DbDriver::Mariadb | DbDriver::Tidb => {
            let c = mysql::MysqlConnector::connect(config).await?;
            Ok(Box::new(c))
        }
        DbDriver::Sqlite => {
            let c = sqlite::SqliteConnector::connect(config).await?;
            Ok(Box::new(c))
        }
        DbDriver::Mssql => {
            let c = mssql::MssqlConnector::connect(config).await?;
            Ok(Box::new(c))
        }
        DbDriver::Clickhouse => {
            let c = clickhouse::ClickHouseConnector::connect(config).await?;
            Ok(Box::new(c))
        }
        DbDriver::Oracle => {
            #[cfg(feature = "oracle")]
            {
                let c = oracle_db::OracleConnector::connect(config).await?;
                Ok(Box::new(c))
            }
            #[cfg(not(feature = "oracle"))]
            {
                Err(AnySqlError::Unsupported(
                    "Oracle requires the 'oracle' feature and Oracle Instant Client installed".into(),
                ))
            }
        }
        DbDriver::Mongodb => {
            let c = mongo::MongoConnector::connect(config).await?;
            Ok(Box::new(c))
        }
        DbDriver::Elasticsearch => {
            let c = elasticsearch::ElasticsearchConnector::connect(config).await?;
            Ok(Box::new(c))
        }
    }
}
