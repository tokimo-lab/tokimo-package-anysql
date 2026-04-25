use std::fmt;

/// 统一错误类型。
#[derive(Debug)]
pub enum AnySqlError {
    /// 连接失败
    Connection(String),
    /// 认证失败
    Auth(String),
    /// SQL 执行失败
    Query(String),
    /// Session 不存在或已过期
    SessionNotFound(String),
    /// 不支持的操作
    Unsupported(String),
    /// 参数错误
    BadInput(String),
    /// 内部错误
    Internal(String),
}

impl fmt::Display for AnySqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection(msg) => write!(f, "connection error: {msg}"),
            Self::Auth(msg) => write!(f, "auth error: {msg}"),
            Self::Query(msg) => write!(f, "query error: {msg}"),
            Self::SessionNotFound(msg) => write!(f, "session not found: {msg}"),
            Self::Unsupported(msg) => write!(f, "unsupported: {msg}"),
            Self::BadInput(msg) => write!(f, "bad input: {msg}"),
            Self::Internal(msg) => write!(f, "internal: {msg}"),
        }
    }
}

impl std::error::Error for AnySqlError {}

impl From<sqlx::Error> for AnySqlError {
    fn from(e: sqlx::Error) -> Self {
        match &e {
            sqlx::Error::Database(db_err) => Self::Query(db_err.message().to_string()),
            sqlx::Error::PoolTimedOut => Self::Connection("connection pool timed out".into()),
            sqlx::Error::Io(io_err) => Self::Connection(io_err.to_string()),
            _ => Self::Query(e.to_string()),
        }
    }
}
