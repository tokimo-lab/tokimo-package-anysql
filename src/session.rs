use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::connector::DatabaseConnector;
use crate::drivers;
use crate::error::AnySqlError;
use crate::types::{ApiDateTime, DbConnectionConfig, DbSessionDto};

pub type SessionId = String;

/// 单个 session 实例
struct Session {
    id: SessionId,
    config: DbConnectionConfig,
    connector: Box<dyn DatabaseConnector>,
    created_at: DateTime<Utc>,
    last_active_at: DateTime<Utc>,
    /// 当前选中的数据库（可运行时切换）
    current_database: Option<String>,
    server_version: Option<String>,
}

impl Session {
    fn touch(&mut self) {
        self.last_active_at = Utc::now();
    }

    fn to_dto(&self) -> DbSessionDto {
        DbSessionDto {
            id: self.id.clone(),
            config: self.config.clone(),
            current_database: self.current_database.clone(),
            server_version: self.server_version.clone(),
            created_at: self.created_at.to_api_string(),
            last_active_at: self.last_active_at.to_api_string(),
        }
    }
}

/// `SessionManager` 管理所有数据库 session。
///
/// - 前端刷新不丢失连接（session 保留在内存中）
/// - 支持自动清理过期 session
/// - 线程安全：内部用 `RwLock<HashMap>`
pub struct SessionManager {
    sessions: RwLock<HashMap<SessionId, Session>>,
    /// Session 过期时间（闲置超时后自动关闭）
    idle_timeout: Duration,
}

impl SessionManager {
    /// 创建一个新的 `SessionManager`。
    ///
    /// `idle_timeout` 表示 session 闲置多久后被自动回收。
    pub fn new(idle_timeout: Duration) -> Arc<Self> {
        let manager = Arc::new(Self {
            sessions: RwLock::new(HashMap::new()),
            idle_timeout,
        });

        // 启动后台清理任务
        let weak = Arc::downgrade(&manager);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_mins(1));
            loop {
                interval.tick().await;
                let Some(mgr) = weak.upgrade() else {
                    break;
                };
                mgr.cleanup_expired().await;
            }
        });

        manager
    }

    /// 创建新连接 session
    pub async fn connect(&self, config: DbConnectionConfig) -> Result<DbSessionDto, AnySqlError> {
        let id = uuid::Uuid::new_v4().to_string();
        self.connect_with_id(id, config).await
    }

    /// 创建新连接 session（使用指定 ID，用于从持久化配置重连）
    pub async fn connect_with_id(&self, id: String, config: DbConnectionConfig) -> Result<DbSessionDto, AnySqlError> {
        // 如果该 ID 已有活跃 session，先移除旧的
        self.sessions.write().await.remove(&id);

        let connector = drivers::create_connector(&config).await?;

        // 测试连通性
        connector.ping().await?;

        // 获取版本信息
        let version = match connector.overview().await {
            Ok(ov) => Some(ov.server_version),
            Err(_) => None,
        };

        let now = Utc::now();
        let current_db = config.database.clone();

        let session = Session {
            id: id.clone(),
            config,
            connector,
            created_at: now,
            last_active_at: now,
            current_database: current_db,
            server_version: version,
        };

        let dto = session.to_dto();
        self.sessions.write().await.insert(id, session);

        Ok(dto)
    }

    /// 断开并删除 session
    pub async fn disconnect(&self, session_id: &str) -> Result<(), AnySqlError> {
        self.sessions
            .write()
            .await
            .remove(session_id)
            .map(|_| ())
            .ok_or_else(|| AnySqlError::SessionNotFound(session_id.into()))
    }

    /// 尝试断开 session（如果存在），不报错
    pub async fn disconnect_if_exists(&self, session_id: &str) {
        self.sessions.write().await.remove(session_id);
    }

    /// 检查某个 session 是否仍然活跃（会 ping 远端）
    pub async fn is_active(&self, session_id: &str) -> bool {
        let sessions = self.sessions.read().await;
        if let Some(s) = sessions.get(session_id) {
            s.connector.ping().await.is_ok()
        } else {
            false
        }
    }

    /// 快速检查 session 是否存在于内存中（不 ping）
    pub async fn has_session(&self, session_id: &str) -> bool {
        self.sessions.read().await.contains_key(session_id)
    }

    /// 获取 session 列表
    pub async fn list_sessions(&self) -> Vec<DbSessionDto> {
        let sessions = self.sessions.read().await;
        sessions.values().map(Session::to_dto).collect()
    }

    /// 获取单个 session 信息
    pub async fn get_session(&self, session_id: &str) -> Result<DbSessionDto, AnySqlError> {
        let sessions = self.sessions.read().await;
        let s = sessions
            .get(session_id)
            .ok_or_else(|| AnySqlError::SessionNotFound(session_id.into()))?;
        Ok(s.to_dto())
    }

    /// 在指定 session 上执行 SQL
    pub async fn execute_sql(
        &self,
        session_id: &str,
        sql: &str,
        max_rows: usize,
    ) -> Result<crate::types::QueryResult, AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = sessions
            .get_mut(session_id)
            .ok_or_else(|| AnySqlError::SessionNotFound(session_id.into()))?;
        s.touch();
        s.connector.execute_sql(sql, max_rows).await
    }

    /// 获取 session 并 touch，返回 connector 的引用（内部辅助宏用）
    fn get_and_touch<'a>(
        sessions: &'a mut HashMap<SessionId, Session>,
        session_id: &str,
    ) -> Result<&'a mut Session, AnySqlError> {
        let s = sessions
            .get_mut(session_id)
            .ok_or_else(|| AnySqlError::SessionNotFound(session_id.into()))?;
        s.touch();
        Ok(s)
    }

    /// 获取数据库概览
    pub async fn overview(&self, session_id: &str) -> Result<crate::types::DatabaseOverview, AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = Self::get_and_touch(&mut sessions, session_id)?;
        s.connector.overview().await
    }

    /// 列出所有数据库
    pub async fn list_databases(&self, session_id: &str) -> Result<Vec<crate::types::DatabaseEntry>, AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = Self::get_and_touch(&mut sessions, session_id)?;
        s.connector.list_databases().await
    }

    /// 列出所有 schema
    pub async fn list_schemas(&self, session_id: &str) -> Result<Vec<crate::types::SchemaEntry>, AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = Self::get_and_touch(&mut sessions, session_id)?;
        s.connector.list_schemas().await
    }

    /// 列出表 / 视图
    pub async fn list_tables(
        &self,
        session_id: &str,
        schema: Option<&str>,
    ) -> Result<Vec<crate::types::TableEntry>, AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = Self::get_and_touch(&mut sessions, session_id)?;
        s.connector.list_tables(schema).await
    }

    /// 获取表结构详情
    pub async fn describe_table(
        &self,
        session_id: &str,
        table: &str,
        schema: Option<&str>,
    ) -> Result<crate::types::TableDetail, AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = Self::get_and_touch(&mut sessions, session_id)?;
        s.connector.describe_table(table, schema).await
    }

    /// 列出存储过程 / 函数
    pub async fn list_routines(
        &self,
        session_id: &str,
        schema: Option<&str>,
    ) -> Result<Vec<crate::types::RoutineEntry>, AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = Self::get_and_touch(&mut sessions, session_id)?;
        s.connector.list_routines(schema).await
    }

    /// 列出触发器
    pub async fn list_triggers(
        &self,
        session_id: &str,
        schema: Option<&str>,
    ) -> Result<Vec<crate::types::TriggerEntry>, AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = Self::get_and_touch(&mut sessions, session_id)?;
        s.connector.list_triggers(schema).await
    }

    /// 查看活跃查询
    pub async fn list_active_queries(&self, session_id: &str) -> Result<Vec<crate::types::ActiveQuery>, AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = Self::get_and_touch(&mut sessions, session_id)?;
        s.connector.list_active_queries().await
    }

    /// 终止查询
    pub async fn kill_query(&self, session_id: &str, pid: &str) -> Result<(), AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = Self::get_and_touch(&mut sessions, session_id)?;
        s.connector.kill_query(pid).await
    }

    /// 列出服务器变量
    pub async fn list_variables(
        &self,
        session_id: &str,
        filter: Option<&str>,
    ) -> Result<Vec<crate::types::ServerVariable>, AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = Self::get_and_touch(&mut sessions, session_id)?;
        s.connector.list_variables(filter).await
    }

    /// 切换当前数据库
    pub async fn switch_database(&self, session_id: &str, database: &str) -> Result<(), AnySqlError> {
        let mut sessions = self.sessions.write().await;
        let s = sessions
            .get_mut(session_id)
            .ok_or_else(|| AnySqlError::SessionNotFound(session_id.into()))?;
        s.touch();
        s.connector.switch_database(database).await?;
        s.current_database = Some(database.to_string());
        Ok(())
    }

    /// 测试连接（不创建 session）
    pub async fn test_connection(config: &DbConnectionConfig) -> Result<crate::types::DatabaseOverview, AnySqlError> {
        let connector = drivers::create_connector(config).await?;
        connector.ping().await?;
        connector.overview().await
    }

    /// 清理过期 session
    async fn cleanup_expired(&self) {
        let now = Utc::now();
        let timeout = chrono::Duration::from_std(self.idle_timeout).unwrap_or_default();

        let mut sessions = self.sessions.write().await;
        let before = sessions.len();
        sessions.retain(|id, s| {
            let expired = now - s.last_active_at > timeout;
            if expired {
                info!(session_id = %id, driver = %s.config.driver, "session expired, removing");
            }
            !expired
        });
        let removed = before - sessions.len();
        if removed > 0 {
            warn!(removed, remaining = sessions.len(), "cleaned up expired sessions");
        }
    }
}
