//! Ownership: every read and write against the Airflow metadata database.
//!
//! This crate owns the [`MetadataStore`] trait — the one seam through which
//! the rest of Ferrox touches persistent state — and its sqlx-backed Postgres
//! implementation ([`PgStore`]). It owns the SQL, the connection pool and its
//! ceiling, the mapping between `ferrox-core` types and table rows, and the
//! batched, audited write path for task-state transitions.
//!
//! It owns no scheduling decisions: callers hand it already-decided
//! [`TaskTransition`]s and ask it questions about stored state. Keeping all SQL
//! behind this trait is what makes the scheduler testable against an in-memory
//! fake and the storage backend swappable (Postgres today, MySQL in Phase 2).

mod error;
mod postgres;
mod record;

use async_trait::async_trait;
use ferrox_core::{DagRun, DagRunState, TaskInstance};

pub use error::StoreError;
pub use postgres::PgStore;
pub use record::{SchedulerHeartbeat, TaskTransition};

/// The metadata persistence boundary for the whole system.
///
/// Implementations must be cheap to share across tasks (`Send + Sync`): the
/// scheduler holds one and calls into it concurrently from spawned work.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Cheap round trip that proves the backend is reachable.
    async fn ping(&self) -> Result<(), StoreError>;

    /// Insert a DAG run, or update its mutable fields if it already exists.
    async fn insert_dag_run(&self, run: &DagRun) -> Result<(), StoreError>;

    /// Fetch one DAG run, or `None` if there is no such run.
    async fn dag_run(&self, dag_id: &str, run_id: &str) -> Result<Option<DagRun>, StoreError>;

    /// Move a DAG run to a new state. Errors with [`StoreError::NotFound`] if
    /// the run does not exist.
    async fn set_dag_run_state(
        &self,
        dag_id: &str,
        run_id: &str,
        state: DagRunState,
    ) -> Result<(), StoreError>;

    /// Insert a task instance, or update it in place if it already exists.
    async fn insert_task_instance(&self, ti: &TaskInstance) -> Result<(), StoreError>;

    /// All task instances belonging to a run — the input the scheduler walks to
    /// evaluate downstream readiness.
    async fn task_instances_for_run(
        &self,
        dag_id: &str,
        run_id: &str,
    ) -> Result<Vec<TaskInstance>, StoreError>;

    /// Persist a batch of state transitions in one round trip, writing each to
    /// the task-instance table and the audit table atomically. An empty batch
    /// is a no-op.
    async fn apply_transitions(&self, transitions: &[TaskTransition]) -> Result<(), StoreError>;

    /// Record a scheduler heartbeat against its `job` row.
    async fn record_heartbeat(&self, hb: &SchedulerHeartbeat) -> Result<(), StoreError>;
}
