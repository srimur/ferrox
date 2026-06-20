use chrono::{DateTime, Utc};
use ferrox_core::TaskState;

/// A decided, validated task-instance state change, ready to be persisted.
///
/// The scheduler produces these by driving [`ferrox_core::TaskInstance`]
/// through its state machine; the store's job is only to write them durably
/// and audit them. The store does not re-validate the transition — that
/// decision was already made against the live `from` state upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskTransition {
    pub dag_id: String,
    pub task_id: String,
    pub run_id: String,
    pub map_index: i32,
    pub to: TaskState,
    pub at: DateTime<Utc>,
}

/// A scheduler liveness signal written on every tick window so Airflow's
/// webserver can show scheduler health (§3.2.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerHeartbeat {
    /// Primary key of the `job` row this scheduler owns.
    pub job_id: i32,
    pub hostname: String,
    pub at: DateTime<Utc>,
}
