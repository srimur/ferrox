use thiserror::Error;

use crate::ids::TaskId;
use crate::state::{DagRunState, TaskState};

/// Errors raised when a domain invariant is violated.
///
/// These are programmer- or data-level faults in the model itself —
/// an illegal state transition, a malformed DAG. They are deliberately narrow;
/// I/O, parsing, and SQL failures belong to the crates that own those
/// concerns, not here.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CoreError {
    #[error("illegal task-instance transition: {from} -> {to}")]
    InvalidTransition { from: TaskState, to: TaskState },

    #[error("illegal dag-run transition: {from} -> {to}")]
    InvalidRunTransition { from: DagRunState, to: DagRunState },

    #[error("unknown state {0:?}")]
    UnknownState(String),

    #[error("dag id must not be empty")]
    EmptyDagId,

    #[error("task {0} is defined more than once")]
    DuplicateTask(TaskId),

    #[error("edge references task {0}, which is not defined in the DAG")]
    UnknownTask(TaskId),

    #[error("task {0} is listed under a key that does not match its own id")]
    MismatchedTaskKey(TaskId),

    #[error("dag {dag_id:?} has a dependency cycle reachable through task {task}")]
    Cycle { dag_id: String, task: TaskId },
}
