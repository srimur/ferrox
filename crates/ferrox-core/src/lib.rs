//! Ownership: the Ferrox domain model and the rules that keep it consistent.
//!
//! This crate owns the typed vocabulary every other crate speaks — DAG
//! definitions ([`DagDef`]), scheduled runs ([`DagRun`]), task instances
//! ([`TaskInstance`]), and the state enums they carry — plus the two
//! invariants that protect that vocabulary: the task-instance state machine
//! (the only legal mutations on a task instance) and DAG well-formedness
//! (no dangling edges, no cycles).
//!
//! It has zero internal dependencies and zero knowledge of how things are
//! parsed, stored, scheduled, or served. Persistence and wire formats are
//! expressed by deriving [`serde`] on these types; the crates that own those
//! concerns map to and from them.

// Production code surfaces failure through `Result`/`CoreError`; `unwrap` and
// `expect` are reserved for tests, where they are the point.
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod dag;
mod error;
mod ids;
mod run;
mod state;
mod task_instance;

pub use dag::{DagDef, DefaultArgs, Schedule, TaskDef, TriggerRule};
pub use error::CoreError;
pub use ids::TaskId;
pub use run::DagRun;
pub use state::{DagRunState, RunType, TaskState};
pub use task_instance::TaskInstance;
