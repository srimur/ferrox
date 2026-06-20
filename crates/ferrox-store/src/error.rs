use ferrox_core::CoreError;
use thiserror::Error;

/// Errors raised by the metadata store.
///
/// Anything crossing in from another crate is mapped here explicitly: sqlx
/// failures become [`StoreError::Database`], and domain-rule violations from
/// `ferrox-core` become [`StoreError::Domain`]. The remaining variants cover
/// faults the store itself detects — a missing row, or a column whose stored
/// value cannot be read back into the typed model.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    Domain(#[from] CoreError),

    #[error("{kind} not found: {id}")]
    NotFound { kind: &'static str, id: String },

    #[error("metadata column {column} holds an unreadable value: {detail}")]
    Corrupt {
        column: &'static str,
        detail: String,
    },

    #[error("{requested} transitions submitted but only {applied} matched a task instance")]
    TransitionGap { requested: usize, applied: usize },
}
