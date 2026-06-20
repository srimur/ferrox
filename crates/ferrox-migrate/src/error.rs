use thiserror::Error;

/// Errors raised while validating an Airflow metadata database.
#[derive(Debug, Error)]
pub enum MigrateError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}
