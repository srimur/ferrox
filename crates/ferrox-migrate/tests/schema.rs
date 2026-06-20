//! Integration tests for schema validation against a real Postgres.
//!
//! `#[ignore]`d like the store's; run with:
//!
//! ```text
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/ferrox_test \
//!     cargo test -p ferrox-migrate --test schema -- --ignored --test-threads=1
//! ```

use ferrox_migrate::validate_schema;
use sqlx::postgres::PgPoolOptions;

const GOOD_SCHEMA: &str = r#"
DROP TABLE IF EXISTS task_instance;
DROP TABLE IF EXISTS dag_run;
DROP TABLE IF EXISTS job;

CREATE TABLE dag_run (
    dag_id text, run_id text, logical_date timestamptz,
    state text, run_type text, conf jsonb
);
CREATE TABLE task_instance (
    dag_id text, task_id text, run_id text, map_index int, state text,
    try_number int, hostname text, queued_dttm timestamptz,
    start_date timestamptz, end_date timestamptz
);
CREATE TABLE job (
    id int, hostname text, latest_heartbeat timestamptz, state text
);
"#;

fn url() -> String {
    std::env::var("DATABASE_URL").expect("set DATABASE_URL to run these tests")
}

async fn apply(sql: &str) {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url())
        .await
        .expect("connect");
    sqlx::raw_sql(sql).execute(&pool).await.expect("apply sql");
}

#[tokio::test]
#[ignore = "requires a live Postgres via DATABASE_URL"]
async fn a_full_airflow_schema_validates() {
    apply(GOOD_SCHEMA).await;
    let report = validate_schema(&url()).await.expect("validate");
    assert!(report.is_compatible(), "{report}");
}

#[tokio::test]
#[ignore = "requires a live Postgres via DATABASE_URL"]
async fn a_dropped_column_is_caught() {
    apply(GOOD_SCHEMA).await;
    apply("ALTER TABLE task_instance DROP COLUMN end_date").await;

    let report = validate_schema(&url()).await.expect("validate");
    assert!(!report.is_compatible());
    assert!(report
        .missing_columns
        .contains(&("task_instance".to_owned(), "end_date".to_owned())));
}

#[tokio::test]
#[ignore = "requires a live Postgres via DATABASE_URL"]
async fn a_missing_table_is_caught() {
    apply(GOOD_SCHEMA).await;
    apply("DROP TABLE job").await;

    let report = validate_schema(&url()).await.expect("validate");
    assert!(!report.is_compatible());
    assert!(report.missing_tables.contains("job"));
}
