//! Integration tests for [`PgStore`] against a real Postgres.
//!
//! These are `#[ignore]`d so the default `cargo test` (which has no database)
//! stays green; run them explicitly against a live instance:
//!
//! ```text
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/ferrox_test \
//!     cargo test -p ferrox-store --test postgres -- --ignored --test-threads=1
//! ```
//!
//! They recreate a minimal slice of the Airflow metadata schema and exercise
//! every `MetadataStore` method, so the SQL is run for real rather than
//! assumed correct.

use chrono::{DateTime, TimeZone, Utc};
use ferrox_core::{DagRun, DagRunState, RunType, TaskInstance, TaskState};
use ferrox_store::{MetadataStore, PgStore, SchedulerHeartbeat, StoreError, TaskTransition};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

const SCHEMA: &str = r#"
DROP TABLE IF EXISTS ferrox_ti_state_audit;
DROP TABLE IF EXISTS task_instance;
DROP TABLE IF EXISTS dag_run;
DROP TABLE IF EXISTS job;

CREATE TABLE dag_run (
    dag_id       text        NOT NULL,
    run_id       text        NOT NULL,
    logical_date timestamptz NOT NULL,
    state        text        NOT NULL,
    run_type     text        NOT NULL,
    conf         jsonb       NOT NULL,
    PRIMARY KEY (dag_id, run_id)
);

CREATE TABLE task_instance (
    dag_id      text        NOT NULL,
    task_id     text        NOT NULL,
    run_id      text        NOT NULL,
    map_index   int         NOT NULL,
    state       text        NOT NULL,
    try_number  int         NOT NULL,
    hostname    text,
    queued_dttm timestamptz,
    start_date  timestamptz,
    end_date    timestamptz,
    PRIMARY KEY (dag_id, task_id, run_id, map_index)
);

CREATE TABLE job (
    id               int PRIMARY KEY,
    hostname         text,
    latest_heartbeat timestamptz,
    state            text
);

CREATE TABLE ferrox_ti_state_audit (
    id         bigserial PRIMARY KEY,
    dag_id     text        NOT NULL,
    task_id    text        NOT NULL,
    run_id     text        NOT NULL,
    map_index  int         NOT NULL,
    state      text        NOT NULL,
    changed_at timestamptz NOT NULL
);
"#;

const DAG_ID: &str = "etl";
const RUN_ID: &str = "manual__2026-06-20";

fn ts(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).single().unwrap()
}

async fn fresh_schema() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .expect("set DATABASE_URL to a Postgres instance to run these tests");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to Postgres");
    sqlx::raw_sql(SCHEMA)
        .execute(&pool)
        .await
        .expect("apply schema");
    pool
}

fn scheduled(task: &str) -> TaskInstance {
    TaskInstance::new(task, DAG_ID, RUN_ID)
}

#[tokio::test]
#[ignore = "requires a live Postgres via DATABASE_URL"]
async fn dag_run_round_trips_and_transitions() {
    let pool = fresh_schema().await;
    let store = PgStore::from_pool(pool.clone());

    store.ping().await.expect("ping");

    let run = DagRun::new(RUN_ID, DAG_ID, ts(1_700_000_000), RunType::Manual)
        .with_conf(serde_json::json!({ "region": "eu" }));
    store.insert_dag_run(&run).await.expect("insert dag run");

    let fetched = store
        .dag_run(DAG_ID, RUN_ID)
        .await
        .expect("fetch dag run")
        .expect("dag run exists");
    assert_eq!(fetched, run);

    store
        .set_dag_run_state(DAG_ID, RUN_ID, DagRunState::Running)
        .await
        .expect("set running");
    let after = store.dag_run(DAG_ID, RUN_ID).await.unwrap().unwrap();
    assert_eq!(after.state, DagRunState::Running);

    // The conf survived the state update untouched.
    assert_eq!(after.conf, serde_json::json!({ "region": "eu" }));

    assert!(store.dag_run(DAG_ID, "nope").await.unwrap().is_none());

    let missing = store
        .set_dag_run_state(DAG_ID, "nope", DagRunState::Success)
        .await;
    assert!(matches!(
        missing,
        Err(StoreError::NotFound {
            kind: "dag_run",
            ..
        })
    ));
}

#[tokio::test]
#[ignore = "requires a live Postgres via DATABASE_URL"]
async fn task_instances_upsert_and_read_back() {
    let pool = fresh_schema().await;
    let store = PgStore::from_pool(pool.clone());

    for task in ["extract", "transform", "load"] {
        store
            .insert_task_instance(&scheduled(task))
            .await
            .expect("insert ti");
    }

    let listed = store
        .task_instances_for_run(DAG_ID, RUN_ID)
        .await
        .expect("list tis");
    assert_eq!(listed.len(), 3);
    // ORDER BY task_id, map_index.
    assert_eq!(listed[0].task_id, "extract");
    assert_eq!(listed[1].task_id, "load");
    assert_eq!(listed[2].task_id, "transform");
    assert!(listed.iter().all(|ti| ti.state == TaskState::Scheduled));
    assert!(listed.iter().all(|ti| ti.try_number == 1));

    // Re-inserting the same instance updates in place rather than duplicating.
    let mut moved = scheduled("extract");
    moved.transition_to(TaskState::Queued, ts(5)).unwrap();
    moved.hostname = Some("worker-7".to_owned());
    store.insert_task_instance(&moved).await.expect("upsert ti");

    let listed = store.task_instances_for_run(DAG_ID, RUN_ID).await.unwrap();
    assert_eq!(listed.len(), 3, "upsert must not create a new row");
    let extract = listed.iter().find(|ti| ti.task_id == "extract").unwrap();
    assert_eq!(extract.state, TaskState::Queued);
    assert_eq!(extract.hostname.as_deref(), Some("worker-7"));
    assert_eq!(extract.queued_at, Some(ts(5)));
}

#[tokio::test]
#[ignore = "requires a live Postgres via DATABASE_URL"]
async fn batched_transitions_update_state_timestamps_and_audit() {
    let pool = fresh_schema().await;
    let store = PgStore::from_pool(pool.clone());

    for task in ["extract", "transform"] {
        let mut ti = scheduled(task);
        // Drive each to RUNNING so a SUCCESS transition is legal and stamps end_date.
        ti.transition_to(TaskState::Queued, ts(1)).unwrap();
        ti.transition_to(TaskState::Running, ts(2)).unwrap();
        store.insert_task_instance(&ti).await.unwrap();
    }

    let batch = vec![
        TaskTransition {
            dag_id: DAG_ID.to_owned(),
            task_id: "extract".to_owned(),
            run_id: RUN_ID.to_owned(),
            map_index: -1,
            to: TaskState::Success,
            at: ts(10),
        },
        TaskTransition {
            dag_id: DAG_ID.to_owned(),
            task_id: "transform".to_owned(),
            run_id: RUN_ID.to_owned(),
            map_index: -1,
            to: TaskState::Failed,
            at: ts(11),
        },
    ];

    // An empty batch must be a no-op, not an error.
    store.apply_transitions(&[]).await.expect("empty batch");
    store.apply_transitions(&batch).await.expect("apply batch");

    let listed = store.task_instances_for_run(DAG_ID, RUN_ID).await.unwrap();
    let extract = listed.iter().find(|ti| ti.task_id == "extract").unwrap();
    let transform = listed.iter().find(|ti| ti.task_id == "transform").unwrap();

    assert_eq!(extract.state, TaskState::Success);
    assert_eq!(extract.ended_at, Some(ts(10)));
    // The running start stamped earlier is preserved by the CASE clause.
    assert_eq!(extract.started_at, Some(ts(2)));

    assert_eq!(transform.state, TaskState::Failed);
    assert_eq!(transform.ended_at, Some(ts(11)));

    let audit_count: i64 = sqlx::query("SELECT count(*) AS n FROM ferrox_ti_state_audit")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("n");
    assert_eq!(audit_count, 2, "every transition writes one audit row");

    let audited_states: Vec<String> =
        sqlx::query("SELECT state FROM ferrox_ti_state_audit ORDER BY changed_at")
            .fetch_all(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get::<String, _>("state"))
            .collect();
    assert_eq!(audited_states, vec!["success", "failed"]);
}

#[tokio::test]
#[ignore = "requires a live Postgres via DATABASE_URL"]
async fn heartbeat_updates_an_existing_job_and_reports_missing_ones() {
    let pool = fresh_schema().await;
    let store = PgStore::from_pool(pool.clone());

    sqlx::query(
        "INSERT INTO job (id, hostname, latest_heartbeat, state) VALUES (1, 'old', $1, 'running')",
    )
    .bind(ts(0))
    .execute(&pool)
    .await
    .unwrap();

    let hb = SchedulerHeartbeat {
        job_id: 1,
        hostname: "scheduler-a".to_owned(),
        at: ts(9_999),
    };
    store.record_heartbeat(&hb).await.expect("heartbeat");

    let row = sqlx::query("SELECT hostname, latest_heartbeat FROM job WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("hostname"), "scheduler-a");
    assert_eq!(row.get::<DateTime<Utc>, _>("latest_heartbeat"), ts(9_999));

    let missing = store
        .record_heartbeat(&SchedulerHeartbeat {
            job_id: 404,
            hostname: "ghost".to_owned(),
            at: ts(1),
        })
        .await;
    assert!(matches!(
        missing,
        Err(StoreError::NotFound { kind: "job", .. })
    ));
}
