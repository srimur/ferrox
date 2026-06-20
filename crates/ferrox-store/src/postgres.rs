use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ferrox_core::{DagRun, DagRunState, RunType, TaskInstance, TaskState};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::Row;
use std::str::FromStr;

use crate::error::StoreError;
use crate::record::{SchedulerHeartbeat, TaskTransition};
use crate::MetadataStore;

/// Postgres-backed [`MetadataStore`].
///
/// Queries use sqlx's runtime API rather than the compile-time macros so the
/// crate builds in CI and release environments that have no database (see ADR
/// 0002). Every query is parameterized; no value is ever interpolated into SQL.
#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    /// Default connection ceiling. Keeping this low is the point: Ferrox aims
    /// to retire PgBouncer by holding steady state under 20 connections.
    pub const DEFAULT_MAX_CONNECTIONS: u32 = 20;

    /// Open a pool against `url` with the given connection ceiling.
    pub async fn connect(url: &str, max_connections: u32) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    /// Wrap an already-configured pool — used by tests and by callers that
    /// share one pool across the store and other components.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

const SELECT_DAG_RUN: &str = "SELECT dag_id, run_id, logical_date, state, run_type, conf \
     FROM dag_run WHERE dag_id = $1 AND run_id = $2";

const INSERT_DAG_RUN: &str =
    "INSERT INTO dag_run (dag_id, run_id, logical_date, state, run_type, conf) \
     VALUES ($1, $2, $3, $4, $5, $6) \
     ON CONFLICT (dag_id, run_id) DO UPDATE \
     SET state = EXCLUDED.state, run_type = EXCLUDED.run_type, conf = EXCLUDED.conf";

const UPDATE_DAG_RUN_STATE: &str =
    "UPDATE dag_run SET state = $3 WHERE dag_id = $1 AND run_id = $2";

const UPSERT_TASK_INSTANCE: &str = "INSERT INTO task_instance \
     (dag_id, task_id, run_id, map_index, state, try_number, hostname, queued_dttm, start_date, end_date) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
     ON CONFLICT (dag_id, task_id, run_id, map_index) DO UPDATE \
     SET state = EXCLUDED.state, try_number = EXCLUDED.try_number, hostname = EXCLUDED.hostname, \
         queued_dttm = EXCLUDED.queued_dttm, start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date";

const SELECT_RUN_TASK_INSTANCES: &str = "SELECT \
     dag_id, task_id, run_id, map_index, state, try_number, hostname, queued_dttm, start_date, end_date \
     FROM task_instance WHERE dag_id = $1 AND run_id = $2 ORDER BY task_id, map_index";

// A single multi-row UPDATE driven by parallel arrays, so a batch of
// transitions is one round trip rather than N (§3.2.5, batch writes). The
// timestamp lands in the column the target state implies, leaving the others
// untouched.
const APPLY_TRANSITIONS: &str = "UPDATE task_instance AS ti SET \
     state = v.state, \
     queued_dttm = CASE WHEN v.state = 'queued' THEN v.at ELSE ti.queued_dttm END, \
     start_date  = CASE WHEN v.state = 'running' THEN v.at ELSE ti.start_date END, \
     end_date    = CASE WHEN v.state IN ('success', 'failed', 'upstream_failed') THEN v.at ELSE ti.end_date END \
     FROM UNNEST($1::text[], $2::text[], $3::text[], $4::int[], $5::text[], $6::timestamptz[]) \
     AS v(dag_id, task_id, run_id, map_index, state, at) \
     WHERE ti.dag_id = v.dag_id AND ti.task_id = v.task_id \
       AND ti.run_id = v.run_id AND ti.map_index = v.map_index \
     RETURNING ti.task_id";

const AUDIT_TRANSITIONS: &str =
    "INSERT INTO ferrox_ti_state_audit (dag_id, task_id, run_id, map_index, state, changed_at) \
     SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::int[], $5::text[], $6::timestamptz[])";

const HEARTBEAT: &str =
    "UPDATE job SET latest_heartbeat = $2, hostname = $3, state = 'running' WHERE id = $1";

/// The transition batch transposed into per-column arrays, ready to bind to the
/// `UNNEST(...)` parameters. Pure and allocation-only so it can be tested
/// without a database.
struct TransitionColumns {
    dag_ids: Vec<String>,
    task_ids: Vec<String>,
    run_ids: Vec<String>,
    map_indexes: Vec<i32>,
    states: Vec<String>,
    ats: Vec<DateTime<Utc>>,
}

fn transition_columns(transitions: &[TaskTransition]) -> TransitionColumns {
    let mut cols = TransitionColumns {
        dag_ids: Vec::with_capacity(transitions.len()),
        task_ids: Vec::with_capacity(transitions.len()),
        run_ids: Vec::with_capacity(transitions.len()),
        map_indexes: Vec::with_capacity(transitions.len()),
        states: Vec::with_capacity(transitions.len()),
        ats: Vec::with_capacity(transitions.len()),
    };
    for t in transitions {
        cols.dag_ids.push(t.dag_id.clone());
        cols.task_ids.push(t.task_id.clone());
        cols.run_ids.push(t.run_id.clone());
        cols.map_indexes.push(t.map_index);
        cols.states.push(t.to.as_str().to_owned());
        cols.ats.push(t.at);
    }
    cols
}

fn dag_run_from_row(row: &PgRow) -> Result<DagRun, StoreError> {
    let state: String = row.try_get("state")?;
    let run_type: String = row.try_get("run_type")?;
    Ok(DagRun {
        run_id: row.try_get("run_id")?,
        dag_id: row.try_get("dag_id")?,
        logical_date: row.try_get("logical_date")?,
        state: DagRunState::from_str(&state)?,
        run_type: RunType::from_str(&run_type)?,
        conf: row.try_get("conf")?,
    })
}

fn task_instance_from_row(row: &PgRow) -> Result<TaskInstance, StoreError> {
    let state: String = row.try_get("state")?;
    let try_number_raw: i32 = row.try_get("try_number")?;
    let try_number = u32::try_from(try_number_raw).map_err(|_| StoreError::Corrupt {
        column: "try_number",
        detail: format!("negative value {try_number_raw}"),
    })?;
    Ok(TaskInstance {
        task_id: row.try_get("task_id")?,
        dag_id: row.try_get("dag_id")?,
        run_id: row.try_get("run_id")?,
        map_index: row.try_get("map_index")?,
        state: TaskState::from_str(&state)?,
        try_number,
        hostname: row.try_get("hostname")?,
        queued_at: row.try_get("queued_dttm")?,
        started_at: row.try_get("start_date")?,
        ended_at: row.try_get("end_date")?,
    })
}

#[async_trait]
impl MetadataStore for PgStore {
    async fn ping(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn insert_dag_run(&self, run: &DagRun) -> Result<(), StoreError> {
        sqlx::query(INSERT_DAG_RUN)
            .bind(&run.dag_id)
            .bind(&run.run_id)
            .bind(run.logical_date)
            .bind(run.state.as_str())
            .bind(run.run_type.as_str())
            .bind(&run.conf)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn dag_run(&self, dag_id: &str, run_id: &str) -> Result<Option<DagRun>, StoreError> {
        let row = sqlx::query(SELECT_DAG_RUN)
            .bind(dag_id)
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(dag_run_from_row).transpose()
    }

    async fn set_dag_run_state(
        &self,
        dag_id: &str,
        run_id: &str,
        state: DagRunState,
    ) -> Result<(), StoreError> {
        let result = sqlx::query(UPDATE_DAG_RUN_STATE)
            .bind(dag_id)
            .bind(run_id)
            .bind(state.as_str())
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::NotFound {
                kind: "dag_run",
                id: format!("{dag_id}/{run_id}"),
            });
        }
        Ok(())
    }

    async fn insert_task_instance(&self, ti: &TaskInstance) -> Result<(), StoreError> {
        // Airflow's try_number column is a signed int; surface an overflow as
        // corrupt data rather than silently clamping it.
        let try_number = i32::try_from(ti.try_number).map_err(|_| StoreError::Corrupt {
            column: "try_number",
            detail: format!("{} exceeds i32::MAX", ti.try_number),
        })?;
        sqlx::query(UPSERT_TASK_INSTANCE)
            .bind(&ti.dag_id)
            .bind(&ti.task_id)
            .bind(&ti.run_id)
            .bind(ti.map_index)
            .bind(ti.state.as_str())
            .bind(try_number)
            .bind(&ti.hostname)
            .bind(ti.queued_at)
            .bind(ti.started_at)
            .bind(ti.ended_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn task_instances_for_run(
        &self,
        dag_id: &str,
        run_id: &str,
    ) -> Result<Vec<TaskInstance>, StoreError> {
        let rows = sqlx::query(SELECT_RUN_TASK_INSTANCES)
            .bind(dag_id)
            .bind(run_id)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(task_instance_from_row).collect()
    }

    async fn apply_transitions(&self, transitions: &[TaskTransition]) -> Result<(), StoreError> {
        if transitions.is_empty() {
            return Ok(());
        }
        let cols = transition_columns(transitions);

        // State write and audit insert share a transaction: a task instance is
        // never updated without a matching audit row, and vice versa. The
        // UPDATE returns one row per instance it actually touched; if that is
        // fewer than we asked for, some transition targeted a task instance
        // that does not exist, so we abort (dropping `tx` rolls back) rather
        // than audit a change that never happened.
        let mut tx = self.pool.begin().await?;
        let applied = sqlx::query(APPLY_TRANSITIONS)
            .bind(&cols.dag_ids)
            .bind(&cols.task_ids)
            .bind(&cols.run_ids)
            .bind(&cols.map_indexes)
            .bind(&cols.states)
            .bind(&cols.ats)
            .fetch_all(&mut *tx)
            .await?;
        if applied.len() != transitions.len() {
            return Err(StoreError::TransitionGap {
                requested: transitions.len(),
                applied: applied.len(),
            });
        }
        sqlx::query(AUDIT_TRANSITIONS)
            .bind(&cols.dag_ids)
            .bind(&cols.task_ids)
            .bind(&cols.run_ids)
            .bind(&cols.map_indexes)
            .bind(&cols.states)
            .bind(&cols.ats)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn record_heartbeat(&self, hb: &SchedulerHeartbeat) -> Result<(), StoreError> {
        let result = sqlx::query(HEARTBEAT)
            .bind(hb.job_id)
            .bind(hb.at)
            .bind(&hb.hostname)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::NotFound {
                kind: "job",
                id: hb.job_id.to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn transition(task: &str, to: TaskState, secs: i64) -> TaskTransition {
        TaskTransition {
            dag_id: "etl".to_owned(),
            task_id: task.to_owned(),
            run_id: "run-1".to_owned(),
            map_index: -1,
            to,
            at: at(secs),
        }
    }

    #[test]
    fn columns_transpose_in_order() {
        let batch = vec![
            transition("extract", TaskState::Queued, 1),
            transition("load", TaskState::Success, 2),
        ];
        let cols = transition_columns(&batch);

        assert_eq!(cols.task_ids, vec!["extract", "load"]);
        assert_eq!(cols.states, vec!["queued", "success"]);
        assert_eq!(cols.ats, vec![at(1), at(2)]);
        assert_eq!(cols.map_indexes, vec![-1, -1]);
        // Every column has the same length — the arrays must line up by index
        // for UNNEST to reassemble the rows correctly.
        assert_eq!(cols.dag_ids.len(), batch.len());
        assert_eq!(cols.run_ids.len(), batch.len());
    }

    #[test]
    fn empty_batch_produces_empty_columns() {
        let cols = transition_columns(&[]);
        assert!(cols.task_ids.is_empty());
        assert!(cols.states.is_empty());
    }
}
