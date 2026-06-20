use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::state::{DagRunState, RunType};

/// One scheduled (or manually triggered) execution of a DAG. Mirrors `dag_run`.
///
/// A run owns no task state itself — that lives in the [`crate::TaskInstance`]
/// rows keyed by its `run_id` — so the only thing it enforces is that once it
/// reaches a terminal state it stays there.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DagRun {
    pub run_id: String,
    pub dag_id: String,
    pub logical_date: DateTime<Utc>,
    pub state: DagRunState,
    pub run_type: RunType,
    pub conf: serde_json::Value,
}

impl DagRun {
    /// A run as the scheduler first creates it: queued, awaiting its tasks.
    pub fn new(
        run_id: impl Into<String>,
        dag_id: impl Into<String>,
        logical_date: DateTime<Utc>,
        run_type: RunType,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            dag_id: dag_id.into(),
            logical_date,
            state: DagRunState::Queued,
            run_type,
            conf: serde_json::Value::Null,
        }
    }

    /// Attach trigger configuration (the JSON body of a manual trigger),
    /// returning the run so it can be built in one expression.
    pub fn with_conf(mut self, conf: serde_json::Value) -> Self {
        self.conf = conf;
        self
    }

    /// Move the run to `state`. A terminal run is immutable: re-deciding a
    /// finished run's outcome is always a bug, so it is rejected rather than
    /// silently ignored.
    pub fn set_state(&mut self, state: DagRunState) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        self.state = state;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn a_new_run_is_queued_with_no_conf() {
        let run = DagRun::new("r1", "etl", date(), RunType::Scheduled);
        assert_eq!(run.state, DagRunState::Queued);
        assert_eq!(run.conf, serde_json::Value::Null);
    }

    #[test]
    fn conf_can_be_attached_fluently() {
        let run = DagRun::new("r1", "etl", date(), RunType::Manual)
            .with_conf(serde_json::json!({ "region": "eu" }));
        assert_eq!(run.conf["region"], "eu");
    }

    #[test]
    fn a_terminal_run_will_not_change_state() {
        let mut run = DagRun::new("r1", "etl", date(), RunType::Scheduled);
        assert!(run.set_state(DagRunState::Running));
        assert!(run.set_state(DagRunState::Success));
        assert!(!run.set_state(DagRunState::Failed));
        assert_eq!(run.state, DagRunState::Success);
    }
}
