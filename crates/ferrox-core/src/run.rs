use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::CoreError;
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

    /// Move the run to `state`, validating it against the run state machine.
    /// A terminal run is immutable, and only the legal edges are accepted —
    /// re-deciding a finished run, or moving it backwards, is always a bug, so
    /// it surfaces as an error rather than a silently dropped mutation.
    pub fn transition_to(&mut self, state: DagRunState) -> Result<(), CoreError> {
        if !self.state.can_transition_to(state) {
            return Err(CoreError::InvalidRunTransition {
                from: self.state,
                to: state,
            });
        }
        self.state = state;
        Ok(())
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
    fn a_run_walks_its_state_machine() {
        let mut run = DagRun::new("r1", "etl", date(), RunType::Scheduled);
        run.transition_to(DagRunState::Running).unwrap();
        run.transition_to(DagRunState::Success).unwrap();
        assert_eq!(run.state, DagRunState::Success);
    }

    #[test]
    fn a_terminal_run_rejects_further_transitions() {
        let mut run = DagRun::new("r1", "etl", date(), RunType::Scheduled);
        run.transition_to(DagRunState::Running).unwrap();
        run.transition_to(DagRunState::Success).unwrap();
        let err = run.transition_to(DagRunState::Failed).unwrap_err();
        assert_eq!(
            err,
            CoreError::InvalidRunTransition {
                from: DagRunState::Success,
                to: DagRunState::Failed,
            }
        );
        assert_eq!(run.state, DagRunState::Success);
    }

    #[test]
    fn a_run_cannot_move_backwards() {
        let mut run = DagRun::new("r1", "etl", date(), RunType::Scheduled);
        run.transition_to(DagRunState::Running).unwrap();
        assert!(run.transition_to(DagRunState::Queued).is_err());
    }
}
