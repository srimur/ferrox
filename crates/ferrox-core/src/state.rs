use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::CoreError;

/// Lifecycle state of a [`crate::TaskInstance`].
///
/// The variants and their legal transitions are the state machine from the
/// design doc (§4.2). [`TaskState::can_transition_to`] is the single source of
/// truth for what mutations are permitted; everything else (timestamps,
/// retries) hangs off transitions that it has already approved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Scheduled,
    Queued,
    Running,
    Success,
    Failed,
    UpForRetry,
    UpstreamFailed,
}

impl TaskState {
    /// The wire/DB spelling, matching Airflow's `task_instance.state` values.
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Scheduled => "scheduled",
            TaskState::Queued => "queued",
            TaskState::Running => "running",
            TaskState::Success => "success",
            TaskState::Failed => "failed",
            TaskState::UpForRetry => "up_for_retry",
            TaskState::UpstreamFailed => "upstream_failed",
        }
    }

    /// Whether a transition `self -> to` is one the state machine allows.
    ///
    /// This encodes §4.2 verbatim; the creation edge (`None -> SCHEDULED`) is
    /// modelled by [`crate::TaskInstance::new`] producing a `Scheduled`
    /// instance, not by a transition into the machine.
    pub fn can_transition_to(self, to: TaskState) -> bool {
        use TaskState::*;
        matches!(
            (self, to),
            (Scheduled, Queued)
                | (Queued, Running)
                | (Running, Success)
                | (Running, Failed)
                | (Running, UpstreamFailed)
                | (Failed, UpForRetry)
                | (UpForRetry, Scheduled)
        )
    }

    /// A state with no outgoing transitions: the run of this task is over and
    /// will not change again.
    pub fn is_terminal(self) -> bool {
        matches!(self, TaskState::Success | TaskState::UpstreamFailed)
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TaskState {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "scheduled" => Ok(TaskState::Scheduled),
            "queued" => Ok(TaskState::Queued),
            "running" => Ok(TaskState::Running),
            "success" => Ok(TaskState::Success),
            "failed" => Ok(TaskState::Failed),
            "up_for_retry" => Ok(TaskState::UpForRetry),
            "upstream_failed" => Ok(TaskState::UpstreamFailed),
            other => Err(CoreError::UnknownState(other.to_owned())),
        }
    }
}

/// Lifecycle state of a [`crate::DagRun`], mirroring `dag_run.state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DagRunState {
    Queued,
    Running,
    Success,
    Failed,
}

impl DagRunState {
    pub fn as_str(self) -> &'static str {
        match self {
            DagRunState::Queued => "queued",
            DagRunState::Running => "running",
            DagRunState::Success => "success",
            DagRunState::Failed => "failed",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, DagRunState::Success | DagRunState::Failed)
    }

    /// Whether a run may move from `self` to `to`. A run is created `Queued`,
    /// starts `Running`, and ends `Success` or `Failed`; a `Queued` run can
    /// also fail outright (e.g. its DAG went missing). Terminal states are
    /// final.
    pub fn can_transition_to(self, to: DagRunState) -> bool {
        use DagRunState::*;
        matches!(
            (self, to),
            (Queued, Running) | (Queued, Failed) | (Running, Success) | (Running, Failed)
        )
    }
}

impl fmt::Display for DagRunState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DagRunState {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "queued" => Ok(DagRunState::Queued),
            "running" => Ok(DagRunState::Running),
            "success" => Ok(DagRunState::Success),
            "failed" => Ok(DagRunState::Failed),
            other => Err(CoreError::UnknownState(other.to_owned())),
        }
    }
}

/// Why a [`crate::DagRun`] exists, mirroring `dag_run.run_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunType {
    Scheduled,
    Manual,
    /// Airflow spells this `backfill` on the wire.
    #[serde(rename = "backfill")]
    BackfillJob,
}

impl RunType {
    pub fn as_str(self) -> &'static str {
        match self {
            RunType::Scheduled => "scheduled",
            RunType::Manual => "manual",
            RunType::BackfillJob => "backfill",
        }
    }
}

impl fmt::Display for RunType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RunType {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "scheduled" => Ok(RunType::Scheduled),
            "manual" => Ok(RunType::Manual),
            "backfill" => Ok(RunType::BackfillJob),
            other => Err(CoreError::UnknownState(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_TASK_STATES: [TaskState; 7] = [
        TaskState::Scheduled,
        TaskState::Queued,
        TaskState::Running,
        TaskState::Success,
        TaskState::Failed,
        TaskState::UpForRetry,
        TaskState::UpstreamFailed,
    ];

    #[test]
    fn the_happy_path_is_walkable() {
        assert!(TaskState::Scheduled.can_transition_to(TaskState::Queued));
        assert!(TaskState::Queued.can_transition_to(TaskState::Running));
        assert!(TaskState::Running.can_transition_to(TaskState::Success));
    }

    #[test]
    fn the_retry_cycle_closes() {
        assert!(TaskState::Running.can_transition_to(TaskState::Failed));
        assert!(TaskState::Failed.can_transition_to(TaskState::UpForRetry));
        assert!(TaskState::UpForRetry.can_transition_to(TaskState::Scheduled));
    }

    #[test]
    fn success_is_a_dead_end() {
        for to in ALL_TASK_STATES {
            assert!(!TaskState::Success.can_transition_to(to));
        }
        assert!(TaskState::Success.is_terminal());
    }

    #[test]
    fn a_successful_task_never_runs_again() {
        // Guards the §8.1 invariant called out by name in the design doc.
        assert!(!TaskState::Success.can_transition_to(TaskState::Running));
    }

    #[test]
    fn terminal_states_have_no_exits() {
        for state in ALL_TASK_STATES {
            if state.is_terminal() {
                assert!(ALL_TASK_STATES
                    .iter()
                    .all(|&to| !state.can_transition_to(to)));
            }
        }
    }

    #[test]
    fn every_state_round_trips_through_its_string() {
        for state in ALL_TASK_STATES {
            assert_eq!(TaskState::from_str(state.as_str()).unwrap(), state);
        }
        for state in [
            DagRunState::Queued,
            DagRunState::Running,
            DagRunState::Success,
            DagRunState::Failed,
        ] {
            assert_eq!(DagRunState::from_str(state.as_str()).unwrap(), state);
        }
        for ty in [RunType::Scheduled, RunType::Manual, RunType::BackfillJob] {
            assert_eq!(RunType::from_str(ty.as_str()).unwrap(), ty);
        }
    }

    #[test]
    fn unknown_strings_are_rejected() {
        assert!(matches!(
            TaskState::from_str("deferred"),
            Err(CoreError::UnknownState(s)) if s == "deferred"
        ));
    }

    #[test]
    fn backfill_uses_the_airflow_spelling_on_the_wire() {
        assert_eq!(
            serde_json::to_string(&RunType::BackfillJob).unwrap(),
            "\"backfill\""
        );
    }
}
