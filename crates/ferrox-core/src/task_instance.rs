use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::CoreError;
use crate::state::TaskState;

/// A single attempt to run one task in one DAG run. Mirrors `task_instance`.
///
/// State is mutated only through [`TaskInstance::transition_to`], which is the
/// reason the field is not given a public setter: every change has to pass the
/// state machine and carry the timestamp/try-number bookkeeping that goes with
/// it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInstance {
    pub task_id: String,
    pub dag_id: String,
    pub run_id: String,
    /// `-1` for an ordinary task; `>= 0` for one expanded by dynamic mapping.
    pub map_index: i32,
    pub state: TaskState,
    pub try_number: u32,
    pub hostname: Option<String>,
    pub queued_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
}

impl TaskInstance {
    /// A freshly eligible, unmapped task instance: the `None -> SCHEDULED`
    /// edge of the state machine, on its first attempt.
    pub fn new(
        task_id: impl Into<String>,
        dag_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            dag_id: dag_id.into(),
            run_id: run_id.into(),
            map_index: -1,
            state: TaskState::Scheduled,
            try_number: 1,
            hostname: None,
            queued_at: None,
            started_at: None,
            ended_at: None,
        }
    }

    /// Apply a state transition at time `at`, validating it against the state
    /// machine and updating the timestamps and try counter that the new state
    /// implies.
    ///
    /// `at` is supplied by the caller rather than read from the clock so the
    /// bookkeeping stays deterministic and testable; the scheduler passes the
    /// tick's timestamp.
    pub fn transition_to(&mut self, to: TaskState, at: DateTime<Utc>) -> Result<(), CoreError> {
        if !self.state.can_transition_to(to) {
            return Err(CoreError::InvalidTransition {
                from: self.state,
                to,
            });
        }

        match to {
            TaskState::Queued => self.queued_at = Some(at),
            TaskState::Running => self.started_at = Some(at),
            TaskState::Success | TaskState::Failed | TaskState::UpstreamFailed => {
                self.ended_at = Some(at)
            }
            TaskState::Scheduled => {
                // Re-entry from UP_FOR_RETRY: a new attempt starts clean.
                self.try_number += 1;
                self.hostname = None;
                self.queued_at = None;
                self.started_at = None;
                self.ended_at = None;
            }
            TaskState::UpForRetry => {}
        }

        self.state = to;
        Ok(())
    }

    /// Whether a `FAILED` instance still has retries left, given the task's
    /// configured ceiling. The companion timing check (retry delay elapsed)
    /// belongs to the scheduler, which owns the clock and the per-task delay.
    pub fn can_retry(&self, max_retries: u32) -> bool {
        self.state == TaskState::Failed && self.try_number <= max_retries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn any_task_state() -> impl Strategy<Value = TaskState> {
        prop_oneof![
            Just(TaskState::Scheduled),
            Just(TaskState::Queued),
            Just(TaskState::Running),
            Just(TaskState::Success),
            Just(TaskState::Failed),
            Just(TaskState::UpForRetry),
            Just(TaskState::UpstreamFailed),
        ]
    }

    proptest! {
        /// Whatever sequence of transitions is thrown at a task instance, the
        /// state machine's invariants hold: only legal edges are accepted,
        /// rejected ones leave the instance untouched, a terminal state is a
        /// dead end, and the try counter never goes backwards (§8.1).
        #[test]
        fn random_transition_sequences_preserve_invariants(
            targets in prop::collection::vec(any_task_state(), 0..32)
        ) {
            let mut ti = TaskInstance::new("t", "d", "r");
            for (i, to) in targets.into_iter().enumerate() {
                let from = ti.state;
                let prev_try = ti.try_number;
                let result = ti.transition_to(to, at(i as i64));

                if from.can_transition_to(to) {
                    prop_assert!(result.is_ok());
                    prop_assert_eq!(ti.state, to);
                    prop_assert!(ti.try_number >= prev_try);
                } else {
                    prop_assert!(result.is_err());
                    prop_assert_eq!(ti.state, from, "a rejected transition must not mutate state");
                    prop_assert_eq!(ti.try_number, prev_try);
                }

                if from.is_terminal() {
                    prop_assert_eq!(ti.state, from, "a terminal state must never be left");
                }
            }
        }
    }

    fn run_to_success(ti: &mut TaskInstance) {
        ti.transition_to(TaskState::Queued, at(1)).unwrap();
        ti.transition_to(TaskState::Running, at(2)).unwrap();
        ti.transition_to(TaskState::Success, at(3)).unwrap();
    }

    #[test]
    fn a_new_instance_is_scheduled_on_its_first_try() {
        let ti = TaskInstance::new("t", "d", "r");
        assert_eq!(ti.state, TaskState::Scheduled);
        assert_eq!(ti.try_number, 1);
        assert_eq!(ti.map_index, -1);
        assert!(ti.queued_at.is_none());
    }

    #[test]
    fn the_happy_path_stamps_each_timestamp_once() {
        let mut ti = TaskInstance::new("t", "d", "r");
        run_to_success(&mut ti);
        assert_eq!(ti.state, TaskState::Success);
        assert_eq!(ti.queued_at, Some(at(1)));
        assert_eq!(ti.started_at, Some(at(2)));
        assert_eq!(ti.ended_at, Some(at(3)));
    }

    #[test]
    fn an_illegal_transition_is_rejected_and_leaves_state_untouched() {
        let mut ti = TaskInstance::new("t", "d", "r");
        let err = ti
            .transition_to(TaskState::Success, at(1))
            .expect_err("scheduled cannot jump straight to success");
        assert_eq!(
            err,
            CoreError::InvalidTransition {
                from: TaskState::Scheduled,
                to: TaskState::Success,
            }
        );
        assert_eq!(ti.state, TaskState::Scheduled);
    }

    #[test]
    fn a_retry_increments_the_try_and_clears_the_previous_attempt() {
        let mut ti = TaskInstance::new("t", "d", "r");
        ti.transition_to(TaskState::Queued, at(1)).unwrap();
        ti.transition_to(TaskState::Running, at(2)).unwrap();
        ti.hostname = Some("worker-1".to_owned());
        ti.transition_to(TaskState::Failed, at(3)).unwrap();

        assert!(ti.can_retry(2));
        ti.transition_to(TaskState::UpForRetry, at(4)).unwrap();
        ti.transition_to(TaskState::Scheduled, at(5)).unwrap();

        assert_eq!(ti.state, TaskState::Scheduled);
        assert_eq!(ti.try_number, 2);
        assert_eq!(ti.hostname, None);
        assert_eq!(ti.started_at, None);
        assert_eq!(ti.ended_at, None);
    }

    #[test]
    fn retries_run_out() {
        let mut ti = TaskInstance::new("t", "d", "r");
        ti.transition_to(TaskState::Queued, at(1)).unwrap();
        ti.transition_to(TaskState::Running, at(2)).unwrap();
        ti.transition_to(TaskState::Failed, at(3)).unwrap();
        assert!(ti.can_retry(1));
        assert!(!ti.can_retry(0));
    }

    #[test]
    fn can_retry_only_applies_to_failed_instances() {
        let ti = TaskInstance::new("t", "d", "r");
        assert!(!ti.can_retry(5));
    }
}
