use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::CoreError;
use crate::ids::TaskId;

/// How often a DAG produces runs. Built from the Python parse (§4.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Schedule {
    /// A cron expression, kept as text; the scheduler crate owns parsing it.
    Cron(String),
    /// A fixed wall-clock interval — Airflow's `timedelta` schedule.
    Interval(Duration),
    /// Triggered by the named datasets becoming available, not by the clock.
    Dataset(Vec<String>),
    /// Never scheduled automatically; runs are created by manual trigger only.
    Manual,
}

/// Defaults applied to every task in a DAG unless the task overrides them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultArgs {
    pub owner: String,
    pub retries: u32,
    pub retry_delay: Duration,
    pub depends_on_past: bool,
}

impl Default for DefaultArgs {
    fn default() -> Self {
        // Matches Airflow's own defaults so a DAG that sets nothing behaves
        // identically under Ferrox.
        Self {
            owner: "airflow".to_owned(),
            retries: 0,
            retry_delay: Duration::from_secs(300),
            depends_on_past: false,
        }
    }
}

/// When a task becomes eligible relative to the states of its upstreams.
/// Defaults to `AllSuccess`, Airflow's default trigger rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerRule {
    #[default]
    AllSuccess,
    AllFailed,
    AllDone,
    OneSuccess,
    OneFailed,
    NoneFailed,
}

/// One node of a DAG: a task and the policy needed to schedule and retry it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskDef {
    pub task_id: TaskId,
    /// The Airflow operator class name (e.g. `PythonOperator`). Ferrox does not
    /// execute operators, but it carries the name for routing and display.
    pub operator: String,
    pub retries: u32,
    pub retry_delay: Duration,
    pub trigger_rule: TriggerRule,
}

impl TaskDef {
    /// A task that inherits its retry policy from the DAG's [`DefaultArgs`] and
    /// uses the default trigger rule.
    pub fn new(
        task_id: impl Into<TaskId>,
        operator: impl Into<String>,
        args: &DefaultArgs,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            operator: operator.into(),
            retries: args.retries,
            retry_delay: args.retry_delay,
            trigger_rule: TriggerRule::AllSuccess,
        }
    }
}

/// A parsed DAG: its schedule plus the task graph. Built from a Python file by
/// `ferrox-parser` and consumed by `ferrox-scheduler` (§4.1).
///
/// Fields are public to match the parse output one-to-one, but a `DagDef` is
/// only sound once [`DagDef::validate`] has accepted it: edges must reference
/// real tasks and the graph must be acyclic. [`DagDef::build`] runs that check
/// for you.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DagDef {
    pub dag_id: String,
    pub schedule: Schedule,
    pub start_date: DateTime<Utc>,
    pub catchup: bool,
    pub tags: Vec<String>,
    pub tasks: HashMap<TaskId, TaskDef>,
    /// Directed edges as `(upstream, downstream)` pairs.
    pub edges: Vec<(TaskId, TaskId)>,
    pub default_args: DefaultArgs,
    pub concurrency: u32,
}

impl DagDef {
    /// Default per-DAG task concurrency, matching Airflow's `max_active_tasks`.
    pub const DEFAULT_CONCURRENCY: u32 = 16;

    /// Assemble a DAG from a task list and edge list, rejecting duplicate task
    /// ids and then running [`DagDef::validate`]. Optional fields take their
    /// defaults; set them on the returned value if the parse provides them.
    pub fn build(
        dag_id: impl Into<String>,
        schedule: Schedule,
        start_date: DateTime<Utc>,
        tasks: Vec<TaskDef>,
        edges: Vec<(TaskId, TaskId)>,
    ) -> Result<Self, CoreError> {
        let mut map = HashMap::with_capacity(tasks.len());
        for task in tasks {
            let id = task.task_id.clone();
            if map.insert(id.clone(), task).is_some() {
                return Err(CoreError::DuplicateTask(id));
            }
        }

        let dag = Self {
            dag_id: dag_id.into(),
            schedule,
            start_date,
            catchup: false,
            tags: Vec::new(),
            tasks: map,
            edges,
            default_args: DefaultArgs::default(),
            concurrency: Self::DEFAULT_CONCURRENCY,
        };
        dag.validate()?;
        Ok(dag)
    }

    /// Check the DAG's structural invariants: a non-empty id, every task keyed
    /// by its own id, every edge endpoint defined, and no dependency cycle.
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.dag_id.is_empty() {
            return Err(CoreError::EmptyDagId);
        }

        for (key, task) in &self.tasks {
            if key != &task.task_id {
                return Err(CoreError::MismatchedTaskKey(task.task_id.clone()));
            }
        }

        for (upstream, downstream) in &self.edges {
            for endpoint in [upstream, downstream] {
                if !self.tasks.contains_key(endpoint) {
                    return Err(CoreError::UnknownTask(endpoint.clone()));
                }
            }
        }

        self.assert_acyclic()
    }

    /// Tasks directly upstream of `task` (its dependencies).
    pub fn upstreams<'a>(&'a self, task: &'a TaskId) -> impl Iterator<Item = &'a TaskId> {
        self.edges
            .iter()
            .filter(move |(_, down)| down == task)
            .map(|(up, _)| up)
    }

    /// Tasks directly downstream of `task` (its dependents).
    pub fn downstreams<'a>(&'a self, task: &'a TaskId) -> impl Iterator<Item = &'a TaskId> {
        self.edges
            .iter()
            .filter(move |(up, _)| up == task)
            .map(|(_, down)| down)
    }

    /// Tasks with no upstream — the entry points the scheduler can start first.
    pub fn roots(&self) -> impl Iterator<Item = &TaskId> {
        let has_upstream: HashSet<&TaskId> = self.edges.iter().map(|(_, down)| down).collect();
        self.tasks
            .keys()
            .filter(move |id| !has_upstream.contains(id))
    }

    fn assert_acyclic(&self) -> Result<(), CoreError> {
        let mut adjacency: HashMap<&TaskId, Vec<&TaskId>> = HashMap::new();
        for (up, down) in &self.edges {
            adjacency.entry(up).or_default().push(down);
        }

        // Iterative DFS with a three-state mark (unseen / on-stack / done).
        // A node reached while still on the stack closes a cycle.
        let mut on_stack: HashSet<&TaskId> = HashSet::new();
        let mut done: HashSet<&TaskId> = HashSet::new();

        for root in self.tasks.keys() {
            if done.contains(root) {
                continue;
            }
            let mut stack = vec![(root, false)];
            while let Some((node, children_visited)) = stack.pop() {
                if children_visited {
                    on_stack.remove(node);
                    done.insert(node);
                    continue;
                }
                if done.contains(node) {
                    continue;
                }
                on_stack.insert(node);
                stack.push((node, true));
                for next in adjacency.get(node).into_iter().flatten() {
                    if on_stack.contains(next) {
                        return Err(CoreError::Cycle {
                            dag_id: self.dag_id.clone(),
                            task: (*next).clone(),
                        });
                    }
                    if !done.contains(next) {
                        stack.push((next, false));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch() -> DateTime<Utc> {
        DateTime::from_timestamp(0, 0).unwrap()
    }

    fn task(id: &str) -> TaskDef {
        TaskDef::new(id, "PythonOperator", &DefaultArgs::default())
    }

    fn edge(up: &str, down: &str) -> (TaskId, TaskId) {
        (TaskId::from(up), TaskId::from(down))
    }

    fn linear_dag() -> DagDef {
        DagDef::build(
            "etl",
            Schedule::Cron("0 * * * *".to_owned()),
            epoch(),
            vec![task("extract"), task("transform"), task("load")],
            vec![edge("extract", "transform"), edge("transform", "load")],
        )
        .expect("a linear pipeline is a valid DAG")
    }

    #[test]
    fn a_linear_pipeline_validates() {
        let dag = linear_dag();
        assert_eq!(dag.tasks.len(), 3);
        assert_eq!(dag.concurrency, DagDef::DEFAULT_CONCURRENCY);
    }

    #[test]
    fn duplicate_task_ids_are_rejected() {
        let err = DagDef::build(
            "dup",
            Schedule::Manual,
            epoch(),
            vec![task("a"), task("a")],
            vec![],
        )
        .expect_err("two tasks share an id");
        assert_eq!(err, CoreError::DuplicateTask(TaskId::from("a")));
    }

    #[test]
    fn an_edge_to_an_unknown_task_is_rejected() {
        let err = DagDef::build(
            "dangling",
            Schedule::Manual,
            epoch(),
            vec![task("a")],
            vec![edge("a", "ghost")],
        )
        .expect_err("ghost is not a defined task");
        assert_eq!(err, CoreError::UnknownTask(TaskId::from("ghost")));
    }

    #[test]
    fn an_empty_dag_id_is_rejected() {
        let err = DagDef::build("", Schedule::Manual, epoch(), vec![task("a")], vec![])
            .expect_err("empty dag id");
        assert_eq!(err, CoreError::EmptyDagId);
    }

    #[test]
    fn a_cycle_is_detected() {
        let err = DagDef::build(
            "loopy",
            Schedule::Manual,
            epoch(),
            vec![task("a"), task("b"), task("c")],
            vec![edge("a", "b"), edge("b", "c"), edge("c", "a")],
        )
        .expect_err("a -> b -> c -> a is a cycle");
        assert!(matches!(err, CoreError::Cycle { dag_id, .. } if dag_id == "loopy"));
    }

    #[test]
    fn a_self_loop_is_a_cycle() {
        let err = DagDef::build(
            "selfish",
            Schedule::Manual,
            epoch(),
            vec![task("a")],
            vec![edge("a", "a")],
        )
        .expect_err("a depends on itself");
        assert!(matches!(err, CoreError::Cycle { .. }));
    }

    #[test]
    fn a_diamond_is_acyclic() {
        let dag = DagDef::build(
            "diamond",
            Schedule::Manual,
            epoch(),
            vec![task("a"), task("b"), task("c"), task("d")],
            vec![
                edge("a", "b"),
                edge("a", "c"),
                edge("b", "d"),
                edge("c", "d"),
            ],
        );
        assert!(dag.is_ok());
    }

    #[test]
    fn upstreams_and_downstreams_read_the_edge_list() {
        let dag = linear_dag();
        let transform = TaskId::from("transform");
        let ups: Vec<_> = dag.upstreams(&transform).collect();
        let downs: Vec<_> = dag.downstreams(&transform).collect();
        assert_eq!(ups, vec![&TaskId::from("extract")]);
        assert_eq!(downs, vec![&TaskId::from("load")]);
    }

    #[test]
    fn roots_are_the_tasks_without_upstreams() {
        let dag = linear_dag();
        let roots: Vec<_> = dag.roots().collect();
        assert_eq!(roots, vec![&TaskId::from("extract")]);
    }

    #[test]
    fn a_dag_round_trips_through_json() {
        let dag = linear_dag();
        let json = serde_json::to_string(&dag).unwrap();
        let back: DagDef = serde_json::from_str(&json).unwrap();
        assert_eq!(dag, back);
    }
}
