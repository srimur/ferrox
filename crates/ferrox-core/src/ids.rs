use std::fmt;

use serde::{Deserialize, Serialize};

/// Identifier for a task within a DAG.
///
/// Task ids are graph keys — they index [`crate::DagDef::tasks`] and appear on
/// both sides of every edge — so they get a distinct type rather than a bare
/// `String`. Dag and run ids stay `String` because they are only ever values,
/// never keys, in this model.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(String);

impl TaskId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for TaskId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for TaskId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl AsRef<str> for TaskId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_serde_as_a_bare_string() {
        let id = TaskId::new("extract");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"extract\"");
        assert_eq!(serde_json::from_str::<TaskId>(&json).unwrap(), id);
    }

    #[test]
    fn equal_ids_hash_together() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TaskId::from("load"));
        assert!(set.contains(&TaskId::from("load")));
        assert!(!set.contains(&TaskId::from("transform")));
    }
}
