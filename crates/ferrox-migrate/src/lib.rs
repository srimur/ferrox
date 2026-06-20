//! Ownership: pre-flight validation of an existing Airflow deployment.
//!
//! This crate owns the gate between "Airflow is running here" and "Ferrox may
//! start": it inspects a live metadata database and reports whether every
//! table and column the store layer relies on is present. It is deliberately
//! read-only — it diagnoses compatibility, it does not migrate or mutate the
//! schema. Translating an Airflow environment into a `ferrox.toml` is the other
//! half of this crate's remit and lands later in Phase 1.

// Errors flow through `MigrateError`; `unwrap`/`expect` are for tests only.
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod error;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

pub use error::MigrateError;

/// The tables and columns the store layer reads or writes, and therefore the
/// minimum an Airflow database must expose for Ferrox to run against it. Kept
/// in lockstep with `ferrox-store`'s SQL.
const REQUIRED_SCHEMA: &[(&str, &[&str])] = &[
    (
        "dag_run",
        &[
            "dag_id",
            "run_id",
            "logical_date",
            "state",
            "run_type",
            "conf",
        ],
    ),
    (
        "task_instance",
        &[
            "dag_id",
            "task_id",
            "run_id",
            "map_index",
            "state",
            "try_number",
            "hostname",
            "queued_dttm",
            "start_date",
            "end_date",
        ],
    ),
    ("job", &["id", "hostname", "latest_heartbeat", "state"]),
];

/// The outcome of a compatibility check: what, if anything, is missing.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CompatibilityReport {
    pub missing_tables: BTreeSet<String>,
    /// `(table, column)` pairs present in [`REQUIRED_SCHEMA`] but absent from a
    /// table that does exist.
    pub missing_columns: BTreeSet<(String, String)>,
}

impl CompatibilityReport {
    /// Whether the database satisfies everything the store layer needs.
    pub fn is_compatible(&self) -> bool {
        self.missing_tables.is_empty() && self.missing_columns.is_empty()
    }
}

impl fmt::Display for CompatibilityReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_compatible() {
            return write!(f, "compatible: all required tables and columns are present");
        }
        writeln!(f, "incompatible:")?;
        for table in &self.missing_tables {
            writeln!(f, "  missing table: {table}")?;
        }
        for (table, column) in &self.missing_columns {
            writeln!(f, "  missing column: {table}.{column}")?;
        }
        Ok(())
    }
}

/// Connect to `db_url` and report whether its schema can back Ferrox.
///
/// Read-only: it queries `information_schema` and touches no application data.
pub async fn validate_schema(db_url: &str) -> Result<CompatibilityReport, MigrateError> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        // A validation run is interactive; an unreachable host should fail in
        // seconds, not block on the 30s pool default.
        .acquire_timeout(Duration::from_secs(10))
        .connect(db_url)
        .await?;

    let table_names: Vec<String> = REQUIRED_SCHEMA
        .iter()
        .map(|(table, _)| (*table).to_owned())
        .collect();

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_name, column_name FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = ANY($1)",
    )
    .bind(&table_names)
    .fetch_all(&pool)
    .await?;

    let mut present: HashMap<String, HashSet<String>> = HashMap::new();
    for (table, column) in rows {
        present.entry(table).or_default().insert(column);
    }

    Ok(report_from_present(&present))
}

fn report_from_present(present: &HashMap<String, HashSet<String>>) -> CompatibilityReport {
    let mut report = CompatibilityReport::default();
    for (table, columns) in REQUIRED_SCHEMA {
        match present.get(*table) {
            None => {
                report.missing_tables.insert((*table).to_owned());
            }
            Some(have) => {
                for column in *columns {
                    if !have.contains(*column) {
                        report
                            .missing_columns
                            .insert(((*table).to_owned(), (*column).to_owned()));
                    }
                }
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn present(pairs: &[(&str, &[&str])]) -> HashMap<String, HashSet<String>> {
        pairs
            .iter()
            .map(|(table, cols)| {
                (
                    (*table).to_owned(),
                    cols.iter().map(|c| (*c).to_owned()).collect(),
                )
            })
            .collect()
    }

    fn full_schema() -> Vec<(&'static str, &'static [&'static str])> {
        REQUIRED_SCHEMA.to_vec()
    }

    #[test]
    fn a_complete_schema_is_compatible() {
        let have = present(&full_schema());
        let report = report_from_present(&have);
        assert!(report.is_compatible(), "{report}");
        assert_eq!(report, CompatibilityReport::default());
    }

    #[test]
    fn a_missing_table_is_reported() {
        let mut schema = full_schema();
        schema.retain(|(table, _)| *table != "job");
        let report = report_from_present(&present(&schema));
        assert!(!report.is_compatible());
        assert!(report.missing_tables.contains("job"));
        assert!(report.missing_columns.is_empty());
    }

    #[test]
    fn a_missing_column_is_reported_without_flagging_the_table() {
        let mut schema = full_schema();
        for (table, cols) in &mut schema {
            if *table == "task_instance" {
                *cols = &[
                    "dag_id",
                    "task_id",
                    "run_id",
                    "map_index",
                    "state",
                    "try_number",
                    "hostname",
                    "queued_dttm",
                    "start_date",
                    // end_date intentionally dropped
                ];
            }
        }
        let report = report_from_present(&present(&schema));
        assert!(!report.is_compatible());
        assert!(report.missing_tables.is_empty());
        assert!(report
            .missing_columns
            .contains(&("task_instance".to_owned(), "end_date".to_owned())));
    }

    #[test]
    fn display_summarizes_a_clean_report() {
        let report = CompatibilityReport::default();
        assert_eq!(
            report.to_string(),
            "compatible: all required tables and columns are present"
        );
    }
}
