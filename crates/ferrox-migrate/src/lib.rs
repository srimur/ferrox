//! Ownership: pre-flight validation of an existing Airflow deployment.
//!
//! This crate owns the checks that run before Ferrox takes over a metadata
//! database: confirming the schema version is one Ferrox supports, that the
//! tables and columns the store layer reads actually exist, and that
//! Airflow's scheduler environment can be translated into a `ferrox.toml`.
//! It is the gate between "Airflow is running here" and "Ferrox may start."
//!
//! Implementation lands in Phase 1; see `docs/DEVLOG.md`.
