//! Ownership: the HTTP surface Airflow tooling talks to.
//!
//! This crate owns the axum router and handlers that reproduce Airflow's
//! stable REST API (v1) closely enough that the unchanged Airflow webserver
//! and CLI can point at Ferrox instead of Airflow's own scheduler API. It
//! owns request/response shapes and auth-backend wiring; it owns no
//! scheduling or storage logic, delegating those to the crates below it.
//!
//! Implementation lands in Phase 2; see `docs/DEVLOG.md`.
