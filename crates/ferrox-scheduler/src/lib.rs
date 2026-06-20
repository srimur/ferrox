//! Ownership: the scheduling decision loop — the brain of Ferrox.
//!
//! This crate owns the single coordinator task that, each tick, resolves
//! which DAG runs are due, walks each run's graph to decide which tasks are
//! ready, creates the corresponding task instances, and hands them to the
//! executor. It owns the in-memory run cache and the rule that its lock is
//! never held across an await. It does not parse Python, talk SQL directly,
//! or run tasks — it orchestrates the crates that do.
//!
//! Implementation lands in Phase 1; see `docs/DEVLOG.md`.
