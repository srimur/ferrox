//! Ownership: turning Python DAG files into [`ferrox_core`]-shaped definitions.
//!
//! This crate owns the boundary between untrusted Python on disk and the
//! typed Rust core. It watches the DAG directory for filesystem events,
//! drives the PyO3 sub-interpreter that extracts graph topology from each
//! file, and maintains the in-memory and on-disk caches that keep steady
//! state at zero re-parses. Nothing downstream of this crate touches Python.
//!
//! Implementation lands in Phase 1; see `docs/DEVLOG.md`.
