# Changelog

All notable changes to Ferrox are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com), and the project adheres to
[Semantic Versioning](https://semver.org).

## [Unreleased]

### Added
- Cargo workspace scaffold: `ferrox` binary plus `ferrox-core`,
  `ferrox-store`, `ferrox-parser`, `ferrox-executor`, `ferrox-scheduler`,
  `ferrox-api`, and `ferrox-migrate` crates, with shared versions in
  `[workspace.dependencies]`.
- `ferrox-core`: the domain model — `DagDef`, `DagRun`, `TaskInstance`,
  `TaskId`, and the `Schedule`/`DagRunState`/`RunType`/`TaskState`/
  `TriggerRule` enums — plus the task-instance state machine
  (`TaskState::can_transition_to`, `TaskInstance::transition_to`), DAG
  validation (dangling-edge and cycle detection), and the `CoreError` type.
- `ferrox-store`: the `MetadataStore` trait and its `PgStore` (sqlx/Postgres)
  implementation, including a single-round-trip batched + audited task-state
  transition write, with `StoreError`, `TaskTransition`, and
  `SchedulerHeartbeat`.
- ADRs 0001–0005 recording the PyO3, sqlx, Tokio-runtime, axum, and bincode
  decisions.
- `ferrox-store` adds an audit table, `ferrox_ti_state_audit`, written in the
  same transaction as every task-state transition.
- Integration tests for `ferrox-store` against a real Postgres (gated
  `#[ignore]`, driven by `DATABASE_URL`), plus a CI `integration` job that runs
  them against a `postgres:16` service.
- CI workflow running `cargo fmt --check`, `cargo clippy -- -D warnings`,
  `cargo test --workspace`, and `cargo build --release`.
- Security-audit workflow running `cargo audit` on pushes to `main` and weekly.
- `ferrox` binary clap skeleton with `start`, `validate`, and `migrate`
  subcommands.

### Changed

### Fixed

### Removed
