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
- `ferrox-migrate`: schema compatibility validation (`validate_schema`,
  `CompatibilityReport`) checking a live Airflow database for every table and
  column the store layer needs, with integration tests against real Postgres.
- A working `ferrox validate --db <url>` command, wired to `ferrox-migrate`,
  exiting 0 when compatible and 1 otherwise.
- Property tests (`proptest`) over random task-state transition sequences,
  asserting the §8.1 state-machine invariants.
- `deny(clippy::unwrap_used, clippy::expect_used)` on non-test code in the
  library crates, enforcing the no-`unwrap`/`expect` standard mechanically.
- `DagRunState::can_transition_to` and adversarial DAG cycle tests
  (reconvergent diamond chains, parallel edges, disconnected components).

### Changed
- `DagRun::set_state(_) -> bool` is now `DagRun::transition_to(_) -> Result`,
  validated against a `DagRunState` machine — an illegal or backwards run
  transition is an error, not a silently dropped mutation.
- `ferrox-store::apply_transitions` now uses `UPDATE ... RETURNING` and audits
  only the task instances actually updated; a transition targeting a
  non-existent instance aborts the batch with `StoreError::TransitionGap`
  (rolling back, so no audit row leaks).
- DAG cycle detection reimplemented with Kahn's algorithm (clearer, and it
  de-duplicates parallel edges instead of miscounting them).

### Fixed
- Audit log could record a task-state transition that updated no row; audit
  rows now derive from the rows the `UPDATE` actually touched.
- `try_number` overflow on write was silently clamped to `i32::MAX`; it now
  surfaces as `StoreError::Corrupt`.

### Removed
- Unused `TaskState::is_failure` (will return when the scheduler's dependency
  evaluation needs it).
