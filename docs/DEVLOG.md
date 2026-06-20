# Ferrox Development Log

Newest entries at the top.

## 2026-06-20 — Exercised the store against a real Postgres

The store's SQL had only ever been parsed, never run — the worst kind of
"done." Stood up Postgres 16 in Docker and wrote `crates/ferrox-store/tests/
postgres.rs`: it recreates a minimal slice of the Airflow schema (`dag_run`,
`task_instance`, `job`) plus our `ferrox_ti_state_audit`, then drives every
`MetadataStore` method end to end and asserts on what actually landed in the
tables.

This caught nothing — all four tests passed first run — which is the point: the
batched `UNNEST` UPDATE, the `CASE`-per-column timestamp logic, the audit
transaction, the `ON CONFLICT` upserts, the jsonb `conf` round-trip, and both
`NotFound` paths are now confirmed against a real engine, not assumed. The
empty-batch no-op and the upsert-doesn't-duplicate invariants are covered too.

The tests are `#[ignore]`d and read `DATABASE_URL`, so the no-database
`cargo test --workspace` stays green; a new CI `integration` job runs them
against a `postgres:16` service so they don't rot. Also ran the `ferrox` binary
as a user would (`--help`, `--version`, subcommand arg validation, the
not-yet-available exit) and confirmed `cargo build --release` (thin LTO) is
clean.

## 2026-06-20 — ferrox-core and ferrox-store land

Implemented `ferrox-core` in full. The Section 4 types map one-to-one to the
design doc, with two deliberate type choices: `TaskId` is a newtype because it
is a graph key (it indexes `DagDef.tasks` and sits on both ends of every edge),
while `dag_id`/`run_id` stay `String` because they are only ever values. The
task-instance state machine lives entirely in `TaskState::can_transition_to`,
which encodes §4.2 verbatim; `TaskInstance::transition_to` layers the
timestamp and try-number bookkeeping on top and refuses any edge the machine
rejects. `DagDef::validate` enforces structural soundness — non-empty id, every
task keyed by its own id, no dangling edges, and acyclicity via an iterative
three-colour DFS (chosen over recursion so a pathological DAG can't blow the
stack). 29 unit tests, including the named §8.1 invariant that a `Success` task
never transitions back to `Running`.

Implemented `ferrox-store`: the `MetadataStore` trait and a `PgStore` backed by
sqlx. The headline piece is `apply_transitions`, which writes a whole batch of
task-state changes as one multi-row `UPDATE` driven by `UNNEST` over parallel
arrays — the design doc's batch-write strategy — and writes the audit rows in
the same transaction so state and audit can never diverge. The array transpose
(`transition_columns`) is factored out as a pure function and unit-tested
without a database.

Two decisions worth recording. First, per ADR 0002, the store uses sqlx's
runtime query API, not the compile-time macros: CI and release builds have no
Postgres, and the macros need a live DB. Correctness shifts onto integration
tests against a real Postgres, which arrive with the scheduler. Second, the
transition audit goes to a new Ferrox-owned table, `ferrox_ti_state_audit`,
rather than overloading Airflow's `log` table — keeping our writes off a table
the webserver also touches. `ferrox-migrate` will own creating it.

The whole workspace is green: `cargo clippy -- -D warnings`, `cargo fmt
--check`, and `cargo test --workspace` (31 tests) all pass.

Open questions:
- The store's SQL assumes specific Airflow column names (`logical_date`,
  `queued_dttm`, the `job` table shape). The real 2.7→3.x column matrix is
  unpinned; `ferrox-migrate` needs to resolve `execution_date` vs
  `logical_date` and the `job` heartbeat schema per version.
- Integration tests need testcontainers + Docker, absent here; they land with
  the scheduler so the batched write path is exercised against a real Postgres.

## 2026-06-20 — Repository bootstrap and architecture baseline

Stood up the Cargo workspace described in the design doc: a thin `ferrox`
binary plus seven internal crates (`ferrox-core`, `ferrox-store`,
`ferrox-parser`, `ferrox-executor`, `ferrox-scheduler`, `ferrox-api`,
`ferrox-migrate`). Shared dependency versions live in `[workspace.dependencies]`
so the lockfile and crate manifests stay aligned.

Locked in the dependency direction as a one-way chain — core → store → parser
→ executor → scheduler → api → migrate — with `ferrox-core` having zero
internal dependencies. This is stricter than a general DAG: each crate may
depend only on crates earlier in the chain, which keeps every layer
independently testable and makes the parser/API layers approachable to
contributors without deep Rust experience (Risk 10.4).

Recorded the five foundational decisions as ADRs 0001–0005: PyO3 over a
subprocess-per-file parser, sqlx over Diesel, a Tokio multi-threaded runtime
with a single scheduler coordinator, axum for the API, and bincode for the
on-disk DagDef cache. Each reflects the actual forces in the design doc rather
than a restatement of the choice.

One decision that deviates from a literal reading of the doc: the doc sells
sqlx on "compile-time checked queries," but those macros need a live database
at build time, and CI/release builds have none. The store will use sqlx's
runtime-checked `query`/`query_as` API by default and reserve the compile-time
macros for hot-path queries backed by an offline `.sqlx` cache. Captured this
in ADR 0002 so the rationale isn't lost.

Five of the seven crates are currently compiling stubs — each carries only its
module-level ownership doc until its phase arrives. This is deliberate: the
no-dead-code rule means types and errors get written when first used, not
speculatively. The `ferrox` binary is a clap skeleton whose subcommands exit
with a clear "not available yet" rather than pretending to work.

Next: implement `ferrox-core` in full (Section 4 types, the task-instance
state machine, crate-local errors) and then `ferrox-store` (the `MetadataStore`
trait and its Postgres impl). Both must pass `cargo clippy -- -D warnings` and
`cargo test --workspace` before the next crate is touched.

Open questions:
- Sub-interpreter isolation (Risk 10.1) is unproven; the subprocess fallback
  may still be needed. Decide once the parser has a real isolation fixture.
- The exact Airflow schema version matrix (2.7–3.x) the store targets first is
  not yet pinned; `ferrox-migrate` will need a concrete list.
