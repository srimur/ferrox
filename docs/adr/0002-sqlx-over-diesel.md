# 0002 — Use sqlx for the metadata store instead of Diesel

Date: 2026-06-20
Status: Accepted

## Context

Ferrox shares Airflow's existing Postgres/MySQL metadata database. The store
layer is on the hot path — task-state reads, heartbeat writes, DAG-run
inserts, batched state transitions — and one of Ferrox's headline goals is to
cut steady-state connections from 50–200 down to under 20 pooled, removing the
need for PgBouncer. The data access layer therefore has to be async-native,
hold a tight connection ceiling, and map cleanly onto a schema Ferrox does not
own (Airflow's), across multiple Airflow versions.

The two mainstream Rust options:

- **Diesel** — a synchronous ORM with its own schema DSL and migration
  system. It assumes it owns the schema and expresses queries through Rust
  types rather than SQL. Async support is bolted on and secondary.
- **sqlx** — not an ORM. Async-first on Tokio, queries written as SQL,
  optional compile-time verification of those queries against a live database,
  row mapping via `FromRow`. No schema ownership assumptions.

Ferrox must speak Airflow's exact schema, including hand-tuned multi-row batch
UPDATEs and per-version query variants. An ORM's abstraction works against us
here: we want the real SQL visible and reviewable, and we want async to reach
the target connection profile.

## Decision

Use sqlx with the Tokio runtime as the `MetadataStore` backend. Write queries
as explicit SQL and map rows with `FromRow`. Use sqlx's parameterized query
API exclusively — never string interpolation into SQL — which also satisfies
the security requirement against injection via crafted DAG/task IDs.

Connection pooling uses sqlx's built-in pool with a default ceiling of 20.

On query verification: sqlx's `query!` compile-time macros require a reachable
database at build time. Ferrox must build in CI and release environments that
have no Postgres, so the store uses the runtime-checked `query`/`query_as`
API by default. Compile-time-verified macros are reserved for hot-path
queries via sqlx's offline `.sqlx` prepared-statement cache, regenerated
deliberately rather than depended on for every build.

## Consequences

Easier: real SQL is visible and reviewable, batch UPDATEs and version-specific
query sets are straightforward, and async pooling gets us to the sub-20
connection target without an external pooler. No fight with an ORM over a
schema we don't control.

Harder: without the compile-time macros on every query we lose some of sqlx's
signature safety, shifting more correctness onto integration tests against a
real Postgres (via testcontainers). Maintaining the offline `.sqlx` cache for
hot-path macros is an explicit upkeep cost.

Committed: all database access goes through the `MetadataStore` trait, and all
queries are parameterized. These two rules are load-bearing for both
testability and the security posture.
