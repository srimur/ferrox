# 0004 — Build the REST API server on axum

Date: 2026-06-20
Status: Accepted

## Context

Ferrox does not replace Airflow's web UI; it must keep it working. The
strategy is to expose an API wire-compatible with Airflow's stable REST API
(v1) so the unchanged Airflow webserver and CLI can point at Ferrox instead of
Airflow's own scheduler API. That makes the HTTP layer a compatibility
surface, not a greenfield API: it has to match specific routes, payloads, and
the existing authentication backends (FAB auth / JWT), with `/health`
unauthenticated and everything else gated.

The server also lives inside the same process as the Tokio-based scheduler and
sqlx store, so it must integrate with that runtime without dragging in a
second async stack.

Options considered: `actix-web` (its own actor runtime, historically some
`unsafe` in internals), lower-level `hyper` directly (maximum control, but we
would hand-roll routing, extraction, and middleware), and `axum` (a routing
and extraction layer over `hyper`/`tower`, maintained by the Tokio team).

## Decision

Use axum for the API server. Model the Airflow v1 endpoints as typed handlers
with axum extractors, compose cross-cutting concerns (auth, tracing, request
limits) as `tower` middleware, and share the runtime with the scheduler.

MVP endpoint set: `GET/POST /dags`, `GET/POST /dagRuns`, `GET /taskInstances`,
`GET /health`, and `POST /dags/{dag_id}/dagRuns` for manual triggers.

## Consequences

Easier: first-class Tokio integration means the API shares one runtime, one
connection pool, and one tracing pipeline with the scheduler. Type-safe
extractors turn malformed requests into rejections at the boundary, and the
`tower` ecosystem supplies auth, timeouts, and observability middleware
without bespoke plumbing.

Harder: wire compatibility is dictated by Airflow's OpenAPI spec, not by what
is ergonomic in axum — response shapes and error envelopes must match exactly,
which we hold ourselves to via a generated test client against the official
spec. axum's frequent extractor/middleware API evolution is an upkeep cost.

Committed: the API is a compatibility layer over the scheduler and store, not a
home for business logic. Handlers translate HTTP to and from the core types
and delegate; scheduling and persistence decisions stay in their own crates.
