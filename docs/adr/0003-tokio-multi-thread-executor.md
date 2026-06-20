# 0003 — Run on a Tokio multi-threaded runtime with a single scheduler coordinator

Date: 2026-06-20
Status: Accepted

## Context

Airflow's scheduling loop is single-threaded Python bound by the GIL: all
dependency resolution, DB writes, and task queuing happen serially, capping
throughput at roughly 500–1000 task instances per minute. Ferrox's targets —
≤500ms p99 scheduling latency at 10,000 active task instances, with the
scheduling work fanned across all cores — require genuine parallelism.

Two tensions shape the runtime design:

1. **Parallelism vs. coordination correctness.** Naively running multiple
   independent scheduler instances against the same DAG runs invites
   double-scheduling and lost-update races on shared state.
2. **Concurrency vs. deadlock safety.** Heavy use of async with shared
   in-memory caches makes it easy to hold a lock across an `.await` and
   deadlock or stall the whole runtime under load.

## Decision

Use Tokio with the multi-threaded executor
(`Builder::new_multi_thread()`), defaulting the worker pool to the number of
logical CPUs.

Structure the scheduler as a **single coordinator task** that owns the tick
and orchestrates work by spawning sub-tasks, rather than as competing
scheduler instances. The coordinator guards its in-memory DAG/run cache with a
`tokio::Mutex<SchedulerState>` (and `Arc<RwLock<…>>` for the read-mostly
`DagDef` cache), with lock scope kept minimal.

Adopt one hard rule, enforced in review: **never hold a lock across an await.**
Read state into a local, drop the lock, perform async work, re-acquire only to
write back. Blocking DB calls go through `spawn_blocking`; in-memory graph
traversal stays directly async.

## Consequences

Easier: dependency resolution and DB writes for all eligible DAGs run
concurrently across cores, which is what unlocks the latency and throughput
targets without a GIL. A single coordinator makes the scheduling decision
sequence reasoning-friendly and free of inter-scheduler races.

Harder: the single coordinator is a throughput ceiling and a single point of
failure, so it must be supervised — wrapped to catch panics and restart with
backoff, recovering task state from the DB rather than from memory. The
no-lock-across-await rule is a standing discipline that constrains how every
async path touching shared state is written.

Committed: horizontal HA (multiple Ferrox instances) is deferred to Phase 3
and will be built on DB-based leader election layered on top of this
single-coordinator model — not by relaxing the single-coordinator invariant
within a process.
