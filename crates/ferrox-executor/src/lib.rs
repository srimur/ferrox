//! Ownership: dispatching ready task instances to a backend that runs them.
//!
//! This crate owns the `Executor` trait and its adapters — Local
//! (subprocess), Celery (AMQP/Redis envelope), and Kubernetes (pod spec
//! submission). Every adapter takes the same task-instance payload and
//! reports state transitions back through a result channel, so the
//! scheduler is agnostic to where a task actually runs.
//!
//! Implementation lands in Phase 1 (Local) and Phase 2 (Celery, K8s);
//! see `docs/DEVLOG.md`.
