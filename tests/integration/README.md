# Integration tests

Full scheduler-loop tests against an embedded Postgres (testcontainers-rs):
a DAG with known topology is driven through a mock executor and the resulting
task-state transition sequence is asserted against the expected order. Failure
injection (DB drops, parse timeouts, executor rejections) lives here too.

Populated as the scheduler and store crates land.
