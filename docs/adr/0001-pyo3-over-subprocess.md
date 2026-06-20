# 0001 — Embed Python via PyO3 instead of spawning a subprocess per DAG file

Date: 2026-06-20
Status: Accepted

## Context

Airflow DAGs are Python programs. Their structure — tasks, dependencies,
schedules — exists only after the file is executed, not merely parsed as
text. Ferrox cannot avoid running Python to learn a DAG's topology; it can
only choose *how* it runs it.

Airflow itself forks a fresh Python subprocess per `.py` file and re-imports
it, including every top-level import (`pandas`, `tensorflow`, …). This is the
single largest cost in its scheduler: 30–120s parse cycles and 100% CPU
spikes. The fork has no shared memory and pays full interpreter and import
startup on every file, every cycle.

Two viable strategies exist for Ferrox:

1. **Subprocess per file** — the Airflow approach. Strong OS-level isolation,
   simple failure containment (kill the process), but high fork and
   cold-import overhead and no sharing of the initialized interpreter.
2. **Embedded interpreter (PyO3)** — initialize one Python interpreter in the
   Ferrox process and run each DAG file in its own sub-interpreter
   (`Py_NewInterpreter`), which isolates per-module global state while
   sharing the already-warm interpreter core.

The parse step does not execute tasks. A patched Airflow SDK intercepts
`@dag`/`@task` decorators and serializes the resulting graph, stubbing out
operator bodies. So the heavy scientific imports used *inside* task functions
need not be importable at parse time — only the much cheaper DAG-definition
code runs.

## Decision

Embed Python via PyO3 and parse each DAG file in a dedicated sub-interpreter,
on a dedicated OS thread (`spawn_blocking`) wrapped in a Rust-controlled
`tokio::time::timeout`. The interpreter is initialized once at startup. Parse
failures (import error, timeout, crash) are captured as a structured
`ParseError` and cached; the scheduler skips error-state DAGs and keeps going.

A subprocess pool is retained as a documented fallback if sub-interpreter
isolation proves insufficient in practice (Risk 10.1).

## Consequences

Easier: warm-interpreter reuse removes per-file fork and cold-start cost,
which is the main lever on the 45–120s → <3s parse-cycle target. Parsing
shares process memory with the cache, so a parsed `DagDef` is available to the
scheduler without IPC serialization.

Harder: PyO3 sub-interpreter isolation is weaker than process isolation and
historically fragile around C extensions that assume a single interpreter. We
take on responsibility for a test fixture that proves cross-file global-state
isolation, and we accept the GIL as a constraint on parse parallelism (sized
by `parse_threads`, not CPU count). Embedding Python also means the binary is
no longer trivially static and must manage interpreter lifetime and the GIL
correctly across thread boundaries.

Committed: Python is called *only* for parsing — never in the scheduling loop
or executor hot path. This boundary is what keeps the GIL out of the
performance-critical paths and must be preserved.
