# 0005 — Use bincode for the on-disk DagDef cache

Date: 2026-06-20
Status: Accepted

## Context

Eliminating the re-parse problem is Ferrox's core performance thesis. In-memory
caching of parsed `DagDef`s handles steady state, but a process restart would,
without persistence, force a full re-parse of every DAG file — re-paying the
expensive Python interpreter cost for a 500-DAG deployment at the worst
possible moment (startup, often during incident recovery).

So parsed `DagDef`s are also serialized to a local on-disk cache, keyed and
invalidated per file by mtime plus content hash. This cache is purely a
local-performance artifact: it is written and read only by the same Ferrox
binary on the same host, never shipped across a network or exposed to other
tools, and always reconstructable by re-parsing the source file.

The format question is what to serialize with. The two candidates were
`serde_json` — already used for the API and the Python parse payload — and
`bincode`, a compact binary serde format.

## Decision

Use `serde_json` where data crosses a boundary or must be human-readable (API
responses, the DAG parse payload from Python), and use **bincode** for the
on-disk `DagDef` cache.

Cache entries are validated on read against the source file's mtime and
content hash; a mismatch (or any deserialization failure) discards the entry
and triggers a fresh parse, so a stale or format-changed cache is never a
correctness risk — only a missed optimization.

## Consequences

Easier: bincode's compact binary encoding is faster to read and write and
smaller on disk than JSON for the same `DagDef` graphs, which directly serves
the goal of a restart re-parsing zero unchanged files. Reusing serde means the
same `DagDef` type serializes to both formats with no parallel schema.

Harder: bincode output is not human-inspectable, so debugging the cache means
a small dump tool rather than reading a file. bincode's format is tied to the
struct layout, so a `DagDef` shape change invalidates existing cache files —
acceptable precisely because the cache is disposable and host-local.

Committed: the on-disk cache is treated as disposable and never authoritative.
Source files remain the source of truth; the cache must always be safe to
delete, and the mtime+hash validation that guarantees this is non-negotiable.
