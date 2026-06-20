# Compatibility tests

Two suites guard against drift from Airflow:

- **DAG parse compatibility** — a curated set of real-world open-source Airflow
  DAGs is parsed by Ferrox and the resulting `DagDef` is compared against
  Airflow's own parsed representation.
- **REST API compatibility** — the `ferrox-api` surface is exercised by a
  client generated from Airflow's official OpenAPI spec.

Populated as the parser and API crates land.
