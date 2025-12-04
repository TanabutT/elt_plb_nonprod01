# Architecture & Folder Layout

This document summarizes the chosen folder layout for the ELT project and the reasons behind each top-level folder.

- `dags/` - Airflow DAG definitions (minimal wiring only)
- `src/` - All function-only production code, divided into `extract`, `load`, `transform`, `validation`, `utils`
- `config/` - Variable templates and connection examples
- `infra/` - IaC and deployment templates (placeholder)
- `scripts/` - Developer utility scripts
- `tests/` - Unit and integration tests

Keep code function-only as documented in the project style guide.
