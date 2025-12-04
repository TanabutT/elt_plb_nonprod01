
# ELT pipelines - function-first starter

This repository contains an ELT starter structure (MSSQL -> GCS -> BigQuery) intended for Google Cloud Composer (Airflow) later.

Key points:
- Function-only Python modules (no classes) — single responsibility, easy to read and test.
- Bronze / Silver / Gold zoning is the core architecture — `src/extract`, `src/load`, `src/transform`, `src/validation`.
- CI pipeline runs tests (see `.github/workflows/ci.yml`).

See `docs/README_ARCHITECTURE.md` for a short explanation of the layout.

