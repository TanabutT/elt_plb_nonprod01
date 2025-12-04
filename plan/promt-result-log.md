promt:
be honest and as an expert data enginner,
create folder structure for this repo but ask me first before implement the plan for this is in ./plan/buildfolder-structure.md
plan for build folder structure only design it with best practice and suitable for detail of the task
note: I need python for airflow to be only function not the class for easy to read and understand keep it simple, each fuction store in each file should be one function per file or might be group of function in one file but not too many function in one file and not too many line of code in one function. I don't want to see any class in this repo.


result:
Goal / constraints
Project: ELT pipelines for MSSQL → GCS → BigQuery (bronze → silver → gold)
CI/CD: GitHub Actions that push artifacts/scripts to GCS buckets separated by zone
Schedule: daily runs, with incremental / CDC after first run
Important constraint: Python code must be function-based (no classes). Prefer one function per file or small grouping; functions should be short and readable. No classes anywhere.
Proposed top-level structure
root/

.github/
workflows/ # GitHub Actions CI/CD pipelines
dags/ # Airflow DAG wrappers (small wiring only)
src/ # All production Python functions (modular)
extract/
load/
transform/
validation/
utils/
config/ # config templates, env var examples, airflow variables
infra/ # IaC / Deployment assets (Terraform / Composer config)
scripts/ # Dev scripts and small CLIs (python or bash)
tests/
unit/
integration/
docs/ # Design docs + runbooks (keep buildfolder-structure.md)
data/ # Small sample data & fixtures for tests
ci/ # CI helper scripts used by workflows (optional)
logging/ # logging configs or example sinks
docker-compose.yaml
Dockerfile
requirements.txt
README.md
CHANGELOG.md
Detailed layout and rationales
1) dags (Airflow)
Purpose: Each file is a DAG (only minimal wiring here).
Keep DAG files minimal: define DAG, schedule, and create tasks that reference functions living in src/.
Example: dags/extract_bronze_<service>.py — defines DAG for one service, but all heavy-lifting logic imported from src/extract/*.
Rationale: Separates orchestration from logic and keeps DAGs readable for operators.
2) src/ (function-first code)
Purpose: Actual ETL function implementations. Everything here must be functions (no classes).
Subfolders:
extract/: connect to MSSQL, extract rows, convert to parquet, write to GCS. Files should represent a single responsibility, e.g. mssql_to_parquet.py with function extract_table_to_parquet(...).
load/: load parquet from GCS to BigQuery staging (one function per operation, e.g., gcs_to_bq_dataset.py).
transform/: transformation logic for silver/gold. Keep functions short and composable (e.g., normalize_customer_records.py exporting transform_*).
validation/: functions for row count and aggregate validations and writing audit logs to BQ or GCS.
utils/: small helper functions (GCS, BQ clients, incremental markers, retry helpers). Keep minimal; utility functions should be pure and reusable.
File-per-function guideline: Prefer one main function per file, or a couple of tiny helper functions if it improves grouping readability.
3) config/
Add airflow_vars.yaml, env.template, connections.template, gcs_buckets.yaml and a README.md to document required environment variables (e.g. GCP_PROJECT, GCS_BUCKET_BRONZE, BQ_DATASET_STAGING).
Rationale: Keeps secrets out and documents required runtime settings.
4) infra/
Terraform or Cloud Deployment Manager templates for Google Composer environment + GCS + BigQuery datasets + service accounts.
Keep example deployment code and README for how to deploy to GCP.
5) scripts/
Dev and maintenance scripts: local runner for DAGs, extract small-data sample, run linter, or run validations locally.
6) tests/
Use pytest; unit tests for each function file; integration tests that simulate a minimal end-to-end with sample data (local CI runs against emulator or dedicated test project).
Test layout mirrors src/ structure: tests/unit/extract/test_mssql_to_parquet.py.
7) docs/
Keep your existing plan files here: buildfolder-structure.md (this task) and promt-result-log.md. Add a README_ARCHITECTURE.md for quick reference and a DEV_GUIDE.md explaining "functions-only" conventions.
8) CI / GitHub Actions (.github/workflows)
Example workflows:
ci.yml → run linters, tests
deploy-to-gcs.yml → package python scripts/artifacts and push to GCS bronze/silver/gold buckets by path (e.g., gs://plb-bucket/bronze/<service>/<date>/artifact.zip)
Keep step to validate that code is function-only (optional lint plugin / custom check).
9) CDC / incremental strategy
Provide template functions and layout for storing last-run markers in a BQ audit table or GCS small metadata file. E.g., src/utils/increment_marker.py manages last-run timestamps for each table.
For large tables use timestamp-based incremental extract; store partitioning and incremental "watermark" metadata in audit_dataset.etl_state.
Short example (small code style demo)
src/extract/mssql_to_parquet.py
function: extract_table_to_parquet(connection_str, table_name, output_path, columns=None, where_clause=None) — do not use classes.
tests/unit/extract/test_mssql_to_parquet.py
tests for behavior with sample data and mocked connections.
Questions for you before I implement
Do you want placeholder files and directory tree created now, or do you prefer a full starter implementation for one pipeline (bronze extraction) first?
Any naming preferences for datasets or bucket names? (e.g., datasets: bronze_<service>, silver_<service>, gold_<mart>)
Which CI/CD behavior do you prefer initially?
(A) Minimal: run tests + push packaged scripts to GCS
(B) Full: also deploy infra (Terraform) or update Composer environment (I'd include deployment workflows if desired)
Confirm the "one function per file" is strict or flexible for tiny helper functions grouped with main one (recommended).




promt2:
approve
flexible for tiny helper functions grouped with main one (recommended)
placeholder files and directory tree create now
Minimal: run tests + push packaged scripts to GCS

then 
pipeline for bronze extraction later ask me again