# Implementation Plan: Customer Intelligence Platform

## Overview

This plan delivers the Customer Intelligence Platform in three progressive phases. Phase 1 produces a complete, locally runnable MVP — all services wired end-to-end via Docker Compose, all data flowing from synthetic generation through dbt marts to ML scores, API, and dashboards. Phase 2 hardens the MVP with performance tuning, quality gates enforcement, and load testing. Phase 3 polishes the project to portfolio-ready state with full documentation, property-based tests, and a final integration test sweep.

Each task is atomic, independently executable by a coding agent, and references the specific requirements and design decisions that govern it.

---

## Tasks

## Phase 1 — Local MVP

### 1. Infrastructure and Scaffolding

- [ ] 1. Scaffold project directory structure and root configuration files
  - Create the complete directory tree: `api/`, `dags/utils/`, `dbt/models/staging/`, `dbt/models/intermediate/`, `dbt/models/marts/`, `dbt/macros/`, `dbt/tests/`, `dbt/seeds/`, `generator/`, `great_expectations/`, `infra/`, `ml/features/`, `ml/models/`, `ml/scoring/`, `tests/generator/`, `tests/ml/`, `tests/api/`
  - Create `.gitignore` listing `.env`, `__pycache__/`, `*.pyc`, `mlflow/artifacts/`, `.dbt/`
  - Create `.env.example` with all required variables (see Security Design section): `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY`, `AIRFLOW_ADMIN_PASSWORD`, `DATABASE_URL`, `DB_POOL_MIN`, `DB_POOL_MAX`, `MLFLOW_BACKEND_STORE_URI`, `MLFLOW_TRACKING_URI`, `METABASE_DB_PASSWORD`, `MB_DB_USER`, `MB_DB_PASS`, `MB_DB_DBNAME` — each with a descriptive comment
  - Create `CHANGELOG.md` with an initial `## [0.1.0] - Unreleased` entry
  - _Requirements: 14.1, 14.3, NFR-4.5, NFR-6.1_


- [ ] 2. Write `docker-compose.yml` with all nine services and named volumes
  - Define all services from the design: `postgres` (postgres:15.6-alpine, mem_limit 2g, cpus 1.5), `airflow-webserver` (apache/airflow:2.7.3-python3.11, mem_limit 1g), `airflow-scheduler` (mem_limit 1.5g), `airflow-init` (one-shot), `mlflow` (ghcr.io/mlflow/mlflow:v2.9.2, mem_limit 512m), `fastapi` (custom build, mem_limit 512m), `metabase` (metabase/metabase:v0.48.3, mem_limit 1g), `data-generator` (custom build, mem_limit 1g)
  - Add a `cip_internal` bridge network with `internal: true` — all services use it; UI services bind to `127.0.0.1` only
  - Define named volumes: `postgres_data`, `airflow_logs`, `mlflow_artifacts`, `metabase_data`
  - Add health checks for every service using the exact commands from the design
  - Mount `./infra/init.sql` into postgres at `/docker-entrypoint-initdb.d/init.sql:ro`
  - Mount `./dags` into airflow services at `/opt/airflow/dags:ro`
  - All secrets read from `.env` via variable substitution — no hardcoded values
  - _Requirements: 14.1, 14.4, 14.5, 14.6, 14.8, NFR-5.1, NFR-6.5_

- [ ] 3. Write `infra/init.sql` — PostgreSQL schema and role initialization
  - Create all six data schemas with `CREATE SCHEMA IF NOT EXISTS`: `raw`, `staging`, `intermediate`, `marts`, `ml`, `observability`
  - Create all PostgreSQL roles with `CREATE ROLE IF NOT EXISTS`: `raw_writer`, `staging_writer`, `mart_writer`, `mart_reader`, `metabase_reader` — each with `LOGIN PASSWORD` from env vars
  - Apply the full permission matrix from the Security Design section: `REVOKE ALL ON SCHEMA raw FROM PUBLIC`, then grant per-role access as specified
  - Grant `ALTER DEFAULT PRIVILEGES` on each schema to ensure future tables inherit the correct permissions
  - Add idempotency guards: all `CREATE` statements use `IF NOT EXISTS`
  - _Requirements: 14.1, 14.2, NFR-6.3_

- [ ] 4. Write `Makefile` with all required targets
  - `make setup`: `docker compose build && docker compose pull && @test -f .env || (cp .env.example .env && echo 'Copied .env.example to .env — edit secrets before running')`
  - `make run`: `docker compose up -d`
  - `make stop`: `docker compose down`
  - `make test`: `docker compose run --rm airflow-scheduler bash -c "cd /opt/airflow && dbt test --profiles-dir dbt/" && pytest tests/`
  - `make docs`: `docker compose run --rm airflow-scheduler bash -c "dbt docs generate --profiles-dir dbt/ && dbt docs serve --profiles-dir dbt/"`
  - `make clean`: prompt user for confirmation, then `docker compose down -v` (destructive — removes volumes)
  - `make logs`: `docker compose logs -f`
  - `make lint`: `ruff check . && sqlfluff lint dbt/models/`
  - `make trigger`: trigger master_pipeline via Airflow REST API using curl + env vars
  - _Requirements: 14.1, NFR-4_


- [ ] 5. Write `api/Dockerfile` and `generator/Dockerfile`
  - `api/Dockerfile`: base `python:3.11-slim`, install `fastapi`, `uvicorn[standard]`, `asyncpg`, `pydantic>=2`, `python-dotenv`; copy `api/` source; expose port 8000; entrypoint `uvicorn main:app --host 0.0.0.0 --port 8000`
  - `generator/Dockerfile`: base `python:3.11-slim`, install `faker`, `psycopg2-binary`, `python-dotenv`; copy `generator/` source; entrypoint `python main.py`
  - Pin all dependency versions in both Dockerfiles
  - _Requirements: 14.1, NFR-6.1_

- [ ] 6. Write Airflow shared utilities module (`dags/utils/`)
  - `dags/utils/db.py`: parameterized query wrapper using `psycopg2`; raises on string-interpolated queries; provides `get_connection()` reading `DATABASE_URL` from env
  - `dags/utils/logging.py`: `log_task_start(dag_id, task_id, run_date, attempt)`, `log_rows_loaded(dag_id, task_id, run_date, rows, duration_seconds)`, `log_task_end(dag_id, task_id, run_date, status, duration_seconds)` — each emits structured JSON via Python `logging`
  - `dags/utils/dbt_runner.py`: `run_dbt(select, profiles_dir)` and `test_dbt(select, profiles_dir)` returning `BashOperator`-compatible command strings
  - `dags/utils/ge_runner.py`: `run_checkpoint(suite_name, datasource_name)` calling GE Python API; on failure, parses results and calls `write_dq_failure()`
  - `dags/utils/sla.py`: `sla_miss_callback(context)` writing a `sla_miss` record to `observability.pipeline_run_log`
  - `dags/utils/mlflow_utils.py`: `register_model(name, run_id, metrics)`, `promote_model(name, version)`, `get_production_model_metrics(name)` using `mlflow.tracking.MlflowClient`
  - _Requirements: 2.2, 2.3, 13.2, NFR-4.3_

- [ ] 7. Checkpoint — verify infrastructure compiles and services reach healthy state
  - Run `docker compose up -d postgres mlflow` and confirm both reach `healthy`
  - Verify `infra/init.sql` executes without errors by inspecting postgres logs
  - Verify all six schemas exist: `SELECT schema_name FROM information_schema.schemata`
  - Ensure all tests pass, ask the user if questions arise


### 2. Synthetic Data Generator

- [ ] 8. Implement synthetic data generator — customers, orders, and order items
  - `generator/main.py`: entry point; reads `SEED_CUSTOMERS`, `SEED_ORDERS` from env; orchestrates all domain generators; calls `assert_referential_integrity()` after all inserts
  - `generator/customers.py`: `generate_customers(n=100_000)` using Faker — produces `customer_id` (UUID4), `name`, `email` (unique), `acquisition_channel` (one of 5 values), `country_code` (ISO alpha-2), `account_created_at` (UTC date); writes to `raw.customers` via `ON CONFLICT (customer_id) DO UPDATE` upsert
  - `generator/orders.py`: `generate_orders(customer_ids, n=250_000)` — each order gets 1–10 `order_items` with `product_id`, `quantity > 0`, `unit_price_usd >= 0`; writes to `raw.orders` and `raw.order_items` inside a single transaction; upserts on `order_id` and `order_item_id`
  - All generation runs inside a transaction; on exception, roll back completely
  - _Requirements: 1.1, 1.3, 1.7, 1.8_

- [ ] 9. Implement synthetic data generator — events, campaigns, and tickets
  - `generator/events.py`: `generate_events(customer_ids, n_min=1_000_000, n_max=5_000_000)` — distributes events across all customers ensuring every customer gets ≥1 event; each event has `session_id`, `event_type`, `page_url`, `device_type`, `occurred_at`; upserts on `event_id`
  - `generator/campaigns.py`: `generate_campaigns(n_min=500, n_max=2_000)` — `platform` in `{google_ads, meta_ads}`; `clicks <= impressions` enforced at generation; `daily_spend_usd >= 0`; upserts on `(campaign_id, campaign_date)`
  - `generator/tickets.py`: `generate_tickets(customer_ids, n=50_000)` — `description` ≥ 10 words enforced; `resolved_at` populated only when `status = closed`; upserts on `ticket_id`
  - `generator/integrity.py`: `assert_referential_integrity(conn)` — runs three SQL COUNT queries checking orphan FKs in events, orders, tickets; raises `AssertionError` if any count > 0
  - _Requirements: 1.2, 1.4, 1.5, 1.6, 1.7, 1.8_

- [ ]* 10. Write unit tests for synthetic data generator
  - `tests/generator/test_customers.py`: assert exactly 100,000 customer rows after generation; assert email uniqueness
  - `tests/generator/test_referential_integrity.py`: assert zero orphan FKs across events, orders, tickets after full generation run
  - `tests/generator/test_campaigns.py`: assert `clicks <= impressions` for all campaign records
  - `tests/generator/test_idempotency.py`: run generator twice; assert row count is identical on second run (no duplicates inserted)
  - Use a test PostgreSQL database (separate from dev); clean up after each test
  - _Requirements: 1.1, 1.4, 1.6, 1.8_
  - _Correctness Properties: Property 1 (Referential Integrity), Property 2 (Campaign Clicks ≤ Impressions), Property 3 (Idempotent Data Generation)_


### 3. Airflow Ingestion DAGs

- [ ] 11. Implement Airflow ingestion DAGs for all five source domains
  - Create `dags/ingest_crm.py`, `dags/ingest_events.py`, `dags/ingest_orders.py`, `dags/ingest_campaigns.py`, `dags/ingest_tickets.py`
  - Each DAG: `schedule_interval="0 2 * * *"`, `catchup=False`, `retries=3`, `retry_delay=timedelta(minutes=5)`, `execution_timeout=timedelta(minutes=45)`
  - Task sequence per DAG: `log_start` (PythonOperator calling `log_task_start`) → `load_{domain}` (PythonOperator calling the corresponding generator upsert function) → `log_end` (PythonOperator calling `log_task_end` with row count)
  - `load_{domain}` task writes a `running` record to `observability.pipeline_run_log` at start, updates to `success` on completion
  - `on_failure_callback` on each DAG writes `failed` to `pipeline_run_log` and stops downstream tasks with `trigger_rule=ALL_SUCCESS`
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 13.1, 13.2_

- [ ] 12. Implement Great Expectations suites for all five raw tables
  - `great_expectations/great_expectations.yml`: configure `FileSystemDataContext`, PostgreSQL `SqlAlchemyExecutionEngine` datasource reading connection string from env
  - Create one expectation suite per table: `raw_customers_suite`, `raw_events_suite`, `raw_orders_suite`, `raw_campaigns_suite`, `raw_tickets_suite`
  - `raw_customers_suite`: `expect_column_values_to_not_be_null` (customer_id, name, email), `expect_column_values_to_be_unique` (customer_id, email), `expect_column_values_to_be_in_set` (acquisition_channel), `expect_column_value_lengths_to_equal` (country_code, 2), null rate ≥ 95% for all required fields
  - `raw_orders_suite`: `expect_column_values_to_be_in_set` (order_status), `expect_column_values_to_be_between` (total_amount_usd, min_value=0)
  - `raw_campaigns_suite`: `expect_column_pair_values_A_to_be_greater_than_or_equal_to_B` (impressions ≥ clicks), `expect_column_values_to_be_in_set` (platform)
  - `raw_tickets_suite`: `expect_column_values_to_be_in_set` (status, priority)
  - `raw_events_suite`: not-null on event_id, customer_id, occurred_at, event_type; null rate ≥ 95%
  - Each suite configured as a GE `Checkpoint`; `ge_runner.py` calls `context.run_checkpoint(checkpoint_name)` and parses `CheckpointResult` for failures
  - _Requirements: 4.2, 4.3, 4.6_


### 4. dbt Project — Staging Layer

- [ ] 13. Initialize dbt project and write core configuration files
  - `dbt/dbt_project.yml`: name `customer_intelligence`, version `1.0.0`, profile `cip`; set model materializations: `staging → view`, `intermediate → table`, `marts → table` (override incremental in model config)
  - `dbt/profiles.yml`: profile `cip` with target `dev`; reads all connection params from `env_var()` — `CIP_DB_HOST`, `CIP_DB_PORT`, `CIP_DB_USER`, `CIP_DB_PASSWORD`, `CIP_DB_NAME`; schema `public` (overridden per layer via `generate_schema_name` macro)
  - `dbt/packages.yml`: `dbt-utils>=1.1.0`, `dbt-expectations>=0.9.0`
  - `dbt/macros/generate_schema_name.sql`: custom macro that routes `stg_*` models to `staging`, `int_*` to `intermediate`, `mart_*` to `marts` schema
  - `dbt/macros/get_run_date.sql`: returns `current_date` cast to date (allows override via var for backfill)
  - `dbt/macros/audit_columns.sql`: adds `_run_date` using `{{ get_run_date() }}`
  - `dbt/seeds/acquisition_channels.csv`, `order_statuses.csv`, `country_codes.csv` — populated with the values referenced in requirements
  - _Requirements: 3.1, 3.8, NFR-4.1, NFR-4.2, NFR-5.2_

- [ ] 14. Implement dbt staging models for CRM, Events, and Orders
  - `dbt/models/staging/crm/stg_crm__customers.sql`: select from `raw.customers`; rename `_ingested_at → ingested_at`; cast `account_created_at` to DATE; deduplicate using `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC) = 1`; materialize as `view`
  - `dbt/models/staging/events/stg_events__events.sql`: select from `raw.events`; deduplicate on `event_id` by latest `_ingested_at`; cast `occurred_at` to TIMESTAMPTZ
  - `dbt/models/staging/orders/stg_orders__orders.sql`: deduplicate on `order_id`; cast `total_amount_usd` to NUMERIC(12,2); filter rows with NULL `customer_id`
  - `dbt/models/staging/orders/stg_orders__order_items.sql`: select from `raw.order_items`; cast types; no deduplication (PK is unique at source)
  - Source YAML files for each sub-directory: `_stg_crm__sources.yml`, `_stg_events__sources.yml`, `_stg_orders__sources.yml` with `loaded_at_field: _ingested_at` and source freshness config
  - _Requirements: 3.1, 3.2_

- [ ] 15. Implement dbt staging models for Campaigns and Support Tickets
  - `dbt/models/staging/campaigns/stg_campaigns__campaigns.sql`: select from `raw.campaigns`; deduplicate on `(campaign_id, campaign_date)` by latest `_ingested_at`; compute `click_through_rate = clicks::float / NULLIF(impressions, 0)`
  - `dbt/models/staging/support/stg_support__tickets.sql`: select from `raw.tickets`; deduplicate on `ticket_id`; compute `resolution_hours = EXTRACT(EPOCH FROM (resolved_at - created_at)) / 3600.0` (NULL when `resolved_at IS NULL`)
  - Source YAMLs: `_stg_campaigns__sources.yml`, `_stg_support__sources.yml`
  - Schema YAML tests for all five staging models: `not_null` on PKs, `unique` on PKs, `accepted_values` on all enum columns, `relationships` FK tests referencing `stg_crm__customers` for `customer_id`
  - Non-empty `description` on every model and every column in the schema YAML
  - _Requirements: 3.1, 3.2, 3.8, 4.1_

- [ ]* 16. Write property test for staging deduplication invariant
  - **Property 4: Staging Deduplication Preserves Exactly One Row Per Primary Key**
  - Generate a staging model input with intentionally duplicated PKs (same `customer_id` with different `_ingested_at` timestamps); run `stg_crm__customers`; assert zero duplicate PKs in output; assert the surviving row has the latest `_ingested_at`
  - Use `hypothesis` with strategies generating lists of (customer_id, _ingested_at) pairs including duplicates; assert `COUNT(*) = COUNT(DISTINCT customer_id)` after model execution
  - **Validates: Requirements 3.1**


### 5. dbt Project — Intermediate and Mart Layers

- [ ] 17. Implement dbt intermediate models
  - `dbt/models/intermediate/int_sessions__with_duration.sql`: group `stg_events__events` by `session_id`; compute `session_start = MIN(occurred_at)`, `session_end = MAX(occurred_at)`, `session_duration_seconds = EXTRACT(EPOCH FROM session_end - session_start)`; one row per session; materialize as `table`
  - `dbt/models/intermediate/int_orders__with_items.sql`: join `stg_orders__orders` to `stg_orders__order_items` on `order_id`; compute `item_count = COUNT(order_item_id)`, `avg_item_value_usd = total_amount_usd / NULLIF(item_count, 0)`; one row per order
  - `dbt/models/intermediate/int_customer_orders__aggregated.sql`: group `int_orders__with_items` by `customer_id`; compute `total_order_count`, `total_spend_usd`, trailing 365-day `order_frequency_365d`, `total_spend_365d_usd`, `order_count_last_30d`, `order_count_prior_30d`, `days_since_last_order` relative to `{{ get_run_date() }}`
  - `dbt/models/intermediate/int_customer_events__aggregated.sql`: group events by `customer_id`; compute `most_recent_event_date`, `days_since_last_event`
  - `dbt/models/intermediate/int_customer_tickets__aggregated.sql`: group tickets by `customer_id`; compute `open_ticket_count` (status IN ('open', 'in_progress'))
  - `dbt/models/intermediate/int_customers__enriched.sql`: join `stg_crm__customers` with computed `customer_tenure_days = {{ get_run_date() }} - account_created_at`
  - Add intermediate layer YAML with `not_null` tests on derived fields and `dbt_utils.expression_is_true` for bounds (e.g., `item_count >= 1`, `session_duration_seconds >= 0`)
  - _Requirements: 3.3_

- [ ] 18. Implement dbt mart models — `mart_customers`, `mart_orders`, `mart_campaigns`
  - `dbt/models/marts/mart_customers.sql`: join `int_customers__enriched` with `int_customer_orders__aggregated` and `int_customer_events__aggregated`; compute `is_active = (order_count in trailing 365d >= 1 OR event in trailing 365d exists)`; materialize as `table`
  - `dbt/models/marts/mart_orders.sql`: select from `int_orders__with_items`; expose `item_count`, `avg_item_value_usd`; materialize as `incremental` with `delete+insert` on `_run_date`
  - `dbt/models/marts/mart_campaigns.sql`: select from `stg_campaigns__campaigns`; expose `click_through_rate`; add `anomaly_flag BOOLEAN DEFAULT FALSE` (updated later by ML); materialize as `table`
  - `dbt/models/marts/_marts__models.yml` section for these three models: non-empty `description` on model and every column
  - _Requirements: 3.4, 3.7, 3.8_

- [ ] 19. Implement dbt mart models — `mart_support_tickets` and `mart_customer_360`
  - `dbt/models/marts/mart_support_tickets.sql`: select from `stg_support__tickets`; expose `resolution_hours`; add `cluster_id INTEGER NULL`, `cluster_label VARCHAR(200) NULL`, `cluster_confidence NUMERIC(5,4) NULL` (populated by NLP model); materialize as `table`
  - `dbt/models/marts/mart_customer_360.sql`: join all `int_customer_*__aggregated` models on `customer_id`; compute RFM quintile scores using `NTILE(5)` window functions over active customers; derive `rfm_score` string as `'R' || recency_score || 'F' || frequency_score || 'M' || monetary_score`; set `recency_days = LEAST(days_since_last_order, 999)`; add `active_campaign_count` subquery; set `rfm_score = 'R0F0M0'` and `is_active = FALSE` for customers with `order_frequency_365d = 0`; materialize as `incremental` with `delete+insert` on `_run_date`
  - Add YAML tests: `not_null` on all required fields, `unique` on `customer_id`, `accepted_values` on implicit enum fields
  - _Requirements: 3.4, 3.5, 3.7, 3.8, 5.1, 5.2, 5.5_

- [ ] 20. Implement dbt mart models — `mart_ml_scores` and observability table DDL
  - `dbt/models/marts/mart_ml_scores.sql`: incremental model reading from `ml.ml_scores`; strategy `delete+insert` on `_run_date`; expose all columns from `ml.ml_scores` plus a check that `churn_score BETWEEN 0.0 AND 1.0`; materialize as `incremental`
  - `infra/init.sql` additions (append): DDL for `observability.pipeline_run_log`, `observability.dq_failures`, `observability.ml_insights`, `ml.ml_scores`, `ml.anomaly_metrics` — all with exact column definitions, types, constraints, and indexes from the data models section
  - DDL for all mart tables as empty CREATE TABLE statements (dbt will manage the data; the empty tables ensure FK references resolve at init time)
  - Add complete YAML documentation for `mart_ml_scores` in `_marts__models.yml`
  - _Requirements: 3.4, 3.6, 3.7, 3.8, 6.5, 7.5_


- [ ] 21. Write dbt schema tests and singular custom tests
  - `dbt/tests/assert_no_orphan_fk_orders.sql`: assert `SELECT COUNT(*) = 0` from `stg_orders__orders` LEFT JOIN `stg_crm__customers` WHERE customer resolved to NULL
  - `dbt/tests/assert_no_orphan_fk_events.sql`: same pattern for events
  - `dbt/tests/assert_no_orphan_fk_tickets.sql`: same pattern for tickets
  - `dbt/tests/assert_mart_customer_360_row_count.sql`: query prior-day row count from `observability.pipeline_run_log`; if prior count exists, assert current count is within ±20%; if no prior count, insert a baseline warning log and return 0 rows (test passes)
  - Add `dbt_utils.recency` test on `_run_date` for mart layer models
  - Ensure all schema YAML tests reference `meta: {severity: warn}` for volume tests so pipeline does not halt
  - _Requirements: 4.1, 4.4, 4.5, 4.7_

- [ ]* 22. Write property test for RFM score invariants
  - **Property 6: RFM Score Invariants**
  - Use `hypothesis` with strategies generating random `mart_customer_360`-shaped DataFrames; assert `recency_score IN (0..5)`, `frequency_score IN (0..5)`, `monetary_score IN (0..5)`; assert `rfm_score` matches regex `R[0-5]F[0-5]M[0-5]`; assert `recency_days <= 999` for all rows
  - Also verify: customers with `order_frequency_365d = 0` always have `rfm_score = 'R0F0M0'`
  - **Validates: Requirements 3.5, 5.1, 5.2, 5.5**
  - _Correctness Properties: Property 6 (RFM Score Invariants), Property 7 (Inactive Customer Defaults)_

- [ ]* 23. Write property test for intermediate layer derived field consistency
  - **Property 5: Derived Intermediate Fields Are Mathematically Consistent**
  - For `int_orders__with_items`: use `hypothesis` to generate order + order_items pairs; verify `item_count = len(items)`, `avg_item_value_usd = total_amount_usd / item_count` (within floating-point tolerance), `item_count >= 1`
  - For `int_sessions__with_duration`: generate session event sets; verify `session_duration_seconds = (max_occurred_at - min_occurred_at).total_seconds()`, `session_duration_seconds >= 0`
  - **Validates: Requirements 3.3**

- [ ] 24. Checkpoint — verify dbt project compiles and staging/intermediate/mart tests pass
  - Run `dbt deps` to install packages; run `dbt compile` and assert no compilation errors
  - Run `dbt seed` to load reference seeds
  - Run `dbt run --select staging` then `dbt test --select staging`; assert all tests pass
  - Run `dbt run --select intermediate` then `dbt run --select marts`; assert all models materialize
  - Verify `mart_customer_360` contains exactly one row per customer with no NULL `rfm_score`
  - Ensure all tests pass, ask the user if questions arise


### 6. ML Scoring Pipeline

- [ ] 25. Implement ML feature engineering module
  - `ml/features/feature_store.py`: `load_customer_features(run_date, pg_conn_string) -> pd.DataFrame` — exports `mart_customer_360` to a DuckDB in-memory database as a Parquet-backed relation; returns a DataFrame with all feature columns
  - `ml/features/transformers.py`: `log_transform_monetary(df)` (log1p on `total_spend_365d_usd`, `total_spend_usd`); `onehot_encode(df, columns)` for `acquisition_channel` and `segment_label`; `clip_outliers(df, column, lower, upper)` for recency cap; all functions return a new DataFrame without modifying input
  - DuckDB usage is local in-process — no server required; import `duckdb` and use `duckdb.connect(':memory:')`
  - _Requirements: NFR-3.3, design ML Feature Engineering Strategy_

- [ ] 26. Implement Customer Segmentation model
  - `ml/models/segmentation.py`: `train_and_score(features_df, run_date, mlflow_tracking_uri) -> pd.DataFrame`
  - Training: `StandardScaler` on RFM quintile columns; evaluate `KMeans` for k ∈ {4,5,6,7,8}; select k by `silhouette_score`; if all silhouette scores differ by ≤ 0.01, default k=4 and log WARNING to Airflow task logger
  - Assign human-readable labels by ranking centroid composite RFM (`Champions`, `Loyal`, `At-Risk`, `Inactive`) — deterministic by centroid rank
  - Inactive customers (order_frequency_365d = 0): bypass clustering, assign `segment_label = "Inactive"`
  - Log to MLflow: `algorithm`, `k`, `random_state`, `silhouette_score`, `inertia`; register in registry as `customer_segmentation`; promote version with highest silhouette to `production`
  - Return DataFrame with `customer_id`, `segment_label` columns
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8_

- [ ]* 27. Write property test for segmentation model
  - **Property 7: Inactive Customers Receive the Correct Default Scores**
  - Use `hypothesis` to generate customer feature DataFrames with varying proportions of inactive customers (order_frequency_365d = 0); assert all inactive customers receive `segment_label = "Inactive"`; assert all active customers receive a non-Inactive label
  - Also assert total unique labels across any run is between 4 and 9 (k range + "Inactive")
  - **Validates: Requirements 5.5**

- [ ] 28. Implement Customer LTV Scoring model
  - `ml/models/ltv.py`: `train_and_score(features_df, run_date, mlflow_tracking_uri) -> pd.DataFrame`
  - Target variable: `total_spend_365d_usd` (proxy for 12-month forward LTV, scaled)
  - Training: 80/20 stratified split by `acquisition_channel`; `GradientBoostingRegressor`; `np.clip(predictions, 0, None)` to enforce minimum 0.00
  - Cold start — no orders, cohort exists: return mean LTV of that `acquisition_channel` cohort from training set
  - Cold start — no orders, no cohort: return 0.00
  - Log to MLflow: `algorithm`, `n_estimators`, `learning_rate`, `feature_list`, `train_date_range`, RMSE and MAE on val set
  - Registry: if new RMSE < current production RMSE → register + promote; else retain current + log comparison result
  - Return DataFrame with `customer_id`, `ltv_score` columns
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.6, 6.7_

- [ ]* 29. Write property test for LTV model output bounds
  - **Property 8 (partial): LTV Score Is Non-Negative**
  - Use `hypothesis` to generate customer feature DataFrames with varying spend histories including zeros and very large values; assert `ltv_score >= 0.00` for every row in model output
  - Also test cold-start: customers with no order history and a cohort present receive the cohort mean (not 0 or null); customers with no cohort receive exactly 0.00
  - **Validates: Requirements 6.2, 6.6, 6.7**


- [ ] 30. Implement Churn Risk Scoring model
  - `ml/models/churn.py`: `train_and_score(features_df, run_date, mlflow_tracking_uri) -> pd.DataFrame`
  - Features: `days_since_last_order`, `days_since_last_event`, `open_ticket_count`, `order_frequency_trend`, `segment_label` (one-hot)
  - Training: 80/20 stratified by churn label; `RandomForestClassifier`; scores from `predict_proba[:,1]`
  - Tier assignment: `churn_risk_tier = "Low"` if score < 0.33; `"Medium"` if 0.33 ≤ score < 0.67; `"High"` if score ≥ 0.67
  - Active customers only: filter input to rows where `is_active = True` before training and scoring
  - Log to MLflow: `algorithm`, AUC-ROC, precision, recall, feature_importances (JSON map)
  - Registry: AUC-ROC > 0.70 → register + promote with `production` tag; else retain current + log WARNING
  - Failure fallback: on any exception during scoring, log ERROR and return None (caller retains prior day values)
  - Return DataFrame with `customer_id`, `churn_score`, `churn_risk_tier` columns
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8_

- [ ]* 31. Write property tests for churn model score bounds and tier assignment
  - **Property 8: mart_ml_scores Value Bounds** and **Property 9: Churn Risk Tier Determined Exactly by Score Thresholds**
  - Use `hypothesis` with `st.floats(min_value=0.0, max_value=1.0)` to generate synthetic churn scores; assert tier assignment is exactly `Low` for score < 0.33, `Medium` for [0.33, 0.67), `High` for ≥ 0.67 — no gaps, no overlaps
  - Also generate complete `mart_ml_scores` rows and assert `churn_score IN [0.0, 1.0]`, `ltv_score >= 0.0`, `churn_risk_tier IN {"Low", "Medium", "High"}`
  - **Validates: Requirements 7.1, 7.3, 3.6, 6.2**
  - _Correctness Properties: Property 8 (Value Bounds), Property 9 (Tier Assignment)_

- [ ] 32. Implement Anomaly Detection model
  - `ml/models/anomaly.py`: `score(run_date, pg_conn_string) -> pd.DataFrame` — no ML library dependency; pure Python + pandas
  - Load last 30 days of metric values from `ml.anomaly_metrics` per metric name
  - Compute `rolling_mean_30d`, `rolling_std_30d` for each metric; compute `z_score = (observed - mean) / std`
  - Flag condition: `|z_score| >= 2.0` → `anomaly_flag = True`; severity: `Warning` if 2.0 ≤ |z| < 3.0, `Critical` if |z| ≥ 3.0
  - Baseline pending: < 30 days of data → `severity = "baseline_pending"`, `anomaly_flag = False`, no ERROR logged
  - Critical flag: emit Airflow task log at ERROR level with metric name, observed value, expected range, date
  - Write results to `ml.anomaly_metrics`
  - Six metrics evaluated: `total_daily_revenue_usd`, `total_daily_campaign_spend_usd`, `daily_order_count`, `daily_new_customer_count`, `daily_event_count`, `daily_ctr_per_campaign` — each computed from mart layer via SQL query
  - _Requirements: 8.1, 8.2, 8.3, 8.5, 8.6_

- [ ]* 33. Write property test for anomaly detection flag logic
  - **Property 10: Anomaly Flag Is Set Exactly When Z-Score Exceeds Threshold**
  - Use `hypothesis` with `st.lists(st.floats(...))` to generate 30-day metric windows; compute expected z-score; assert `anomaly_flag` is True iff `|z_score| >= 2.0`; assert `baseline_pending` when input length < 30
  - Assert severity boundaries: `Warning` exactly when 2.0 ≤ |z| < 3.0; `Critical` exactly when |z| ≥ 3.0
  - **Validates: Requirements 8.2, 8.5**
  - _Correctness Properties: Property 10 (Anomaly Flag Logic)_


- [ ] 34. Implement NLP Support Ticket Clustering model
  - `ml/models/nlp.py`: `train_and_score(tickets_df, run_date, mlflow_tracking_uri) -> pd.DataFrame`
  - Preprocessing: lowercase, remove punctuation (`re.sub`), NLTK stop word removal, NLTK `WordNetLemmatizer`
  - Vectorization: `TfidfVectorizer(max_features=5000)` on preprocessed text
  - Clustering: `KMeans` for k ∈ {5..15}; selection criterion: highest coherence score via `gensim.models.CoherenceModel`
  - Cluster label: top 5 TF-IDF terms per centroid, concatenated as ≤ 10-word string
  - Retraining trigger: if daily ticket volume > 500 OR no prior model artifact; else load prior MLflow artifact and score only
  - Confidence: `1 - (distance_to_centroid / max_distance_in_cluster)` normalized to [0.0, 1.0]
  - If coherence < 0.40: log WARNING to Airflow task log
  - Log to MLflow: `model_type`, `num_clusters`, `coherence_score`, `training_doc_count`, `vectorizer_params`, `k`
  - Return DataFrame with `ticket_id`, `cluster_id`, `cluster_label`, `cluster_confidence` columns
  - _Requirements: 9.1, 9.2, 9.3, 9.5, 9.6, 9.7, 9.8_

- [ ]* 35. Write property tests for NLP clustering invariants
  - **Property 11: NLP Cluster Count Is Within the Valid Range** and **Property 12: Every Processed Ticket Has a Valid Cluster Assignment**
  - Assert that after any NLP run, `SELECT COUNT(DISTINCT cluster_id) FROM mart_support_tickets WHERE cluster_id IS NOT NULL` returns an integer in [5, 15]
  - Use `hypothesis` with ticket DataFrames to assert `cluster_confidence BETWEEN 0.0 AND 1.0` for all assigned tickets; assert every ticket in input has exactly one (non-null) cluster_id in output
  - **Validates: Requirements 9.1, 9.3**
  - _Correctness Properties: Property 11 (Cluster Count Range), Property 12 (Ticket Cluster Assignment)_

- [ ] 36. Implement Insights Generator
  - `dags/insights_generator.py` (also importable as Python module): `generate_insights(run_date, pg_conn_string) -> dict`
  - Query exclusively from `marts.*` tables (no raw/staging queries)
  - Compute: top 3 segments by customer count (ties broken alphabetically), segment with highest avg `ltv_score`, count of `churn_risk_tier = "High"` customers, active anomaly flags summary, top 2 ticket clusters by volume
  - Return structured JSON dict with fields: `top_segments` (array), `highest_ltv_segment`, `high_churn_count`, `anomalies` (summary string or `"None detected"` if no active flags), `top_ticket_clusters` (array)
  - On missing mart table: raise exception, log table name, do NOT write partial output; prior day's record in `observability.ml_insights` is preserved
  - Write result to `observability.ml_insights` (upsert on `run_date`)
  - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6_

- [ ]* 37. Write property test for Insights Generator JSON contract
  - **Property 13: Daily Insight JSON Contains All Required Fields**
  - Use `hypothesis` to generate mock mart query results (varying segment counts, anomaly states, cluster distributions); call `generate_insights()` against a test database; assert output is valid JSON; assert `top_segments` has ≥ 3 entries; assert `anomalies` is never absent or null (either summary or `"None detected"`); assert `top_ticket_clusters` has ≥ 2 entries
  - Test the "no anomalies" case explicitly: assert `anomalies == "None detected"`
  - **Validates: Requirements 10.2, 10.4**
  - _Correctness Properties: Property 13 (Insight JSON Fields)_

- [ ] 38. Implement ML scoring orchestration and scores promotion
  - `ml/scoring/score_all.py`: `score_all(run_date)` — calls each model in sequence: `segmentation → ltv → churn → anomaly → nlp`; merges results on `customer_id`; writes merged result to `ml.ml_scores` via `ON CONFLICT (customer_id, score_date) DO UPDATE`
  - `ml/scoring/promote.py`: `promote_scores(run_date)` — `INSERT INTO marts.mart_ml_scores SELECT * FROM ml.ml_scores WHERE score_date = $1 ON CONFLICT DO NOTHING`; on success, log row count
  - Churn scoring failure fallback: if `churn.train_and_score()` raises, retain prior day's `churn_score` and `churn_risk_tier` from `mart_ml_scores` for all affected customers; log ERROR
  - Write `model_run_id` (MLflow run ID) alongside each score row for traceability
  - _Requirements: 6.5, 7.5, 7.8, 8.4_

- [ ] 39. Checkpoint — verify end-to-end ML pipeline run succeeds
  - Run `python ml/scoring/score_all.py --run-date $(date +%Y-%m-%d)` in a local venv with `DuckDB` available
  - Assert `ml.ml_scores` contains rows for all 100K customers
  - Assert `mart_ml_scores.ltv_score >= 0` for all rows; `churn_score BETWEEN 0 AND 1` for all rows
  - Assert at least 4 distinct `segment_label` values; at least 5 distinct `cluster_id` values in `mart_support_tickets`
  - Check MLflow UI at `localhost:5000` for experiment runs and registered models
  - Ensure all tests pass, ask the user if questions arise


### 7. Airflow DAGs — Transformation and Master Pipeline

- [ ] 40. Implement Airflow transformation DAGs
  - `dags/transform_staging.py`: BashOperator running `dbt run --select staging --profiles-dir /opt/airflow/dbt/`; on completion, BashOperator running `dbt test --select staging`; if test fails, write failure to `observability.dq_failures` via PythonOperator calling `ge_runner.py` dbt test parser
  - `dags/transform_intermediate.py`: BashOperator running `dbt run --select intermediate`; depends on `transform_staging` success via `ExternalTaskSensor`
  - `dags/transform_marts.py`: BashOperator running `dbt run --select marts`; then `dbt test --select marts` (volume tests); write `qg_tests_total/passed/failed` to `pipeline_run_log`
  - `dags/quality_gates.py`: PythonOperator calling `ge_runner.py` for all five GE checkpoints; if any checkpoint fails, use `AirflowException` to halt downstream; write GE failure to `dq_failures`
  - All transformation DAGs: `schedule_interval=None` (triggered only), `retries=2`, `retry_delay=timedelta(minutes=5)`, `execution_timeout=timedelta(minutes=30)`
  - _Requirements: 2.5, 4.1, 4.3, 4.7_

- [ ] 41. Implement Airflow ML scoring and insights DAGs
  - `dags/ml_scoring.py`: PythonOperator per model step — `ml_segmentation`, `ml_ltv` and `ml_churn` (parallel using `depends_on=[]`), `ml_anomaly`, `ml_nlp`, `ml_scores_promotion`; each task calls the corresponding `ml/models/*.py` function; on churn failure, PythonOperator calls fallback logic; log all MLflow run IDs to Airflow XCom
  - `dags/insights_generator.py` (DAG wrapper): PythonOperator calling `generate_insights()`; `trigger_rule=ALL_SUCCESS` ensures it only runs after all mart + ML scoring tasks succeed; on failure, log missing table name and retain prior day record
  - Each ML task writes start/end records to `pipeline_run_log` via `utils/logging.py`
  - SLA: `sla=timedelta(hours=2)` on the `ml_scoring` DAG (must complete by 05:30 UTC per design)
  - _Requirements: 10.5, 10.6, 7.8, 13.3_

- [ ] 42. Implement master pipeline DAG and SLA observability
  - `dags/master_pipeline.py`: `schedule_interval="0 2 * * *"`, `catchup=False`, `sla=timedelta(hours=4)`, `on_sla_miss=sla_miss_callback`
  - Task graph exactly as specified in design: `[start]` → 5 parallel `TriggerDagRunOperator` for ingestion DAGs → 5 `ExternalTaskSensor` (wait for all) → `ge_quality_gate` → `dbt_staging` → `dbt_schema_tests_staging` → `dbt_intermediate` → `dbt_marts` → `dbt_volume_tests_marts` → `ml_segmentation` → parallel `[ml_ltv, ml_churn]` → `ml_anomaly` → `ml_nlp` → `ml_scores_promotion` → `insights_generator` → `pipeline_run_log_success` → `[end]`
  - `dags/utils/sla.py`: `sla_miss_callback` writes a `pipeline_run_log` record with `status="sla_miss"`, elapsed duration, row counts, QG failure counts
  - `pipeline_run_log_success` task: PythonOperator writing final `success` record with `rows_ingested`, `rows_transformed`, `qg_tests_total/passed/failed`, `completed_at`, `duration_seconds`
  - Also implement `dags/dq_retention_cleanup.py`: `schedule_interval="0 1 * * *"`; DELETE from `dq_failures` where `run_date < CURRENT_DATE - INTERVAL '90 days'`
  - _Requirements: 13.3, 13.4, 4.5, 2.1_


- [ ] 43. Implement structured resource usage logging in master pipeline
  - Add `log_resource_usage_start` and `log_resource_usage_end` tasks to `master_pipeline.py` — each is a BashOperator running `docker stats --no-stream --format '{{json .}}'` for each CIP container
  - Parse JSON output in a PythonOperator; extract `MemUsage` (in MB) and `CPUPerc` (as float); write to current `pipeline_run_log` record fields `memory_usage_mb_start`, `cpu_pct_start` (start) and `memory_usage_mb_end`, `cpu_pct_end` (end)
  - `log_resource_usage_start` is the first task after `[start]`; `log_resource_usage_end` runs just before `pipeline_run_log_success`
  - _Requirements: 13.6, NFR-4_

### 8. FastAPI REST Layer

- [ ] 44. Implement FastAPI application skeleton and `GET /health` endpoint
  - `api/main.py`: create FastAPI app with title `"Customer Intelligence Platform API"`, version `"1.0.0"`; configure asyncpg connection pool (min=`DB_POOL_MIN`, max=`DB_POOL_MAX`) in `startup` event; release pool in `shutdown`; global exception handler converts all unhandled exceptions to HTTP 503 with sanitized message (no stack traces)
  - `api/routers/health.py`: `GET /health` — execute `SELECT MAX(run_date) FROM observability.pipeline_run_log WHERE status = 'success'`; return `{"status": "healthy", "db": "connected", "run_date": ...}`; on any DB exception return 503
  - `api/database.py`: `get_connection()` async context manager wrapping asyncpg pool; all queries must use `$1, $2, ...` parameterized syntax — never string interpolation
  - `api/models/`: Pydantic response models for each endpoint with `Field(description=...)` and example values
  - OpenAPI: each router decorated with `tags`, `summary`, `description`, `response_model`, and `responses={404: ..., 422: ..., 503: ...}`
  - _Requirements: 11.5, 11.7, 13.5, NFR-6.4_

- [ ] 45. Implement FastAPI customer endpoints and segment endpoint
  - `api/routers/customers.py`: `GET /customers/{customer_id}` — validate UUID format (regex or `uuid.UUID` parse), raise 422 if invalid; query `marts.mart_customers WHERE customer_id = $1`; return 404 if not found; `GET /customers/{customer_id}/scores` — query `marts.mart_ml_scores WHERE customer_id = $1 ORDER BY score_date DESC LIMIT 1`; return 404 if not found
  - `api/routers/segments.py`: `GET /segments` — validate `limit` (1–1000, default 100), `offset` (≥0, default 0); query aggregates `marts.mart_ml_scores` for most recent `score_date`, grouped by `segment_label`; apply `OFFSET $1 LIMIT $2`; return paginated envelope with `total`, `limit`, `offset`, `items`
  - UUID validation: wrap `uuid.UUID(customer_id, version=4)` in try/except; raise 422 with `{"detail": [{"field": "customer_id", "received": customer_id, "constraint": "must be a valid UUID (RFC 4122)"}]}`
  - _Requirements: 11.1, 11.2, 11.3, 11.6_

- [ ] 46. Implement FastAPI insights and anomalies endpoints
  - `api/routers/insights.py`: `GET /insights/latest` — query `observability.ml_insights WHERE run_date = (SELECT MAX(run_date) FROM observability.ml_insights)`; return `insight_json` field directly; if no rows, return 503 with `{"status": "unavailable", "detail": "..."}` 
  - `api/routers/anomalies.py`: `GET /anomalies` — validate `limit`, `offset`, optional `severity` (one of `Warning`, `Critical`), optional `run_date` (defaults to most recent in `ml.anomaly_metrics`); query `ml.anomaly_metrics WHERE anomaly_flag = TRUE AND run_date = $1` filtered by severity if provided; return paginated envelope; raise 422 for invalid severity values
  - _Requirements: 11.1, 11.3, 11.6, 11.7_

- [ ]* 47. Write FastAPI unit tests
  - `tests/api/test_health.py`: mock asyncpg pool; assert 200 when DB reachable with correct JSON shape; assert 503 when DB raises `asyncpg.ConnectionFailureError`
  - `tests/api/test_customers.py`: mock DB responses; assert 404 for unknown customer_id; assert 422 for malformed UUID (`"not-a-uuid"`, empty string, 37+ chars); assert 200 response matches Pydantic model shape
  - `tests/api/test_segments.py`: assert 422 when `limit > 1000`; assert correct pagination fields in response; assert 422 when `limit < 1`
  - `tests/api/test_anomalies.py`: assert severity filter works; assert default run_date resolves to most recent; assert 422 for invalid severity value
  - Use `pytest` with `httpx.AsyncClient` and `pytest-asyncio`; mock DB calls with `unittest.mock.AsyncMock`
  - _Requirements: 11.1, 11.2, 11.3, 11.6, 11.7_

- [ ]* 48. Write property test for API parameter validation
  - **Property 14: API Parameter Validation Blocks All Invalid Inputs**
  - Use `hypothesis` with `st.text()`, `st.integers()`, and `st.none()` to generate arbitrary path and query parameter values; assert any UUID path parameter that fails `uuid.UUID()` parse returns HTTP 422; assert any `limit > 1000` returns HTTP 422; assert no DB query is executed (verified via mock spy) when validation fails
  - **Validates: Requirements 11.3, 11.6**
  - _Correctness Properties: Property 14 (API Validation)_


### 9. Metabase Dashboard Configuration

- [ ] 49. Implement Metabase dashboard provisioning via API
  - `infra/metabase_setup.py`: Python script using `requests` to Metabase REST API (`http://metabase:3000/api/`); authenticates with admin credentials from env vars; creates the PostgreSQL database connection using `metabase_reader` credentials pointing to `marts` schema
  - Create all four dashboards programmatically: call `POST /api/dashboard` for each; add cards (questions) via `POST /api/card` then `POST /api/dashboard/{id}/cards`
  - Dashboard 1 — Customer Overview: `COUNT(*) WHERE is_active=TRUE` (scalar), acquisition by month (line chart), segment distribution (donut), top 10 by LTV (table)
  - Dashboard 2 — Churn Risk: customers by risk tier (bar chart), 30-day high-risk trend (line chart), high-risk customer table with filters for `churn_risk_tier` and `acquisition_channel`
  - Dashboard 3 — Campaign Performance: total daily spend (line chart), spend vs CTR scatter, anomaly flags conditional table with `anomaly_flag` conditional formatting (red TRUE, neutral FALSE)
  - Dashboard 4 — Support Intelligence: open ticket count (scalar), volume by cluster (bar), high-priority proportion per cluster (stacked bar), avg resolution time per cluster (bar)
  - Script is idempotent: check for existing database connection and dashboards before creating
  - Add a Docker Compose service `metabase-setup` (one-shot) that runs `infra/metabase_setup.py` after `metabase` is healthy
  - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6_

### 10. Observability and End-to-End Wiring

- [ ] 50. Wire DQ failure logging across ingestion, GE, and dbt layers
  - In `dags/utils/ge_runner.py`: after `context.run_checkpoint()`, parse `CheckpointResult`; for each failed expectation, call `write_dq_failure(run_date, failure_type="great_expectations", source_domain, table_name, checkpoint_name, failing_expectation)` which does an INSERT into `observability.dq_failures`
  - In `dags/transform_staging.py`: after `dbt test --select staging`, parse dbt test results JSON from `target/run_results.json`; for each failed test, call `write_dq_failure(run_date, failure_type="dbt_test", model_name, test_name, failing_column, sample_failing_rows)` — `sample_failing_rows` is a JSONB of the first 10 failing PKs
  - Write a helper `dags/utils/dq_writer.py` with `write_dq_failure(**kwargs)` that inserts into `observability.dq_failures` using parameterized queries
  - _Requirements: 4.6, 4.7_

- [ ] 51. Implement `infra/init.sh` idempotent initialization script
  - Shell script that checks for existing schemas (`SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'raw'`) and skips DDL if already present
  - Checks `observability.pipeline_run_log` for a successful run today; if found, skips DAG trigger
  - Runs `python infra/metabase_setup.py` if Metabase is reachable and dashboards do not yet exist
  - Called by `airflow-init` container as part of its startup command
  - _Requirements: 14.2, 14.7_

- [ ]* 52. Write property test for idempotent initialization
  - **Property 15: Idempotent Initialization Script**
  - Run `infra/init.sh` against a test database; capture schema structure and seed row counts; run script a second time; assert schema structure and row counts are identical; assert no new DAG runs were triggered
  - Use `hypothesis` to vary environment state (schemas present/absent, pipeline_run_log present/absent) and assert idempotency holds in all cases
  - **Validates: Requirements 14.7**
  - _Correctness Properties: Property 15 (Idempotent Init)_

- [ ]* 53. Write property test for idempotent data generation (generator-level)
  - **Property 3: Idempotent Data Generation**
  - Use `hypothesis` to generate a run_date and call the generator twice for that date against a test database; assert `SELECT COUNT(*) FROM raw.customers` returns the same value both times; same for all five raw tables
  - **Validates: Requirements 1.8, 14.7**
  - _Correctness Properties: Property 3 (Idempotent Generation)_


### 11. Documentation and One-Command Startup

- [ ] 54. Write `README.md` with architecture overview and quickstart guide
  - Architecture section: embed Mermaid diagram showing the seven layers (Source → Raw → Staging → Intermediate → Marts → ML → API/Dashboard); map each layer to its tooling
  - Quickstart section: numbered steps — prerequisites (Docker Engine >= 24.0, 8 GB RAM), `git clone`, `make setup`, `make run`, wait for services healthy (~5 min), open URLs (Airflow :8080, MLflow :5000, FastAPI :8000/docs, Metabase :3000)
  - Services section: table listing each service, image, external port, and purpose
  - Makefile commands: table listing each `make` target and what it does
  - Data model section: brief description of the six schemas and key mart tables
  - ML models section: one-paragraph description of each model with quality thresholds
  - _Requirements: 14.1, 14.3, NFR-4.5_

- [ ] 55. Write end-to-end startup validation test
  - `tests/integration/test_startup.py`: integration test that assumes all services are running (`docker compose up` has been executed externally); polls `GET /health` every 5 seconds until HTTP 200 is returned (max 30 attempts); asserts `run_date` field is today's date (pipeline has completed); calls all five other endpoints and asserts HTTP 200 responses with non-empty bodies; queries PostgreSQL directly and asserts `mart_customer_360` has exactly 100,000 rows; asserts `mart_ml_scores` has at least 1 row per active customer
  - This test serves as the single "all green" acceptance check for Phase 1 completion
  - _Requirements: 14.4, 11.1, NFR-1.5_

- [ ] 56. Final Phase 1 checkpoint — full pipeline run and endpoint verification
  - Execute `make run` from a clean state; wait for all services healthy
  - Trigger `master_pipeline` DAG via `make trigger`; monitor in Airflow UI until all tasks reach `success`
  - Run `pytest tests/integration/test_startup.py -v` and assert all assertions pass
  - Verify all four Metabase dashboards display data (open each in browser)
  - Run `dbt docs generate --profiles-dir dbt/` and assert no errors
  - Ensure all tests pass, ask the user if questions arise

---

## Phase 2 — Hardening and Quality

### 12. Performance Tuning and Quality Gates

- [ ] 57. Add dbt source freshness checks and incremental tuning
  - Add `freshness: warn_after: {count: 1, period: day}` to all five source definitions in staging YAML files
  - Add `dbt source freshness` BashOperator task to `dags/quality_gates.py`, running after GE checkpoints
  - Review and optimize `mart_orders` and `mart_customer_360` incremental models: ensure `unique_key` is set to `_run_date` for `delete+insert` strategy; add appropriate indexes to `_run_date` partition column in `infra/init.sql`
  - Add `dbt_utils.recency` test (`date_col: _run_date`, `datepart: day`, `interval: 1`) to all mart model YAML files
  - _Requirements: 3.7, NFR-3.3_

- [ ] 58. Implement dbt volume test for mart layer row count guard
  - `dbt/tests/assert_mart_row_count_within_20pct.sql`: generic singular test that takes `model_name` and `schema_name` as parameters; queries `observability.pipeline_run_log` for the prior `_run_date` row count; if prior count exists AND current count deviates by > 20%, returns rows (test fails as warn); if no prior count, logs baseline warning to `pipeline_run_log.notes` and returns 0 rows (test passes)
  - Add this test to `_marts__models.yml` for `mart_customer_360`, `mart_orders`, `mart_ml_scores` with `severity: warn`
  - _Requirements: 4.4_

- [ ]* 59. Add ML model quality gate enforcement in pipeline
  - Modify `dags/ml_scoring.py`: after `ml_churn` task completes, add a `validate_churn_quality` PythonOperator that reads AUC-ROC from MLflow; if AUC-ROC < 0.70, log WARNING and skip model registration (retain prior); if prior production model exists and new model fails gate, XCom-pass `use_prior=True` to downstream scoring task
  - Modify `ml/models/ltv.py`: after training, compare new RMSE to production model RMSE via `mlflow_utils.get_production_model_metrics`; register only if new RMSE is strictly lower
  - Add NLP coherence gate: if coherence < 0.40, log WARNING to Airflow task log but do not fail the DAG task (scoring continues with lower quality model)
  - _Requirements: 6.4, 7.5, 7.6, 9.8_


- [ ] 60. Write API load test
  - `tests/performance/test_api_load.py`: use `locust` or `pytest-benchmark` with `httpx` to send 50 concurrent requests to each endpoint for 60 seconds; capture p95 response time; assert p95 < 500ms for all endpoints
  - Test targets: `GET /health`, `GET /customers/{id}` (with valid UUID from mart_customers), `GET /customers/{id}/scores`, `GET /segments`, `GET /insights/latest`, `GET /anomalies`
  - Write results to `tests/performance/results_{timestamp}.json` for comparison across runs
  - _Requirements: 11.4, NFR-1.5_

- [ ]* 61. Write property-based tests for referential integrity
  - **Property 1: Referential Integrity of Generated Data** and **Property 2: Campaign Clicks Never Exceed Impressions**
  - Use `hypothesis` with `st.uuids()` and `st.integers()` to generate synthetic raw table rows; insert via generator upsert functions; run `assert_referential_integrity()`; assert it never raises for data generated by the official generators
  - Generate campaign records with arbitrary impressions/clicks values; assert `clicks <= impressions` invariant holds for all records output by `generate_campaigns()`
  - **Validates: Requirements 1.2, 1.4, 1.6**
  - _Correctness Properties: Property 1 (Referential Integrity), Property 2 (Campaign Clicks ≤ Impressions)_

- [ ] 62. Write end-to-end pipeline timing validation test
  - `tests/integration/test_pipeline_timing.py`: trigger `master_pipeline` DAG via Airflow REST API; poll `pipeline_run_log` every 60 seconds until `status = 'success'`; assert total `duration_seconds < 7200` (2 hours); assert `ml_scoring` DAG completed before SLA (check `completed_at < SLA deadline`)
  - _Requirements: NFR-1.1, NFR-1.2, NFR-1.3, NFR-1.4_

- [ ] 63. Checkpoint — all hardening tests pass
  - Run `pytest tests/performance/ -v` and confirm p95 < 500ms
  - Run `pytest tests/integration/test_pipeline_timing.py -v` and confirm pipeline completes within 2 hours
  - Run `dbt source freshness --profiles-dir dbt/` and confirm all sources are fresh
  - Ensure all tests pass, ask the user if questions arise

---

## Phase 3 — Production Polish

### 13. Documentation and Final Validation

- [ ] 64. Generate and verify complete dbt documentation site
  - Run `dbt docs generate --profiles-dir dbt/` inside the airflow-worker container; assert command exits 0
  - Verify coverage: parse `target/catalog.json`; assert every model in `staging/`, `intermediate/`, `marts/` has a non-empty `description`; assert every column in every mart model has a non-empty `description`; write a Python script `scripts/verify_dbt_docs_coverage.py` that reads `catalog.json` and prints missing descriptions, raising a non-zero exit code if any are found
  - Run `scripts/verify_dbt_docs_coverage.py` and assert exit 0
  - _Requirements: 3.8, NFR-4.2_

- [ ] 65. Polish `README.md` and add Mermaid architecture diagram
  - Add a `## Architecture` section with a Mermaid `graph TD` diagram showing: Docker Compose boundary, all nine services, data flow arrows between them, and the seven-layer logical stack
  - Add a `## Data Model` section with a brief table of mart models and their key columns
  - Add a `## Development Guide` section documenting the four developer iteration loops from the design (dbt model change, new Airflow DAG, ML model change, API endpoint change)
  - Populate `CHANGELOG.md` `## [0.1.0]` entry with a list of all implemented features grouped by component
  - _Requirements: 14.1, NFR-4.5_

- [ ] 66. Finalize Makefile and validate all targets
  - Run `make setup` from a clean environment; assert it completes without errors and `.env.example` is present
  - Run `make lint`; assert `ruff check .` and `sqlfluff lint dbt/models/` both exit 0 (fix any lint errors found)
  - Run `make test`; assert all pytest and dbt test suites pass
  - Run `make docs`; assert `dbt docs generate` exits 0
  - Verify `make clean` prompts for confirmation before removing volumes
  - _Requirements: 14.1, NFR-4_


- [ ]* 67. Write full property-based test suite for all 15 correctness properties
  - `tests/properties/test_all_properties.py`: consolidate or import all property tests from prior tasks (Properties 1–15); run the complete suite with `pytest -v tests/properties/` using `hypothesis` with `settings(max_examples=200)` for deterministic CI behavior
  - Ensure each property is tagged with its property number and requirement clause number in the test docstring
  - Properties covered:
    - Property 1: Referential Integrity (Requirements 1.2, 1.6)
    - Property 2: Campaign Clicks ≤ Impressions (Requirement 1.4)
    - Property 3: Idempotent Data Generation (Requirements 1.8, 14.7)
    - Property 4: Staging Deduplication (Requirement 3.1)
    - Property 5: Derived Field Consistency (Requirement 3.3)
    - Property 6: RFM Score Invariants (Requirements 3.5, 5.1, 5.2)
    - Property 7: Inactive Customer Defaults (Requirement 5.5)
    - Property 8: mart_ml_scores Value Bounds (Requirements 3.6, 6.2, 7.1)
    - Property 9: Churn Risk Tier by Thresholds (Requirement 7.3)
    - Property 10: Anomaly Flag Logic (Requirements 8.2, 8.5)
    - Property 11: NLP Cluster Count Range (Requirement 9.1)
    - Property 12: Ticket Cluster Assignment Completeness (Requirement 9.3)
    - Property 13: Insight JSON Fields (Requirements 10.2, 10.4)
    - Property 14: API Input Validation (Requirements 11.3, 11.6)
    - Property 15: Idempotent Initialization (Requirement 14.7)
  - _All Correctness Properties from design.md_

- [ ] 68. Final integration test — full pipeline run and all endpoints verified
  - `tests/integration/test_final_acceptance.py`: comprehensive test that:
    1. Confirms all eight Docker services report `healthy` (poll `docker compose ps --format json`)
    2. Triggers `master_pipeline` DAG and waits for `success` (polls Airflow API, max 2 hours)
    3. Asserts `GET /health` returns `{"status": "healthy"}` with today's `run_date`
    4. Asserts `GET /customers/{valid_id}` returns HTTP 200 with all required fields
    5. Asserts `GET /customers/{valid_id}/scores` returns HTTP 200 with `ltv_score >= 0`, `churn_score IN [0,1]`, `churn_risk_tier IN {"Low","Medium","High"}`
    6. Asserts `GET /segments` returns HTTP 200 with at least 4 segment items
    7. Asserts `GET /insights/latest` returns HTTP 200 with valid JSON matching the insight schema
    8. Asserts `GET /anomalies` returns HTTP 200 (anomaly_flag results may be empty on first run — assert shape, not content)
    9. Queries PostgreSQL directly: `mart_customer_360` has exactly 100,000 rows; `mart_ml_scores` has ≥ 1 row; `mart_support_tickets` has ≥ 1 cluster_id populated; `observability.ml_insights` has ≥ 1 row
    10. Asserts `dbt docs generate` exits 0
  - This test is the definitive "portfolio-ready" acceptance gate
  - _Requirements: 14.1, 14.4, NFR success metrics_

- [ ] 69. Final checkpoint — portfolio-ready state confirmed
  - Run `pytest tests/ -v --tb=short` and assert 0 failures (excluding optional `*` property tests if hypothesis is not configured)
  - Open Airflow UI at `localhost:8080`; confirm `master_pipeline` has a green run
  - Open MLflow at `localhost:5000`; confirm 4 registered models (`customer_segmentation`, `customer_ltv`, `customer_churn`, `support_nlp`) each with a `production` stage version
  - Open FastAPI at `localhost:8000/docs`; confirm all six endpoints are documented with schemas
  - Open Metabase at `localhost:3000`; confirm all four dashboards show data
  - Ensure all tests pass, ask the user if questions arise

---

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP delivery
- Property-based tests use `hypothesis` library; install via `pip install hypothesis`
- Each phase ends with a working, testable product — Phase 1 is the complete MVP; do not start Phase 2 until Phase 1's checkpoint passes
- All database queries must use parameterized patterns (`$1`, `$2`) — string interpolation with user input is forbidden
- dbt models rely on `{{ get_run_date() }}` macro for `_run_date` partition key — ensures idempotent re-runs
- ML models write to `ml.ml_scores` first; only the `ml_scores_promotion` task moves data to `marts.mart_ml_scores` — this prevents partial ML runs from corrupting mart data
- Tasks referencing Correctness Properties are implementing formal behavioral guarantees from the design document's Properties section


## Task Dependency Graph

Tasks are numbered sequentially (1–69) across all phases. The waves below map each sequential task to an execution wave. Tasks within the same wave are independent and can run in parallel. Sub-tasks marked with `*` (optional) are included but may be skipped.

```json
{
  "waves": [
    {
      "id": 0,
      "comment": "Infrastructure foundation — all independent scaffolding",
      "tasks": ["1", "2", "3", "4", "5"]
    },
    {
      "id": 1,
      "comment": "Airflow utilities (depends on scaffold); generator customers+orders (depends on init.sql schema)",
      "tasks": ["6", "8"]
    },
    {
      "id": 2,
      "comment": "Generator events+campaigns+tickets (depends on customers being writable); dbt project init (depends on scaffold)",
      "tasks": ["9", "13"]
    },
    {
      "id": 3,
      "comment": "Generator unit tests; GE suites (depends on raw tables existing); dbt staging CRM+Events+Orders",
      "tasks": ["10", "12", "14"]
    },
    {
      "id": 4,
      "comment": "Ingestion DAGs (depends on GE and utils); dbt staging Campaigns+Tickets (depends on dbt init)",
      "tasks": ["11", "15"]
    },
    {
      "id": 5,
      "comment": "dbt intermediate models (depends on staging); staging deduplication property test",
      "tasks": ["16", "17"]
    },
    {
      "id": 6,
      "comment": "dbt mart customers+orders+campaigns (depends on intermediate); observability DDL (depends on init.sql)",
      "tasks": ["18", "20"]
    },
    {
      "id": 7,
      "comment": "dbt mart support+360 (depends on intermediate complete); transformation DAGs (depends on utils+GE)",
      "tasks": ["19", "40"]
    },
    {
      "id": 8,
      "comment": "dbt schema tests (depends on mart models); ML feature engineering (depends on mart_customer_360)",
      "tasks": ["21", "25"]
    },
    {
      "id": 9,
      "comment": "dbt RFM property test; dbt intermediate property test; ML segmentation (depends on feature store)",
      "tasks": ["22", "23", "26"]
    },
    {
      "id": 10,
      "comment": "dbt checkpoint; ML LTV + Churn (can run in parallel, both depend on feature store)",
      "tasks": ["24", "28", "30"]
    },
    {
      "id": 11,
      "comment": "Segmentation property test; LTV property test; Churn property tests; Anomaly model",
      "tasks": ["27", "29", "31", "32"]
    },
    {
      "id": 12,
      "comment": "Anomaly property test; NLP model (depends on mart_support_tickets)",
      "tasks": ["33", "34"]
    },
    {
      "id": 13,
      "comment": "NLP property tests; Insights Generator (depends on all ML models)",
      "tasks": ["35", "36"]
    },
    {
      "id": 14,
      "comment": "Insights property test; ML scoring orchestration + promotion (depends on all models)",
      "tasks": ["37", "38"]
    },
    {
      "id": 15,
      "comment": "ML pipeline checkpoint; ML scoring DAG (depends on scoring orchestration); master pipeline DAG",
      "tasks": ["39", "41", "42"]
    },
    {
      "id": 16,
      "comment": "Resource usage logging (depends on master pipeline); FastAPI skeleton+health (independent of Airflow)",
      "tasks": ["43", "44"]
    },
    {
      "id": 17,
      "comment": "FastAPI customer+segment endpoints; DQ failure wiring (depends on transform DAGs)",
      "tasks": ["45", "50"]
    },
    {
      "id": 18,
      "comment": "FastAPI insights+anomalies endpoints (depends on ML pipeline); FastAPI unit tests; idempotent init script",
      "tasks": ["46", "47", "51"]
    },
    {
      "id": 19,
      "comment": "FastAPI validation property test; idempotency property tests; Metabase dashboards (depends on FastAPI+mart data)",
      "tasks": ["48", "52", "53", "49"]
    },
    {
      "id": 20,
      "comment": "README (depends on all components); startup validation test (depends on all services)",
      "tasks": ["54", "55"]
    },
    {
      "id": 21,
      "comment": "Phase 1 final checkpoint",
      "tasks": ["56"]
    },
    {
      "id": 22,
      "comment": "Phase 2 — source freshness + incremental tuning; volume test; API load test; pipeline timing test",
      "tasks": ["57", "58", "60", "62"]
    },
    {
      "id": 23,
      "comment": "Phase 2 — ML quality gates (depends on ML models); referential integrity property tests",
      "tasks": ["59", "61"]
    },
    {
      "id": 24,
      "comment": "Phase 2 hardening checkpoint",
      "tasks": ["63"]
    },
    {
      "id": 25,
      "comment": "Phase 3 — dbt docs verification; README polish; Makefile validation (independent)",
      "tasks": ["64", "65", "66"]
    },
    {
      "id": 26,
      "comment": "Phase 3 — complete property test suite (depends on all property tests written)",
      "tasks": ["67"]
    },
    {
      "id": 27,
      "comment": "Phase 3 — final integration acceptance test (depends on everything)",
      "tasks": ["68"]
    },
    {
      "id": 28,
      "comment": "Phase 3 final checkpoint",
      "tasks": ["69"]
    }
  ]
}
```

