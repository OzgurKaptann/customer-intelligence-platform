# Design Document

**Customer Intelligence Platform (CIP)**

---

## Overview

The Customer Intelligence Platform is a batch-first, streaming-ready data and ML system that unifies five source domains (CRM, Events, Orders, Campaigns, Tickets) into a governed, analytics-ready PostgreSQL warehouse. A layered dbt transformation pipeline produces clean marts consumed by four Metabase dashboards, a FastAPI REST layer, and an MLflow-tracked ML scoring pipeline. Apache Airflow orchestrates all daily batch runs. The entire platform runs on Docker Compose on a single local machine (8 GB RAM, 4 CPU cores minimum) and is designed for zero-code-change migration to managed cloud services.

### Design Principles

| Principle | Application |
|---|---|
| Separation of concerns | Each layer (Raw → Staging → Intermediate → Mart → ML → API) has a single, explicit contract |
| Idempotency everywhere | All ingestion, dbt, and ML runs are safe to re-execute for the same date |
| Fail-fast at boundaries | Quality gates block downstream propagation of bad data at each layer boundary |
| Observable by default | Every task emits structured logs; SLA misses are recorded to `pipeline_run_log` |
| Cloud-agnostic portability | Docker Compose services map 1:1 to managed cloud equivalents; no proprietary APIs |
| No PII | All data is synthetic; security model enforced through PostgreSQL role matrix |


---

## Architecture

### High-Level Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CUSTOMER INTELLIGENCE PLATFORM                        │
│                         Docker Compose Internal Network                       │
│                                                                               │
│  ┌─────────────┐    ┌──────────────────────────────────────────────────┐     │
│  │   Airflow   │    │              PostgreSQL (single instance)         │     │
│  │  Scheduler  │───▶│  ┌──────┐ ┌─────────┐ ┌──────┐ ┌────────────┐  │     │
│  │  Webserver  │    │  │ raw  │ │ staging │ │ int  │ │   marts    │  │     │
│  │   Worker    │    │  └──────┘ └─────────┘ └──────┘ └────────────┘  │     │
│  └──────┬──────┘    │  ┌──────┐ ┌───────────────────────────────────┐ │     │
│         │           │  │  ml  │ │       observability               │ │     │
│         │           │  └──────┘ └───────────────────────────────────┘ │     │
│  ┌──────▼──────┐    └──────────────────────────────────────────────────┘     │
│  │  Synthetic  │                        ▲                                     │
│  │    Data     │────────────────────────┘                                     │
│  │  Generator  │                        │                                     │
│  └─────────────┘    ┌──────────────────────────────────────────────────┐     │
│                     │                  dbt                              │     │
│  ┌─────────────┐    │  stg_* models → int_* models → mart_* models     │     │
│  │   MLflow    │    └──────────────────────────────────────────────────┘     │
│  │  Tracking   │                        │                                     │
│  │  Registry   │◀───────────────────────┤                                     │
│  └─────────────┘    ┌──────────────────▼──────────────────────────────┐     │
│                     │              ML Scoring Pipeline                  │     │
│  ┌─────────────┐    │  Segmentation │ LTV │ Churn │ Anomaly │ NLP      │     │
│  │   DuckDB    │◀───│  (feature engineering via DuckDB)                │     │
│  │  (local)    │    └──────────────────────────────────────────────────┘     │
│  └─────────────┘                       │                                     │
│                                        ▼                                     │
│  ┌─────────────┐    ┌──────────────────────────────────────────────────┐     │
│  │  Metabase   │◀───│              FastAPI REST Layer                   │     │
│  │  Dashboards │    │  /health │ /customers │ /segments │ /insights     │     │
│  └─────────────┘    └──────────────────────────────────────────────────┘     │
│                                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │         Great Expectations  ·  dbt tests  ·  dq_failures table        │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```


### Layered Architecture

```
Layer              Responsibility                         Storage        Tooling
───────────────────────────────────────────────────────────────────────────────
Source Systems     Synthetic data generation              In-memory      Python (Faker)
Raw Zone           Append-only landing zone               PostgreSQL/raw dbt sources
Staging Layer      Clean, cast, deduplicate               PostgreSQL/stg dbt (stg_*)
Intermediate Layer Join, enrich, derive                   PostgreSQL/int dbt (int_*)
Mart Layer         Business-facing analytics tables       PostgreSQL/mrt dbt (mart_*)
ML Layer           Scoring, segmentation, anomaly         PostgreSQL/ml  Python + MLflow
API Layer          REST endpoints for mart + ML output    HTTP           FastAPI
Dashboard Layer    Business user visualization            HTTP           Metabase
Observability      Quality gate logs, pipeline metadata   PostgreSQL/obs GE + dbt tests
```

### Data Flow Summary (Daily Batch)

```
02:00 UTC  ─▶  Ingestion DAGs (5 domains in parallel)
                    │
                    ▼ Raw Zone populated
               Quality Gate 1: Great Expectations checkpoints on raw tables
                    │ PASS
                    ▼
               Staging DAGs (dbt stg_* models)
                    │
                    ▼
               Quality Gate 2: dbt schema tests (PK, FK, not-null, accepted-values)
                    │ PASS
                    ▼
               Intermediate DAGs (dbt int_* models — joins, derivations)
                    │
                    ▼
               Mart DAGs (dbt mart_* models including mart_customer_360)
                    │
                    ▼
               Quality Gate 3: dbt row-count volume tests (±20% vs prior day)
                    │
                    ▼
               ML Scoring Pipeline (Segmentation → LTV → Churn → Anomaly → NLP)
                    │
                    ▼
               mart_ml_scores populated
                    │
                    ▼
               Insights Generator (daily JSON narrative)
                    │
                    ▼
               SLA check @ 06:00 UTC — all tasks must reach success
                    │
                    ▼
               Metabase scheduled refresh (within 5 min of mart update)
               FastAPI serves current-day scores from mart tables
```


---

## Components and Interfaces

### Service Inventory

| Service | Image | Internal Port | UI Port | Role |
|---|---|---|---|---|
| `postgres` | `postgres:15.6` | 5432 | — | Primary data store for all schemas |
| `airflow-webserver` | `apache/airflow:2.7.3` | 8080 | 8080 | Pipeline UI and REST API |
| `airflow-scheduler` | `apache/airflow:2.7.3` | — | — | DAG scheduling and triggering |
| `airflow-worker` | `apache/airflow:2.7.3` | — | — | Task execution (CeleryExecutor or LocalExecutor) |
| `airflow-init` | `apache/airflow:2.7.3` | — | — | One-shot DB init + user creation |
| `redis` | `redis:7.2-alpine` | 6379 | — | Airflow CeleryExecutor broker (LocalExecutor alternative) |
| `mlflow` | `ghcr.io/mlflow/mlflow:v2.9.2` | 5000 | 5000 | Experiment tracking and model registry |
| `fastapi` | Custom (python:3.11-slim) | 8000 | 8000 | REST API serving mart + ML outputs |
| `metabase` | `metabase/metabase:v0.48.3` | 3000 | 3000 | Business dashboards |
| `data-generator` | Custom (python:3.11-slim) | — | — | One-shot synthetic data seed |

### Inter-Service Interface Contracts

| Producer | Consumer | Protocol | Interface |
|---|---|---|---|
| `data-generator` | `postgres` | TCP/SQL | psycopg2 COPY/INSERT into `raw.*` tables |
| `airflow-scheduler` | `postgres` | TCP/SQL | Airflow metadata DB (separate `airflow` database) |
| `airflow-worker` | `postgres` | TCP/SQL | dbt runs write to `staging`, `intermediate`, `marts` |
| `airflow-worker` | `mlflow` | HTTP | MLflow Tracking API (log params, metrics, artifacts) |
| `airflow-worker` | `postgres` | TCP/SQL | ML models read `mart_customer_360`, write `ml.ml_scores` |
| `fastapi` | `postgres` | TCP/SQL | asyncpg connection pool, reads `marts.*` and `ml.*` |
| `metabase` | `postgres` | TCP/SQL | Read-only JDBC connection to `marts.*` schema only |
| `mlflow` | `postgres` | TCP/SQL | MLflow backend store (separate `mlflow` database) |

### Configuration Boundaries

All service configuration is defined in `docker-compose.yml`. All secrets and environment-specific values are in `.env` (gitignored). Application code reads configuration exclusively from environment variables — no hardcoded hosts, ports, or credentials.


---

## Data Models

### PostgreSQL Schema Layout

The CIP uses a single PostgreSQL instance with six logical schemas, each with a distinct role and access control profile.

```
postgres (instance)
├── airflow          — Airflow metadata (managed by Airflow)
├── mlflow           — MLflow backend store (managed by MLflow)
├── raw              — Raw Zone: append-only source data
├── staging          — dbt staging models (stg_*)
├── intermediate     — dbt intermediate models (int_*)
├── marts            — dbt mart models (mart_*)
├── ml               — ML scoring outputs and feature snapshots
└── observability    — pipeline_run_log, dq_failures, insights
```

---

### Raw Zone Tables (`raw` schema)

#### `raw.customers`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `customer_id` | `VARCHAR(36)` | PRIMARY KEY | UUID format |
| `name` | `VARCHAR(255)` | NOT NULL | Synthetic full name |
| `email` | `VARCHAR(255)` | NOT NULL, UNIQUE | Synthetic email |
| `acquisition_channel` | `VARCHAR(50)` | NOT NULL | `organic`, `paid_search`, `social`, `referral`, `direct` |
| `country_code` | `CHAR(2)` | NOT NULL | ISO 3166-1 alpha-2 |
| `account_created_at` | `DATE` | NOT NULL | UTC date |
| `_ingested_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT NOW() | Row arrival time |
| `_run_date` | `DATE` | NOT NULL | Pipeline run date partition |

Indexes: `PRIMARY KEY (customer_id)`, `INDEX ON (country_code)`, `INDEX ON (_run_date)`

#### `raw.events`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `event_id` | `VARCHAR(36)` | PRIMARY KEY | UUID |
| `session_id` | `VARCHAR(36)` | NOT NULL | Session grouping key |
| `customer_id` | `VARCHAR(36)` | NOT NULL | FK → raw.customers |
| `event_type` | `VARCHAR(100)` | NOT NULL | e.g., `page_view`, `click`, `session_start` |
| `page_url` | `TEXT` | NOT NULL | |
| `device_type` | `VARCHAR(50)` | NOT NULL | e.g., `desktop`, `mobile`, `tablet` |
| `occurred_at` | `TIMESTAMPTZ` | NOT NULL | UTC event timestamp |
| `_ingested_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT NOW() | |
| `_run_date` | `DATE` | NOT NULL | Partition key |

Indexes: `PRIMARY KEY (event_id)`, `INDEX ON (customer_id)`, `INDEX ON (session_id)`, `INDEX ON (occurred_at)`, `INDEX ON (_run_date)`

#### `raw.orders`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `order_id` | `VARCHAR(36)` | PRIMARY KEY | UUID |
| `customer_id` | `VARCHAR(36)` | NOT NULL | FK → raw.customers |
| `order_status` | `VARCHAR(20)` | NOT NULL | `completed`, `pending`, `cancelled`, `refunded` |
| `total_amount_usd` | `NUMERIC(12,2)` | NOT NULL | |
| `ordered_at` | `TIMESTAMPTZ` | NOT NULL | UTC |
| `_ingested_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT NOW() | |
| `_run_date` | `DATE` | NOT NULL | |

Indexes: `PRIMARY KEY (order_id)`, `INDEX ON (customer_id)`, `INDEX ON (ordered_at)`, `INDEX ON (_run_date)`

#### `raw.order_items`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `order_item_id` | `VARCHAR(36)` | PRIMARY KEY | UUID (generated) |
| `order_id` | `VARCHAR(36)` | NOT NULL | FK → raw.orders |
| `product_id` | `VARCHAR(36)` | NOT NULL | |
| `quantity` | `INTEGER` | NOT NULL, CHECK > 0 | |
| `unit_price_usd` | `NUMERIC(10,2)` | NOT NULL, CHECK >= 0 | |
| `_ingested_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT NOW() | |
| `_run_date` | `DATE` | NOT NULL | |

Indexes: `PRIMARY KEY (order_item_id)`, `INDEX ON (order_id)`


#### `raw.campaigns`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `campaign_id` | `VARCHAR(36)` | PRIMARY KEY | UUID |
| `platform` | `VARCHAR(20)` | NOT NULL | `google_ads`, `meta_ads` |
| `campaign_name` | `VARCHAR(255)` | NOT NULL | |
| `daily_spend_usd` | `NUMERIC(12,2)` | NOT NULL, CHECK >= 0 | |
| `impressions` | `INTEGER` | NOT NULL, CHECK >= 0 | |
| `clicks` | `INTEGER` | NOT NULL, CHECK >= 0 | |
| `campaign_date` | `DATE` | NOT NULL | |
| `_ingested_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT NOW() | |
| `_run_date` | `DATE` | NOT NULL | |

Indexes: `PRIMARY KEY (campaign_id, campaign_date)` (composite — one record per campaign per day), `INDEX ON (campaign_date)`, `INDEX ON (_run_date)`

Note: The natural key for idempotency is `(campaign_id, campaign_date)` — daily metrics are upserted using this composite key.

#### `raw.tickets`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `ticket_id` | `VARCHAR(36)` | PRIMARY KEY | UUID |
| `customer_id` | `VARCHAR(36)` | NOT NULL | FK → raw.customers |
| `subject` | `VARCHAR(500)` | NOT NULL | |
| `description` | `TEXT` | NOT NULL | Min 10 words enforced at generation |
| `status` | `VARCHAR(20)` | NOT NULL | `open`, `in_progress`, `closed` |
| `priority` | `VARCHAR(10)` | NOT NULL | `low`, `medium`, `high` |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | UTC |
| `resolved_at` | `TIMESTAMPTZ` | NULL | Populated when status = `closed` |
| `_ingested_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT NOW() | |
| `_run_date` | `DATE` | NOT NULL | |

Indexes: `PRIMARY KEY (ticket_id)`, `INDEX ON (customer_id)`, `INDEX ON (status)`, `INDEX ON (created_at)`, `INDEX ON (_run_date)`

---

### Mart Layer Tables (`marts` schema)

#### `marts.mart_customers`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `customer_id` | `VARCHAR(36)` | PRIMARY KEY | |
| `name` | `VARCHAR(255)` | NOT NULL | |
| `email` | `VARCHAR(255)` | NOT NULL | |
| `acquisition_channel` | `VARCHAR(50)` | NOT NULL | |
| `country_code` | `CHAR(2)` | NOT NULL | |
| `account_created_at` | `DATE` | NOT NULL | |
| `customer_tenure_days` | `INTEGER` | NOT NULL | Days from account_created_at to run_date |
| `is_active` | `BOOLEAN` | NOT NULL | True if ≥1 Order or Event in trailing 365 days |
| `_run_date` | `DATE` | NOT NULL | |

#### `marts.mart_orders`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `order_id` | `VARCHAR(36)` | PRIMARY KEY | |
| `customer_id` | `VARCHAR(36)` | NOT NULL | |
| `order_status` | `VARCHAR(20)` | NOT NULL | |
| `total_amount_usd` | `NUMERIC(12,2)` | NOT NULL | |
| `item_count` | `INTEGER` | NOT NULL | Derived: count of line items |
| `avg_item_value_usd` | `NUMERIC(10,2)` | NOT NULL | Derived: total / item_count |
| `ordered_at` | `TIMESTAMPTZ` | NOT NULL | |
| `_run_date` | `DATE` | NOT NULL | |

#### `marts.mart_campaigns`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `campaign_id` | `VARCHAR(36)` | NOT NULL | |
| `campaign_date` | `DATE` | NOT NULL | |
| `platform` | `VARCHAR(20)` | NOT NULL | |
| `campaign_name` | `VARCHAR(255)` | NOT NULL | |
| `daily_spend_usd` | `NUMERIC(12,2)` | NOT NULL | |
| `impressions` | `INTEGER` | NOT NULL | |
| `clicks` | `INTEGER` | NOT NULL | |
| `click_through_rate` | `NUMERIC(8,6)` | NOT NULL | Derived: clicks / NULLIF(impressions, 0) |
| `anomaly_flag` | `BOOLEAN` | NOT NULL, DEFAULT FALSE | Set by Anomaly_Model |
| `_run_date` | `DATE` | NOT NULL | |

Primary Key: `(campaign_id, campaign_date)`


#### `marts.mart_support_tickets`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `ticket_id` | `VARCHAR(36)` | PRIMARY KEY | |
| `customer_id` | `VARCHAR(36)` | NOT NULL | |
| `subject` | `VARCHAR(500)` | NOT NULL | |
| `status` | `VARCHAR(20)` | NOT NULL | |
| `priority` | `VARCHAR(10)` | NOT NULL | |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | |
| `resolved_at` | `TIMESTAMPTZ` | NULL | |
| `resolution_hours` | `NUMERIC(8,2)` | NULL | Derived: (resolved_at - created_at) in hours |
| `cluster_id` | `INTEGER` | NULL | Assigned by NLP_Processor |
| `cluster_label` | `VARCHAR(200)` | NULL | 1–10 word label from NLP |
| `cluster_confidence` | `NUMERIC(5,4)` | NULL | [0.00, 1.00] |
| `_run_date` | `DATE` | NOT NULL | |

#### `marts.mart_customer_360`

One row per customer. The central feature store for ML models.

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `customer_id` | `VARCHAR(36)` | PRIMARY KEY | |
| `recency_score` | `SMALLINT` | NOT NULL, CHECK 0–5 | RFM dimension |
| `frequency_score` | `SMALLINT` | NOT NULL, CHECK 0–5 | RFM dimension |
| `monetary_score` | `SMALLINT` | NOT NULL, CHECK 0–5 | RFM dimension |
| `rfm_score` | `VARCHAR(10)` | NOT NULL | e.g., `R5F3M4` |
| `recency_days` | `INTEGER` | NOT NULL | Days since last order (capped 999) |
| `order_frequency_365d` | `INTEGER` | NOT NULL | Order count in trailing 365 days |
| `total_spend_365d_usd` | `NUMERIC(14,2)` | NOT NULL | Total spend in trailing 365 days |
| `total_order_count` | `INTEGER` | NOT NULL | All-time order count |
| `total_spend_usd` | `NUMERIC(14,2)` | NOT NULL | All-time spend |
| `most_recent_event_date` | `DATE` | NULL | Latest event date (UTC) |
| `days_since_last_event` | `INTEGER` | NULL | Run date minus most_recent_event_date |
| `days_since_last_order` | `INTEGER` | NULL | Run date minus last order date |
| `order_count_last_30d` | `INTEGER` | NOT NULL, DEFAULT 0 | |
| `order_count_prior_30d` | `INTEGER` | NOT NULL, DEFAULT 0 | |
| `order_frequency_trend` | `INTEGER` | NOT NULL | order_count_last_30d - order_count_prior_30d |
| `active_campaign_count` | `INTEGER` | NOT NULL, DEFAULT 0 | Campaigns with spend in trailing 30 days |
| `open_ticket_count` | `INTEGER` | NOT NULL, DEFAULT 0 | Tickets with status open/in_progress |
| `acquisition_channel` | `VARCHAR(50)` | NOT NULL | |
| `customer_tenure_days` | `INTEGER` | NOT NULL | |
| `is_active` | `BOOLEAN` | NOT NULL | |
| `_run_date` | `DATE` | NOT NULL | |

Indexes: `PRIMARY KEY (customer_id)`, `INDEX ON (rfm_score)`, `INDEX ON (is_active)`, `INDEX ON (_run_date)`

#### `marts.mart_ml_scores`

One row per customer per daily run date.

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `customer_id` | `VARCHAR(36)` | NOT NULL | |
| `score_date` | `DATE` | NOT NULL | Run date |
| `ltv_score` | `NUMERIC(12,2)` | NOT NULL, CHECK >= 0 | Predicted 12-month LTV in USD |
| `churn_score` | `NUMERIC(5,4)` | NOT NULL, CHECK [0.0, 1.0] | Churn probability |
| `churn_risk_tier` | `VARCHAR(10)` | NOT NULL | `Low`, `Medium`, `High` |
| `segment_label` | `VARCHAR(100)` | NOT NULL | e.g., `Champions`, `Inactive` |
| `anomaly_flag` | `BOOLEAN` | NOT NULL, DEFAULT FALSE | Set by Anomaly_Model |
| `anomaly_detail` | `JSONB` | NULL | {metric, observed, expected_range, severity} |
| `_run_date` | `DATE` | NOT NULL | |
| `_created_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT NOW() | |

Primary Key: `(customer_id, score_date)`, Index: `INDEX ON (churn_risk_tier)`, `INDEX ON (segment_label)`, `INDEX ON (_run_date)`


---

### Observability Tables (`observability` schema)

#### `observability.dq_failures`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `failure_id` | `BIGSERIAL` | PRIMARY KEY | Auto-increment |
| `run_date` | `DATE` | NOT NULL | Pipeline run date |
| `failure_type` | `VARCHAR(20)` | NOT NULL | `great_expectations` or `dbt_test` |
| `source_domain` | `VARCHAR(50)` | NOT NULL | e.g., `crm`, `events` |
| `table_name` | `VARCHAR(200)` | NOT NULL | Fully-qualified table name |
| `checkpoint_or_test_name` | `VARCHAR(500)` | NOT NULL | GE checkpoint or dbt test identifier |
| `failing_column` | `VARCHAR(200)` | NULL | Column that failed (dbt tests) |
| `failing_expectation` | `TEXT` | NULL | Full expectation description (GE) |
| `sample_failing_rows` | `JSONB` | NULL | First 10 failing rows keyed by PK |
| `logged_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT NOW() | |

Indexes: `INDEX ON (run_date)`, `INDEX ON (source_domain)`, `INDEX ON (failure_type)`

Retention: A scheduled daily job deletes rows where `run_date < CURRENT_DATE - INTERVAL '90 days'`.

#### `observability.pipeline_run_log`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `log_id` | `BIGSERIAL` | PRIMARY KEY | |
| `run_date` | `DATE` | NOT NULL | |
| `dag_name` | `VARCHAR(200)` | NOT NULL | Airflow DAG ID |
| `status` | `VARCHAR(30)` | NOT NULL | `running`, `success`, `failed`, `sla_miss` |
| `started_at` | `TIMESTAMPTZ` | NOT NULL | |
| `completed_at` | `TIMESTAMPTZ` | NULL | |
| `duration_seconds` | `INTEGER` | NULL | |
| `rows_ingested` | `INTEGER` | NULL | Total rows loaded this run |
| `rows_transformed` | `INTEGER` | NULL | Total rows written to mart layer |
| `qg_tests_total` | `INTEGER` | NULL | Quality gate tests executed |
| `qg_tests_passed` | `INTEGER` | NULL | |
| `qg_tests_failed` | `INTEGER` | NULL | |
| `sla_breach_at` | `TIMESTAMPTZ` | NULL | Populated if status = `sla_miss` |
| `memory_usage_mb_start` | `NUMERIC(8,2)` | NULL | Container memory at DAG start |
| `memory_usage_mb_end` | `NUMERIC(8,2)` | NULL | Container memory at DAG end |
| `cpu_pct_start` | `NUMERIC(5,2)` | NULL | |
| `cpu_pct_end` | `NUMERIC(5,2)` | NULL | |
| `notes` | `TEXT` | NULL | Free-form warnings / error summary |

Indexes: `INDEX ON (run_date)`, `INDEX ON (dag_name, run_date)`, `INDEX ON (status)`

#### `observability.ml_insights`

Stores the Insights_Generator daily JSON output.

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `insight_id` | `BIGSERIAL` | PRIMARY KEY | |
| `run_date` | `DATE` | NOT NULL, UNIQUE | One insight record per run date |
| `insight_json` | `JSONB` | NOT NULL | Full structured JSON from Insights_Generator |
| `generated_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT NOW() | |
| `source_dag_run_id` | `VARCHAR(200)` | NULL | Airflow run ID for traceability |

Index: `INDEX ON (run_date)`


### ML Schema Tables (`ml` schema)

#### `ml.ml_scores` (mirror/staging of marts.mart_ml_scores for ML pipeline writes)

The ML pipeline writes scores here first; a final dbt mart model or insert-select promotes them to `marts.mart_ml_scores`. This separation prevents partial ML runs from corrupting the mart.

| Column | Type | Notes |
|---|---|---|
| `customer_id` | `VARCHAR(36)` | |
| `score_date` | `DATE` | |
| `model_run_id` | `VARCHAR(200)` | MLflow run ID for traceability |
| `ltv_score` | `NUMERIC(12,2)` | |
| `churn_score` | `NUMERIC(5,4)` | |
| `churn_risk_tier` | `VARCHAR(10)` | |
| `segment_label` | `VARCHAR(100)` | |
| `anomaly_flag` | `BOOLEAN` | |
| `anomaly_detail` | `JSONB` | |
| `scored_at` | `TIMESTAMPTZ` | |

Primary Key: `(customer_id, score_date)`

#### `ml.anomaly_metrics`

Stores per-metric daily values and anomaly evaluation results.

| Column | Type | Notes |
|---|---|---|
| `metric_id` | `BIGSERIAL` | PRIMARY KEY |
| `run_date` | `DATE` | |
| `metric_name` | `VARCHAR(100)` | e.g., `total_daily_revenue_usd` |
| `observed_value` | `NUMERIC(18,4)` | |
| `rolling_mean_30d` | `NUMERIC(18,4)` | NULL if baseline_pending |
| `rolling_std_30d` | `NUMERIC(18,4)` | NULL if baseline_pending |
| `z_score` | `NUMERIC(8,4)` | NULL if baseline_pending |
| `anomaly_flag` | `BOOLEAN` | |
| `severity` | `VARCHAR(20)` | NULL, `Warning`, `Critical`, `baseline_pending` |
| `expected_range_low` | `NUMERIC(18,4)` | mean - 2*std |
| `expected_range_high` | `NUMERIC(18,4)` | mean + 2*std |

Indexes: `INDEX ON (run_date)`, `INDEX ON (metric_name, run_date)`

---

## dbt Layer Design

### Project Structure

```
dbt/
├── dbt_project.yml
├── profiles.yml             (reads from env vars, not committed)
├── packages.yml             (dbt_utils, dbt_expectations)
├── seeds/
│   └── (reference data: channel labels, status codes)
├── macros/
│   ├── generate_schema_name.sql
│   ├── get_run_date.sql
│   └── audit_columns.sql
├── models/
│   ├── staging/
│   │   ├── crm/
│   │   │   ├── stg_crm__customers.sql
│   │   │   └── _stg_crm__sources.yml
│   │   ├── events/
│   │   │   ├── stg_events__events.sql
│   │   │   └── _stg_events__sources.yml
│   │   ├── orders/
│   │   │   ├── stg_orders__orders.sql
│   │   │   ├── stg_orders__order_items.sql
│   │   │   └── _stg_orders__sources.yml
│   │   ├── campaigns/
│   │   │   ├── stg_campaigns__campaigns.sql
│   │   │   └── _stg_campaigns__sources.yml
│   │   └── support/
│   │       ├── stg_support__tickets.sql
│   │       └── _stg_support__sources.yml
│   ├── intermediate/
│   │   ├── int_customers__enriched.sql
│   │   ├── int_orders__with_items.sql
│   │   ├── int_sessions__with_duration.sql
│   │   ├── int_customer_orders__aggregated.sql
│   │   ├── int_customer_events__aggregated.sql
│   │   └── int_customer_tickets__aggregated.sql
│   └── marts/
│       ├── mart_customers.sql
│       ├── mart_orders.sql
│       ├── mart_campaigns.sql
│       ├── mart_support_tickets.sql
│       ├── mart_customer_360.sql
│       ├── mart_ml_scores.sql
│       └── _marts__models.yml
├── tests/
│   ├── assert_no_orphan_fk_orders.sql
│   ├── assert_no_orphan_fk_events.sql
│   ├── assert_no_orphan_fk_tickets.sql
│   └── assert_mart_customer_360_row_count.sql
└── analyses/
    └── rfm_distribution.sql
```


### Materialization Strategy

| Layer | Default Materialization | Override Conditions |
|---|---|---|
| Staging | `view` | All staging models — thin clean layer, no storage cost |
| Intermediate | `table` | Full refresh each run; joins are expensive as views |
| Marts (small) | `table` | `mart_customers`, `mart_campaigns`, `mart_support_tickets` |
| Marts (large) | `incremental` | `mart_orders`, `mart_customer_360`, `mart_ml_scores` (event-derived) |
| Mart events-derived | `incremental (delete+insert)` | Partition by `_run_date`; prevents full rebuild on 5M events |

Incremental strategy: `delete+insert` on `_run_date` partition key. Each daily run deletes the current `_run_date` partition then inserts fresh rows. This is idempotent and avoids merge complexity on PostgreSQL.

### Naming Convention

| Pattern | Example | Rule |
|---|---|---|
| `stg_{source}__{entity}` | `stg_crm__customers` | One model per source table |
| `int_{domain}__{description}` | `int_customer_orders__aggregated` | Grain and join described |
| `mart_{entity}` | `mart_customer_360` | Business-facing name |

Double underscore (`__`) separates the source/domain prefix from the entity name — a dbt community convention that makes lineage immediately readable.

### Key Model Logic Descriptions

**`stg_crm__customers`**: Selects from `raw.customers`, renames `_ingested_at` to `ingested_at`, casts `account_created_at` to DATE, filters to latest record per `customer_id` using `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC) = 1`. No business logic.

**`stg_orders__orders`**: Deduplicates on `order_id` by latest `_ingested_at`. Casts `total_amount_usd` to NUMERIC(12,2). Validates `order_status` is in accepted set. Filters out rows with NULL `customer_id`.

**`stg_events__events`**: Deduplicates on `event_id`. Casts `occurred_at` to TIMESTAMPTZ. Does not join to sessions — session aggregation is deferred to intermediate layer.

**`int_sessions__with_duration`**: Groups events by `session_id`, computes `session_start = MIN(occurred_at)`, `session_end = MAX(occurred_at)`, `session_duration_seconds = EXTRACT(EPOCH FROM session_end - session_start)`. One row per session.

**`int_orders__with_items`**: Joins `stg_orders__orders` to `stg_orders__order_items` on `order_id`. Computes `item_count = COUNT(order_item_id)` and `avg_item_value_usd = total_amount_usd / NULLIF(item_count, 0)`. One row per order.

**`int_customer_orders__aggregated`**: Groups `int_orders__with_items` by `customer_id`. Computes all-time `total_order_count`, `total_spend_usd`, trailing 365-day `order_frequency_365d` and `total_spend_365d_usd`, `order_count_last_30d`, `order_count_prior_30d`, and `days_since_last_order` relative to `{{ get_run_date() }}`.

**`int_customer_events__aggregated`**: Groups `stg_events__events` by `customer_id`. Computes `most_recent_event_date` and `days_since_last_event`.

**`int_customer_tickets__aggregated`**: Groups `stg_support__tickets` by `customer_id`. Computes `open_ticket_count` (status in `open`, `in_progress`).

**`mart_customer_360`**: Joins all `int_customer_*__aggregated` models on `customer_id`. Computes RFM quintile scores using `NTILE(5)` window functions over the active customer population. Computes `rfm_score` string. Sets `is_active` flag. Marks customers with no trailing 365-day orders as `Inactive` with `rfm_score = R0F0M0`. Adds `active_campaign_count` from a subquery against `stg_campaigns__campaigns`.

**`mart_ml_scores`**: Incremental model that merges ML scoring outputs from `ml.ml_scores` after the ML pipeline DAG completes. Deletes current `_run_date` partition and inserts fresh rows. Does not compute scores — reads from the ML write path.

### Source Definitions

Each staging domain has a `_stg_{domain}__sources.yml` declaring:
- `database: "{{ env_var('CIP_DB_NAME') }}"`
- `schema: raw`
- Source tables with column-level descriptions
- `loaded_at_field: _ingested_at` for source freshness checks
- `freshness: warn_after: {count: 1, period: day}`

### Schema YAML Structure (Mart Layer)

`_marts__models.yml` declares every mart model with:
- `name`, `description` (non-empty, references business context)
- `columns`: each with `name`, `description`, and applicable `tests`
- Staging models carry: `not_null`, `unique` on PKs; `accepted_values` on status columns; `relationships` for FK columns
- Mart models carry: `not_null` on required fields; custom volume test referencing prior-day count

### Seed Files

| File | Contents | Purpose |
|---|---|---|
| `seeds/acquisition_channels.csv` | channel_code, label | Reference join for readable channel names |
| `seeds/order_statuses.csv` | status_code, is_revenue_positive | Used in revenue calculations |
| `seeds/country_codes.csv` | alpha_2, country_name, region | Enrichment for geographic dashboards |


---

## Airflow DAG Design

### DAG Inventory

| DAG ID | Schedule | Description | SLA |
|---|---|---|---|
| `ingest_crm` | `0 2 * * *` | Load/upsert customers into raw.customers | 03:00 UTC |
| `ingest_events` | `0 2 * * *` | Load/upsert events into raw.events | 03:30 UTC |
| `ingest_orders` | `0 2 * * *` | Load/upsert orders + items into raw.orders | 03:30 UTC |
| `ingest_campaigns` | `0 2 * * *` | Load/upsert campaigns into raw.campaigns | 03:00 UTC |
| `ingest_tickets` | `0 2 * * *` | Load/upsert tickets into raw.tickets | 03:30 UTC |
| `transform_staging` | Triggered | dbt run --select staging | — |
| `transform_intermediate` | Triggered | dbt run --select intermediate | — |
| `transform_marts` | Triggered | dbt run --select marts | — |
| `ml_scoring` | Triggered | Segmentation → LTV → Churn → Anomaly → NLP | 05:30 UTC |
| `insights_generator` | Triggered | Generate daily insight JSON | 05:50 UTC |
| `quality_gates` | Triggered | GE checkpoints + dbt test runs | — |
| `master_pipeline` | `0 2 * * *` | Orchestrator DAG — triggers all above in sequence | 06:00 UTC |
| `dq_retention_cleanup` | `0 1 * * *` | Delete dq_failures rows older than 90 days | — |

### Master Pipeline DAG Task Graph

```
[start]
   │
   ├──▶ [trigger_ingest_crm]
   ├──▶ [trigger_ingest_events]      ← all 5 ingestion DAGs run in parallel
   ├──▶ [trigger_ingest_orders]
   ├──▶ [trigger_ingest_campaigns]
   └──▶ [trigger_ingest_tickets]
              │ (wait for all 5 ExternalTaskSensors)
              ▼
   [ge_quality_gate]                  ← Great Expectations on all raw tables
              │ PASS
              ▼
   [dbt_staging]                      ← dbt run --select staging
              │
              ▼
   [dbt_schema_tests_staging]         ← dbt test --select staging
              │ PASS
              ▼
   [dbt_intermediate]                 ← dbt run --select intermediate
              │
              ▼
   [dbt_marts]                        ← dbt run --select marts
              │
              ▼
   [dbt_volume_tests_marts]           ← dbt test --select marts (row count)
              │
              ▼
   [ml_segmentation]
              │
              ▼
   [ml_ltv]          [ml_churn]       ← LTV and Churn can run in parallel
        └──────────┬──────────┘
                   ▼
   [ml_anomaly]
              │
              ▼
   [ml_nlp]
              │
              ▼
   [ml_scores_promotion]              ← Promote ml.ml_scores → marts.mart_ml_scores
              │
              ▼
   [insights_generator]
              │
              ▼
   [pipeline_run_log_success]         ← Write success record to pipeline_run_log
              │
              ▼
   [end]
```

### Retry and SLA Configuration

| Setting | Value | Rationale |
|---|---|---|
| `retries` | 3 (ingestion), 2 (dbt/ML) | Ingestion may hit transient DB contention |
| `retry_delay` | `timedelta(minutes=5)` | Allows transient failures to resolve |
| `execution_timeout` | 45 min (ingestion), 30 min (dbt), 30 min (ML) | Prevents hung tasks from blocking SLA |
| `sla` | `timedelta(hours=4)` on master DAG | 06:00 UTC deadline = 4h from 02:00 start |
| `on_sla_miss` | `sla_miss_callback` function | Writes to `pipeline_run_log` with status `sla_miss` |
| `depends_on_past` | `False` | Allows backfill without blocking |
| `catchup` | `False` | No automatic backfill on startup |

### Inter-DAG Dependencies

Ingestion DAGs use `TriggerDagRunOperator` from the master pipeline DAG. Downstream DAGs wait on `ExternalTaskSensor` with `allowed_states=['success']`. This ensures the transformation DAG cannot start until all five ingestion DAGs complete successfully, as required.

### Shared Utilities Pattern

```
dags/
├── master_pipeline.py
├── ingest_crm.py
├── ingest_events.py
├── ingest_orders.py
├── ingest_campaigns.py
├── ingest_tickets.py
├── transform_staging.py
├── transform_intermediate.py
├── transform_marts.py
├── ml_scoring.py
├── insights_generator.py
├── quality_gates.py
└── utils/
    ├── __init__.py
    ├── db.py             ← connection helpers, parameterized query wrapper
    ├── logging.py        ← structured log emitters (run_start, run_end, row_count)
    ├── dbt_runner.py     ← BashOperator wrapper for dbt CLI commands
    ├── ge_runner.py      ← Great Expectations checkpoint runner
    ├── sla.py            ← SLA miss callback, pipeline_run_log writer
    └── mlflow_utils.py   ← MLflow client helpers (register, promote, compare)
```


---

## AI/ML Architecture

### ML Pipeline Flow

```
mart_customer_360  (feature store)
        │
        ▼
[Feature Engineering]  ←── DuckDB (in-memory, reduces PostgreSQL pressure)
        │
        ├──▶ [Segmentation Model]  → segment_label per customer
        │         ↓ MLflow log
        ├──▶ [LTV Model]           → ltv_score per customer
        │         ↓ MLflow log
        ├──▶ [Churn Model]         → churn_score, churn_risk_tier per customer
        │         ↓ MLflow log
        ├──▶ [Anomaly Model]       → anomaly_flag, anomaly_detail per customer
        │         ↓ MLflow log
        └──▶ [NLP Processor]       → cluster_id, cluster_label, confidence per ticket
                  ↓ MLflow log
                  ↓
        [ml.ml_scores] write
                  ↓
        [ml_scores_promotion DAG task]
                  ↓
        [marts.mart_ml_scores]
```

### Feature Engineering Strategy

All models use `mart_customer_360` as the primary feature source. DuckDB is used for feature engineering transformations (joins, window functions, aggregations) before model training to avoid holding large DataFrames in memory via the PostgreSQL connection.

Feature engineering steps:
1. Export `mart_customer_360` to DuckDB in-memory database as a Parquet-backed relation
2. Apply feature transformations in DuckDB SQL (log-transform skewed monetary values, clip outliers)
3. Return a Pandas DataFrame to the Python model training code
4. Persist feature snapshots to `ml/feature_snapshots/` as Parquet files (MLflow artifacts)

### Model Specifications

#### Segmentation Model

| Aspect | Design |
|---|---|
| Algorithm | k-means (scikit-learn `KMeans`) |
| Feature space | recency_score (1–5), frequency_score (1–5), monetary_score (1–5) |
| Pre-processing | StandardScaler on RFM quintile values |
| Cluster range | k ∈ {4, 5, 6, 7, 8} |
| Selection criterion | Highest silhouette score (`sklearn.metrics.silhouette_score`) |
| Tie-breaking | If all silhouette scores differ by ≤ 0.01, default to k=4 and log WARNING |
| Inactive customers | Bypass clustering; assigned `Inactive` label, `R0F0M0` RFM score |
| MLflow experiment | `customer_segmentation` |
| Logged params | algorithm, k, random_state |
| Logged metrics | silhouette_score, inertia |
| Registry name | `customer_segmentation` |
| Promotion rule | Highest silhouette_score → `production` stage tag |
| Output | `segment_label` per customer (string cluster name derived from centroid RFM profile) |

Segment naming convention: Centroids are ranked by composite RFM and assigned human-readable labels (e.g., `Champions`, `Loyal`, `At-Risk`, `Inactive`). Labels are deterministic relative to centroid rank, not random.

#### LTV Model

| Aspect | Design |
|---|---|
| Algorithm | Gradient Boosting Regressor (scikit-learn `GradientBoostingRegressor`) |
| Target variable | Estimated 12-month forward revenue (proxy: trailing 365-day spend scaled) |
| Features | order_frequency_365d, avg_order_value, customer_tenure_days, acquisition_channel (one-hot), segment_label (one-hot) |
| Train/val split | 80% train / 20% validation (stratified by acquisition_channel) |
| Minimum score | 0.00 USD — `np.clip(predictions, 0, None)` |
| Cold-start (no orders, cohort exists) | Mean LTV of acquisition_channel cohort from training data |
| Cold-start (no orders, no cohort) | 0.00 USD |
| MLflow experiment | `customer_ltv` |
| Logged metrics | RMSE (val set), MAE (val set), training_sample_count |
| Logged params | algorithm, n_estimators, learning_rate, feature_list, train_date_range |
| Registry name | `customer_ltv` |
| Promotion rule | New model RMSE < current production RMSE → promote; else retain current |

#### Churn Model

| Aspect | Design |
|---|---|
| Algorithm | Random Forest Classifier (scikit-learn `RandomForestClassifier`) |
| Target variable | Binary: 1 if no Order or Event in the 90 days after score date (historical proxy) |
| Features | days_since_last_order, days_since_last_event, open_ticket_count, order_frequency_trend, segment_label (one-hot) |
| Train/val split | 80% / 20% (stratified by churn label) |
| Score range | [0.0, 1.0] — `predict_proba[:,1]` |
| Risk tiers | Low: score < 0.33; Medium: 0.33 ≤ score < 0.67; High: score ≥ 0.67 |
| Active customers only | Customers with ≥1 Order or Event in trailing 365 days |
| MLflow experiment | `customer_churn` |
| Logged metrics | AUC-ROC (val set), precision, recall, feature_importances (JSON map) |
| Registry name | `customer_churn` |
| Promotion rule | AUC-ROC > 0.70 → register + promote; else retain current + log WARNING |
| Failure fallback | On scoring run failure, retain prior day's Churn_Score values + log ERROR |


#### Anomaly Detection Model

| Aspect | Design |
|---|---|
| Algorithm | Statistical z-score on 30-day rolling window (no ML library dependency) |
| Metrics evaluated | total_daily_revenue_usd, total_daily_campaign_spend_usd, daily_order_count, daily_new_customer_count, daily_event_count, daily_ctr_per_campaign |
| Baseline window | 30 days of historical metric values from `ml.anomaly_metrics` |
| Flag threshold | z-score ≥ 2.0 (absolute value) |
| Severity | Warning: 2.0 ≤ |z| < 3.0; Critical: |z| ≥ 3.0 |
| Baseline pending | < 30 days of data → set status = `baseline_pending`, no flag emitted |
| Critical logging | On Critical flag: Airflow task log ERROR with metric name, observed, expected range, date |
| Output | `anomaly_flag` (bool), `anomaly_detail` (JSONB) per customer and per metric |

Design decision: Pure z-score (not ML-based) is chosen because the metric space is small (6 metrics), the 30-day window is well-defined, and it is fully explainable without model retraining. An ML-based approach would require substantially more historical data than synthetic generation provides.

#### NLP Processor (Support Ticket Clustering)

| Aspect | Design |
|---|---|
| Preprocessing | Lowercase, remove punctuation, stop word removal (NLTK), lemmatization (NLTK WordNetLemmatizer) |
| Vectorization | TF-IDF (scikit-learn `TfidfVectorizer`, max_features=5000) |
| Clustering | k-means on TF-IDF vectors |
| Cluster range | k ∈ {5, 6, ..., 15} |
| Selection criterion | Highest coherence score (computed via `gensim.models.coherencemodel`) |
| Quality threshold | coherence ≥ 0.40; if below, log WARNING to Airflow run log |
| Cluster label generation | Top 5 TF-IDF terms per centroid → concatenated as 1–10 word label |
| Model persistence | Saved as MLflow artifact; loaded for incremental runs |
| Retraining trigger | Daily ticket volume > 500 new records OR no prior model exists |
| Incremental scoring | Apply fitted TF-IDF + k-means transform to new tickets without refit |
| MLflow experiment | `support_nlp_clustering` |
| Logged metrics | coherence_score, num_clusters, training_doc_count |
| Logged params | vectorizer_params, algorithm, k |

### MLflow Experiment and Registry Design

| Experiment Name | Run metadata | Registry Model Name | Production Criterion |
|---|---|---|---|
| `customer_segmentation` | k, silhouette, inertia | `customer_segmentation` | Highest silhouette |
| `customer_ltv` | RMSE, MAE, features, date_range | `customer_ltv` | Lower RMSE than current prod |
| `customer_churn` | AUC-ROC, precision, recall, feature_importances | `customer_churn` | AUC-ROC > 0.70 |
| `support_nlp_clustering` | coherence, k, doc_count | `support_nlp` | Highest coherence |

MLflow backend store: PostgreSQL (`mlflow` database). Artifact store: local filesystem volume (`/mlflow/artifacts`) mounted to named Docker volume. Each registered model version includes a tag `run_date=YYYY-MM-DD` for traceability.

### ML Module Structure

```
ml/
├── __init__.py
├── features/
│   ├── __init__.py
│   ├── feature_store.py       ← loads mart_customer_360 via DuckDB
│   └── transformers.py        ← log-transform, one-hot, clipping
├── models/
│   ├── segmentation.py        ← train, score, register
│   ├── ltv.py
│   ├── churn.py
│   ├── anomaly.py
│   └── nlp.py
└── scoring/
    ├── score_all.py            ← orchestrates all model scoring for a run date
    └── promote.py              ← writes ml.ml_scores → marts.mart_ml_scores
```


---

## FastAPI Endpoint Design

### Overview

The FastAPI service connects to PostgreSQL using an `asyncpg` connection pool (min=5, max=20). All responses derive exclusively from `marts.*` and `ml.*` schemas. The API is stateless; no session or caching layer is needed at MVP scale.

### Connection Pool Configuration

```python
pool_min_size = 5   # env: DB_POOL_MIN (default 5)
pool_max_size = 20  # env: DB_POOL_MAX (default 20)
# Released on each request completion via context manager
```

### Endpoint Specifications

#### `GET /health`

**Purpose**: System health check for orchestrator and load balancer probes.

**Response 200 (DB reachable)**:
```json
{
  "status": "healthy",
  "db": "connected",
  "run_date": "2024-01-15"
}
```

**Response 503 (DB unreachable)**:
```json
{
  "status": "unhealthy",
  "db": "disconnected",
  "detail": "Could not acquire database connection: <reason>"
}
```

Implementation: Executes `SELECT MAX(run_date) FROM observability.pipeline_run_log WHERE status = 'success'`. On any connection exception, returns 503. No authentication required.

---

#### `GET /customers/{customer_id}`

**Path Parameter**: `customer_id` — string, must be non-empty, max 36 chars, UUID-format validated.

**Response 200**:
```json
{
  "customer_id": "...",
  "name": "...",
  "email": "...",
  "acquisition_channel": "organic",
  "country_code": "US",
  "account_created_at": "2022-03-15",
  "customer_tenure_days": 450,
  "is_active": true,
  "total_order_count": 12,
  "total_spend_usd": "1234.56",
  "rfm_score": "R4F3M5",
  "run_date": "2024-01-15"
}
```

**Response 404**:
```json
{
  "error": "Customer not found",
  "id": "<customer_id received>"
}
```

**Response 422** (invalid format):
```json
{
  "detail": [
    {
      "field": "customer_id",
      "received": "not-a-uuid",
      "constraint": "must be a valid UUID (RFC 4122)"
    }
  ]
}
```

Query: Parameterized `SELECT ... FROM marts.mart_customers WHERE customer_id = $1` (asyncpg). No string interpolation.

---

#### `GET /customers/{customer_id}/scores`

**Path Parameter**: Same validation as above.

**Response 200**:
```json
{
  "customer_id": "...",
  "score_date": "2024-01-15",
  "ltv_score": "450.75",
  "churn_score": 0.72,
  "churn_risk_tier": "High",
  "segment_label": "At-Risk",
  "anomaly_flag": false
}
```

**Response 404**: Same shape as `/customers/{id}`.

Query: `SELECT ... FROM marts.mart_ml_scores WHERE customer_id = $1 ORDER BY score_date DESC LIMIT 1`.

---

#### `GET /segments`

**Purpose**: List active segment labels with customer counts and average scores.

**Query Parameters**:
- `limit`: integer, default=100, max=1000, min=1
- `offset`: integer, default=0, min=0

**Response 200**:
```json
{
  "total": 6,
  "limit": 100,
  "offset": 0,
  "items": [
    {
      "segment_label": "Champions",
      "customer_count": 18450,
      "avg_ltv_score": "1245.30",
      "avg_churn_score": 0.12
    }
  ]
}
```

**Response 422** (limit > 1000): Standard 422 shape with `field: "limit"`.

Query: Aggregates `marts.mart_ml_scores` for the most recent `score_date`, grouped by `segment_label`. Offset/limit applied via SQL `OFFSET $1 LIMIT $2`.


#### `GET /insights/latest`

**Purpose**: Return the most recent daily narrative insight JSON.

**Response 200**: Returns the `insight_json` field from `observability.ml_insights WHERE run_date = (SELECT MAX(run_date) FROM observability.ml_insights)`. The JSON structure is defined in the Insights_Generator design below.

**Response 503** (no pipeline run completed yet):
```json
{
  "status": "unavailable",
  "detail": "No insights have been generated yet. Pipeline may not have completed."
}
```

#### `GET /anomalies`

**Purpose**: List active anomaly flags and metric details.

**Query Parameters**:
- `limit`: integer, default=100, max=1000
- `offset`: integer, default=0
- `severity`: optional string filter (`Warning`, `Critical`)
- `run_date`: optional DATE filter (defaults to most recent)

**Response 200**:
```json
{
  "total": 3,
  "limit": 100,
  "offset": 0,
  "run_date": "2024-01-15",
  "items": [
    {
      "metric_name": "total_daily_revenue_usd",
      "observed_value": 48250.00,
      "expected_range_low": 22100.50,
      "expected_range_high": 38900.75,
      "z_score": 2.8,
      "severity": "Warning",
      "flag_date": "2024-01-15"
    }
  ]
}
```

Query: `SELECT ... FROM ml.anomaly_metrics WHERE anomaly_flag = TRUE AND run_date = $1 ORDER BY severity DESC OFFSET $2 LIMIT $3`.

---

### Error Shape Standardization

All API errors follow a consistent envelope:

| Status | Scenario | Body fields |
|---|---|---|
| 404 | Resource not found | `error` (string), `id` (echoed value) |
| 422 | Validation failure | `detail` (array of `{field, received, constraint}`) |
| 503 | DB unavailable | `status`, `detail` |
| 500 | Unhandled exception | Should never reach client — FastAPI exception handler logs + returns 503 |

A global `exception_handler` catches all unhandled exceptions and returns a 503 with a generic `detail` rather than leaking stack traces.

### OpenAPI Documentation

FastAPI auto-generates `/docs` (Swagger UI) and `/openapi.json`. Each endpoint is decorated with:
- `summary`, `description`, `tags`
- `response_model` (Pydantic model)
- All error responses declared via `responses={404: ..., 422: ..., 503: ...}`

All Pydantic models use explicit field types, `Field(description=...)`, and example values for clean Swagger rendering.


---

## Metabase Dashboard Architecture

### Database Connection Design

Metabase connects to PostgreSQL using a dedicated `metabase_reader` role that has `CONNECT` on the `cip` database and `SELECT` on all tables in the `marts` schema only. It is explicitly denied access to `raw`, `staging`, `intermediate`, `ml`, and `observability` schemas. Connection details are provided via Metabase's environment variable configuration (`MB_DB_*`), not stored in Metabase's internal config.

```sql
-- Role definition (applied at init)
CREATE ROLE metabase_reader LOGIN PASSWORD '${METABASE_DB_PASSWORD}';
GRANT CONNECT ON DATABASE cip TO metabase_reader;
GRANT USAGE ON SCHEMA marts TO metabase_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO metabase_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO metabase_reader;
```

### Scheduled Refresh Strategy

Each Metabase dashboard question (card) is configured with a cache/refresh TTL of 5 minutes. This ensures that within 5 minutes of the dbt mart models completing, dashboard consumers see current-day data. The refresh is pull-based — Metabase re-executes queries on its own schedule.

For the portfolio MVP, Metabase's built-in scheduled queries replace any external push notification mechanism.

### Dashboard Specifications

#### Dashboard 1: Customer Overview

| Card | Type | Source Table | Key Fields |
|---|---|---|---|
| Total Active Customers | Scalar | `mart_customers` | `COUNT(*) WHERE is_active = TRUE` |
| Customer Acquisition by Month | Line chart | `mart_customers` | `account_created_at` truncated to month, COUNT |
| Segment Distribution | Donut chart | `mart_ml_scores` | `segment_label`, COUNT |
| Top 10 Customers by LTV | Table | `mart_ml_scores` JOIN `mart_customers` | customer_id, name, ltv_score DESC LIMIT 10 |

#### Dashboard 2: Churn Risk

| Card | Type | Source Table | Key Fields |
|---|---|---|---|
| Customers by Risk Tier | Bar chart | `mart_ml_scores` | `churn_risk_tier`, COUNT and % |
| 30-Day High-Risk Trend | Line chart | `mart_ml_scores` | Daily count WHERE churn_risk_tier='High', last 30 score_dates |
| High-Risk Customer Table | Filterable table | `mart_ml_scores` JOIN `mart_customers` | customer_id, name, ltv_score, most_recent_event_date, churn_score |

Filters: `churn_risk_tier` dropdown (default: High), `acquisition_channel` dropdown.

#### Dashboard 3: Campaign Performance

| Card | Type | Source Table | Key Fields |
|---|---|---|---|
| Total Daily Campaign Spend | Line chart | `mart_campaigns` | `SUM(daily_spend_usd)` by `campaign_date` |
| Spend vs CTR by Campaign | Scatter chart | `mart_campaigns` | `daily_spend_usd` (x), `click_through_rate` (y), colored by `platform` |
| Anomaly Flags | Conditional table | `mart_campaigns` | `campaign_id`, `campaign_date`, `anomaly_flag` with red/green indicator |

The `anomaly_flag` column uses Metabase's conditional formatting to visually distinguish `TRUE` (red) from `FALSE` (green/neutral).

#### Dashboard 4: Support Intelligence

| Card | Type | Source Table | Key Fields |
|---|---|---|---|
| Total Open Tickets | Scalar | `mart_support_tickets` | `COUNT(*) WHERE status IN ('open', 'in_progress')` |
| Volume by Cluster | Bar chart | `mart_support_tickets` | `cluster_label`, COUNT |
| High-Priority Proportion per Cluster | Stacked bar | `mart_support_tickets` | `cluster_label`, % WHERE priority = 'high' |
| Avg Resolution Time per Cluster | Bar chart | `mart_support_tickets` | `cluster_label`, `AVG(resolution_hours)` WHERE status = 'closed' |


---

## Data Quality Strategy

### Great Expectations Suite Design

One GE suite per Raw Zone table. Each suite is a checkpoint triggered after the corresponding ingestion DAG completes successfully.

| Suite Name | Target Table | Key Expectations |
|---|---|---|
| `raw_customers_suite` | `raw.customers` | Column set completeness; `customer_id` not null + unique; `acquisition_channel` in accepted set; `country_code` length = 2; not-null rate ≥ 95% for all required fields |
| `raw_events_suite` | `raw.events` | `event_id` not null + unique; `customer_id` not null; `occurred_at` not null; `event_type` not null; not-null rate ≥ 95% |
| `raw_orders_suite` | `raw.orders` | `order_id` not null + unique; `order_status` in accepted set; `total_amount_usd` ≥ 0; not-null rate ≥ 95% |
| `raw_campaigns_suite` | `raw.campaigns` | `campaign_id` not null; `platform` in `{google_ads, meta_ads}`; `clicks` ≤ `impressions`; `daily_spend_usd` ≥ 0; not-null rate ≥ 95% |
| `raw_tickets_suite` | `raw.tickets` | `ticket_id` not null + unique; `status` in accepted set; `priority` in accepted set; `description` not null; not-null rate ≥ 95% |

GE Context: `FileSystemDataContext`, stored under `great_expectations/`. Each suite runs against a PostgreSQL SQLAlchemy data source. Checkpoints are called from the Airflow `ge_runner.py` utility.

On failure: GE checkpoint result is parsed; failing expectations are written to `observability.dq_failures` and the downstream transformation DAG is halted via Airflow's `AirflowSkipException` or task failure propagation.

### dbt Test Strategy Per Layer

**Staging Layer tests** (must pass before Intermediate runs):
- `not_null` on all PK columns
- `unique` on all PK columns
- `accepted_values` on all status/enum columns
- `relationships` (FK checks) on `customer_id` FK columns referencing `stg_crm__customers`

**Intermediate Layer tests**:
- `not_null` on derived fields (`session_duration_seconds`, `item_count`)
- `dbt_utils.expression_is_true` for derived field bounds (e.g., `item_count >= 1`)

**Mart Layer tests**:
- `not_null` on all required fields
- `unique` on PKs
- `dbt_utils.recency` — `_run_date` within last 1 day (freshness)
- Custom singular test: `assert_mart_row_count_within_20pct` — compares today's count to previous `_run_date` row count; WARN-only on first run, WARN on deviation, does not halt pipeline

### DQ Failures Table Lifecycle

Records are inserted into `observability.dq_failures` by:
1. `ge_runner.py` — on GE checkpoint failure (type: `great_expectations`)
2. dbt test runner post-processing — parses dbt test results JSON and writes type: `dbt_test` rows including first 10 failing PKs

A daily retention cleanup DAG (`dq_retention_cleanup`) runs at 01:00 UTC and deletes rows where `run_date < CURRENT_DATE - INTERVAL '90 days'`.

### Daily DQ Report Structure

The daily DQ report is written as a record to `observability.pipeline_run_log` (fields `qg_tests_total`, `qg_tests_passed`, `qg_tests_failed`) and as a structured JSON log entry in the Airflow task log at INFO level:

```json
{
  "report_date": "2024-01-15",
  "tests_total": 142,
  "tests_passed": 140,
  "tests_failed": 2,
  "failed_tables": ["raw.events", "staging.stg_orders__orders"],
  "baseline_warnings": []
}
```

This log entry is retained in Airflow's log storage (configured to local volume in Docker Compose) for the duration of the log retention window (default: 90 days via Airflow's `log_retention_days` config).


---

## Observability Strategy

### Structured Logging Pattern

Every Airflow task emits structured log entries via `utils/logging.py`:

```
# Task start
{"event": "task_start", "dag_id": "ingest_crm", "task_id": "load_customers", "run_date": "2024-01-15", "attempt": 1}

# Row count completed
{"event": "rows_loaded", "dag_id": "ingest_crm", "task_id": "load_customers", "run_date": "2024-01-15", "rows": 100000, "duration_seconds": 42}

# Task end
{"event": "task_end", "dag_id": "ingest_crm", "task_id": "load_customers", "run_date": "2024-01-15", "status": "success", "duration_seconds": 44}
```

All log entries use `logging.INFO` level for success events and `logging.ERROR` for failures (churn scoring failure, ML model quality threshold miss, critical anomaly flag).

### SLA Miss Detection

The `master_pipeline` DAG defines an `sla_miss_callback` function. When Airflow detects the SLA window (06:00 UTC = 4h from 02:00 start) is exceeded:

1. `sla_miss_callback` is invoked by Airflow
2. Callback queries Airflow DB for current task statuses and row counts
3. Callback writes a record to `observability.pipeline_run_log` with status = `sla_miss`
4. The DAG run is visually flagged in the Airflow Web UI

The `GET /health` endpoint reads the most recent record from `pipeline_run_log` and reflects the `run_date` of the last successful run.

### Resource Usage Logging

At the start and end of each master pipeline DAG run, a `log_resource_usage` task uses `docker stats --no-stream --format json` (via BashOperator) to capture memory MB and CPU % for each CIP container. These values are parsed and written to the current `pipeline_run_log` record's `memory_usage_mb_start/end` and `cpu_pct_start/end` fields.

This approach is consistent with the constraint that Docker Compose resource limits (`mem_limit`, `cpus`) are set, making container-level stats meaningful.

### `GET /health` Endpoint Design

The health endpoint serves as the single observable surface for the pipeline's last completed run date:

```
GET /health
  → Query: SELECT MAX(run_date) FROM observability.pipeline_run_log WHERE status = 'success'
  → If query succeeds and returns a date → 200, status: healthy, db: connected
  → If query succeeds but returns NULL (no successful run yet) → 200, status: healthy, db: connected, run_date: null
  → If query throws connection error → 503, status: unhealthy, db: disconnected
```

---

## Docker Compose Architecture

### Complete Service Inventory

#### `postgres`

| Attribute | Value |
|---|---|
| Image | `postgres:15.6-alpine` |
| Container name | `cip-postgres` |
| Internal port | 5432 |
| External port | None (internal network only) |
| Environment | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` from `.env` |
| Volumes | `postgres_data:/var/lib/postgresql/data` (named), `./infra/init.sql:/docker-entrypoint-initdb.d/init.sql:ro` |
| Health check | `pg_isready -U ${POSTGRES_USER}` every 10s, 5 retries, 30s start period |
| `mem_limit` | `2g` |
| `cpus` | `1.5` |
| Networks | `cip_internal` |


#### `airflow-webserver`

| Attribute | Value |
|---|---|
| Image | `apache/airflow:2.7.3-python3.11` |
| Container name | `cip-airflow-webserver` |
| Internal port | 8080 |
| External port | `8080:8080` (UI access) |
| Environment | `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__WEBSERVER__SECRET_KEY` from `.env` |
| Volumes | `./dags:/opt/airflow/dags:ro`, `./plugins:/opt/airflow/plugins:ro`, `airflow_logs:/opt/airflow/logs` |
| Depends on | `postgres` (healthy), `airflow-init` |
| Health check | `curl --fail http://localhost:8080/health` every 30s |
| `mem_limit` | `1g` |
| `cpus` | `0.5` |
| Command | `webserver` |

#### `airflow-scheduler`

| Attribute | Value |
|---|---|
| Image | `apache/airflow:2.7.3-python3.11` |
| Container name | `cip-airflow-scheduler` |
| Internal port | — |
| External port | None |
| Environment | Same core Airflow env vars as webserver |
| Volumes | Same DAGs/plugins/logs volumes as webserver |
| Depends on | `postgres` (healthy), `airflow-init` |
| Health check | `airflow jobs check --job-type SchedulerJob --hostname "*"` every 30s |
| `mem_limit` | `1.5g` |
| `cpus` | `1.0` |
| Command | `scheduler` |

#### `airflow-init`

| Attribute | Value |
|---|---|
| Image | `apache/airflow:2.7.3-python3.11` |
| Container name | `cip-airflow-init` |
| Role | One-shot init: `airflow db init && airflow users create` |
| Restart | `on-failure` |
| Depends on | `postgres` (healthy) |
| Health check | None (completes and exits) |

#### `mlflow`

| Attribute | Value |
|---|---|
| Image | `ghcr.io/mlflow/mlflow:v2.9.2` |
| Container name | `cip-mlflow` |
| Internal port | 5000 |
| External port | `5000:5000` (UI access) |
| Environment | `MLFLOW_BACKEND_STORE_URI` (PostgreSQL mlflow DB), `MLFLOW_DEFAULT_ARTIFACT_ROOT=/mlflow/artifacts` |
| Volumes | `mlflow_artifacts:/mlflow/artifacts` |
| Depends on | `postgres` (healthy) |
| Health check | `curl --fail http://localhost:5000/health` every 30s |
| `mem_limit` | `512m` |
| `cpus` | `0.5` |
| Command | `mlflow server --backend-store-uri ${MLFLOW_BACKEND_STORE_URI} --default-artifact-root /mlflow/artifacts --host 0.0.0.0` |

#### `fastapi`

| Attribute | Value |
|---|---|
| Image | `cip-fastapi:latest` (custom build from `./api/Dockerfile`) |
| Container name | `cip-fastapi` |
| Internal port | 8000 |
| External port | `8000:8000` (API access + Swagger UI) |
| Environment | `DATABASE_URL`, `DB_POOL_MIN=5`, `DB_POOL_MAX=20` from `.env` |
| Volumes | None (stateless) |
| Depends on | `postgres` (healthy) |
| Health check | `curl --fail http://localhost:8000/health` every 15s |
| `mem_limit` | `512m` |
| `cpus` | `0.5` |

#### `metabase`

| Attribute | Value |
|---|---|
| Image | `metabase/metabase:v0.48.3` |
| Container name | `cip-metabase` |
| Internal port | 3000 |
| External port | `3000:3000` (Dashboard UI) |
| Environment | `MB_DB_TYPE=postgres`, `MB_DB_HOST=postgres`, `MB_DB_PORT=5432`, `MB_DB_DBNAME`, `MB_DB_USER`, `MB_DB_PASS` from `.env` (Metabase's own metadata DB — separate from CIP data DB) |
| Volumes | `metabase_data:/metabase-data` |
| Depends on | `postgres` (healthy) |
| Health check | `curl --fail http://localhost:3000/api/health` every 30s |
| `mem_limit` | `1g` |
| `cpus` | `0.5` |

#### `data-generator`

| Attribute | Value |
|---|---|
| Image | `cip-generator:latest` (custom build from `./generator/Dockerfile`) |
| Container name | `cip-data-generator` |
| Role | One-shot seed container; runs synthetic data generation then exits |
| Environment | `DATABASE_URL`, `SEED_CUSTOMERS=100000`, `SEED_EVENTS_MIN=1000000`, `SEED_EVENTS_MAX=5000000`, `SEED_ORDERS=250000`, `SEED_TICKETS=50000` |
| Depends on | `postgres` (healthy) |
| Restart | `on-failure` (max 3) |
| Health check | None (exits on completion) |
| `mem_limit` | `1g` |
| `cpus` | `1.0` |

### Named Volumes

```yaml
volumes:
  postgres_data:       # PostgreSQL data directory
  airflow_logs:        # Airflow task logs (90-day retention)
  mlflow_artifacts:    # MLflow model artifacts and feature snapshots
  metabase_data:       # Metabase application data and dashboard config
```

### Network Isolation

```yaml
networks:
  cip_internal:
    driver: bridge
    internal: true   # No external internet access for data services
```

All services attach to `cip_internal`. No service exposes ports to `0.0.0.0` except UI services (Airflow, MLflow, FastAPI, Metabase) which bind to `127.0.0.1:{port}:{port}` in the default configuration, accessible only from localhost.


---

## Local Development Workflow

### First-Run Initialization Sequence

```
1. cp .env.example .env                         # Copy and edit secrets
2. docker compose build                          # Build custom images (fastapi, generator)
3. docker compose up -d postgres                 # Start DB first
   (wait for postgres healthy)
4. docker compose up -d airflow-init             # Init Airflow schema, create admin user
   (wait for airflow-init to exit 0)
5. docker compose up -d                          # Start all remaining services
   (wait for all services healthy: ~5 min)
6. data-generator container runs automatically   # Seeds raw tables (automated)
7. Airflow triggers master_pipeline DAG          # Auto-triggered within 5 min of healthy state
8. Pipeline completes (target: < 30 min)         # All marts, ML scores, insights populated
```

The initialization script (`infra/init.sh`) is called by the `airflow-init` container and performs:
- Creates PostgreSQL schemas: `raw`, `staging`, `intermediate`, `marts`, `ml`, `observability`
- Creates all PostgreSQL roles (see Security Design)
- Grants all role permissions
- Checks for existing schemas (idempotency — skips if already present)
- Does NOT re-trigger DAGs if `pipeline_run_log` contains a successful run for today

### Makefile Targets

```makefile
make setup      # docker compose build + pull + env check
make run        # docker compose up -d (all services)
make stop       # docker compose down
make test       # Run dbt tests + GE checkpoints + pytest for API
make docs       # dbt docs generate + dbt docs serve (opens browser)
make clean      # docker compose down -v (DESTRUCTIVE: removes volumes)
make logs       # docker compose logs -f (tail all services)
make reset-db   # Drop and recreate all CIP schemas (DESTRUCTIVE)
make trigger    # Manually trigger master_pipeline DAG via Airflow REST API
make lint       # ruff check on all Python; sqlfluff lint on dbt models
```

### Developer Iteration Loops

**dbt model change**:
```
1. Edit model SQL in dbt/models/
2. dbt run --select <model_name> --profiles-dir dbt/ (inside airflow-worker or local venv)
3. dbt test --select <model_name>
4. Review output in Metabase / psql
```

**New Airflow DAG**:
```
1. Add Python file to dags/
2. Airflow scheduler auto-detects within ~30s (file watcher)
3. Trigger manually from Airflow UI for testing
4. Check logs in Airflow UI → task logs
```

**ML model change**:
```
1. Edit ml/models/<model>.py
2. Run python ml/scoring/score_all.py --run-date YYYY-MM-DD (local venv with DuckDB)
3. Check MLflow UI (localhost:5000) for new experiment run
4. Verify mart_ml_scores updated via psql or FastAPI /customers/{id}/scores
```

**API endpoint change**:
```
1. Edit api/routers/<domain>.py
2. docker compose restart fastapi (hot-reload in dev mode via uvicorn --reload)
3. Test via http://localhost:8000/docs
4. Run pytest api/tests/ for regression
```


---

## Security Design

### PostgreSQL Role and Permission Matrix

| Role | Schema Access | Permissions | Used By |
|---|---|---|---|
| `postgres` (superuser) | All | All | Init scripts only — never used at runtime |
| `raw_writer` | `raw` | INSERT, UPDATE, SELECT | `data-generator`, Airflow ingestion tasks |
| `staging_writer` | `staging`, `intermediate` | CREATE, INSERT, UPDATE, SELECT, DELETE | Airflow dbt staging/intermediate tasks |
| `mart_writer` | `marts`, `ml`, `observability` | CREATE, INSERT, UPDATE, SELECT, DELETE | Airflow dbt mart tasks, ML scoring pipeline |
| `mart_reader` | `marts`, `ml`, `observability` | SELECT only | FastAPI connection pool |
| `metabase_reader` | `marts` only | SELECT only | Metabase JDBC connection |
| `airflow_meta` | `airflow` (separate DB) | All on airflow DB | Airflow internal — isolated DB |
| `mlflow_meta` | `mlflow` (separate DB) | All on mlflow DB | MLflow backend store |

Role assignment:
- `raw_writer`: Used by generator and ingestion DAG tasks
- `staging_writer`: Used only during dbt staging + intermediate runs
- `mart_writer`: Used only during dbt mart runs + ML scoring writes
- `mart_reader`: FastAPI asyncpg pool connects with this role — read-only, no write surface
- `metabase_reader`: Most restricted role; cannot see any non-mart schema

Schema-level isolation is enforced by explicit `REVOKE ALL ON SCHEMA raw FROM PUBLIC` at init time, then only the specific role is granted access.

### Environment Variable and Secret Management

- All credentials (DB passwords, Fernet key, API keys) live in `.env` at project root
- `.env` is listed in `.gitignore` — never committed
- `.env.example` is committed with placeholder values and descriptive comments for every variable
- Docker Compose reads `.env` automatically for variable substitution
- No secret values appear in `docker-compose.yml` or any source file
- `CHANGELOG.md` documents when environment variable names change

Required `.env` variables (sample, not exhaustive):
```
# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=cip_admin
POSTGRES_PASSWORD=<secret>
POSTGRES_DB=cip

# Airflow
AIRFLOW_FERNET_KEY=<generated>
AIRFLOW_SECRET_KEY=<generated>
AIRFLOW_ADMIN_PASSWORD=<secret>

# FastAPI
DATABASE_URL=postgresql+asyncpg://mart_reader:<pw>@postgres:5432/cip
DB_POOL_MIN=5
DB_POOL_MAX=20

# MLflow
MLFLOW_BACKEND_STORE_URI=postgresql://mlflow_meta:<pw>@postgres:5432/mlflow
MLFLOW_TRACKING_URI=http://mlflow:5000

# Metabase
METABASE_DB_PASSWORD=<secret>
MB_DB_USER=cip_admin
MB_DB_PASS=<secret>
MB_DB_DBNAME=metabase
```

### No-PII Enforcement

The synthetic data generator creates data using Faker with no real-world PII constraints. The platform design does not collect, store, or expose real personal data. API responses expose synthetic names and emails as-is from the mart layer (acceptable since all data is synthetic per the requirements constraint).

In a production migration, PII masking would be applied at the Staging Layer before mart exposure — this is the designated extension point.

### Parameterized Query Pattern

FastAPI uses `asyncpg` which enforces parameterized queries natively via `$1, $2, ...` positional parameters. No string formatting of user-provided values is permitted. Example:

```python
# CORRECT
await conn.fetchrow("SELECT * FROM marts.mart_customers WHERE customer_id = $1", customer_id)

# NEVER
await conn.fetchrow(f"SELECT * FROM marts.mart_customers WHERE customer_id = '{customer_id}'")
```

All query helpers in `utils/db.py` enforce this pattern via a typed wrapper that only accepts parameterized queries.

### Docker Network Isolation

The `cip_internal` bridge network is configured with `internal: true` in Docker Compose, preventing any container from making outbound internet requests. This isolates all services from external network access. UI ports are bound to `127.0.0.1` only on the host, not `0.0.0.0`.


---

## Error Handling

### Layer-Specific Error Handling

#### Ingestion Layer
- On DB connection failure: retry up to 3 times (5-min delay). On exhaustion: task marked failed, downstream DAG blocked.
- On upsert key conflict: log warning + continue (idempotent by design).
- On data generation error: task fails immediately (no partial writes — generator uses transactions).

#### dbt Transformation Layer
- On model compilation error: Airflow task fails, no partial mart update.
- On staging schema test failure: halt intermediate layer run for that domain; write to `dq_failures`; emit alert log.
- On mart volume test deviation: log WARNING only; pipeline continues. Mart tables remain from prior successful run until current run completes successfully.

#### ML Scoring Layer
- On churn scoring run failure: retain prior day's values in `mart_ml_scores`; log ERROR. No null propagation to API.
- On LTV/segmentation failure: task fails; prior day's values are retained in mart (incremental model does not overwrite if insert task fails).
- On MLflow unavailability: ML tasks log WARNING and skip model registration; scoring continues.
- On anomaly detection with < 30 days of data: set `baseline_pending` status; no flag emitted; no task failure.

#### API Layer
- All database exceptions caught by a global exception handler → 503 with sanitized message.
- All validation errors caught by FastAPI's built-in validator → 422.
- 404 raised explicitly when `fetchrow` returns None.
- No stack traces exposed in API responses.

#### Insights Generator
- If any required mart table is missing or returns no rows: task fails, prior day's `ml_insights` record is preserved as latest, missing table name is logged.
- Partial output is never written to `ml_insights`.

### Cascade Failure Design

The master pipeline DAG uses `trigger_rule=ALL_SUCCESS` on each sequential task group. A failure in GE quality gates halts all transformation tasks for that domain but allows other domain pipelines to proceed if the failure is domain-isolated. The master pipeline's SLA timer continues regardless of individual domain failures, ensuring the SLA miss is correctly detected and logged even during partial failures.

---

## Testing Strategy

### Overview

The CIP uses a dual testing approach: example-based unit tests for specific behaviors and edge cases, combined with property-based tests for universal behavioral invariants across the ML scoring and data transformation logic.

### Unit and Integration Tests

**Synthetic Data Generator** (`tests/generator/`):
- Test that exactly 100,000 customer records are generated
- Test that all order customer_ids resolve to valid customer records (no orphan FKs)
- Test that campaign clicks ≤ impressions for all records
- Test idempotent re-run produces identical row count

**dbt Model Tests** (dbt schema YAML):
- All PK uniqueness and not-null tests per layer
- All FK relationship tests
- Accepted-values tests on all enum columns
- Custom volume tests on mart layer

**Great Expectations** (`great_expectations/`):
- One suite per raw table with expectations defined above
- Checkpoint execution tested in CI via `great_expectations checkpoint run <suite>`

**FastAPI** (`api/tests/`):
- Test `GET /health` returns 200 when DB is reachable
- Test `GET /health` returns 503 when DB is unreachable (mocked)
- Test `GET /customers/{id}` returns 404 for unknown ID
- Test `GET /customers/{id}` returns 422 for malformed UUID
- Test `GET /segments` respects limit/offset pagination
- Test `GET /segments` returns 422 when limit > 1000
- Test `GET /anomalies` filters by severity correctly
- Test all endpoints return 503 on DB connection failure (mocked)

**ML Models** (`tests/ml/`):
- Test segmentation assigns exactly one label per active customer
- Test LTV produces non-negative scores for all customers
- Test churn score is within [0.0, 1.0] for all customers
- Test churn risk tier assignment matches score thresholds (Low/Medium/High)
- Test anomaly detection with < 30 days of data emits `baseline_pending` (not a flag)
- Test NLP produces between 5 and 15 cluster labels


---

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 0: Overview and Reflection

Before writing properties, the following redundancies were identified and consolidated:

- Requirements 3.6, 6.2, and 7.1 all constrain `mart_ml_scores` value ranges (LTV ≥ 0, churn ∈ [0,1]). These are consolidated into a single **mart_ml_scores value bounds** property.
- Requirements 1.2, 1.6 both address referential integrity (event FK, order FK, ticket FK). Consolidated into one **referential integrity** property covering all three tables.
- Requirements 3.5 and 5.1/5.2 address RFM score format and component bounds. Consolidated into one **RFM score invariant** property.
- Requirement 7.3 (tier assignment logic) is a distinct property from the score bounds, retained separately.
- Requirements 9.1 and 9.3 address distinct NLP invariants (cluster count range vs. ticket assignment completeness), retained as separate properties.

**Validates: Requirements 1.1, 1.2, 1.6, 3.5, 3.6, 5.1, 5.2, 6.2, 7.1, 7.3, 9.1, 9.3**

---

### Property 1: Referential Integrity of Generated Data

*For any* record in `raw.events`, `raw.orders`, or `raw.tickets`, the `customer_id` field must resolve to a valid record in `raw.customers`. No orphan foreign keys shall exist after generation or re-generation.

**Validates: Requirements 1.2, 1.6**

---

### Property 2: Campaign Clicks Never Exceed Impressions

*For any* campaign record in `raw.campaigns`, the `clicks` value must be less than or equal to the `impressions` value.

**Validates: Requirements 1.4**

---

### Property 3: Idempotent Data Generation

*For any* run date, executing the synthetic data generator a second time for the same run date must produce an identical row count in each raw table as the first execution — the data generation is idempotent under re-execution.

**Validates: Requirements 1.8, 14.7**

---

### Property 4: Staging Deduplication Preserves Exactly One Row Per Primary Key

*For any* staging model output, no primary key value appears more than once. When the source data contains multiple records with the same primary key, the surviving record in the staging model must always be the one with the latest `_ingested_at` timestamp.

**Validates: Requirements 3.1**

---

### Property 5: Derived Intermediate Fields Are Mathematically Consistent

*For any* session record in the intermediate layer, `session_duration_seconds` must be non-negative and equal to the difference between `session_end` and `session_start` in seconds. *For any* order record in the intermediate layer, `avg_item_value_usd` must equal `total_amount_usd` divided by `item_count`, and `item_count` must be at least 1.

**Validates: Requirements 3.3**

---

### Property 6: RFM Score Invariants

*For any* customer in `mart_customer_360`, the `recency_score`, `frequency_score`, and `monetary_score` fields must each be integers in the range [0, 5]. The `rfm_score` string must match the pattern `R{r}F{f}M{m}` where `r`, `f`, `m` correspond exactly to the respective dimension scores. Additionally, `recency_days` must never exceed 999.

**Validates: Requirements 3.5, 5.1, 5.2**

---

### Property 7: Inactive Customers Receive the Correct Default Scores

*For any* customer with `order_frequency_365d = 0` (no orders in the trailing 365 days), the `segment_label` in `mart_ml_scores` must be exactly `Inactive` and the `rfm_score` in `mart_customer_360` must be exactly `R0F0M0`.

**Validates: Requirements 5.5**

---

### Property 8: mart_ml_scores Value Bounds

*For any* row in `mart_ml_scores`, the following invariants must hold simultaneously: `ltv_score ≥ 0.00`, `churn_score ∈ [0.0, 1.0]`, and `churn_risk_tier ∈ {Low, Medium, High}`.

**Validates: Requirements 3.6, 6.2, 7.1**

---

### Property 9: Churn Risk Tier Is Determined Exactly by Score Thresholds

*For any* row in `mart_ml_scores`, the `churn_risk_tier` value must be determined exclusively by the `churn_score` value according to the threshold mapping: `Low` when `churn_score < 0.33`, `Medium` when `0.33 ≤ churn_score < 0.67`, and `High` when `churn_score ≥ 0.67`. No other assignment is valid.

**Validates: Requirements 7.3**

---

### Property 10: Anomaly Flag Is Set Exactly When Z-Score Exceeds Threshold

*For any* metric observation in `ml.anomaly_metrics` where at least 30 days of baseline data exist, `anomaly_flag` must be `TRUE` if and only if `|observed_value - rolling_mean_30d| / rolling_std_30d ≥ 2.0`. No flag is set (and `severity = baseline_pending`) when fewer than 30 days of baseline are available.

**Validates: Requirements 8.2, 8.5**

---

### Property 11: NLP Cluster Count Is Within the Valid Range

*For any* completed NLP run, the count of distinct `cluster_id` values assigned across all processed tickets must be an integer in the range [5, 15] inclusive.

**Validates: Requirements 9.1**

---

### Property 12: Every Processed Ticket Has a Valid Cluster Assignment

*For any* ticket record in `mart_support_tickets` that has been processed by the NLP model (i.e., `cluster_id IS NOT NULL`), the `cluster_confidence` value must be in the range [0.00, 1.00] inclusive.

**Validates: Requirements 9.3**

---

### Property 13: Daily Insight JSON Contains All Required Fields

*For any* insight record in `observability.ml_insights`, the `insight_json` must be parseable as valid JSON and must contain non-null values for all required fields: `top_segments` (array of ≥3 entries), `highest_ltv_segment`, `high_churn_count`, `anomalies` (either a summary string or `"None detected"`), and `top_ticket_clusters` (array of ≥2 entries).

**Validates: Requirements 10.2, 10.4**

---

### Property 14: API Parameter Validation Blocks All Invalid Inputs

*For any* request to any API endpoint containing a parameter value that violates the declared type or constraint (malformed UUID, negative offset, limit > 1000, unrecognized enum value), the API must return HTTP 422 and must not execute any database query.

**Validates: Requirements 11.3, 11.6**

---

### Property 15: Idempotent Initialization Script

*For any* already-initialized CIP environment, running the initialization script a second time must leave the schema structure and seed data row counts identical to the state before the second run — no schemas are recreated, no seed data is re-inserted, no DAGs are re-triggered.

**Validates: Requirements 14.7**


---

## Future Scalability Roadmap

### Phase 2: Cloud Migration Mapping

Each Docker Compose service maps to a managed cloud equivalent with zero application code changes — only `docker-compose.yml` configuration and environment variables change.

| CIP Service | AWS Equivalent | GCP Equivalent | Notes |
|---|---|---|---|
| `postgres` | Amazon RDS PostgreSQL | Cloud SQL (PostgreSQL) | Connection string only changes in `.env` |
| `airflow-scheduler/webserver/worker` | Amazon MWAA | Cloud Composer | DAG code unchanged (standard operators only) |
| `mlflow` | Amazon SageMaker Model Registry + S3 artifact store | Vertex AI Metadata + GCS | MLflow client API unchanged; backend config changes |
| `fastapi` | AWS Lambda + API Gateway OR ECS Fargate | Cloud Run | Stateless FastAPI — no code changes for container deploy |
| `metabase` | Self-hosted on EC2 OR replaced with Looker/Quicksight | Self-hosted on GCE OR replaced with Looker Studio | Connection config change only |
| `postgres (MLflow backend)` | Amazon RDS PostgreSQL (separate instance) | Cloud SQL | |
| Named volumes (data) | Amazon EFS or S3-backed EBS | Cloud Filestore or GCS | Volume mounts → managed storage paths |

Migration strategy: Replace one service at a time using Docker Compose's `external` service references. The application code's only coupling to infrastructure is via environment variables.

### Phase 3: Streaming Extension

The batch layer remains unchanged. A streaming lane is added alongside it, feeding the same `raw` schema via a dedicated ingestion path.

```
[Kafka / Kinesis]   ←── Clickstream events (real-time)
        │
        ▼
[Flink / Spark Streaming]   ─── micro-batch writes ──▶  raw.events (same schema)
        │                                                     │
        │                         (batch dbt runs use same    │
        │                          raw.events table — no      │
        │                          schema changes needed)      ▼
        │                                              [Staging Layer]  ← unchanged
        │                                              [Intermediate]   ← unchanged
        │                                              [Mart Layer]     ← unchanged
        │
        └──▶ [Near-real-time Churn Score Update]
              (stream processor writes directly to ml.ml_scores
               for high-volume customer sessions, bypassing daily batch)
```

Key design decisions that enable this without breaking the batch layer:
- `raw.events` uses `_ingested_at` not `_run_date` as the stream partition key
- dbt incremental models filter by `_run_date` — streaming writes with today's date are included in the next batch run automatically
- The `master_pipeline` DAG gains a `wait_for_streaming_lag` sensor that checks Kafka consumer group lag before triggering transformation

### Phase 4: Feature Store and Drift Monitoring

```
┌─────────────────────────────────────────────────────────┐
│  Phase 4 Additions (no Phase 1/2/3 changes)              │
│                                                           │
│  [Feast Feature Store]  ←── mart_customer_360 as source  │
│       │                     Serves features to models    │
│       │                     without re-querying PG        │
│       ▼                                                   │
│  [Evidently / Whylogs]  ←── Model predictions + actuals  │
│       │                     Detects feature drift and     │
│       │                     prediction distribution shift │
│       │                                                   │
│       ▼                                                   │
│  [Automated Retraining DAG]                               │
│       │                                                   │
│       └──▶ Triggers ML model retraining when drift       │
│             score exceeds threshold                       │
│             Reuses existing ml/ module structure          │
└─────────────────────────────────────────────────────────┘
```

The extension point for Phase 4 is the `ml/features/feature_store.py` module, which is designed to be replaced by a Feast online store client without changing model training code. Drift monitoring attaches to the `ml_scoring` DAG as a downstream task consuming `ml.ml_scores` outputs.

