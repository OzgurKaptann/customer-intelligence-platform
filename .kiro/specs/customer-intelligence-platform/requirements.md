# Requirements Document

**Customer Intelligence Platform**

## Introduction

The Customer Intelligence Platform (CIP) is a production-grade batch analytics system that ingests, transforms, models, and activates customer data across CRM, e-commerce, web events, marketing campaigns, and support channels. It delivers AI/ML-powered insights — including customer segmentation, lifetime value scoring, churn risk, anomaly detection, and NLP-based support summarization — to business users, analysts, and engineers via dashboards, SQL data marts, and a REST API.

This project is designed as a portfolio artifact demonstrating the engineering practices of a modern data team: modular dbt transformations, orchestrated Airflow pipelines, MLflow-tracked models, Great Expectations data quality gates, and a FastAPI serving layer — all deployed locally via Docker Compose with a cloud-migration-ready architecture.

---

## Glossary

- **CIP**: Customer Intelligence Platform — the system defined in this document.
- **Ingestion_Layer**: The component responsible for loading raw data from all source systems into the Raw_Zone.
- **Raw_Zone**: The PostgreSQL schema (`raw`) containing unmodified, append-only source data.
- **Staging_Layer**: The dbt layer (`stg_*` models) that cleans, casts, and standardizes Raw_Zone data.
- **Intermediate_Layer**: The dbt layer (`int_*` models) that joins and enriches Staging_Layer models.
- **Mart_Layer**: The dbt layer (`mart_*` models) that produces business-facing, analytics-ready tables.
- **ML_Layer**: The suite of MLflow-tracked models that produce segmentation, LTV, churn, and anomaly scores.
- **API_Layer**: The FastAPI service that exposes Mart_Layer and ML_Layer outputs as REST endpoints.
- **Dashboard_Layer**: The Metabase instance connected to the Mart_Layer for business user consumption.
- **Orchestrator**: Apache Airflow, responsible for scheduling and sequencing all pipeline DAGs.
- **Quality_Gate**: A Great Expectations checkpoint or dbt test that must pass before a pipeline stage proceeds.
- **Customer**: A unique individual or account record present in the CRM source data.
- **Event**: A single clickstream interaction (page view, click, session start/end) from the web or mobile surface.
- **Order**: A completed or attempted e-commerce transaction associated with a Customer.
- **Campaign**: A paid marketing effort (Google Ads or Meta Ads style) with associated spend and performance data.
- **Ticket**: A customer support interaction record containing free-text description and resolution metadata.
- **RFM_Score**: A composite Recency–Frequency–Monetary score derived from Order history per Customer.
- **LTV_Score**: A predicted lifetime value in currency units assigned to each Customer by the LTV_Model.
- **Churn_Score**: A predicted probability (0–1) of a Customer churning within 90 days.
- **Segment**: A named cluster label assigned to each Customer by the Segmentation_Model.
- **Anomaly_Flag**: A boolean indicator set by the Anomaly_Model when a metric deviates beyond expected bounds.
- **DAG**: A Directed Acyclic Graph — an Airflow pipeline definition.
- **SLA**: Service Level Agreement — a time-bound commitment for pipeline or API behavior.
- **Docker_Compose**: The local container orchestration tool used to run all CIP services.
- **DuckDB**: A lightweight in-process analytical database used for local ML feature engineering experiments.
- **MLflow**: The experiment tracking and model registry used by the ML_Layer.
- **Great_Expectations**: The data quality framework used for schema and value validation.
- **dbt**: Data build tool — the SQL transformation framework used across Staging, Intermediate, and Mart layers.


---

## Product Vision and Problem Statement

Modern businesses generate customer data across dozens of disconnected systems — CRM records, web events, purchase histories, ad campaigns, and support queues — yet rarely unify them into a coherent picture of customer behavior and health. The result is reactive decision-making, wasted marketing spend, and undetected churn.

The Customer Intelligence Platform solves this by creating a single, governed, analytics-ready view of every customer. It applies machine learning to score, segment, and flag customers automatically, and surfaces these insights where decisions are made: in dashboards for business users, in SQL marts for analysts, and in APIs for product engineers.

The platform is explicitly designed to demonstrate production engineering standards: schema-enforced ingestion, layered dbt transformations, experiment-tracked ML models, automated data quality gates, and a portable Docker Compose deployment that mirrors cloud-native architecture patterns.

---

## Business Objectives

1. Unify customer data from five distinct source domains into a single governed data model within daily batch SLAs.
2. Produce ML-derived scores (LTV, churn risk, segment) for all active Customers by end of each daily pipeline run.
3. Detect anomalies in marketing spend, revenue, and engagement metrics and surface them to stakeholders within 24 hours of occurrence.
4. Reduce analyst time spent on data preparation by providing clean, documented, tested SQL marts.
5. Demonstrate a reusable, cloud-agnostic architecture that can migrate to AWS or GCP without code rewrites.
6. Serve as a portfolio reference demonstrating senior-level data engineering, analytics engineering, and ML engineering practices.

---

## Target Users and Personas

### Persona 1: Business Analyst / Marketing Manager
- **Goal**: Understand customer segments, campaign ROI, and churn risk trends without writing SQL.
- **Interface**: Metabase dashboards.
- **Pain Point**: Currently works from spreadsheet exports; lacks real-time segment visibility.

### Persona 2: Data Analyst
- **Goal**: Query clean, well-documented data marts to build ad-hoc reports and answer business questions.
- **Interface**: Direct SQL access to Mart_Layer via PostgreSQL; dbt documentation site.
- **Pain Point**: Spends 60% of time cleaning raw data before analysis.

### Persona 3: Data / ML Engineer
- **Goal**: Build, retrain, and monitor ML models; extend pipelines; consume model outputs via API.
- **Interface**: Airflow UI, MLflow UI, FastAPI endpoints, dbt CLI.
- **Pain Point**: No existing ML infrastructure; models trained ad hoc with no tracking or versioning.

### Persona 4: Portfolio Reviewer / Technical Evaluator
- **Goal**: Assess engineering quality, architectural decision-making, and production readiness.
- **Interface**: README, architecture diagrams, dbt docs, this requirements document, Docker Compose setup.
- **Pain Point**: Most portfolio projects lack production-grade observability, testing, and documentation.


---

## Requirements

### Requirement 1: Synthetic Data Generation

**User Story:** As a data engineer, I want realistic synthetic source data generated and seeded into the system, so that the platform has a consistent, reproducible dataset for development, testing, and portfolio demonstration.

#### Acceptance Criteria

1. THE Ingestion_Layer SHALL generate synthetic CRM records for exactly 100,000 unique Customers, each containing a unique customer ID, name, email address, acquisition channel (one of: `organic`, `paid_search`, `social`, `referral`, `direct`), country (ISO 3166-1 alpha-2 code), and account creation date (UTC date).
2. THE Ingestion_Layer SHALL generate between 1,000,000 and 5,000,000 synthetic Event records distributed across the 100,000 Customers such that every Customer has at least 1 Event record, each containing a session ID, customer ID, event type, page URL, device type, and UTC timestamp.
3. THE Ingestion_Layer SHALL generate exactly 250,000 synthetic Order records associated with the Customer population, each containing an order ID, customer ID, order status (one of: `completed`, `pending`, `cancelled`, `refunded`), between 1 and 10 line items per order (each with a product ID, quantity, and unit price in USD), total amount in USD, and UTC order timestamp.
4. THE Ingestion_Layer SHALL generate between 500 and 2,000 synthetic Campaign records representing Google Ads and Meta Ads campaigns, each containing a campaign ID, platform name (one of: `google_ads`, `meta_ads`), campaign name, daily spend in USD (non-negative), impressions (non-negative integer), clicks (non-negative integer not exceeding impressions), and date.
5. THE Ingestion_Layer SHALL generate exactly 50,000 synthetic Ticket records, each containing a ticket ID, customer ID, subject line, free-text description (minimum 10 words), status (one of: `open`, `in_progress`, `closed`), priority level (one of: `low`, `medium`, `high`), and UTC creation timestamp.
6. WHEN synthetic data is generated, THE Ingestion_Layer SHALL produce referential integrity between Orders, Events, and Tickets back to the Customer table such that every foreign key resolves to a valid Customer record, verified by a post-generation assertion that returns zero orphan foreign keys.
7. THE Ingestion_Layer SHALL persist all generated synthetic data to the Raw_Zone in PostgreSQL using append-only insert semantics, preserving the original source schema without transformation.
8. WHEN the synthetic data generation script is executed more than once, THE Ingestion_Layer SHALL NOT insert duplicate records into the Raw_Zone; it SHALL use an upsert strategy keyed on each domain's primary key to ensure idempotent re-runs.

---

### Requirement 2: Orchestrated Ingestion Pipelines

**User Story:** As a data engineer, I want all data ingestion to be orchestrated through Airflow DAGs, so that pipeline execution is scheduled, observable, and recoverable.

#### Acceptance Criteria

1. THE Orchestrator SHALL execute a daily ingestion DAG for each of the five source domains (CRM, Events, Orders, Campaigns, Tickets) on a configurable cron schedule defaulting to 02:00 UTC.
2. WHEN an ingestion DAG run starts, THE Orchestrator SHALL log the run start time, source domain, and target Raw_Zone table name.
3. WHEN an ingestion DAG run completes successfully, THE Orchestrator SHALL log the row count inserted and the run duration in seconds.
4. IF an ingestion DAG task fails, THEN THE Orchestrator SHALL retry the task up to 3 times with a 5-minute delay between attempts; IF all 3 retry attempts fail, THEN THE Orchestrator SHALL mark the task as permanently failed, cease execution of downstream tasks within that DAG, and retain the failure state for operator review.
5. THE Orchestrator SHALL enforce that the transformation DAG for a given domain does not start until the corresponding ingestion DAG for that same daily run has completed successfully; the transformation DAG SHALL remain in a `pending` state observable in the Airflow Web UI until that condition is met.
6. WHEN an ingestion DAG run is manually re-triggered for a prior date, THE Orchestrator SHALL support idempotent re-execution without creating duplicate records in the Raw_Zone; each Raw_Zone table SHALL have a declared idempotency strategy (either date-partitioned upsert keyed on the source primary key or truncate-and-reload for the target partition), and a re-triggered run SHALL produce an identical row count to the original successful run for the same date.


---

### Requirement 3: Layered dbt Transformations

**User Story:** As a data analyst, I want a structured, documented transformation layer built with dbt, so that I can query clean, well-typed, and business-contextualized data without touching raw source tables.

#### Acceptance Criteria

1. WHEN a Staging_Layer model is materialized, THE Staging_Layer SHALL produce one staging model per source table that applies column renaming to snake_case, explicit data type casting, and removal of duplicate records using source primary keys; WHERE duplicate records share the same primary key, the record with the latest ingestion timestamp SHALL be retained.
2. THE Staging_Layer SHALL expose no business logic; all Staging_Layer models SHALL contain only structural cleaning operations (renaming, casting, deduplication), and any column that cannot be mapped to a structural operation SHALL be escalated to the Intermediate_Layer.
3. THE Intermediate_Layer SHALL join Staging_Layer models to produce unified customer-level, order-level, and session-level grain tables that resolve foreign keys and compute derived fields; derived fields SHALL include at minimum: session duration in seconds (event end timestamp minus start timestamp), order item count (count of line items per order), and order average item value in USD.
4. THE Mart_Layer SHALL produce the following named marts: `mart_customers`, `mart_orders`, `mart_campaigns`, `mart_support_tickets`, `mart_customer_360`, and `mart_ml_scores`.
5. THE Mart_Layer model `mart_customer_360` SHALL contain one row per Customer and include fields for: individual RFM dimension scores (recency_score, frequency_score, monetary_score, each as integer 0–5), composite RFM_Score string, total order count (all time), total spend in USD (all time), most recent event date (UTC), active Campaign exposure count (campaigns with spend in the trailing 30 days), and open Ticket count (tickets with status `open` or `in_progress`).
6. THE Mart_Layer model `mart_ml_scores` SHALL contain one row per Customer per daily run date and include: LTV_Score (non-negative decimal in USD), Churn_Score (decimal in [0.0, 1.0]), churn_risk_tier (one of `Low`, `Medium`, `High`), Segment label (string), and Anomaly_Flag (boolean).
7. WHEN a dbt model run completes, THE Staging_Layer, Intermediate_Layer, and Mart_Layer models SHALL each be fully refreshed for the current run date such that all row counts and computed field values in the target tables reflect the current day's ingested data, verifiable by a post-run row count assertion.
8. THE dbt project SHALL include a generated documentation site with non-empty model descriptions and non-empty column descriptions for every column in every Staging_Layer, Intermediate_Layer, and Mart_Layer model, and lineage graphs covering all models; the documentation site SHALL be generatable via `dbt docs generate` without errors.

---

### Requirement 4: Data Quality Gates

**User Story:** As a data engineer, I want automated data quality checks at every layer boundary, so that bad data is detected and quarantined before it propagates downstream into marts or ML models.

#### Acceptance Criteria

1. WHEN a Staging_Layer model is materialized, THE Quality_Gate SHALL execute dbt schema tests — including not-null, unique, accepted-values, and referential-integrity tests — on all primary key and foreign key columns before any dependent Intermediate_Layer models are materialized; IF any such test fails, THE Orchestrator SHALL halt the Intermediate_Layer run for the affected source domain, emit an alert containing the source domain name, model name, and failing test name, and mark the affected DAG branch as failed.
2. WHEN an ingestion DAG run completes, THE Quality_Gate SHALL execute Great Expectations checkpoints on the Raw_Zone tables, validating column presence, non-null rates above 95% for fields designated as required in the source schema contract, and value range constraints as defined in the source schema contract for numeric columns.
3. IF a Great Expectations Quality_Gate checkpoint fails on a Raw_Zone table, THEN THE Orchestrator SHALL halt the downstream transformation DAG for that source domain and emit a pipeline failure alert containing the source domain name, table name, checkpoint name, and failing expectation.
4. THE Quality_Gate SHALL execute dbt tests on Mart_Layer models validating that row counts are within ±20% of the prior day's counts; IF no prior day count exists (first run), THE Quality_Gate SHALL skip the comparison, record a baseline warning in the daily data quality report, and surface that warning in the Orchestrator UI without halting the pipeline; deviations on subsequent runs SHALL be flagged as warnings without halting the pipeline.
5. THE Quality_Gate SHALL produce a daily data quality report once per day at the end of all scheduled pipeline runs, recording: total tests executed (count), tests passed (count), tests failed (count), and the name of each affected table; this report SHALL be persisted as a structured log entry accessible via the Orchestrator UI and retained for a minimum of 90 days.
6. WHEN a Great Expectations checkpoint fails on a Raw_Zone table, THE Quality_Gate SHALL log the source domain, table name, checkpoint name, and failing expectation name to a `dq_failures` table in PostgreSQL.
7. WHEN a dbt test failure occurs in the Staging_Layer, THE Quality_Gate SHALL log the model name, test name, failing column, and the first 10 failing rows ordered by primary key to the `dq_failures` table in PostgreSQL.


---

### Requirement 5: Customer Segmentation (RFM + ML Clustering)

**User Story:** As a marketing manager, I want every customer assigned to a named segment based on purchasing behavior, so that I can target campaigns to the right audience.

#### Acceptance Criteria

1. THE Segmentation_Model SHALL compute an RFM_Score for each Customer using: Recency as the whole number of days since the Customer's most recent Order (capped at 999), Frequency as the total Order count in the trailing 365 days, and Monetary as the total spend in USD in the trailing 365 days (rounded to 2 decimal places).
2. THE Segmentation_Model SHALL assign each Customer to a quintile bucket (1–5) for each of the three RFM dimensions; WHERE two Customers share the same raw RFM dimension value, the lower-ranked Customer SHALL be assigned to the lower bucket to ensure deterministic output; the composite RFM_Score SHALL be derived as a string of the form `R{r}F{f}M{m}` (e.g., `R5F3M4`).
3. WHEN a clustering run is triggered, THE Segmentation_Model SHALL apply a partition-based clustering algorithm (such as k-means) to the RFM feature space, evaluating cluster counts from 4 to 8 inclusive and selecting the count with the highest silhouette score; IF the silhouette scores for all evaluated cluster counts differ by ≤ 0.01, THE Segmentation_Model SHALL default to 4 clusters and log a warning to the Orchestrator run log.
4. WHEN a clustering run completes, THE Segmentation_Model SHALL assign exactly one Segment label to every Customer with at least one Order in the trailing 365 days.
5. WHEN a Customer has no Orders in the trailing 365 days, THE Segmentation_Model SHALL assign the Segment label `Inactive` and an RFM_Score of `R0F0M0`.
6. WHEN a Segmentation_Model training run completes, THE ML_Layer SHALL log the algorithm name, hyperparameters, cluster count, silhouette score, and run timestamp to MLflow.
7. WHEN a Segmentation_Model training run completes successfully, THE ML_Layer SHALL register the trained model version in the MLflow Model Registry under the name `customer_segmentation`; the registered version with the highest silhouette score among all registered versions SHALL be promoted to the `production` stage tag.
8. IF all registered `customer_segmentation` model versions have been evaluated and no version holds the `production` stage tag, THEN THE ML_Layer SHALL promote the version with the highest silhouette score to `production` and log a warning to the Orchestrator run log.

---

### Requirement 6: Customer Lifetime Value (LTV) Scoring

**User Story:** As a business analyst, I want a predicted lifetime value score for every customer, so that I can prioritize high-value retention and acquisition strategies.

#### Acceptance Criteria

1. THE LTV_Model SHALL predict a 12-month forward LTV in USD for each Customer using features derived from the `mart_customer_360` model, including: Order frequency (count of Orders placed within the trailing 365-day window), average order value in USD, customer tenure in days, acquisition channel, and Segment label.
2. THE LTV_Model SHALL produce a non-negative LTV_Score for every Customer present in `mart_customer_360`, with a minimum predicted value of 0.00 USD.
3. WHEN the LTV_Model training run completes, THE ML_Layer SHALL log the algorithm name, feature list, training date range, RMSE computed on a held-out validation set comprising at least 20% of total training samples, and run timestamp to MLflow.
4. IF the LTV_Model validation RMSE is strictly lower than the RMSE of the currently registered `production` model in the MLflow Model Registry under the name `customer_ltv`, THEN THE ML_Layer SHALL register the new model version and promote it to the `production` stage tag; OTHERWISE THE ML_Layer SHALL retain the existing production model and log the comparison result to the Orchestrator run log.
5. THE Mart_Layer model `mart_ml_scores` SHALL be updated with the current day's LTV_Score for all Customers no later than 06:00 UTC of the following calendar day.
6. WHEN a Customer with no Order history is scored and the Customer's acquisition channel cohort has at least one historical LTV observation, THE LTV_Model SHALL return a baseline LTV_Score equal to the mean LTV of that cohort rather than a null value.
7. WHEN a Customer with no Order history is scored and the Customer's acquisition channel cohort has no historical LTV observations, THE LTV_Model SHALL return a baseline LTV_Score of 0.00 USD rather than a null value.


---

### Requirement 7: Churn Risk Scoring

**User Story:** As a customer success manager, I want a daily churn risk score for every active customer, so that I can intervene with at-risk customers before they disengage.

#### Acceptance Criteria

1. THE Churn_Model SHALL predict a churn probability in the range [0.0, 1.0] for each Customer, representing the likelihood of no Order or Event activity in the 90 days following the score date.
2. THE Churn_Model SHALL derive features from `mart_customer_360`, including: days since last Order, days since last Event, Ticket open count, Order frequency trend (Order count in the last 30 days minus Order count in the prior 30 days), and current Segment label.
3. THE Churn_Model SHALL classify each Customer into one of three risk tiers — `Low` (score < 0.33), `Medium` (score >= 0.33 and < 0.67), or `High` (score >= 0.67) — and persist the numeric Churn_Score, the risk tier label, and the score date in `mart_ml_scores`.
4. WHEN the Churn_Model training run completes, THE ML_Layer SHALL log the algorithm name, feature importances (as a key-value map of feature name to importance weight), AUC-ROC on a held-out validation set, and run timestamp to MLflow.
5. IF the Churn_Model validation AUC-ROC exceeds 0.70, THEN THE ML_Layer SHALL register the Churn_Model in the MLflow Model Registry under the name `customer_churn` with a `production` stage tag.
6. IF the Churn_Model validation AUC-ROC falls below 0.70, THEN THE ML_Layer SHALL retain the previously registered production model and log a warning containing the observed AUC-ROC value to the Orchestrator run log.
7. WHEN the daily Churn_Model scoring run is triggered, THE Churn_Model SHALL score every Customer defined as active (having at least one Order or Event in the trailing 365 days) and write results to `mart_ml_scores` for the current run date.
8. IF the Churn_Model scoring run fails, THEN THE ML_Layer SHALL retain the most recent prior day's Churn_Score values in `mart_ml_scores` for all affected Customers and log an ERROR entry to the Orchestrator run log identifying the failure.

---

### Requirement 8: Anomaly Detection in Marketing and Revenue Metrics

**User Story:** As a marketing analyst, I want automated detection of unusual spikes or drops in campaign spend, revenue, and engagement metrics, so that I can investigate data quality issues or unexpected business events within 24 hours.

#### Acceptance Criteria

1. THE Anomaly_Model SHALL evaluate the following time-series metrics daily: total daily revenue (USD), total daily Campaign spend (USD), daily Order count, daily new Customer count, daily Event count, and daily click-through rate per Campaign.
2. THE Anomaly_Model SHALL flag a metric value as anomalous when it deviates from its 30-day rolling mean by 2 or more standard deviations.
3. WHEN an Anomaly_Flag is set for a metric, THE Anomaly_Model SHALL record: the metric name, observed value, expected range (mean ± 2 standard deviations), flag date, and a severity level of `Warning` (deviation >= 2 sigma and < 3 sigma) or `Critical` (deviation >= 3 sigma).
4. THE Mart_Layer model `mart_ml_scores` SHALL be updated with the current day's Anomaly_Flag and anomaly detail records no later than 06:00 UTC of the following calendar day.
5. IF fewer than 30 complete days of data are available for a given metric, THEN THE Anomaly_Model SHALL NOT generate Anomaly_Flags for that metric, as the rolling baseline is insufficiently established; the model SHALL record a `baseline_pending` status for that metric in the anomaly detail output.
6. WHEN a `Critical` severity Anomaly_Flag is set, THE Orchestrator SHALL emit a structured log entry at the ERROR level containing the metric name, observed value, expected range, and flag date, visible in the Airflow task log for the anomaly detection task.


---

### Requirement 9: NLP Summarization of Support Tickets

**User Story:** As a customer success manager, I want AI-generated summaries of support ticket clusters, so that I can understand the most common customer pain points without reading thousands of individual tickets.

#### Acceptance Criteria

1. THE NLP_Processor SHALL apply topic modeling or text clustering to the free-text Ticket description field, grouping Tickets into between 5 and 15 thematic clusters per daily run.
2. THE NLP_Processor SHALL produce a descriptive label of between 1 and 10 words for each cluster, derived from the top 5 statistically representative terms within that cluster.
3. THE NLP_Processor SHALL assign each Ticket to exactly one cluster label and persist the cluster ID, cluster label, and assignment confidence score (decimal in [0.00, 1.00]) in `mart_support_tickets`.
4. THE NLP_Processor SHALL compute, for each cluster: the count of Tickets, the proportion of Tickets with priority `high`, the average resolution time in hours for Tickets with status `closed`, and the date of the most recent Ticket in that cluster.
5. WHEN a new batch of Tickets is processed and a previously fitted topic model exists, THE NLP_Processor SHALL apply that existing model to new Tickets without retraining.
6. IF no previously fitted topic model exists, THEN THE NLP_Processor SHALL perform a full training run on all available Ticket descriptions before assigning clusters.
7. IF the daily Ticket volume for the current run exceeds 500 new records, THEN THE NLP_Processor SHALL trigger a full retraining run instead of applying the existing model.
8. WHEN an NLP_Processor training run completes, THE ML_Layer SHALL log the model type, number of clusters, coherence score, training document count, and run timestamp to MLflow; IF the coherence score falls below 0.40, THE ML_Layer SHALL additionally emit a WARNING log entry to the Orchestrator run log.

---

### Requirement 10: AI-Generated Business Insights

**User Story:** As a business executive, I want AI-generated narrative insights summarizing the state of the customer base each day, so that I can stay informed without interpreting raw dashboards.

#### Acceptance Criteria

1. WHEN the daily pipeline completes all Mart_Layer model runs and ML_Layer scoring runs successfully, THE Insights_Generator SHALL produce a daily narrative summary containing: top 3 Segments by Customer count (ties broken alphabetically by Segment label), the Segment with the highest average LTV_Score, the count of Customers classified with churn_risk_tier `High`, a summary of any active Anomaly_Flags, and the top 2 support Ticket clusters by volume.
2. THE Insights_Generator SHALL format the daily narrative as structured JSON parseable without additional transformation, with named fields for each insight category, suitable for rendering in the Dashboard_Layer or returning via the API_Layer.
3. THE Insights_Generator SHALL derive all narrative content exclusively from Mart_Layer outputs; it SHALL NOT query Raw_Zone or Staging_Layer tables directly.
4. IF no Anomaly_Flags are active for the current run date, THEN THE Insights_Generator SHALL include an explicit `"anomalies": "None detected"` field in the JSON output rather than omitting the field.
5. IF all Mart_Layer model runs and ML_Layer scoring runs for the current date have completed with zero failed tasks, THEN THE Orchestrator SHALL trigger the Insights_Generator task as the final step in the daily pipeline DAG.
6. IF any required Mart_Layer source table is unavailable when the Insights_Generator task executes, THEN THE Insights_Generator SHALL NOT produce partial output; it SHALL mark the Insights_Generator task as failed in the Orchestrator, log the name of the missing source table, and retain the prior day's insight JSON output as the latest available record.


---

### Requirement 11: FastAPI REST Layer

**User Story:** As a product engineer, I want a documented REST API exposing customer scores, segments, and insights, so that I can integrate intelligence outputs into external applications without direct database access.

#### Acceptance Criteria

1. THE API_Layer SHALL expose the following endpoints: `GET /customers/{customer_id}` returning the full `mart_customer_360` record for the specified Customer; `GET /customers/{customer_id}/scores` returning the most recent `mart_ml_scores` record; `GET /segments` returning the list of active Segment labels with Customer counts; `GET /insights/latest` returning the most recent Insights_Generator JSON output; and `GET /anomalies` returning all Anomaly_Flag records for the current run date.
2. WHEN a request is made to `GET /customers/{customer_id}` with a customer ID that does not exist in `mart_customer_360`, THE API_Layer SHALL return an HTTP 404 response with a JSON body containing an `error` field describing the issue and an `id` field echoing the invalid customer ID.
3. THE API_Layer SHALL validate all path and query parameters against defined types and value constraints before executing any database query; IF a parameter fails validation, THEN THE API_Layer SHALL return an HTTP 422 response with a JSON body containing a `detail` array describing each validation failure including the field name, received value, and constraint violated.
4. THE API_Layer SHALL return all responses within 500 milliseconds at the 95th percentile under a sustained load of 50 concurrent requests against the local Docker_Compose deployment, measured over a minimum 60-second load test window.
5. THE API_Layer SHALL expose an OpenAPI specification at `/docs` (Swagger UI) and `/openapi.json` that documents all endpoints, request parameters, path parameters, query parameters, and response schemas including error response shapes.
6. THE API_Layer SHALL implement cursor-based or offset-based pagination on list endpoints (`GET /segments`, `GET /anomalies`) via `limit` and `offset` query parameters, with a default limit of 100 records and a maximum limit of 1,000 records per request; requests exceeding the maximum limit SHALL return an HTTP 422 response.
7. WHEN the PostgreSQL connection is unavailable, THE API_Layer SHALL return an HTTP 503 response with a JSON body containing a `status` field set to `"unavailable"` and a `detail` field describing the connectivity issue, rather than an unhandled exception or HTTP 500 response.

---

### Requirement 12: Metabase Dashboard Layer

**User Story:** As a business user, I want pre-built dashboards in Metabase showing customer health, segment distribution, campaign performance, and churn risk, so that I can make data-driven decisions without writing SQL.

#### Acceptance Criteria

1. THE Dashboard_Layer SHALL provide a **Customer Overview** dashboard displaying: total active Customer count, Customer acquisition trend by month, distribution of Customers by Segment, and top 10 Customers by LTV_Score sorted in descending order.
2. THE Dashboard_Layer SHALL provide a **Churn Risk** dashboard displaying: count and percentage of Customers by churn risk tier (`Low`, `Medium`, `High`), 30-day trend of `High` churn-risk Customer count, and a filterable table of `High` churn-risk Customers with their LTV_Score and last activity date.
3. THE Dashboard_Layer SHALL provide a **Campaign Performance** dashboard displaying: total daily Campaign spend, spend vs. click-through rate by Campaign, and an Anomaly_Flag indicator that visually distinguishes between a true Anomaly_Flag value and an unset/false value for any Campaign metric on the current run date.
4. THE Dashboard_Layer SHALL provide a **Support Intelligence** dashboard displaying: total open Ticket count (tickets with status `open` or `in_progress`), Ticket volume by cluster label, proportion of Tickets with priority field value `high` per cluster, and average resolution time in hours per cluster.
5. THE Dashboard_Layer SHALL connect exclusively to the Mart_Layer schema in PostgreSQL and SHALL NOT be granted access to the Raw_Zone or Staging_Layer schemas.
6. WHEN the daily pipeline run completes and Mart_Layer tables are refreshed, THE Dashboard_Layer SHALL reflect updated data within 5 minutes, verifiable by confirming that dashboard metric values and table row counts match the current Mart_Layer state via Metabase's scheduled query refresh.


---

### Requirement 13: Pipeline Observability and Alerting

**User Story:** As a data engineer, I want full observability into pipeline health — task durations, failure states, data volumes, and SLA adherence — so that I can detect and resolve issues before they affect downstream consumers.

#### Acceptance Criteria

1. THE Orchestrator SHALL expose task-level run metadata — start time, end time, duration in seconds, status, and retry count — for all DAG tasks via the Airflow Web UI and the Airflow REST API.
2. WHEN a DAG task completes successfully, THE Orchestrator SHALL emit a structured log entry at the INFO level containing the task name, run date, and row counts processed by that task.
3. THE Orchestrator SHALL define a pipeline-level SLA of 06:00 UTC, representing the deadline by which all tasks in the daily pipeline DAG (ingestion + transformation + ML scoring + Insights_Generator) must reach `success` status; IF any task in the daily pipeline DAG has not reached `success` status by 06:00 UTC, THEN THE Orchestrator SHALL mark the DAG run with an `sla_miss` flag visible in the Airflow Web UI.
4. WHEN an `sla_miss` flag is set on a DAG run, THE Orchestrator SHALL write a record to the `pipeline_run_log` PostgreSQL table recording: run date, DAG name, status (`sla_miss`), elapsed duration at SLA breach time, row counts per stage processed so far, and Quality_Gate failure counts accumulated to that point.
5. WHEN the PostgreSQL database is reachable, THE API_Layer `GET /health` endpoint SHALL return an HTTP 200 response with a JSON body containing `status: "healthy"`, `db: "connected"`, and the `run_date` of the most recently completed pipeline run from the `pipeline_run_log` table; WHEN the PostgreSQL database is unreachable, THE API_Layer `GET /health` endpoint SHALL return an HTTP 503 response with `status: "unhealthy"` and `db: "disconnected"`.
6. WHERE Docker_Compose resource limits are configured for a container, THE CIP system SHALL log that container's memory usage in MB and CPU usage as a percentage to the Orchestrator run log at the start and end of each DAG run.

---

### Requirement 14: Deployment and Portability

**User Story:** As a portfolio reviewer, I want the entire platform to start with a single command on a local machine, so that I can evaluate the architecture end-to-end without cloud credentials or complex setup.

#### Acceptance Criteria

1. THE CIP system SHALL be fully deployable using `docker compose up` from the project root directory, bringing up all required services: PostgreSQL, Airflow (webserver, scheduler, worker), MLflow, FastAPI, Metabase, and a synthetic data seed container.
2. WHEN `docker compose up` is executed for the first time on a clean environment, THE CIP system's initialization script SHALL create all required PostgreSQL schemas as defined by Airflow and dbt configurations, apply dbt seed files, and automatically trigger the first ingestion and transformation DAG run within 5 minutes of all services reaching a healthy state.
3. THE CIP system SHALL document all required environment variables in a `.env.example` file at the project root, with a descriptive comment for each variable and no secrets committed to source control; the `.env` file SHALL be listed in `.gitignore`.
4. WHEN `docker compose up` is executed on a machine with Docker Engine >= 24.0 and at least 8 GB of RAM allocated to Docker, THE CIP system SHALL reach a fully operational state — defined as all services reporting healthy status and the first ingestion and transformation DAG run reaching `success` status in Airflow — within 30 minutes.
5. THE CIP system SHALL use named Docker volumes for PostgreSQL data and MLflow artifacts, ensuring that pipeline run history persists across `docker compose down` and `docker compose up` cycles without data loss.
6. THE CIP system architecture SHALL separate all service configuration (ports, image versions, resource limits) into the `docker-compose.yml` file such that migrating individual services to cloud-managed equivalents requires changes only to the compose file and environment variables, not to application code.
7. WHEN `docker compose up` is executed on an environment where the initialization script has already been run successfully, THE initialization script SHALL detect the existing state and SHALL NOT re-run schema creation, re-seed data, or re-trigger completed DAGs, ensuring idempotent re-execution.
8. THE `docker-compose.yml` file SHALL define a Docker health check for every service, enabling `docker compose ps` to report each service as `healthy` or `unhealthy` in a deterministic and observable way.


---

## Non-Functional Requirements

### NFR-1: Performance

1. THE Ingestion_Layer SHALL complete the full daily load of all five source domains (100K customers, up to 5M events, 250K orders, campaign data, 50K tickets) within 60 minutes of pipeline start time.
2. THE Staging_Layer and Intermediate_Layer dbt runs SHALL complete within 20 minutes for the full model refresh under the target data volumes.
3. THE Mart_Layer dbt run SHALL complete within 15 minutes for all six named mart models under the target data volumes.
4. THE ML_Layer scoring runs (segmentation, LTV, churn, anomaly) SHALL complete within 30 minutes combined for the full 100K Customer population.
5. THE API_Layer SHALL return responses to all defined endpoints within 500ms at the 95th percentile under a load of 50 concurrent requests against the local Docker_Compose deployment.

### NFR-2: Scalability

1. THE CIP architecture SHALL support increasing the Customer population to 1,000,000 records and Event volume to 50,000,000 records by adding PostgreSQL read replicas and increasing dbt model incremental materialization, without changes to the dbt model SQL logic.
2. THE Orchestrator DAG structure SHALL support adding new source domain DAGs without modifying existing DAGs, using dependency injection via Airflow's `TriggerDagRunOperator` pattern.
3. THE ML_Layer SHALL support adding new model types to the daily scoring pipeline by adding a new MLflow experiment and a new Airflow task, without modifying existing model training code.

### NFR-3: Reliability

1. THE Orchestrator SHALL retry failed ingestion and transformation tasks up to 2 times with a 5-minute delay between retries before marking a task as permanently failed.
2. THE CIP system SHALL preserve all Raw_Zone data written in prior successful pipeline runs even when a subsequent pipeline run fails, ensuring no data loss from partial pipeline failures.
3. THE Mart_Layer SHALL use dbt incremental materialization strategies for large models (Events-derived models with > 1M rows) to prevent full table rebuilds on each daily run.
4. THE API_Layer SHALL implement a connection pool to PostgreSQL with a minimum of 5 and maximum of 20 connections, releasing connections on request completion.

### NFR-4: Maintainability

1. THE dbt project SHALL follow a three-layer naming convention (`stg_`, `int_`, `mart_`) consistently across all model files.
2. THE dbt project SHALL include YAML descriptions for every model and every column in the Mart_Layer, enabling generation of a complete dbt documentation site.
3. THE Orchestrator DAGs SHALL be structured as modular Python files with one DAG per source domain, with shared utility functions extracted to a common `utils` module.
4. THE ML_Layer SHALL encapsulate each model's training, scoring, and registration logic in a separate Python module, with shared feature engineering utilities extracted to a common `features` module.
5. THE CIP system SHALL maintain a `CHANGELOG.md` at the project root documenting breaking changes to mart schemas and API contract changes.

### NFR-5: Portability

1. THE CIP system SHALL use only open-source, vendor-neutral components (PostgreSQL, Airflow, dbt-core, MLflow, FastAPI, Metabase) with no dependency on proprietary managed services in the MVP.
2. THE dbt project SHALL use only dbt-postgres adapter features available in dbt-core >= 1.5, ensuring compatibility with dbt-bigquery and dbt-snowflake adapters without SQL rewrites.
3. THE Orchestrator DAGs SHALL use Airflow's standard operators (BashOperator, PythonOperator, TriggerDagRunOperator) without dependency on provider-specific operators in the MVP.

### NFR-6: Security

1. THE CIP system SHALL store all credentials (database passwords, API keys, service tokens) exclusively in environment variables loaded from a `.env` file that is listed in `.gitignore` and never committed to source control.
2. THE API_Layer SHALL not expose Raw_Zone or Staging_Layer data through any endpoint; all API responses SHALL be derived exclusively from Mart_Layer models.
3. THE Dashboard_Layer database user SHALL be granted read-only access to the Mart_Layer PostgreSQL schema and no access to Raw_Zone, Staging_Layer, or system schemas.
4. THE API_Layer SHALL validate and sanitize all customer ID inputs before constructing database queries, using parameterized query patterns to prevent SQL injection.
5. THE CIP system's Docker_Compose configuration SHALL not expose PostgreSQL, MLflow, or Airflow ports to external network interfaces in the default configuration; all inter-service communication SHALL occur on the internal Docker network.


---

## Success Metrics

### Technical Success Metrics

| Metric | Target | Measurement Method |
|---|---|---|
| Daily pipeline end-to-end duration | < 2 hours from 02:00 UTC trigger to Insights_Generator completion | Airflow DAG run duration |
| Pipeline success rate | ≥ 95% of daily runs complete without human intervention over a 30-day period | `pipeline_run_log` table |
| Data quality test pass rate | ≥ 98% of dbt and Great Expectations tests pass on each daily run | `dq_failures` table row count |
| ML model coverage | 100% of Customers with ≥ 1 Order in trailing 365 days receive a Segment, LTV_Score, and Churn_Score | `mart_ml_scores` null count |
| API p95 response time | < 500ms at 50 concurrent requests | Load test against Docker_Compose deployment |
| dbt documentation coverage | 100% of Mart_Layer models and columns have non-empty descriptions | `dbt docs generate` output |
| First-run setup time | < 30 minutes from `docker compose up` to first successful pipeline run on a clean machine | Manual timing during development |

### Business-Facing Success Metrics

| Metric | Target | Measurement Method |
|---|---|---|
| Segment coverage | All active Customers assigned to a named Segment | `mart_ml_scores.segment` null count |
| Churn model discriminative power | AUC-ROC ≥ 0.70 on validation set | MLflow experiment log |
| LTV model accuracy | RMSE ≤ 25% of mean LTV on validation set | MLflow experiment log |
| Anomaly detection sensitivity | ≥ 1 correctly flagged anomaly per 30-day test period when injected test anomalies are present | Manual validation with synthetic anomaly injection |
| Support cluster coherence | NLP cluster coherence score ≥ 0.40 | MLflow experiment log |
| Dashboard time-to-insight | A business user with no SQL knowledge can answer "Who are my highest-value customers at churn risk?" within 2 minutes using Metabase | Usability walkthrough with reviewer |

---

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Synthetic data lacks statistical realism, making ML models trivial to fit | Medium | High | Apply realistic distributional skew to RFM values; inject controlled noise; validate feature variance before model training |
| Docker_Compose resource constraints cause OOM failures during ML training on local machines | Medium | High | Implement DuckDB-based feature engineering for ML preprocessing to reduce PostgreSQL memory pressure; document minimum 8 GB Docker RAM requirement |
| dbt model runtimes exceed SLA windows at target row counts | Low | Medium | Implement incremental materialization for Events-derived models from day one; benchmark model runtimes before MVP sign-off |
| MLflow model registry drift (production model unregistered after re-training) | Low | Medium | Implement MLflow lifecycle checks at pipeline start; fall back to prior production model if new model fails quality threshold |
| Churn and LTV models underperform due to insufficient feature signal in synthetic data | Medium | Medium | Include temporal features, behavioral trends, and segment labels as features; set minimum AUC-ROC threshold as a Quality_Gate |
| Portfolio reviewer cannot reproduce the full stack locally due to environment differences | Low | High | Provide a tested `Makefile` with `make setup` and `make run` targets; document exact Docker and OS versions tested |
| Schema drift in source tables breaks Staging_Layer models | Low | Medium | Pin source table schemas via Great Expectations column presence checks; add dbt source freshness checks |


---

## Assumptions and Constraints

### Assumptions

1. All source data is synthetic and generated by the CIP system itself; there are no live external API integrations in the MVP.
2. The target deployment environment has Docker Engine >= 24.0 installed with at least 8 GB of RAM and 4 CPU cores allocated to Docker.
3. All pipeline runs are batch-only (daily cadence); there is no requirement for sub-daily or streaming data delivery in the MVP.
4. A single PostgreSQL instance is sufficient for all storage needs at the target data volumes (100K customers, up to 5M events, 250K orders, 50K tickets); horizontal database scaling is out of scope for MVP.
5. The portfolio reviewer has access to a web browser and can reach `localhost` ports to interact with Airflow UI, Metabase, FastAPI docs, and MLflow UI.
6. ML model quality is evaluated against synthetic data; real-world model accuracy metrics are not applicable in the MVP context.
7. dbt incremental strategies will use the `merge` or `delete+insert` pattern on a `run_date` partition key; full historical backfill is not required.

### Constraints

1. The MVP MUST NOT depend on Apache Spark, Delta Lake, Apache Iceberg, or any cloud-managed warehouse (BigQuery, Snowflake, Redshift, Databricks).
2. Real-time streaming pipelines (Kafka, Kinesis, Flink) are out of scope for the MVP; the architecture must allow these to be added in a future phase without redesigning the batch layer.
3. The total uncompressed PostgreSQL data size for the full synthetic dataset MUST remain below 20 GB to be feasible on a development machine.
4. All ML model training must complete without GPU acceleration; models must be trainable on CPU within the ML_Layer SLA window.
5. The platform MUST NOT collect, store, or process real personal data; all Customer data is synthetic and contains no PII.
6. Airflow version >= 2.7.0 and dbt-core version >= 1.5.0 are required; earlier versions are not supported.

---

## Scope

### In Scope (MVP — Phase 1)

- Synthetic data generation for all five source domains at target volumes
- Orchestrated daily batch ingestion pipelines via Airflow DAGs
- Three-layer dbt transformation (Staging, Intermediate, Mart) with full documentation
- Data quality gates via dbt tests and Great Expectations checkpoints
- Customer segmentation (RFM + ML clustering) with MLflow tracking
- LTV scoring with MLflow tracking and model registry
- Churn risk scoring with MLflow tracking and model registry
- Anomaly detection on revenue, spend, and engagement metrics
- NLP topic clustering of support tickets with MLflow tracking
- AI-generated daily business insights as structured JSON
- FastAPI REST layer with OpenAPI documentation
- Metabase dashboards (Customer Overview, Churn Risk, Campaign Performance, Support Intelligence)
- Full Docker Compose deployment with one-command startup
- Pipeline observability via Airflow UI and `pipeline_run_log` table
- Project documentation (README, architecture diagram, dbt docs site)

### Out of Scope (Future Phases)

- Real-time or near-real-time data streaming (Kafka, Kinesis, Flink)
- Cloud deployment (AWS, GCP, Azure) — architecture must support it, but deployment is not required
- User authentication and authorization on the API_Layer or Dashboard_Layer
- Multi-tenant data isolation
- A/B testing framework for ML models in production
- Customer-facing UI or product integration of API outputs
- Automated retraining triggers based on model drift detection
- Data lineage tooling beyond dbt's built-in lineage graph
- Custom alerting notifications (email, Slack, PagerDuty) — observability is via Airflow UI only in MVP
- Historical backfill pipelines beyond the initial synthetic dataset seed


---

## High-Level Milestones

### Phase 1 — MVP (Current Scope)

**Goal**: Deliver a fully functional, locally deployable Customer Intelligence Platform demonstrating production-grade data engineering and ML practices.

| Milestone | Deliverables |
|---|---|
| M1.1 — Infrastructure Baseline | Docker Compose stack running with PostgreSQL, Airflow, MLflow, FastAPI skeleton, Metabase. Schemas created. `.env.example` committed. |
| M1.2 — Data Generation and Ingestion | Synthetic data generators complete for all 5 domains. Ingestion DAGs operational. Raw_Zone populated with full target volumes. |
| M1.3 — dbt Transformation Layer | All Staging, Intermediate, and Mart models implemented, tested, and documented. `mart_customer_360` fully populated. Quality Gates operational. |
| M1.4 — ML Scoring Pipeline | Segmentation, LTV, Churn, and Anomaly models implemented, tracked in MLflow, and writing scores to `mart_ml_scores`. |
| M1.5 — NLP and Insights | Support ticket NLP clustering complete. Insights_Generator producing daily JSON output. Full daily pipeline running end-to-end within SLA. |
| M1.6 — API and Dashboards | All FastAPI endpoints live with OpenAPI docs. All four Metabase dashboards built and connected to Mart_Layer. |
| M1.7 — Hardening and Documentation | Performance benchmarks validated. All Quality Gates green. README, architecture diagram, and dbt docs site complete. Portfolio review-ready. |

### Phase 2 — Cloud Migration (Future)

**Goal**: Deploy the platform to AWS or GCP using managed equivalents of each Docker Compose service.

Key changes: Replace PostgreSQL with BigQuery or Redshift, Airflow with MWAA or Cloud Composer, MLflow with SageMaker or Vertex AI, FastAPI with API Gateway + Lambda or Cloud Run.

### Phase 3 — Real-Time Extension (Future)

**Goal**: Add a streaming ingestion lane for clickstream events alongside the existing batch pipeline.

Key changes: Introduce Kafka or Kinesis for event streaming, a streaming transformation layer (Flink or Spark Structured Streaming), and near-real-time Churn_Score updates for high-volume customer sessions.

### Phase 4 — Advanced ML and Experimentation (Future)

**Goal**: Add model drift detection, automated retraining triggers, and A/B testing for ML models.

Key changes: Add Evidently or Whylogs for drift monitoring, MLflow-triggered automated retraining DAGs, and a feature store (Feast or Tecton) to decouple feature engineering from model training.

