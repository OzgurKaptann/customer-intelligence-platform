-- Business-facing ML scores fact table — one row per customer per score_date.
--
-- Promotes the ML pipeline's scoring output from ml.ml_scores (written by the
-- ml_scores_promotion step after the segmentation, LTV, churn, anomaly, and NLP
-- models complete) into the marts layer for API and dashboard consumption. This
-- model does NOT compute scores; it reads the ML write path verbatim, exposing
-- every ml.ml_scores column plus the standard _run_date audit key.
--
-- Only the current run date's partition is promoted: score_date in ml.ml_scores
-- is the ML pipeline's run date, so filtering on it keeps the mart grain at one
-- partition per run and keeps score_date aligned with the _run_date delete+insert
-- key.
--
-- Materialized incrementally with a delete+insert strategy keyed on _run_date:
-- each run deletes the current run date's partition and reinserts the freshly
-- promoted scores, which is idempotent for same-day re-runs. churn_score is
-- bounded to [0.0, 1.0] and ltv_score to >= 0 by the tests declared for this
-- model in _marts__models.yml (Property 8 — mart_ml_scores value bounds).
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='_run_date'
) }}

with ml_scores as (

    select * from {{ source('ml', 'ml_scores') }}
    where score_date = {{ get_run_date() }}

)

select
    customer_id,
    score_date,
    model_run_id,
    ltv_score,
    churn_score,
    churn_risk_tier,
    segment_label,
    coalesce(anomaly_flag, false) as anomaly_flag,
    anomaly_detail,
    scored_at,
    {{ audit_columns() }}
from ml_scores
