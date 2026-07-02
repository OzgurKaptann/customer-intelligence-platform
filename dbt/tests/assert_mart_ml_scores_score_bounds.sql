-- mart_ml_scores value + tier invariants (Requirements 3.6, 6.2, 7.1, 7.3).
-- Returns any row where a score is out of bounds or the churn_risk_tier does not
-- match the churn_score threshold band; the test fails if any such rows exist:
--   * ltv_score >= 0
--   * churn_score in [0.0, 1.0]
--   * churn_risk_tier in (Low, Medium, High)
--   * tier matches score: Low (< 0.33), Medium ([0.33, 0.67)), High (>= 0.67)
select
    customer_id,
    score_date,
    ltv_score,
    churn_score,
    churn_risk_tier
from {{ ref('mart_ml_scores') }}
where ltv_score < 0.0
   or churn_score < 0.0
   or churn_score > 1.0
   or churn_risk_tier not in ('Low', 'Medium', 'High')
   or (churn_score < 0.33 and churn_risk_tier <> 'Low')
   or (churn_score >= 0.33 and churn_score < 0.67 and churn_risk_tier <> 'Medium')
   or (churn_score >= 0.67 and churn_risk_tier <> 'High')
