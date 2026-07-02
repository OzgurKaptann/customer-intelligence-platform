-- RFM invariants for mart_customer_360 (Requirements 3.5, 5.1, 5.2, 5.5).
-- Returns any row that violates the RFM contract; the test fails if any exist:
--   * recency_score / frequency_score / monetary_score each in 0..5
--   * rfm_score matches the pattern R[0-5]F[0-5]M[0-5]
--   * recency_days capped at 999
--   * inactive customers (order_frequency_365d = 0) default to 'R0F0M0'
select
    customer_id,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_score,
    recency_days,
    order_frequency_365d
from {{ ref('mart_customer_360') }}
where recency_score not between 0 and 5
   or frequency_score not between 0 and 5
   or monetary_score not between 0 and 5
   or rfm_score !~ '^R[0-5]F[0-5]M[0-5]$'
   or recency_days > 999
   or (order_frequency_365d = 0 and rfm_score <> 'R0F0M0')
