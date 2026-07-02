-- No negative monetary values across the mart layer (Requirements 1.4, 3.6, 6.2).
-- Every USD-denominated column must be >= 0. Returns one row per violating value,
-- tagged with its source model and column so a failure pinpoints the offending
-- field. The test fails if any such rows exist.
select 'mart_orders' as model_name, 'total_amount_usd' as column_name,
       order_id as record_key, total_amount_usd as offending_value
from {{ ref('mart_orders') }}
where total_amount_usd < 0

union all
select 'mart_orders', 'avg_item_value_usd', order_id, avg_item_value_usd
from {{ ref('mart_orders') }}
where avg_item_value_usd < 0

union all
select 'mart_campaigns', 'daily_spend_usd',
       campaign_id || '|' || campaign_date::text, daily_spend_usd
from {{ ref('mart_campaigns') }}
where daily_spend_usd < 0

union all
select 'mart_customer_360', 'total_spend_usd', customer_id, total_spend_usd
from {{ ref('mart_customer_360') }}
where total_spend_usd < 0

union all
select 'mart_customer_360', 'total_spend_365d_usd', customer_id,
       total_spend_365d_usd
from {{ ref('mart_customer_360') }}
where total_spend_365d_usd < 0

union all
select 'mart_ml_scores', 'ltv_score', customer_id, ltv_score
from {{ ref('mart_ml_scores') }}
where ltv_score < 0
