-- Central per-customer feature store — one row per customer.
--
-- Joins the enriched customer attributes (int_customers__enriched) with the
-- customer-grain order, event, and ticket aggregates to assemble the analytics
-- and ML feature surface described by mart_customer_360 in the design.
--
-- RFM scoring: recency, frequency, and monetary quintiles (1-5) are computed
-- with NTILE(5) over the ACTIVE customer population only (customers with >= 1
-- order in the trailing 365 days). Recency ranks by days_since_last_order DESC
-- so that the most recent buyers receive the highest score (5); frequency and
-- monetary rank ascending so the highest counts / spend receive the highest
-- score. customer_id is used as a deterministic tie-breaker so equal raw values
-- produce stable buckets across runs. Inactive customers (no trailing 365-day
-- orders) bypass scoring: they receive rfm_score 'R0F0M0', all dimension scores
-- 0, recency_days 999, and is_active = FALSE.
--
-- active_campaign_count is a run-wide scalar (campaigns with spend in the
-- trailing 30 days) cross-joined onto every customer row, matching the design's
-- subquery-against-stg_campaigns__campaigns definition.
--
-- Materialized incrementally with a delete+insert strategy keyed on _run_date:
-- each run deletes the current run date's partition and reinserts the freshly
-- computed snapshot, which is idempotent for same-day re-runs.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='_run_date'
) }}

with customers as (

    select * from {{ ref('int_customers__enriched') }}

),

order_aggregates as (

    select * from {{ ref('int_customer_orders__aggregated') }}

),

event_aggregates as (

    select * from {{ ref('int_customer_events__aggregated') }}

),

ticket_aggregates as (

    select * from {{ ref('int_customer_tickets__aggregated') }}

),

active_campaigns as (

    select count(*) as active_campaign_count
    from {{ ref('stg_campaigns__campaigns') }}
    where daily_spend_usd > 0
      and campaign_date >= {{ get_run_date() }} - 30

),

joined as (

    select
        c.customer_id,
        c.acquisition_channel,
        c.customer_tenure_days,
        coalesce(oa.total_order_count, 0) as total_order_count,
        cast(coalesce(oa.total_spend_usd, 0) as numeric(14, 2)) as total_spend_usd,
        coalesce(oa.order_frequency_365d, 0) as order_frequency_365d,
        cast(coalesce(oa.total_spend_365d_usd, 0) as numeric(14, 2))
            as total_spend_365d_usd,
        coalesce(oa.order_count_last_30d, 0) as order_count_last_30d,
        coalesce(oa.order_count_prior_30d, 0) as order_count_prior_30d,
        oa.days_since_last_order,
        ea.most_recent_event_date,
        ea.days_since_last_event,
        coalesce(tk.open_ticket_count, 0) as open_ticket_count,
        coalesce(oa.order_frequency_365d, 0) >= 1 as is_active
    from customers as c
    left join order_aggregates as oa on c.customer_id = oa.customer_id
    left join event_aggregates as ea on c.customer_id = ea.customer_id
    left join ticket_aggregates as tk on c.customer_id = tk.customer_id

),

scored as (

    select
        *,
        -- Quintiles are partitioned by is_active so ranks are computed within
        -- the active population; the inactive partition's ranks are discarded.
        ntile(5) over (
            partition by is_active
            order by days_since_last_order desc, customer_id
        ) as recency_ntile,
        ntile(5) over (
            partition by is_active
            order by order_frequency_365d asc, customer_id
        ) as frequency_ntile,
        ntile(5) over (
            partition by is_active
            order by total_spend_365d_usd asc, customer_id
        ) as monetary_ntile
    from joined

)

select
    s.customer_id,
    cast(case when s.is_active then s.recency_ntile else 0 end as smallint)
        as recency_score,
    cast(case when s.is_active then s.frequency_ntile else 0 end as smallint)
        as frequency_score,
    cast(case when s.is_active then s.monetary_ntile else 0 end as smallint)
        as monetary_score,
    case
        when s.is_active then
            'R' || s.recency_ntile::text
            || 'F' || s.frequency_ntile::text
            || 'M' || s.monetary_ntile::text
        else 'R0F0M0'
    end as rfm_score,
    least(coalesce(s.days_since_last_order, 999), 999) as recency_days,
    s.order_frequency_365d,
    s.total_spend_365d_usd,
    s.total_order_count,
    s.total_spend_usd,
    s.most_recent_event_date,
    s.days_since_last_event,
    s.days_since_last_order,
    s.order_count_last_30d,
    s.order_count_prior_30d,
    s.order_count_last_30d - s.order_count_prior_30d as order_frequency_trend,
    ac.active_campaign_count,
    s.open_ticket_count,
    s.acquisition_channel,
    s.customer_tenure_days,
    s.is_active,
    {{ audit_columns() }}
from scored as s
cross join active_campaigns as ac
