-- Customer-grain order aggregates.
--
-- Groups order-level records (int_orders__with_items) by customer to compute
-- all-time and trailing-window order metrics used downstream by mart_customer_360
-- and the ML feature store. Trailing windows are measured relative to the
-- pipeline run date returned by get_run_date(). One row per customer.
{{ config(materialized='table') }}

with orders as (

    select * from {{ ref('int_orders__with_items') }}

),

aggregated as (

    select
        customer_id,

        -- All-time metrics
        count(*) as total_order_count,
        cast(sum(total_amount_usd) as numeric(14, 2)) as total_spend_usd,

        -- Trailing 365-day window
        count(*) filter (
            where ordered_at::date >= {{ get_run_date() }} - 365
        ) as order_frequency_365d,
        cast(coalesce(sum(total_amount_usd) filter (
            where ordered_at::date >= {{ get_run_date() }} - 365
        ), 0) as numeric(14, 2)) as total_spend_365d_usd,

        -- Rolling 30-day windows for trend analysis
        count(*) filter (
            where ordered_at::date >= {{ get_run_date() }} - 30
        ) as order_count_last_30d,
        count(*) filter (
            where ordered_at::date >= {{ get_run_date() }} - 60
              and ordered_at::date < {{ get_run_date() }} - 30
        ) as order_count_prior_30d,

        -- Recency relative to the run date
        {{ get_run_date() }} - max(ordered_at)::date as days_since_last_order
    from orders
    group by customer_id

)

select
    customer_id,
    total_order_count,
    total_spend_usd,
    order_frequency_365d,
    total_spend_365d_usd,
    order_count_last_30d,
    order_count_prior_30d,
    days_since_last_order,
    {{ audit_columns() }}
from aggregated
