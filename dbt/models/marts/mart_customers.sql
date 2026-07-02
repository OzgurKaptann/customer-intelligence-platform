-- Business-facing customer dimension.
--
-- Joins the enriched customer attributes (int_customers__enriched) with
-- customer-grain order and event aggregates to expose one analytics-ready row
-- per customer. Derives is_active: a customer is active when they placed at
-- least one order in the trailing 365 days OR generated an event within the
-- trailing 365 days. Small dimension, materialized as a full-refresh table.
{{ config(materialized='table') }}

with customers as (

    select * from {{ ref('int_customers__enriched') }}

),

order_aggregates as (

    select * from {{ ref('int_customer_orders__aggregated') }}

),

event_aggregates as (

    select * from {{ ref('int_customer_events__aggregated') }}

)

select
    c.customer_id,
    c.name,
    c.email,
    c.acquisition_channel,
    c.country_code,
    c.account_created_at,
    c.customer_tenure_days,
    (
        coalesce(oa.order_frequency_365d, 0) >= 1
        or (
            ea.days_since_last_event is not null
            and ea.days_since_last_event <= 365
        )
    ) as is_active,
    {{ audit_columns() }}
from customers as c
left join order_aggregates as oa
    on c.customer_id = oa.customer_id
left join event_aggregates as ea
    on c.customer_id = ea.customer_id
