-- Customer-grain event aggregates.
--
-- Groups cleaned events by customer to compute the most recent event date and
-- the number of days since that event relative to the pipeline run date. Feeds
-- the is_active flag and churn features downstream. One row per customer.
{{ config(materialized='table') }}

with events as (

    select * from {{ ref('stg_events__events') }}

),

aggregated as (

    select
        customer_id,
        max(occurred_at)::date as most_recent_event_date
    from events
    group by customer_id

)

select
    customer_id,
    most_recent_event_date,
    {{ get_run_date() }} - most_recent_event_date as days_since_last_event,
    {{ audit_columns() }}
from aggregated
