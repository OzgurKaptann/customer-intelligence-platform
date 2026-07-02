-- Customer-grain support ticket aggregates.
--
-- Groups cleaned support tickets by customer to compute the count of currently
-- open tickets (status open or in_progress). Feeds the open_ticket_count field
-- in mart_customer_360 and churn features. One row per customer.
{{ config(materialized='table') }}

with tickets as (

    select * from {{ ref('stg_support__tickets') }}

),

aggregated as (

    select
        customer_id,
        count(*) filter (
            where status in ('open', 'in_progress')
        ) as open_ticket_count
    from tickets
    group by customer_id

)

select
    customer_id,
    open_ticket_count,
    {{ audit_columns() }}
from aggregated
