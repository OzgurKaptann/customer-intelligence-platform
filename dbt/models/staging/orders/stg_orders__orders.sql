-- Staging model for orders.
--
-- Structural cleaning only. Deduplicates on order_id keeping the record with
-- the latest _ingested_at, casts the monetary total, and drops rows with a
-- NULL customer_id (orphans that cannot resolve to a customer).
{{ config(materialized='view') }}

with source as (

    select * from {{ source('orders', 'orders') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by order_id
            order by _ingested_at desc
        ) as _row_num
    from source

)

select
    order_id,
    customer_id,
    order_status,
    cast(total_amount_usd as numeric(12, 2)) as total_amount_usd,
    cast(ordered_at as timestamptz) as ordered_at,
    _ingested_at as ingested_at,
    _run_date
from deduplicated
where _row_num = 1
  and customer_id is not null
