-- Business-facing order fact table.
--
-- Passes order-grain records from int_orders__with_items through to the mart
-- layer, exposing the derived item_count and avg_item_value_usd. Materialized
-- incrementally with a delete+insert strategy keyed on order_id (the row grain):
-- each run deletes the rows whose order_id is present in the new batch and
-- reinserts the refreshed rows. Because the batch carries all of the current
-- run date's orders, this idempotently refreshes the current _run_date's data
-- while guaranteeing exactly one row per order (order_id is row-level unique,
-- whereas _run_date is shared across many rows and cannot be).
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id'
) }}

with orders as (

    select * from {{ ref('int_orders__with_items') }}

)

select
    order_id,
    customer_id,
    order_status,
    total_amount_usd,
    item_count,
    avg_item_value_usd,
    ordered_at,
    {{ audit_columns() }}
from orders
