-- Staging model for order line items.
--
-- Structural cleaning only (cast types). No deduplication: order_item_id is a
-- unique primary key at the source and is not re-issued across ingestion runs.
{{ config(materialized='view') }}

with source as (

    select * from {{ source('orders', 'order_items') }}

)

select
    order_item_id,
    order_id,
    product_id,
    cast(quantity as integer) as quantity,
    cast(unit_price_usd as numeric(10, 2)) as unit_price_usd,
    _ingested_at as ingested_at,
    _run_date
from source
