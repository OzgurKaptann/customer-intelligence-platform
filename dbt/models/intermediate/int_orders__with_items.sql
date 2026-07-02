-- Order-grain intermediate model enriched with line-item aggregates.
--
-- Joins cleaned order headers to their line items and derives item_count
-- (count of line items per order) and avg_item_value_usd (total order amount
-- divided by item_count). One row per order. An INNER JOIN is used so only
-- orders with at least one line item are emitted, guaranteeing item_count >= 1.
{{ config(materialized='table') }}

with orders as (

    select * from {{ ref('stg_orders__orders') }}

),

order_items as (

    select * from {{ ref('stg_orders__order_items') }}

),

item_aggregates as (

    select
        order_id,
        count(order_item_id) as item_count
    from order_items
    group by order_id

)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.total_amount_usd,
    ia.item_count,
    cast(o.total_amount_usd / nullif(ia.item_count, 0) as numeric(10, 2)) as avg_item_value_usd,
    o.ordered_at,
    {{ audit_columns() }}
from orders as o
inner join item_aggregates as ia
    on o.order_id = ia.order_id
