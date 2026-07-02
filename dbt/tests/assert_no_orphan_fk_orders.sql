-- Referential integrity (Requirements 1.6, 4.1): every order's customer_id must
-- resolve to a customer in stg_crm__customers. Left-joins orders to customers
-- and returns any order rows whose customer_id has no matching customer (an
-- orphan foreign key). The test fails if any such rows exist.
select
    o.order_id,
    o.customer_id
from {{ ref('stg_orders__orders') }} as o
left join {{ ref('stg_crm__customers') }} as c
    on o.customer_id = c.customer_id
where c.customer_id is null
