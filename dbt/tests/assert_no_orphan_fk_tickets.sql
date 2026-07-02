-- Referential integrity (Requirements 1.6, 4.1): every ticket's customer_id must
-- resolve to a customer in stg_crm__customers. Left-joins tickets to customers
-- and returns any ticket rows whose customer_id has no matching customer (an
-- orphan foreign key). The test fails if any such rows exist.
select
    t.ticket_id,
    t.customer_id
from {{ ref('stg_support__tickets') }} as t
left join {{ ref('stg_crm__customers') }} as c
    on t.customer_id = c.customer_id
where c.customer_id is null
