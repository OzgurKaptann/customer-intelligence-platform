-- Referential integrity (Requirements 1.6, 4.1): every event's customer_id must
-- resolve to a customer in stg_crm__customers. Left-joins events to customers
-- and returns any event rows whose customer_id has no matching customer (an
-- orphan foreign key). The test fails if any such rows exist.
select
    e.event_id,
    e.customer_id
from {{ ref('stg_events__events') }} as e
left join {{ ref('stg_crm__customers') }} as c
    on e.customer_id = c.customer_id
where c.customer_id is null
