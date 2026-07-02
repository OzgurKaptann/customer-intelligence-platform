-- Staging model for support tickets.
--
-- Structural cleaning only. Deduplicates on ticket_id keeping the record with
-- the latest _ingested_at, casts timestamp columns, and computes resolution
-- time in hours. resolution_hours is NULL when the ticket has not been resolved
-- (resolved_at IS NULL), which is a direct structural derivation of two source
-- columns rather than business logic.
{{ config(materialized='view') }}

with source as (

    select * from {{ source('support', 'tickets') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by ticket_id
            order by _ingested_at desc
        ) as _row_num
    from source

)

select
    ticket_id,
    customer_id,
    subject,
    description,
    status,
    priority,
    cast(created_at as timestamptz) as created_at,
    cast(resolved_at as timestamptz) as resolved_at,
    extract(epoch from (cast(resolved_at as timestamptz) - cast(created_at as timestamptz))) / 3600.0
        as resolution_hours,
    _ingested_at as ingested_at,
    _run_date
from deduplicated
where _row_num = 1
