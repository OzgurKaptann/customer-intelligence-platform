-- Staging model for web/mobile events.
--
-- Structural cleaning only. Deduplicates on event_id keeping the record with
-- the latest _ingested_at. Session aggregation is deferred to the intermediate
-- layer (int_sessions__with_duration).
{{ config(materialized='view') }}

with source as (

    select * from {{ source('events', 'events') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by event_id
            order by _ingested_at desc
        ) as _row_num
    from source

)

select
    event_id,
    session_id,
    customer_id,
    event_type,
    page_url,
    device_type,
    cast(occurred_at as timestamptz) as occurred_at,
    _ingested_at as ingested_at,
    _run_date
from deduplicated
where _row_num = 1
