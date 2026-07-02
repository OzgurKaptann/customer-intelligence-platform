-- Session-grain intermediate model.
--
-- Groups cleaned events by session_id to derive session boundaries and
-- duration. One row per session. session_duration_seconds is the wall-clock
-- span between the first and last event of the session and is always >= 0
-- (a single-event session has a duration of 0).
{{ config(materialized='table') }}

with events as (

    select * from {{ ref('stg_events__events') }}

),

sessions as (

    select
        session_id,
        min(occurred_at) as session_start,
        max(occurred_at) as session_end
    from events
    group by session_id

)

select
    session_id,
    session_start,
    session_end,
    extract(epoch from (session_end - session_start)) as session_duration_seconds,
    {{ audit_columns() }}
from sessions
