-- Staging model for CRM customers.
--
-- Structural cleaning only (rename, cast, deduplicate) per the Staging_Layer
-- contract. No business logic. When the same customer_id arrives more than
-- once, the record with the latest _ingested_at is retained.
{{ config(materialized='view') }}

with source as (

    select * from {{ source('crm', 'customers') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by _ingested_at desc
        ) as _row_num
    from source

)

select
    customer_id,
    name,
    email,
    acquisition_channel,
    country_code,
    cast(account_created_at as date) as account_created_at,
    _ingested_at as ingested_at,
    _run_date
from deduplicated
where _row_num = 1
