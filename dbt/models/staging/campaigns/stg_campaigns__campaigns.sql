-- Staging model for marketing campaigns.
--
-- Structural cleaning only. Deduplicates on the natural key
-- (campaign_id, campaign_date) keeping the record with the latest _ingested_at,
-- casts spend and metric columns, and computes the click-through rate. No
-- business logic beyond the CTR ratio, which is a direct structural derivation
-- of two source columns.
{{ config(materialized='view') }}

with source as (

    select * from {{ source('campaigns', 'campaigns') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by campaign_id, campaign_date
            order by _ingested_at desc
        ) as _row_num
    from source

)

select
    campaign_id,
    campaign_date,
    platform,
    campaign_name,
    cast(daily_spend_usd as numeric(12, 2)) as daily_spend_usd,
    cast(impressions as integer) as impressions,
    cast(clicks as integer) as clicks,
    clicks::float / nullif(impressions, 0) as click_through_rate,
    _ingested_at as ingested_at,
    _run_date
from deduplicated
where _row_num = 1
