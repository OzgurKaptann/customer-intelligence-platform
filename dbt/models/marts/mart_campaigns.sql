-- Business-facing campaign performance fact table.
--
-- Passes cleaned campaign metrics from stg_campaigns__campaigns through to the
-- mart layer, exposing the derived click_through_rate cast to NUMERIC(8,6).
-- Adds anomaly_flag defaulting to FALSE; the Anomaly_Model updates this column
-- downstream when a campaign metric deviates beyond expected bounds. One row per
-- (campaign_id, campaign_date). Small dimension, materialized as a table.
{{ config(materialized='table') }}

with campaigns as (

    select * from {{ ref('stg_campaigns__campaigns') }}

)

select
    campaign_id,
    campaign_date,
    platform,
    campaign_name,
    daily_spend_usd,
    impressions,
    clicks,
    cast(coalesce(click_through_rate, 0) as numeric(8, 6)) as click_through_rate,
    cast(false as boolean) as anomaly_flag,
    {{ audit_columns() }}
from campaigns
