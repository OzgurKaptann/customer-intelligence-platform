-- Business-facing support ticket fact table.
--
-- Passes cleaned support tickets from stg_support__tickets through to the mart
-- layer, exposing resolution_hours (cast to NUMERIC(8,2)). Adds the NLP
-- enrichment columns cluster_id, cluster_label, and cluster_confidence as
-- nullable placeholders; the NLP_Processor populates these downstream after the
-- ticket clustering model runs. One row per ticket. Small fact table,
-- materialized as a full-refresh table.
{{ config(materialized='table') }}

with tickets as (

    select * from {{ ref('stg_support__tickets') }}

)

select
    ticket_id,
    customer_id,
    subject,
    status,
    priority,
    created_at,
    resolved_at,
    cast(resolution_hours as numeric(8, 2)) as resolution_hours,
    cast(null as integer) as cluster_id,
    cast(null as varchar(200)) as cluster_label,
    cast(null as numeric(5, 4)) as cluster_confidence,
    {{ audit_columns() }}
from tickets
