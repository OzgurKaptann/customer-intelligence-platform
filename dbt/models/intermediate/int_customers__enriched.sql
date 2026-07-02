-- Customer-grain enrichment model.
--
-- Passes through cleaned customer attributes and derives customer_tenure_days
-- (days from account_created_at to the pipeline run date). One row per customer.
-- The base grain for mart_customers and mart_customer_360.
{{ config(materialized='table') }}

with customers as (

    select * from {{ ref('stg_crm__customers') }}

)

select
    customer_id,
    name,
    email,
    acquisition_channel,
    country_code,
    account_created_at,
    {{ get_run_date() }} - account_created_at as customer_tenure_days,
    {{ audit_columns() }}
from customers
