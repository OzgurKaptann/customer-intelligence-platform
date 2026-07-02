{#
    Returns the pipeline run date used across the intermediate and mart layers
    for trailing-window calculations (e.g. days_since_last_order) and as the
    `_run_date` partition key for incremental models.

    Defaults to the database `current_date`. For backfills, supply an explicit
    ISO date via the `run_date` var:

        dbt run --vars '{run_date: "2026-06-01"}'
#}
{% macro get_run_date() -%}
    {%- set override = var('run_date', none) -%}
    {%- if override is not none -%}
        cast('{{ override }}' as date)
    {%- else -%}
        current_date
    {%- endif -%}
{%- endmacro %}
