{#
    Custom schema routing for the Customer Intelligence Platform.

    dbt's default behaviour concatenates the target schema with any custom
    schema, which is not what we want here. Instead we route models to a fixed
    physical schema based on their layer, inferred from the model name prefix:

        stg_*   -> staging
        int_*   -> intermediate
        mart_*  -> marts

    Any model that does not match a known prefix (e.g. ad-hoc analyses) falls
    back to the target's default schema (`public`).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set model_name = node.name | lower -%}

    {%- if model_name.startswith('stg_') -%}
        staging
    {%- elif model_name.startswith('int_') -%}
        intermediate
    {%- elif model_name.startswith('mart_') -%}
        marts
    {%- elif custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}
