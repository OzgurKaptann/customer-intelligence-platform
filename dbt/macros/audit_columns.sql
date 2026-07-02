{#
    Emits the standard audit columns appended to models across all layers.

    Currently adds `_run_date`, the pipeline run date partition key derived
    from get_run_date(). This is the key used by incremental (delete+insert)
    mart models to identify the partition to refresh.

    Usage (inside a model's final SELECT):

        select
            ...,
            {{ audit_columns() }}
        from ...
#}
{% macro audit_columns() -%}
    {{ get_run_date() }} as _run_date
{%- endmacro %}
