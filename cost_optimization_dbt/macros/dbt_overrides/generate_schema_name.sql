-- Override default generate_schema_name so that custom schema names are used as-is,
-- without the default target_schema prefix (e.g. STAGING, not PUBLIC_STAGING).
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
