-- Safe division macro to prevent divide-by-zero errors.
-- Returns NULL when the denominator is zero or NULL.
{% macro safe_divide(numerator, denominator) -%}
    {{ numerator }} / NULLIF({{ denominator }}, 0)
{%- endmacro %}
