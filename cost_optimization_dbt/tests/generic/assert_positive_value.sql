-- Generic test: asserts that a column contains only positive (or zero) values.
-- Usage in YAML: tests: [{ assert_positive_value: {} }]
{% test assert_positive_value(model, column_name) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}
