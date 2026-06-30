-- Generic, parametrised test: assert a column's values fall within [min, max].
-- Usage in schema.yml:
--   tests:
--     - value_within_range:
--         min_value: 1
--         max_value: 180
{% test value_within_range(model, column_name, min_value, max_value) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}

{% endtest %}
