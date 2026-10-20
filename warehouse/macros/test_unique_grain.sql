{# Generic test: the listed columns together must be unique (grain check, no external package). #}
{% test dbt_utils_free_unique_grain(model, columns) %}

select
    {{ columns | join(', ') }},
    count(*) as n_rows
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
