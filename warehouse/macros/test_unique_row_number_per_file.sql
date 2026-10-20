{# Passes when (source_file, source_row_number) is unique. Aggregates per file, so memory stays bounded
   by one file's rows instead of the whole table. #}
{% test unique_row_number_per_file(model) %}

select
    source_file,
    count(*) as n_rows,
    count(distinct source_row_number) as n_distinct_row_numbers
from {{ model }}
group by source_file
having count(*) != count(distinct source_row_number)

{% endtest %}
