{# Cheap source-integrity check: file_row_number must be 0..n-1 exactly once per file (56 groups, no big hash). #}

select
    source_file,
    count(*) as row_count,
    min(source_row_number) as min_row,
    max(source_row_number) as max_row
from {{ ref('stg_yellow_taxi') }}
group by source_file
having min(source_row_number) != 0 or max(source_row_number) + 1 != count(*)
