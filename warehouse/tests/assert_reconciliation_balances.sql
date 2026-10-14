{# Fails for any file where rows do not add up: source = valid + quarantined, valid = fact. #}

select *
from {{ ref('mart_quality_reconciliation') }}
where
    source_rows != valid_rows + quarantined_rows
    or valid_rows != fact_rows
