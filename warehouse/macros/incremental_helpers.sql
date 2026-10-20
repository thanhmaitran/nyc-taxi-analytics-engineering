{#
  Shared by every per-file incremental model (fact_trips, quarantine_trips, mart_quality_reconciliation).

  incremental_file_filter(): SQL predicate on `source_file` selecting files that are not loaded yet in
  {{ this }} plus files listed in var `reprocess_files` (checksum changed).

  drop_reprocessed_files(): pre-hook that deletes the old rows of reprocessed files from {{ this }}.
  Needed because delete+insert only deletes keys that come back in the new data: a file that is now
  entirely clean would otherwise leave its old quarantine rows behind.
#}
{% macro incremental_file_filter() %}
    source_file not in (select distinct t.source_file from {{ this }} as t)
    {% if var('reprocess_files', []) | length > 0 %}
        or source_file in ({{ quoted_list(var('reprocess_files')) }})
    {% endif %}
{% endmacro %}

{% macro drop_reprocessed_files() %}
    {% if is_incremental() and var('reprocess_files', []) | length > 0 %}
        delete from {{ this }} where source_file in ({{ quoted_list(var('reprocess_files')) }})
    {% else %}
        select 1
    {% endif %}
{% endmacro %}
