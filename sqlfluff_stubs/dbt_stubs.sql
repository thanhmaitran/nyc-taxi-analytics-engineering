{# SQLFluff-only stand-ins. dbt itself never loads this directory. #}
{% macro ref(name) %}{{ name }}{% endmacro %}
{% macro source(schema, name) %}{{ schema }}.{{ name }}{% endmacro %}
{% macro config(materialized=none, incremental_strategy=none, unique_key=none, severity=none) %}{% endmacro %}
{% macro is_incremental() %}true{% endmacro %}
{% macro var(name, default=none) %}{{ default }}{% endmacro %}
{% macro col_or_null(relation, column_name, sql_type) %}try_cast({{ column_name }} as {{ sql_type }}){% endmacro %}
{% macro quoted_list(values) %}'x'{% endmacro %}
