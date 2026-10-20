{#
  Return `try_cast(<column> as <type>)` when the source relation has the column (matched
  case-insensitively, e.g. airport_fee vs Airport_fee), otherwise `cast(null as <type>)`.
  This keeps staging working for periods before a column existed (e.g. cbd_congestion_fee).
#}
{% macro col_or_null(relation, column_name, sql_type) %}
    {%- set actual = [] -%}
    {%- for col in adapter.get_columns_in_relation(relation) -%}
        {%- if col.name | lower == column_name | lower -%}
            {%- do actual.append(col.name) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if actual | length > 0 -%}
        try_cast("{{ actual[0] }}" as {{ sql_type }})
    {%- else -%}
        cast(null as {{ sql_type }})
    {%- endif -%}
{% endmacro %}

{# Render a list of strings as a SQL IN-list body: 'a', 'b' #}
{% macro quoted_list(values) %}
    {%- for v in values -%}'{{ v | replace("'", "''") }}'{{ ", " if not loop.last }}{%- endfor -%}
{% endmacro %}
