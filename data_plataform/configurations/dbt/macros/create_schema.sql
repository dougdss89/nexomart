{% macro create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    select 1
  {%- endcall -%}
{%- endmacro %}
