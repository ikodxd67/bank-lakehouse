{# схема модели — ровно то, что указано в +schema (stg, dds, marts), без префикса профиля #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name | trim if custom_schema_name is not none else target.schema }}
{%- endmacro %}
