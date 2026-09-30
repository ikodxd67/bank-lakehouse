{#
  Greenplum-специфика для dbt-postgres. Адаптер dbt-greenplum не обновлялся с мая 2023,
  а Greengage 7 говорит на диалекте PostgreSQL 12, поэтому хватает переопределить
  создание таблицы. Настройки модели:

    distributed_by: ['col', ...] | 'randomly' | 'replicated'   — как строки раскладываются по сегментам
    storage: {appendoptimized: true, orientation: column, ...}  — параметры хранения (WITH (...))
    partition_by_range: {column, start, end, every}            — помесячные и т.п. партиции
#}

{% macro gp_with_clause() -%}
  {%- set storage = config.get('storage') -%}
  {%- if storage -%}
    with ({% for k, v in storage.items() %}{{ k }} = {{ v }}{% if not loop.last %}, {% endif %}{% endfor %})
  {%- endif -%}
{%- endmacro %}

{% macro gp_distribution_clause() -%}
  {%- set dist = config.get('distributed_by') -%}
  {%- if dist is none -%}
  {%- elif dist == 'randomly' -%} distributed randomly
  {%- elif dist == 'replicated' -%} distributed replicated
  {%- else -%} distributed by ({{ dist | join(', ') }})
  {%- endif -%}
{%- endmacro %}

{% macro gp_partition_clause(p) -%}
  partition by range ({{ p.column }}) (
    start ({{ p.start }}) inclusive end ({{ p['end'] }}) exclusive every ({{ p.every }}),
    default partition other
  )
{%- endmacro %}

{% macro postgres__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set part = config.get('partition_by_range') -%}
  {{ sql_header if sql_header is not none }}

  {%- if temporary %}
    {# временные таблицы dbt (инкремент, снапшоты): только распределение, чтобы дальнейший
       delete+insert шёл без перемешивания между сегментами #}
    create temporary table {{ relation }} as ({{ sql }}) {{ gp_distribution_clause() }};
  {%- elif part %}
    {# CREATE TABLE AS не умеет партиции: форму таблицы берём из запроса без данных,
       создаём партиционированную таблицу и заливаем её отдельно #}
    {%- set shape = relation.identifier ~ '__shape' %}
    create temporary table {{ shape }} as ({{ sql }}) with no data;
    create table {{ relation }} (like {{ shape }})
      {{ gp_with_clause() }}
      {{ gp_distribution_clause() }}
      {{ gp_partition_clause(part) }};
    insert into {{ relation }} {{ sql }};
    drop table {{ shape }};
  {%- else %}
    create table {{ relation }}
      {{ gp_with_clause() }}
    as ({{ sql }})
      {{ gp_distribution_clause() }};
  {%- endif %}
{%- endmacro %}
