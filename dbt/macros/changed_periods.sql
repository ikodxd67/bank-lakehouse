{#
  Периоды (дни или месяцы), в которые пришли строки факта новее, чем учтённые витриной.
  Витрина хранит src_export_id — последнюю выгрузку, на которой она построена.
  Инкремент delete+insert по периоду пересчитывает эти периоды целиком: так строка
  клиента, у которого в месяце всё отменили, тоже уйдёт.
#}
{% macro changed_periods(period_expr) -%}
    select distinct {{ period_expr }}
    from {{ ref('fact_card_txn') }}
    where export_id > (select coalesce(max(src_export_id), 0) from {{ this }})
{%- endmacro %}
