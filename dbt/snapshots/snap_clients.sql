{% snapshot snap_clients %}
{#
  SCD2 по клиентам: новая версия, когда меняется город или сегмент. Телефон тоже
  меняется, но в хранилище его нет, и историю по нему хранить незачем.
#}
{{ config(
    unique_key='client_id',
    strategy='check',
    check_cols=['city', 'segment'],
    updated_at='updated_at',
    hard_deletes='invalidate',
    distributed_by=['client_id'],
) }}
select * from {{ ref('stg_clients') }}
{% endsnapshot %}
