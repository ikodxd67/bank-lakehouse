{% snapshot snap_cards %}
{{ config(
    unique_key='card_id',
    strategy='check',
    check_cols=['status', 'product'],
    hard_deletes='invalidate',
    distributed_by=['card_id'],
) }}
select * from {{ ref('stg_cards') }}
{% endsnapshot %}
