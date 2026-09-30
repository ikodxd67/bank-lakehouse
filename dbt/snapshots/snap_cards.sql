{% snapshot snap_cards %}
{{ config(
    unique_key='card_id',
    strategy='check',
    check_cols=['status', 'product'],
    updated_at='updated_at',
    hard_deletes='invalidate',
    distributed_by=['card_id'],
) }}
select * from {{ ref('stg_cards') }}
{% endsnapshot %}
