{# у одного ключа интервалы версий не пересекаются и идут без дыр #}
{% test scd2_no_overlap(model, key, valid_from='valid_from', valid_to='valid_to') %}
with ordered as (
    select {{ key }}, {{ valid_from }} as vf, {{ valid_to }} as vt,
           lead({{ valid_from }}) over (partition by {{ key }} order by {{ valid_from }}) as next_vf
    from {{ model }}
)
select * from ordered where next_vf is not null and next_vf <> vt
{% endtest %}
