{{ config(distributed_by='replicated') }}
{# маленькое измерение, которое соединяется со всем: копия на каждом сегменте убирает перемешивание #}
select
    d::date                                   as date_key,
    extract(isodow from d)::int               as iso_dow,
    extract(isodow from d) in (6, 7)          as is_weekend,
    date_trunc('month', d)::date              as month_start,
    extract(year from d)::int                 as year,
    extract(month from d)::int                as month
from generate_series(date '2024-12-01', date '2027-12-31', interval '1 day') d
