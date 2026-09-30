-- Операции за один месяц [gen.from, gen.to). CLI вызывает файл помесячно, чтобы
-- видеть прогресс и не держать одну транзакцию на 50 млн строк.
--
-- Поток растёт линейно от gen.per_day до gen.per_day * (1 + gen.growth) к gen.until.
-- Перекосы: gen.business_share операций на gen.business_cards корпоративных карт,
-- четверть операций на маркетплейс (merchant_id = 1).
-- Авторизации, не подтверждённые за 7 дней, в истории отсутствуют: их удалил процессинг.

with params as (
    select current_setting('gen.per_day')::numeric          as per_day,
           current_setting('gen.growth')::numeric           as growth,
           current_setting('gen.start')::timestamptz        as start,
           current_setting('gen.until')::timestamptz        as until,
           current_setting('gen.business_share')::float8    as business_share,
           current_setting('gen.business_cards')::bigint    as business_cards,
           current_setting('gen.cards')::bigint             as cards,
           current_setting('gen.merchants')::bigint         as merchants
)
insert into core.card_transactions
    (card_id, merchant_id, txn_ts, amount, currency, status, auth_code, created_at, updated_at, row_version)
select
    card_id, merchant_id, txn_ts,
    round(case
        when foreign_merchant then exp(ln(40) + 1.0 * z)
        when merchant_id = 1  then exp(ln(9000) + 0.9 * z)
        else exp(ln(4500) + 1.1 * z)
    end::numeric, 2),
    case when foreign_merchant then 'USD' else 'KZT' end,
    status,
    lpad(floor(random() * 1e6)::int::text, 6, '0'),
    txn_ts,
    case when status = 'authorized' then txn_ts
         else least(txn_ts + (1 + floor(random() * 3)::int) * interval '1 day', until) end,
    case when status = 'authorized' then 1 else 2 end
from (
    select
        s.*,
        s.merchant_id > p.merchants * 0.95 as foreign_merchant,
        -- нормальное распределение из двух равномерных (Бокс — Мюллер)
        sqrt(-2 * ln(1 - random())) * cos(2 * pi() * random()) as z,
        case
            when s.txn_ts < p.until - interval '7 days' then
                case when s.r < 0.975 then 'posted' when s.r < 0.99 then 'reversed' else 'expired' end
            when s.txn_ts < p.until - interval '2 days' then
                case when s.r < 0.9 then 'posted' when s.r < 0.91 then 'reversed' else 'authorized' end
            else
                case when s.r < 0.3 then 'posted' else 'authorized' end
        end as status,
        p.until
    from (
        select
            case when random() < p.business_share
                 then 1 + floor(random() * p.business_cards)::bigint
                 else p.business_cards + 1 + floor((p.cards - p.business_cards) * power(random(), 1.3))::bigint
            end as card_id,
            case when random() < 0.25 then 1
                 else 2 + floor((p.merchants - 1) * power(random(), 3))::bigint
            end as merchant_id,
            d.day
              + case when random() < 0.05 then random() * interval '24 hours'
                     else interval '8 hours' + random() * interval '15 hours' end as txn_ts,
            random() as r
        from params p
        cross join lateral (
            select day,
                   round(p.per_day * (1 + p.growth * extract(epoch from day - p.start) / extract(epoch from p.until - p.start)))::int as n
            from generate_series(current_setting('gen.from')::timestamptz,
                                 current_setting('gen.to')::timestamptz - interval '1 day',
                                 interval '1 day') as day
            where day < p.until
        ) d
        cross join lateral generate_series(1, d.n)
    ) s
    cross join params p
) t
where status <> 'expired';
