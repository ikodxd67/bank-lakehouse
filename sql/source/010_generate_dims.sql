-- Справочники источника: клиенты, счета, карты, ТСП, курсы.
-- Параметры приходят через set_config из CLI (gen.*), см. bank_lakehouse/source/generate.py.
-- Генерация идёт прямо в базе: 50 млн строк через Python и COPY шли бы в разы дольше.

select setseed(0.42);

-- триггеры версий на время генерации выключены: это исходное состояние, а не изменения
set session_replication_role = replica;

truncate core.card_transactions, core.cards, core.accounts, core.clients, core.merchants, core.fx_rates;

insert into core.clients
select
    id,
    (array['Айгерим','Нурлан','Динара','Ерлан','Асель','Тимур','Мадина','Арман','Жанна','Данияр',
           'Алия','Руслан','Сауле','Максим','Анна','Сергей','Ольга','Бауыржан','Гульнара','Азамат'])[1 + floor(random() * 20)::int]
        || ' ' ||
    (array['Ахметов','Серикова','Иванов','Нурпеисова','Ким','Жумабаев','Петрова','Оспанов','Садыков','Абенова',
           'Кузнецов','Тулегенова','Байжанов','Смирнова','Искаков','Омарова','Ли','Каримов','Есенова','Мухамедов'])[1 + floor(random() * 20)::int],
    date '1955-01-01' + floor(random() * 18250)::int,
    -- четыре формата телефона, как в настоящих анкетах
    case floor(random() * 4)::int
        when 0 then '+7 7' || lpad(floor(random() * 1e9)::bigint::text, 9, '0')
        when 1 then '87' || lpad(floor(random() * 1e9)::bigint::text, 9, '0')
        when 2 then '7(7' || lpad(floor(random() * 1e2)::int::text, 2, '0') || ')' || lpad(floor(random() * 1e7)::int::text, 7, '0')
        else null
    end,
    case when random() < 0.8 then 'client' || id || '@example.kz' end,
    (array['Алматы','Алматы','Алматы','Астана','Астана','Шымкент','Караганда','Актобе','Тараз','Павлодар',
           'Усть-Каменогорск','Семей','Атырау','Костанай','Актау'])[1 + floor(random() * 15)::int],
    case when r < 0.85 then 'mass' when r < 0.98 then 'affluent' else 'private' end,
    ts, ts, 1
from (
    select id, random() as r,
           timestamptz '2015-01-01 00:00+05' + random() * (timestamptz '2025-01-01 00:00+05' - timestamptz '2015-01-01 00:00+05') as ts
    from generate_series(1, current_setting('gen.clients')::bigint) as id
) s;

-- счёт у каждого клиента плюс второй валютный у части
insert into core.accounts
select row_number() over (order by client_id, n), client_id, currency, 'open', created_at, null, created_at, 1
from (
    select c.client_id, 1 as n, 'KZT' as currency, c.created_at from core.clients c
    union all
    select c.client_id, 2, case when random() < 0.7 then 'USD' else 'EUR' end, c.created_at + interval '30 days'
    from core.clients c where random() < 0.3
) a;

-- Первые gen.business_cards карт — корпоративные: на них придётся заметная доля
-- операций (перекос по card_id). Номера карт идут подряд, чтобы генератор операций
-- выбирал карту арифметикой, без соединения с таблицей.
insert into core.cards
select
    row_number() over (order by a.account_id),
    a.account_id,
    '4400 43** **** ' || lpad(floor(random() * 1e4)::int::text, 4, '0'),
    'classic', 'active', a.opened_at, (a.opened_at + interval '5 years')::date, a.opened_at, 1
from core.accounts a;

update core.cards c
set product = case
        when c.card_id <= current_setting('gen.business_cards')::bigint then 'business'
        when cl.segment = 'private' then 'platinum'
        when cl.segment = 'affluent' then 'gold'
        else 'classic' end
from core.accounts a join core.clients cl using (client_id)
where a.account_id = c.account_id;

-- ТСП: первый — маркетплейс (около четверти операций), последние 5 % — зарубежные
insert into core.merchants
select
    id,
    case when id = 1 then 'Маркетплейс' else 'ТСП ' || id end,
    case when id = 1 then '5399'
         else (array['5411','5411','5411','5812','5814','5912','5541','4121','5311','5651',
                     '5732','5999','4814','7011','4511','5200','5661','8099','5691','7832'])[1 + floor(random() * 20)::int] end,
    case when id > m * 0.95 then (array['Istanbul','Dubai','Tbilisi','Bishkek','Tashkent'])[1 + floor(random() * 5)::int]
         else (array['Алматы','Астана','Шымкент','Караганда','Актобе','Тараз','Павлодар','Атырау'])[1 + floor(random() * 8)::int] end,
    case when id > m * 0.95 then (array['TR','AE','GE','KG','UZ'])[1 + floor(random() * 5)::int] else 'KZ' end,
    timestamptz '2024-12-01 00:00+05', 1
from generate_series(1, current_setting('gen.merchants')::bigint) as id,
     lateral (select current_setting('gen.merchants')::bigint as m) p;

-- курсы: случайное блуждание вокруг реалистичного уровня
insert into core.fx_rates
select d::date, cur,
       round((base * exp(sum(step) over (partition by cur order by d)))::numeric, 4),
       d + interval '9 hours', 1
from (
    select d, cur, base, (random() - 0.5) * 0.01 as step
    from generate_series(date '2025-01-01', current_setting('gen.until')::date, interval '1 day') d,
         (values ('USD', 500.0), ('EUR', 540.0), ('RUB', 5.6)) v(cur, base)
) s;

set session_replication_role = origin;
