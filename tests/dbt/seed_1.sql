-- CI: вместо внешних таблиц PXF обычные таблицы с теми же колонками.
-- Проверяются макросы Greenplum (распределение, партиции, хранение), SCD2 и инкремент.
create schema if not exists stg;
create table stg.ext_clients (client_id bigint, birth_year int, city text, segment text, created_at timestamptz,
                              version bigint, is_deleted boolean, updated_at timestamptz);
create table stg.ext_accounts (account_id bigint, client_id bigint, currency text, status text, opened_at timestamptz,
                               closed_at timestamptz, version bigint, is_deleted boolean, updated_at timestamptz);
create table stg.ext_cards (card_id bigint, account_id bigint, product text, status text, issued_at timestamptz,
                            expires_at date, version bigint, is_deleted boolean, updated_at timestamptz);
create table stg.ext_merchants (merchant_id bigint, name text, mcc text, city text, country text,
                                version bigint, is_deleted boolean, updated_at timestamptz);
create table stg.fact_card_txn_delta (
    txn_id bigint, txn_ts timestamptz, txn_date date, card_id bigint, account_id bigint, client_id bigint,
    product text, merchant_id bigint, mcc text, merchant_country text, amount numeric(14,2), currency text,
    fx_rate numeric(12,4), amount_kzt numeric(18,2), status text, is_deleted boolean, version bigint,
    batch_id bigint, export_id bigint
) distributed by (txn_id);

insert into stg.ext_clients values
    (1, 1990, 'Алматы', 'mass', '2020-01-01', 1, false, '2020-01-01'),
    (2, 1985, 'Астана', 'affluent', '2020-01-01', 1, false, '2020-01-01');
insert into stg.ext_accounts values
    (10, 1, 'KZT', 'open', '2020-01-01', null, 1, false, '2020-01-01'),
    (20, 2, 'USD', 'open', '2020-01-01', null, 1, false, '2020-01-01');
insert into stg.ext_cards values
    (100, 10, 'classic', 'active', '2020-01-01', '2030-01-01', 1, false, '2020-01-01'),
    (200, 20, 'gold', 'active', '2020-01-01', '2030-01-01', 1, false, '2020-01-01');
insert into stg.ext_merchants values
    (1, 'Маркетплейс', '5399', 'Алматы', 'KZ', 1, false, '2024-12-01'),
    (2, 'ТСП 2', '5411', 'Dubai', 'AE', 1, false, '2024-12-01');

insert into stg.fact_card_txn_delta values
    (1, '2026-09-01 10:00+05', '2026-09-01', 100, 10, 1, 'classic', 1, '5399', 'KZ', 1000, 'KZT', 1, 1000, 'posted', false, 2, 0, 1),
    (2, '2026-09-02 10:00+05', '2026-09-02', 100, 10, 1, 'classic', 2, '5411', 'AE', 10, 'USD', 500, 5000, 'authorized', false, 1, 0, 1),
    (3, '2026-09-03 10:00+05', '2026-09-03', 200, 20, 2, 'gold', 1, '5399', 'KZ', 700, 'KZT', 1, 700, 'authorized', false, 1, 0, 1);
