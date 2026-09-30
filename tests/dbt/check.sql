-- после второго прохода: 4 операции, у 2 и 3 последние версии, у клиента 1 две версии в SCD2
do $$
declare n int;
begin
    select count(*) into n from dds.fact_card_txn;
    if n <> 4 then raise exception 'в факте % строк вместо 4', n; end if;
    select count(*) into n from dds.fact_card_txn where (txn_id = 2 and status = 'posted') or (txn_id = 3 and is_deleted);
    if n <> 2 then raise exception 'инкремент не применил новые версии'; end if;
    select count(*) into n from dds.dim_client where client_id = 1;
    if n <> 2 then raise exception 'у клиента 1 % версий вместо 2', n; end if;
    -- в Greenplum 7 нет pg_partitions из шестой версии, партиции видны через pg_partition_tree
    select count(*) into n from pg_partition_tree('dds.fact_card_txn') where isleaf;
    if n < 24 then raise exception 'факт не партиционирован по месяцам (% партиций)', n; end if;
    -- сегмент берётся на момент операции: 1 сентября клиент 1 ещё mass
    select count(*) into n from marts.mart_daily_turnover where segment = 'unknown';
    if n <> 0 then raise exception 'операции без версии клиента: %', n; end if;
    select count(*) into n from marts.mart_daily_turnover where txn_date = '2026-09-01' and segment = 'mass';
    if n <> 1 then raise exception 'сегмент на дату операции определён неверно'; end if;
end $$;
select pg_get_table_distributedby('dds.fact_card_txn'::regclass);
