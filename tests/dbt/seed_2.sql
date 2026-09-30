-- вторая выгрузка: операция 2 подтверждена, холд 3 снят, клиент 1 сменил сегмент
insert into stg.fact_card_txn_delta values
    (2, '2026-09-02 10:00+05', '2026-09-02', 100, 10, 1, 'classic', 2, '5411', 'AE', 10, 'USD', 500, 5000, 'posted', false, 2, 5, 2),
    (3, '2026-09-03 10:00+05', '2026-09-03', 200, 20, 2, 'gold', 1, '5399', 'KZ', 700, 'KZT', 1, 700, 'authorized', true, 2, 5, 2),
    (4, '2026-09-04 10:00+05', '2026-09-04', 200, 20, 2, 'gold', 1, '5399', 'KZ', 300, 'KZT', 1, 300, 'posted', false, 2, 5, 2);
update stg.ext_clients set segment = 'affluent', version = 2, updated_at = '2026-09-03' where client_id = 1;
