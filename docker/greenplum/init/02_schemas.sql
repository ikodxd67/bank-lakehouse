create schema if not exists stg;    -- внешние таблицы PXF и загруженные пакеты
create schema if not exists dds;    -- звезда: измерения и факт
create schema if not exists marts;  -- витрины
create schema if not exists meta;   -- журнал загрузок

create table if not exists meta.loaded_exports (
    export_id  bigint primary key,
    rows       bigint not null,
    loaded_at  timestamptz not null default now(),
    seconds    numeric(10, 1)
) distributed by (export_id);
