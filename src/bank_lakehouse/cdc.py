"""Коннектор Debezium для источника: регистрация и состояние через REST Kafka Connect."""

from __future__ import annotations

import logging

import httpx

log = logging.getLogger(__name__)

CONNECTOR = "core-cdc"
TABLES = ("clients", "accounts", "cards", "merchants", "card_transactions", "fx_rates")


def connector_config(slot: str = "bank_cdc") -> dict[str, str]:
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": "source-db",
        "database.port": "5432",
        "database.user": "cdc",
        "database.password": "cdc",
        "database.dbname": "core",
        "topic.prefix": "core",
        "table.include.list": ",".join(f"core.{t}" for t in TABLES),
        # публикацию создаёт миграция источника; у пользователя cdc нет прав на CREATE PUBLICATION
        "publication.name": "cdc_core",
        "publication.autocreate.mode": "disabled",
        "slot.name": slot,
        # Данные истории забирает Spark через JDBC (параллельно и в разы быстрее),
        # коннектор только создаёт слот и читает изменения после этого момента.
        "snapshot.mode": "no_data",
        # numeric как строка: иначе Debezium кодирует его байтами в base64
        "decimal.handling.mode": "string",
        # удаление приходит одним событием op=d с полной строкой в before;
        # tombstone нужен только для compacted-топиков, у нас они обычные
        "tombstones.on.delete": "false",
        # Слот держит WAL, пока коннектор не подтвердит позицию. Если изменений в
        # таблицах из публикации долго нет, позиция не двигается и WAL копится —
        # heartbeat подтверждает её регулярно.
        "heartbeat.interval.ms": "30000",
    }


def register(connect_url: str) -> dict:
    """PUT конфигурации идемпотентен: создаёт коннектор или обновляет существующий."""
    r = httpx.put(f"{connect_url}/connectors/{CONNECTOR}/config", json=connector_config(), timeout=30)
    r.raise_for_status()
    log.info("коннектор %s зарегистрирован", CONNECTOR)
    return r.json()


def status(connect_url: str) -> dict:
    r = httpx.get(f"{connect_url}/connectors/{CONNECTOR}/status", timeout=10)
    r.raise_for_status()
    return r.json()
