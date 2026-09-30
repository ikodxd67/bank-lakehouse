"""Точка входа: bank <команда>."""

from __future__ import annotations

import argparse
import logging
from datetime import date

from bank_lakehouse.config import Settings


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="bank")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("migrate", help="создать схему источника")

    g = sub.add_parser("generate", help="наполнить источник историей")
    g.add_argument("--per-day", type=int, default=55_000, help="операций в день в начале истории")
    g.add_argument("--clients", type=int, default=200_000)
    g.add_argument("--start", type=date.fromisoformat, default=date(2025, 1, 1))
    g.add_argument("--until", type=date.fromisoformat, default=None)

    s = sub.add_parser("simulate", help="живой поток изменений в источнике")
    s.add_argument("--tick", type=float, default=10.0, help="секунд между тактами")
    s.add_argument("--tps", type=float, default=20.0, help="новых операций в секунду")
    s.add_argument("--hold-ttl", default="7 days", help="через сколько удаляется неподтверждённая авторизация")
    s.add_argument("--ticks", type=int, default=None, help="сколько тактов сделать (по умолчанию бесконечно)")

    sub.add_parser("cdc-register", help="зарегистрировать коннектор Debezium")
    sub.add_parser("cdc-status", help="состояние коннектора Debezium")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()

    if args.cmd == "migrate":
        from bank_lakehouse.source.generate import migrate

        migrate(settings.source_dsn)
    elif args.cmd == "generate":
        from bank_lakehouse.source.generate import GenParams, generate

        generate(
            settings.source_dsn,
            GenParams(per_day=args.per_day, clients=args.clients, start=args.start, until=args.until),
        )
    elif args.cmd == "simulate":
        from bank_lakehouse.source.simulate import SimParams, run

        run(
            settings.source_dsn,
            SimParams(tick_seconds=args.tick, txn_per_second=args.tps, hold_ttl=args.hold_ttl),
            ticks=args.ticks,
        )
    elif args.cmd in ("cdc-register", "cdc-status"):
        import json

        from bank_lakehouse import cdc

        fn = cdc.register if args.cmd == "cdc-register" else cdc.status
        print(json.dumps(fn(settings.connect_url), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
