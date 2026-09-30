"""Стадии Spark-приложения из History Server: длительность и разброс времени задач.

Перекос виден по отношению max / медиана времени задач в стадии с перемешиванием:
при равномерных данных оно близко к 1, при перекосе одна задача работает в разы дольше.

    python scripts/spark_stages.py application_1790807162033_0042
"""

from __future__ import annotations

import sys

import httpx

HISTORY = "http://127.0.0.1:18080/api/v1"


def stages(app_id: str) -> list[dict]:
    attempts = httpx.get(f"{HISTORY}/applications/{app_id}", timeout=30).json()["attempts"]
    base = f"{HISTORY}/applications/{app_id}"
    if attempts[0].get("attemptId"):
        base += f"/{attempts[0]['attemptId']}"
    out = []
    for s in httpx.get(f"{base}/stages", params={"status": "complete"}, timeout=30).json():
        q = httpx.get(
            f"{base}/stages/{s['stageId']}/{s['attemptId']}/taskSummary",
            params={"quantiles": "0.5,0.95,1.0"},
            timeout=30,
        ).json()
        med, p95, mx = (v / 1000 for v in q["executorRunTime"])
        out.append(
            {
                "stage": s["stageId"],
                "tasks": s["numTasks"],
                "shuffle_read_mb": round(s["shuffleReadBytes"] / 2**20),
                "median_s": round(med, 1),
                "p95_s": round(p95, 1),
                "max_s": round(mx, 1),
                "skew": round(mx / med, 1) if med else None,
                "name": s["name"][:60],
            }
        )
    return sorted(out, key=lambda r: r["stage"])


def main() -> None:
    app = sys.argv[1]
    info = httpx.get(f"{HISTORY}/applications/{app}", timeout=30).json()
    att = info["attempts"][0]
    print(f"{info['name']}: {att['duration'] / 1000:.1f} с")
    print(f"{'stage':>5} {'tasks':>5} {'shuffle МБ':>10} {'медиана':>8} {'p95':>6} {'max':>6} {'max/мед':>7}  имя")
    for r in stages(app):
        print(
            f"{r['stage']:>5} {r['tasks']:>5} {r['shuffle_read_mb']:>10} {r['median_s']:>8} {r['p95_s']:>6}"
            f" {r['max_s']:>6} {r['skew']!s:>7}  {r['name']}"
        )


if __name__ == "__main__":
    main()
