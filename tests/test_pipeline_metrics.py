import asyncio
import csv
from pathlib import Path

from src.pipeline import Config, connect, run_pipeline


def make_csv(path: Path, rows):
    headers = ["order_id", "customer_id", "amount", "order_date"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def valid(order_id):
    return {
        "order_id": order_id,
        "customer_id": "customer-001",
        "amount": "12.345",
        "order_date": "2026-11-01",
    }


def test_publication_metrics_on_first_run(tmp_path):
    source, db = tmp_path / "orders.csv", tmp_path / "db.sqlite"
    make_csv(source, [valid("metric-1"), valid("metric-2")])

    run_id = asyncio.run(run_pipeline(source, Config(db_path=db), "run-metrics-first"))

    with connect(db) as con:
        metrics = con.execute(
            "SELECT output_count, history_inserted_count, "
            "duplicate_skipped_count, target_upsert_count "
            "FROM runs WHERE run_id=?",
            (run_id,),
        ).fetchone()

    assert metrics == (2, 2, 0, 2)


def test_publication_metrics_on_exact_rerun(tmp_path):
    source, db = tmp_path / "orders.csv", tmp_path / "db.sqlite"
    make_csv(source, [valid("metric-rerun")])

    asyncio.run(run_pipeline(source, Config(db_path=db), "run-metrics-original"))
    rerun_id = asyncio.run(
        run_pipeline(source, Config(db_path=db), "run-metrics-rerun")
    )

    with connect(db) as con:
        metrics = con.execute(
            "SELECT output_count, history_inserted_count, "
            "duplicate_skipped_count, target_upsert_count "
            "FROM runs WHERE run_id=?",
            (rerun_id,),
        ).fetchone()
        history_count = con.execute(
            "SELECT COUNT(*) FROM order_history"
        ).fetchone()[0]
        target_count = con.execute(
            "SELECT COUNT(*) FROM target_orders"
        ).fetchone()[0]

    assert metrics == (0, 0, 1, 0)
    assert history_count == 1
    assert target_count == 1
