import pytest
import asyncio
import csv
import sqlite3

from src.pipeline import Config, NonRetryableError, run_pipeline


def create_event_file(path, event_id, amount, timestamp):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "event_id",
                "order_id",
                "customer_id",
                "amount",
                "order_date",
                "event_timestamp",
            ],
        )

        writer.writeheader()

        writer.writerow({
            "event_id": event_id,
            "order_id": "order-001",
            "customer_id": "customer-001",
            "amount": amount,
            "order_date": "2026-11-10",
            "event_timestamp": timestamp,
        })


def test_late_arriving_event_does_not_overwrite_newer_event(tmp_path):
    db = tmp_path / "test.db"
    newer_file = tmp_path / "newer.csv"
    older_file = tmp_path / "older.csv"

    create_event_file(
        newer_file,
        "event-new",
        "200.00",
        "2026-11-12T10:00:00+00:00",
    )

    create_event_file(
        older_file,
        "event-old",
        "100.00",
        "2026-11-10T10:00:00+00:00",
    )

    config = Config(db_path=db)

    asyncio.run(run_pipeline(newer_file, config))
    asyncio.run(run_pipeline(older_file, config))

    with sqlite3.connect(db) as con:
        current = con.execute(
            "SELECT amount, event_timestamp FROM target_orders "
            "WHERE order_id = ?",
            ("order-001",),
        ).fetchone()

        history_count = con.execute(
            "SELECT COUNT(*) FROM order_history"
        ).fetchone()[0]

    assert current[0] == 200.00
    assert current[1] == "2026-11-12T10:00:00+00:00"
    assert history_count == 2


def test_duplicate_event_is_not_inserted_twice(tmp_path):
    db = tmp_path / "duplicate.db"
    source = tmp_path / "events.csv"

    create_event_file(
        source,
        "event-001",
        "150.00",
        "2026-11-12T10:00:00+00:00",
    )

    config = Config(db_path=db)

    asyncio.run(run_pipeline(source, config))
    asyncio.run(run_pipeline(source, config))

    with sqlite3.connect(db) as con:
        history_count = con.execute(
            "SELECT COUNT(*) FROM order_history"
        ).fetchone()[0]

        target_count = con.execute(
            "SELECT COUNT(*) FROM target_orders"
        ).fetchone()[0]

    assert history_count == 1
    assert target_count == 1


def test_conflicting_duplicate_event_id_is_rejected(tmp_path):
    db_path = tmp_path / "pipeline.db"
    input_path = tmp_path / "orders.csv"
    config = Config(db_path=db_path)

    original_csv = (
        "event_id,order_id,customer_id,amount,order_date,event_timestamp\n"
        "event-001,order-001,customer-001,100.00,2026-11-01,"
        "2026-11-01T10:00:00+00:00\n"
    )

    conflicting_csv = (
        "event_id,order_id,customer_id,amount,order_date,event_timestamp\n"
        "event-001,order-001,customer-001,999.00,2026-11-01,"
        "2026-11-01T10:00:00+00:00\n"
    )

    input_path.write_text(original_csv, encoding="utf-8")

    asyncio.run(
        run_pipeline(source_file=input_path, config=config)
    )

    input_path.write_text(conflicting_csv, encoding="utf-8")

    with pytest.raises(NonRetryableError, match="Conflicting payload"):
        asyncio.run(
            run_pipeline(source_file=input_path, config=config)
        )

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT event_id, amount FROM order_history"
        ).fetchall()

        failed_task_attempts = conn.execute(
            "SELECT COUNT(*) FROM task_runs "
            "WHERE task_name = 'publish' AND status = 'failed'"
        ).fetchone()[0]

    assert rows == [("event-001", 100.0)]
    assert failed_task_attempts == 1
