import asyncio
import csv
import sqlite3
from pathlib import Path

import pytest

from src.pipeline import Config, run_pipeline, connect, init_db, Context, execute_task, TASKS


def make_csv(path: Path, rows, headers=None):
    headers = headers or ["order_id", "customer_id", "amount", "order_date"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def valid(order_id="o-1"):
    return {"order_id": order_id, "customer_id": "customer-001",
            "amount": "12.345", "order_date": "2026-11-01"}


def test_happy_path_and_reconciliation(tmp_path):
    source, db = tmp_path / "orders.csv", tmp_path / "db.sqlite"
    make_csv(source, [valid("o-1"), valid("o-2")])
    run_id = asyncio.run(run_pipeline(source, Config(db_path=db), "run-happy"))
    with connect(db) as con:
        run = con.execute("SELECT status,input_count,output_count,rejected_count FROM runs WHERE run_id=?",
                          (run_id,)).fetchone()
        count = con.execute("SELECT COUNT(*) FROM target_orders").fetchone()[0]
    assert run == ("success", 2, 2, 0)
    assert count == 2


def test_invalid_rows_are_quarantined(tmp_path):
    source, db = tmp_path / "orders.csv", tmp_path / "db.sqlite"
    bad = valid("o-bad")
    bad["amount"] = "-5"
    bad2 = valid("o-ref")
    bad2["customer_id"] = "unknown"
    make_csv(source, [valid(), bad, bad2])
    run_id = asyncio.run(run_pipeline(source, Config(db_path=db), "run-rejects"))
    with connect(db) as con:
        rejected = con.execute("SELECT COUNT(*) FROM rejects WHERE run_id=?", (run_id,)).fetchone()[0]
        published = con.execute("SELECT COUNT(*) FROM target_orders").fetchone()[0]
    assert rejected == 2
    assert published == 1


def test_duplicate_key_is_rejected(tmp_path):
    source, db = tmp_path / "orders.csv", tmp_path / "db.sqlite"
    make_csv(source, [valid("same"), valid("same")])
    run_id = asyncio.run(run_pipeline(source, Config(db_path=db), "run-dup"))
    with connect(db) as con:
        rejected = con.execute("SELECT COUNT(*) FROM rejects WHERE run_id=?", (run_id,)).fetchone()[0]
        published = con.execute("SELECT COUNT(*) FROM target_orders").fetchone()[0]
    assert rejected == 1
    assert published == 1


def test_empty_input_fails_and_does_not_publish(tmp_path):
    source, db = tmp_path / "orders.csv", tmp_path / "db.sqlite"
    make_csv(source, [])
    with pytest.raises(ValueError, match="Empty input"):
        asyncio.run(run_pipeline(source, Config(db_path=db), "run-empty"))
    with connect(db) as con:
        status = con.execute("SELECT status FROM runs WHERE run_id='run-empty'").fetchone()[0]
        count = con.execute("SELECT COUNT(*) FROM target_orders").fetchone()[0]
    assert status == "failed"
    assert count == 0


def test_unexpected_schema_fails_closed(tmp_path):
    source, db = tmp_path / "orders.csv", tmp_path / "db.sqlite"
    make_csv(source, [{"id": "x"}], headers=["id"])
    with pytest.raises(ValueError, match="Unexpected schema"):
        asyncio.run(run_pipeline(source, Config(db_path=db), "run-schema"))
    with connect(db) as con:
        assert con.execute("SELECT COUNT(*) FROM target_orders").fetchone()[0] == 0


def test_rerun_is_idempotent_for_target_keys(tmp_path):
    source, db = tmp_path / "orders.csv", tmp_path / "db.sqlite"
    make_csv(source, [valid("same")])
    asyncio.run(run_pipeline(source, Config(db_path=db), "run-first"))
    asyncio.run(run_pipeline(source, Config(db_path=db), "run-second"))
    with connect(db) as con:
        count = con.execute("SELECT COUNT(*) FROM target_orders").fetchone()[0]
    assert count == 1


def test_timeout_retries_then_fails(tmp_path, monkeypatch):
    db = tmp_path / "db.sqlite"
    init_db(db)
    with connect(db) as con:
        con.execute("INSERT INTO runs(run_id,source_file,status,started_at) VALUES ('r','x','running','now')")
    async def slow(_ctx):
        await asyncio.sleep(0.2)
    monkeypatch.setitem(TASKS, "ingest", slow)
    ctx = Context("r", tmp_path / "unused.csv",
                  Config(db_path=db, max_retries=1, task_timeout_seconds=0.02,
                         retry_delay_seconds=0))
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(execute_task(ctx, "ingest"))
    with connect(db) as con:
        attempts = con.execute("SELECT COUNT(*) FROM task_runs WHERE run_id='r'").fetchone()[0]
    assert attempts == 2

def test_critical_validation_failure_blocks_publishing(tmp_path, monkeypatch):
    source = tmp_path / "orders.csv"
    db = tmp_path / "db.sqlite"

    make_csv(source, [valid("order-100")])

    async def fail_validation(ctx):
        raise RuntimeError("Injected critical validation failure")

    monkeypatch.setitem(TASKS, "validate", fail_validation)

    with pytest.raises(RuntimeError, match="Injected critical"):
        asyncio.run(
            run_pipeline(
                source,
                Config(
                    db_path=db,
                    max_retries=1,
                    retry_delay_seconds=0
                ),
                "run-blocked-publish"
            )
        )

    with connect(db) as con:
        status = con.execute(
            "SELECT status FROM runs WHERE run_id=?",
            ("run-blocked-publish",)
        ).fetchone()[0]

        publish_attempts = con.execute(
            "SELECT COUNT(*) FROM task_runs "
            "WHERE run_id=? AND task_name='publish'",
            ("run-blocked-publish",)
        ).fetchone()[0]

        target_count = con.execute(
            "SELECT COUNT(*) FROM target_orders"
        ).fetchone()[0]

    assert status == "failed"
    assert publish_attempts == 0
    assert target_count == 0

def test_missing_required_fields_are_quarantined(tmp_path):
    source = tmp_path / "orders.csv"
    db = tmp_path / "db.sqlite"

    missing_order = valid("o-1")
    missing_order["order_id"] = ""

    missing_customer = valid("o-2")
    missing_customer["customer_id"] = ""

    make_csv(source, [missing_order, missing_customer, valid("o-3")])

    run_id = asyncio.run(
        run_pipeline(source, Config(db_path=db), "run-null-fields")
    )

    with connect(db) as con:
        rejected = con.execute(
            "SELECT COUNT(*) FROM rejects WHERE run_id=?",
            (run_id,)
        ).fetchone()[0]

        published = con.execute(
            "SELECT COUNT(*) FROM target_orders"
        ).fetchone()[0]

    assert rejected == 2
    assert published == 1


def test_zero_amount_is_valid_boundary_value(tmp_path):
    source = tmp_path / "orders.csv"
    db = tmp_path / "db.sqlite"

    zero_order = valid("o-zero")
    zero_order["amount"] = "0"

    # Write the test record into the CSV file
    make_csv(source, [zero_order])

    run_id = asyncio.run(
        run_pipeline(source, Config(db_path=db), "run-zero-amount")
    )

    with connect(db) as con:
        result = con.execute(
            "SELECT amount FROM target_orders WHERE order_id='o-zero'"
        ).fetchone()

        rejected = con.execute(
            "SELECT COUNT(*) FROM rejects WHERE run_id=?",
            (run_id,)
        ).fetchone()[0]

    assert result[0] == 0
    assert rejected == 0

def test_permanent_schema_error_is_not_retried(tmp_path):
    source = tmp_path / "invalid_schema.csv"
    db = tmp_path / "pipeline.sqlite"

    source.write_text(
        "unexpected_column\nvalue\n",
        encoding="utf-8",
    )

    with pytest.raises(Exception, match="Unexpected schema"):
        asyncio.run(
            run_pipeline(
                source,
                Config(db_path=db, max_retries=3),
                "run-permanent-schema-error",
            )
        )

    with connect(db) as con:
        attempts = con.execute(
            """
            SELECT COUNT(*) FROM task_runs
            WHERE run_id = 'run-permanent-schema-error'
              AND task_name = 'ingest'
            """
        ).fetchone()[0]

    assert attempts == 1
@pytest.mark.parametrize("amount", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_amounts_are_rejected(tmp_path, amount):
    source = tmp_path / "orders.csv"
    db = tmp_path / "db.sqlite"

    row = valid("invalid-amount")
    row["amount"] = amount

    make_csv(source, [row])

    run_id = asyncio.run(
        run_pipeline(
            source,
            Config(db_path=db),
            f"run-{amount.lower()}",
        )
    )

    with connect(db) as con:
        rejected = con.execute(
            "SELECT COUNT(*) FROM rejects WHERE run_id=?",
            (run_id,),
        ).fetchone()[0]

        published = con.execute(
            "SELECT COUNT(*) FROM target_orders"
        ).fetchone()[0]

    assert rejected == 1
    assert published == 0
@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_retries": -1},
        {"max_retries": 1.5},
        {"task_timeout_seconds": 0},
        {"task_timeout_seconds": -1},
        {"task_timeout_seconds": float("inf")},
        {"retry_delay_seconds": -1},
        {"retry_delay_seconds": float("nan")},
        {"valid_customer_ids": set()},
    ],
)
def test_invalid_config_is_rejected(kwargs):
    with pytest.raises(ValueError):
        Config(**kwargs)


def test_valid_config_is_accepted():
    config = Config(
        max_retries=0,
        task_timeout_seconds=1,
        retry_delay_seconds=0,
    )

    assert config.max_retries == 0