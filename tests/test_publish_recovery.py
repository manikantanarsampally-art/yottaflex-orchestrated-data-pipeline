import asyncio
import csv
import sqlite3
import src.pipeline as pipeline
from pathlib import Path

import pytest

from src.pipeline import Config, connect, run_pipeline


EVENT_HEADERS = [
    "event_id",
    "order_id",
    "customer_id",
    "amount",
    "order_date",
    "event_timestamp",
]


def write_event_csv(path: Path, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EVENT_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def event(event_id, order_id, timestamp, amount="25.00"):
    return {
        "event_id": event_id,
        "order_id": order_id,
        "customer_id": "customer-001",
        "amount": amount,
        "order_date": timestamp[:10],
        "event_timestamp": timestamp,
    }


def test_publish_failure_rolls_back_batch_and_corrected_run_recovers(tmp_path):
    db = tmp_path / "pipeline.sqlite"

    # Establish previously published data that must survive the failed batch.
    baseline_source = tmp_path / "baseline.csv"
    write_event_csv(
        baseline_source,
        [event("baseline-event", "baseline-order", "2026-11-01T10:00:00+00:00")],
    )
    asyncio.run(
        run_pipeline(
            baseline_source,
            Config(db_path=db, max_retries=0, retry_delay_seconds=0),
            "run-baseline",
        )
    )

    # The first row can be inserted, but the trigger forces the second row
    # to fail. The publish transaction must roll back both rows.
    batch_source = tmp_path / "batch.csv"
    write_event_csv(
        batch_source,
        [
            event("batch-event-1", "batch-order-1", "2026-11-02T10:00:00+00:00"),
            event("batch-event-2", "batch-order-2", "2026-11-02T11:00:00+00:00"),
        ],
    )

    with connect(db) as con:
        con.execute(
            """
            CREATE TRIGGER fail_second_event
            BEFORE INSERT ON order_history
            WHEN NEW.event_id = 'batch-event-2'
            BEGIN
                SELECT RAISE(ABORT, 'injected_publish_failure');
            END;
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected_publish_failure"):
        asyncio.run(
            run_pipeline(
                batch_source,
                Config(db_path=db, max_retries=0, retry_delay_seconds=0),
                "run-publish-failure",
            )
        )

    with connect(db) as con:
        failed_status = con.execute(
            "SELECT status FROM runs WHERE run_id='run-publish-failure'"
        ).fetchone()[0]
        failed_publish_attempts = con.execute(
            """
            SELECT COUNT(*) FROM task_runs
            WHERE run_id='run-publish-failure' AND task_name='publish'
            """
        ).fetchone()[0]
        history_after_failure = con.execute(
            "SELECT event_id FROM order_history ORDER BY event_id"
        ).fetchall()
        targets_after_failure = con.execute(
            "SELECT order_id FROM target_orders ORDER BY order_id"
        ).fetchall()

    assert failed_status == "failed"
    assert failed_publish_attempts == 1
    assert history_after_failure == [("baseline-event",)]
    assert targets_after_failure == [("baseline-order",)]

    # Remove the injected failure and rerun the corrected batch as a new run.
    with connect(db) as con:
        con.execute("DROP TRIGGER fail_second_event")

    recovered_run_id = asyncio.run(
        run_pipeline(
            batch_source,
            Config(db_path=db, max_retries=0, retry_delay_seconds=0),
            "run-publish-recovered",
        )
    )

    with connect(db) as con:
        recovered_status = con.execute(
            "SELECT status FROM runs WHERE run_id=?",
            (recovered_run_id,),
        ).fetchone()[0]
        final_history = con.execute(
            "SELECT event_id FROM order_history ORDER BY event_id"
        ).fetchall()
        final_targets = con.execute(
            "SELECT order_id FROM target_orders ORDER BY order_id"
        ).fetchall()

    assert recovered_status == "success"
    assert final_history == [
        ("baseline-event",),
        ("batch-event-1",),
        ("batch-event-2",),
    ]
    assert final_targets == [
        ("baseline-order",),
        ("batch-order-1",),
        ("batch-order-2",),
    ]
    with connect(db) as con:
        failed_metrics = con.execute(
            """
            SELECT history_inserted_count,
                   duplicate_skipped_count,
                   target_upsert_count,
                   output_count
            FROM runs
            WHERE run_id = 'run-publish-failure'
            """
        ).fetchone()

    assert failed_metrics == (0, 0, 0, 0)


def test_transient_publish_failure_retries_and_commits_metrics(
    tmp_path, monkeypatch
):
    db = tmp_path / "retry.sqlite"
    source = tmp_path / "retry.csv"

    write_event_csv(
        source,
        [
            event(
                "retry-event-1",
                "retry-order-1",
                "2026-11-03T10:00:00+00:00",
            ),
            event(
                "retry-event-2",
                "retry-order-2",
                "2026-11-03T11:00:00+00:00",
            ),
        ],
    )

    original_publish = pipeline.publish
    calls = {"count": 0}

    async def flaky_publish(ctx):
        calls["count"] += 1

        if calls["count"] == 1:
            raise sqlite3.OperationalError("temporary database failure")

        await original_publish(ctx)

    monkeypatch.setitem(pipeline.TASKS, "publish", flaky_publish)

    run_id = asyncio.run(
        run_pipeline(
            source,
            Config(
                db_path=db,
                max_retries=1,
                retry_delay_seconds=0,
            ),
            "run-automatic-retry",
        )
    )

    with connect(db) as con:
        run = con.execute(
            """
            SELECT status, output_count, history_inserted_count,
                   duplicate_skipped_count, target_upsert_count
            FROM runs WHERE run_id=?
            """,
            (run_id,),
        ).fetchone()

        attempts = con.execute(
            """
            SELECT status FROM task_runs
            WHERE run_id=? AND task_name='publish'
            ORDER BY task_run_id
            """,
            (run_id,),
        ).fetchall()

        history_count = con.execute(
            "SELECT COUNT(*) FROM order_history"
        ).fetchone()[0]

    assert calls["count"] == 2
    assert run == ("success", 2, 2, 0, 2)
    assert attempts == [("failed",), ("success",)]
    assert history_count == 2
