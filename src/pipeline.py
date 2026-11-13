from __future__ import annotations

import math
import argparse
import asyncio
import csv
import hashlib
import json
import logging
import random
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "pipeline.db"
LOG = logging.getLogger("yottaflex")


class NonRetryableError(Exception):
    """Raised when retrying cannot resolve a task failure."""


class PermanentInputError(NonRetryableError, ValueError):
    """Raised for invalid input that will not improve through retries."""


class PermanentFileError(NonRetryableError, FileNotFoundError):
    """Raised when the required source file does not exist."""

LEGACY_COLUMNS = {"order_id", "customer_id", "amount", "order_date"}
EVENT_COLUMNS = {
    "event_id", "order_id", "customer_id", "amount",
    "order_date", "event_timestamp",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_event_timestamp(value: str) -> str:
    """Parse an ISO-8601 timestamp and normalize it to UTC."""
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("event_timestamp_must_include_timezone")
    return parsed.astimezone(timezone.utc).isoformat()


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: Path) -> None:
    with connect(db_path) as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY, source_file TEXT NOT NULL,
            status TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT,
            input_count INTEGER DEFAULT 0, output_count INTEGER DEFAULT 0,
            rejected_count INTEGER DEFAULT 0, failure_reason TEXT,
            history_inserted_count INTEGER NOT NULL DEFAULT 0,
            duplicate_skipped_count INTEGER NOT NULL DEFAULT 0,
            target_upsert_count INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS task_runs (
            task_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL REFERENCES runs(run_id),
            task_name TEXT NOT NULL, status TEXT NOT NULL,
            attempt INTEGER NOT NULL, started_at TEXT NOT NULL,
            ended_at TEXT, error TEXT
        );

        CREATE TABLE IF NOT EXISTS target_orders (
            order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL,
            amount REAL NOT NULL, order_date TEXT NOT NULL,
            source_file TEXT NOT NULL, run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            event_timestamp TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS order_history (
            event_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            amount REAL NOT NULL,
            order_date TEXT NOT NULL,
            event_timestamp TEXT NOT NULL,
            source_file TEXT NOT NULL,
            run_id TEXT NOT NULL,
            ingested_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rejects (
            reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL, source_file TEXT NOT NULL,
            line_number INTEGER NOT NULL, raw_record TEXT NOT NULL,
            reason TEXT NOT NULL, created_at TEXT NOT NULL,
            UNIQUE(run_id, source_file, line_number)
        );
        """)

        # Migrate databases created by the earlier version of this project.
        target_columns = {
            row[1] for row in con.execute("PRAGMA table_info(target_orders)")
        }
        if "event_timestamp" not in target_columns:
            con.execute(
                "ALTER TABLE target_orders "
                "ADD COLUMN event_timestamp TEXT NOT NULL DEFAULT ''"
            )

        run_columns = {row[1] for row in con.execute("PRAGMA table_info(runs)")}
        for column in ("history_inserted_count", "duplicate_skipped_count", "target_upsert_count"):
            if column not in run_columns:
                con.execute(f"ALTER TABLE runs ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0")


@dataclass
class Config:
    db_path: Path = DEFAULT_DB
    max_retries: int = 2
    task_timeout_seconds: float = 10.0
    retry_delay_seconds: float = 0.1
    valid_customer_ids: set[str] = field(default_factory=lambda: {
        "customer-001", "customer-002", "customer-003", "customer-004"
    })

    def __post_init__(self):
        if isinstance(self.max_retries, bool) or not isinstance(self.max_retries, int):
            raise ValueError("max_retries must be a non-negative integer")

        if self.max_retries < 0:
            raise ValueError("max_retries must be a non-negative integer")

        if not math.isfinite(self.task_timeout_seconds) or self.task_timeout_seconds <= 0:
            raise ValueError("task_timeout_seconds must be finite and greater than zero")

        if not math.isfinite(self.retry_delay_seconds) or self.retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be finite and non-negative")

        if not self.valid_customer_ids or any(
            not isinstance(customer_id, str) or not customer_id.strip()
            for customer_id in self.valid_customer_ids
        ):
            raise ValueError("valid_customer_ids must contain non-empty strings")


@dataclass
class Context:
    run_id: str
    source_file: Path
    config: Config
    raw_rows: list[dict[str, Any]] = field(default_factory=list)
    valid_rows: list[dict[str, Any]] = field(default_factory=list)
    transformed_rows: list[dict[str, Any]] = field(default_factory=list)
    rejects: list[dict[str, Any]] = field(default_factory=list)
    input_count: int = 0
    history_inserted_count: int = 0
    duplicate_skipped_count: int = 0
    target_upsert_count: int = 0
    legacy_schema: bool = False


async def ingest(ctx: Context) -> None:
    if not ctx.source_file.exists():
        raise PermanentFileError(
    f"Source file not found: {ctx.source_file}"
)

    with ctx.source_file.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        actual = set(reader.fieldnames or [])

        if actual == EVENT_COLUMNS:
            ctx.legacy_schema = False
        elif actual == LEGACY_COLUMNS:
            # Backward compatibility for the original sample and tests.
            # New generated files use the six-column event schema.
            ctx.legacy_schema = True
        else:
            raise PermanentInputError(
                "Unexpected schema. Expected either "
                f"{sorted(EVENT_COLUMNS)} or {sorted(LEGACY_COLUMNS)}, "
                f"got {sorted(actual)}"
            )

        for line_no, row in enumerate(reader, start=2):
            row["_line_number"] = line_no
            ctx.raw_rows.append(row)

    ctx.input_count = len(ctx.raw_rows)
    if ctx.input_count == 0:
        raise PermanentInputError("Empty input batch")


async def validate(ctx: Context) -> None:
    seen_event_ids: set[str] = set()
    seen_legacy_order_ids: set[str] = set()

    for row in ctx.raw_rows:
        reasons: list[str] = []
        order_id = (row.get("order_id") or "").strip()
        customer_id = (row.get("customer_id") or "").strip()

        if not order_id:
            reasons.append("missing_order_id")
        if not customer_id:
            reasons.append("missing_customer_id")
        elif customer_id not in ctx.config.valid_customer_ids:
            reasons.append("invalid_customer_reference")

        try:
            amount = float(row.get("amount", ""))

            if not math.isfinite(amount):
                reasons.append("invalid_amount")

            elif amount < 0:
                reasons.append("negative_amount")
        except (TypeError, ValueError):
            amount = None
            reasons.append("invalid_amount")

        order_date = (row.get("order_date") or "").strip()
        try:
            datetime.strptime(order_date, "%Y-%m-%d")
        except ValueError:
            reasons.append("invalid_order_date")

        if ctx.legacy_schema:
            # Stable event ID for old-format records, allowing safe reruns.
            legacy_payload = json.dumps(
                {
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "amount": row.get("amount", ""),
                    "order_date": order_date,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            event_id = "legacy-" + hashlib.sha256(
                legacy_payload.encode("utf-8")
            ).hexdigest()
            event_timestamp = ""
            if order_date:
                try:
                    parsed_date = datetime.strptime(order_date, "%Y-%m-%d")
                    event_timestamp = parsed_date.replace(
                        tzinfo=timezone.utc
                    ).isoformat()
                except ValueError:
                    pass

            # Preserve the original duplicate-order behavior for legacy input.
            if order_id and order_id in seen_legacy_order_ids:
                reasons.append("duplicate_order_id_in_batch")
            if order_id:
                seen_legacy_order_ids.add(order_id)
        else:
            event_id = (row.get("event_id") or "").strip()
            if not event_id:
                reasons.append("missing_event_id")
            elif event_id in seen_event_ids:
                reasons.append("duplicate_event_id_in_batch")
            if event_id:
                seen_event_ids.add(event_id)

            try:
                event_timestamp = normalize_event_timestamp(
                    row.get("event_timestamp") or ""
                )
            except (TypeError, ValueError):
                event_timestamp = ""
                reasons.append("invalid_event_timestamp")

        if reasons:
            ctx.rejects.append({
                "line_number": row["_line_number"],
                "raw_record": json.dumps({
                    k: v for k, v in row.items() if not k.startswith("_")
                }),
                "reason": ";".join(reasons),
            })
        else:
            ctx.valid_rows.append({
                "event_id": event_id,
                "order_id": order_id,
                "customer_id": customer_id,
                "amount": amount,
                "order_date": order_date,
                "event_timestamp": event_timestamp,
            })


async def transform(ctx: Context) -> None:
    # Deterministic transformation: round currency to two decimal places.
    ctx.transformed_rows = [
        {**row, "amount": round(float(row["amount"]), 2)}
        for row in ctx.valid_rows
    ]




async def publish(ctx: Context) -> None:
    """
    Publish events atomically.

    Metrics are accumulated locally and copied to the run context
    only after the SQLite transaction commits successfully.
    """
    history_inserted = 0
    duplicates_skipped = 0
    targets_upserted = 0

    with connect(ctx.config.db_path) as con:
        for row in ctx.transformed_rows:
            inserted = con.execute(
                """
                INSERT OR IGNORE INTO order_history
                (
                    event_id, order_id, customer_id, amount,
                    order_date, event_timestamp, source_file,
                    run_id, ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["event_id"],
                    row["order_id"],
                    row["customer_id"],
                    row["amount"],
                    row["order_date"],
                    row["event_timestamp"],
                    ctx.source_file.name,
                    ctx.run_id,
                    utc_now(),
                ),
            )

            if inserted.rowcount == 0:
                existing = con.execute(
                    """
                    SELECT order_id, customer_id, amount,
                           order_date, event_timestamp
                    FROM order_history
                    WHERE event_id = ?
                    """,
                    (row["event_id"],),
                ).fetchone()

                incoming_payload = (
                    row["order_id"],
                    row["customer_id"],
                    round(float(row["amount"]), 2),
                    row["order_date"],
                    row["event_timestamp"],
                )

                existing_payload = (
                    (
                        existing[0],
                        existing[1],
                        round(float(existing[2]), 2),
                        existing[3],
                        existing[4],
                    )
                    if existing
                    else None
                )

                if existing_payload != incoming_payload:
                    raise NonRetryableError(
                        f"Conflicting payload for event_id "
                        f"'{row['event_id']}': existing event data "
                        "does not match incoming event data"
                    )

                duplicates_skipped += 1
                continue

            history_inserted += 1

            target_result = con.execute(
                """
                INSERT INTO target_orders
                (
                    order_id, customer_id, amount, order_date,
                    source_file, run_id, updated_at, event_timestamp
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    customer_id = excluded.customer_id,
                    amount = excluded.amount,
                    order_date = excluded.order_date,
                    source_file = excluded.source_file,
                    run_id = excluded.run_id,
                    updated_at = excluded.updated_at,
                    event_timestamp = excluded.event_timestamp
                WHERE excluded.event_timestamp >
                      target_orders.event_timestamp
                """,
                (
                    row["order_id"],
                    row["customer_id"],
                    row["amount"],
                    row["order_date"],
                    ctx.source_file.name,
                    ctx.run_id,
                    utc_now(),
                    row["event_timestamp"],
                ),
            )

            targets_upserted += max(target_result.rowcount, 0)

    # Reached only after the SQLite transaction commits successfully.
    ctx.history_inserted_count += history_inserted
    ctx.duplicate_skipped_count += duplicates_skipped
    ctx.target_upsert_count += targets_upserted



TASKS: dict[str, Callable[[Context], Awaitable[None]]] = {
    "ingest": ingest,
    "validate": validate,
    "transform": transform,
    "publish": publish,
}

DEPENDENCIES = {
    "ingest": [],
    "validate": ["ingest"],
    "transform": ["validate"],
    "publish": ["transform"],
}


def record_task(
    db_path: Path,
    run_id: str,
    name: str,
    status: str,
    attempt: int,
    started: str,
    error: str | None = None,
) -> None:
    with connect(db_path) as con:
        con.execute("""
            INSERT INTO task_runs
            (run_id, task_name, status, attempt, started_at, ended_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (run_id, name, status, attempt, started, utc_now(), error))


async def execute_task(ctx: Context, name: str) -> None:
    fn = TASKS[name]

    for attempt in range(1, ctx.config.max_retries + 2):
        started = utc_now()

        try:
            await asyncio.wait_for(
                fn(ctx),
                timeout=ctx.config.task_timeout_seconds,
            )

            record_task(
                ctx.config.db_path,
                ctx.run_id,
                name,
                "success",
                attempt,
                started,
            )

            LOG.info(
                "task=%s status=success attempt=%s",
                name,
                attempt,
            )
            return

        except NonRetryableError as exc:
            error = f"{type(exc).__name__}: {exc}"

            record_task(
                ctx.config.db_path,
                ctx.run_id,
                name,
                "failed",
                attempt,
                started,
                error,
            )

            LOG.error(
                "task=%s status=failed attempt=%s non_retryable=true error=%s",
                name,
                attempt,
                error,
            )
            raise

        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

            record_task(
                ctx.config.db_path,
                ctx.run_id,
                name,
                "failed",
                attempt,
                started,
                error,
            )

            LOG.warning(
                "task=%s status=failed attempt=%s error=%s",
                name,
                attempt,
                error,
            )

            if attempt > ctx.config.max_retries:
                raise

            await asyncio.sleep(ctx.config.retry_delay_seconds)


async def run_pipeline(
    source_file: Path,
    config: Config | None = None,
    run_id: str | None = None,
) -> str:
    config = config or Config()
    init_db(config.db_path)
    run_id = run_id or str(uuid.uuid4())
    ctx = Context(run_id=run_id, source_file=source_file, config=config)

    with connect(config.db_path) as con:
        con.execute("""
            INSERT INTO runs(run_id, source_file, status, started_at)
            VALUES (?, ?, 'running', ?)
        """, (run_id, source_file.name, utc_now()))

    try:
        completed: set[str] = set()
        for task_name in TASKS:
            if not set(DEPENDENCIES[task_name]).issubset(completed):
                raise RuntimeError(f"Dependencies not met for {task_name}")
            await execute_task(ctx, task_name)
            completed.add(task_name)

        with connect(config.db_path) as con:
            con.executemany("""
                INSERT INTO rejects
                (run_id, source_file, line_number, raw_record, reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                (
                    run_id, source_file.name, r["line_number"],
                    r["raw_record"], r["reason"], utc_now()
                )
                for r in ctx.rejects
            ])
            con.execute("""
                UPDATE runs SET status='success', ended_at=?, input_count=?,
                output_count=?, rejected_count=?, history_inserted_count=?,
                duplicate_skipped_count=?, target_upsert_count=? WHERE run_id=?
            """, (
                utc_now(), ctx.input_count, ctx.target_upsert_count,
                len(ctx.rejects), ctx.history_inserted_count,
                ctx.duplicate_skipped_count, ctx.target_upsert_count, run_id
            ))
        return run_id

    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"
        with connect(config.db_path) as con:
            for r in ctx.rejects:
                con.execute("""
                    INSERT OR IGNORE INTO rejects
                    (run_id, source_file, line_number, raw_record, reason, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    run_id, source_file.name, r["line_number"],
                    r["raw_record"], r["reason"], utc_now()
                ))
            con.execute("""
                UPDATE runs SET status='failed', ended_at=?, input_count=?,
                output_count=0, rejected_count=?, failure_reason=?,
                history_inserted_count=0, duplicate_skipped_count=0,
                target_upsert_count=0 WHERE run_id=?
            """, (
                utc_now(), ctx.input_count, len(ctx.rejects), reason, run_id
            ))
        LOG.exception("RUN FAILED run_id=%s reason=%s", run_id, reason)
        print(f"FAILURE NOTIFICATION: run_id={run_id} reason={reason}")
        raise


def generate_sample(
    path: Path,
    rows: int = 30,
    seed: int = 42,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    customers = [
        "customer-001",
        "customer-002",
        "customer-003",
        "customer-004",
    ]

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "event_id", "order_id", "customer_id",
                "amount", "order_date", "event_timestamp",
            ],
        )
        writer.writeheader()

        for i in range(rows):
            event_date = datetime(
                2026, 11, (i % 28) + 1, 10, 0, 0,
                tzinfo=timezone.utc,
            )
            writer.writerow({
                "event_id": f"event-{i+1:06d}",
                "order_id": f"order-{i+1:06d}",
                "customer_id": rng.choice(customers),
                "amount": f"{rng.uniform(1, 500):.2f}",
                "order_date": event_date.strftime("%Y-%m-%d"),
                "event_timestamp": event_date.isoformat(),
            })


def report(db_path: Path, run_id: str) -> None:
    with connect(db_path) as con:
        run = con.execute(
            "SELECT run_id,status,input_count,output_count,rejected_count,"
            "history_inserted_count,duplicate_skipped_count,target_upsert_count,failure_reason "
            "FROM runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        tasks = con.execute(
            "SELECT task_name,status,attempt,error FROM task_runs WHERE run_id=? "
            "ORDER BY task_run_id",
            (run_id,),
        ).fetchall()

    print("\nRUN REPORT")
    print(run)
    print("TASK ATTEMPTS")
    for task in tasks:
        print(task)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="YottaFlex orchestrated data pipeline"
    )
    parser.add_argument(
        "--input", type=Path, default=ROOT / "data" / "orders.csv"
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument(
        "--generate", type=int, metavar="ROWS",
        help="Generate a reproducible input CSV and exit",
    )
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    if args.generate is not None:
        generate_sample(args.input, args.generate)
        print(f"Generated {args.generate} rows at {args.input}")
        return

    config = Config(
        db_path=args.db,
        max_retries=args.retries,
        task_timeout_seconds=args.timeout,
    )
    run_id = asyncio.run(run_pipeline(args.input, config))
    report(args.db, run_id)


if __name__ == "__main__":
    main()
