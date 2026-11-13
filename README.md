# YottaFlex - Orchestrated Data Pipeline

## 1. Project Overview

A Python-based, locally runnable data engineering solution that demonstrates an orchestrated ETL workflow with dependent tasks, configurable retries, task timeouts, execution tracking, data validation, reject quarantine, idempotent publishing, event history, and failure recovery.

The pipeline processes order events from CSV files, validates business rules, transforms accepted records, and publishes them into SQLite.

### Key Capabilities

- Dependency-based task orchestration.
- Configurable retries and task timeouts.
- Run-level and task-level execution tracking.
- Schema and business-rule validation.
- Rejected-record quarantine with source traceability.
- Idempotent event publishing.
- Append-only event history and current-state publishing.
- Late-arriving event protection.
- Transaction rollback on publication failure.
- Reproducible sample-data generation.
- Automated tests for positive and negative scenarios.

## 2. Architecture

```text
                         CSV Source
                             |
                             v
                          1. INGEST
                       Read and check schema
                             |
                             v
                         2. VALIDATE
                      Required fields and rules
                             |
                       +-----+-----+
                       |           |
                       v           v
                  Valid Records  Rejects
                       |        (Quarantine)
                       v
                     3. TRANSFORM
                    Normalize records
                    Round amount values
                             |
                             v
                          4. PUBLISH
                             |
                       +-----+-----+
                       |           |
                       v           v
                  order_history  target_orders
                  Event history  Latest state
                       |           |
                       +-----+-----+
                             |
                           SQLite
                             |
                             v
                    Run and Task Tracking
```

The workflow executes in dependency order:

`ingest → validate → transform → publish`

A task runs only after its required upstream dependencies succeed. A critical upstream failure prevents downstream publication.

Each task records its execution status, attempt number, timestamps, and error details.

## 3. Technology Stack

- Python 3.10+
- SQLite
- CSV
- asyncio
- pytest
- Python standard library

No external cloud services or credentials are required.

## 4. Project Structure

```text
yottaflex-orchestrated-data-pipeline/
│
├── src/
│   ├── __init__.py
│   └── pipeline.py
│
├── tests/
│   ├── test_pipeline.py
│   ├── test_event_history.py
│   ├── test_pipeline_metrics.py
│   └── test_publish_recovery.py
│
├── data/
│   └── orders.csv
│
├── requirements.txt
├── README.md
└── .gitignore
```

The SQLite database is created and initialized by the application when required. Local virtual environments, caches, and temporary databases are not required for execution.

## 5. Data Model and Data Dictionary

### Source Event Schema

The generated event CSV contains six columns.

| Column | Type | Description |
|---|---|---|
| event_id | String | Unique event identifier |
| order_id | String | Business order identifier |
| customer_id | String | Customer reference |
| amount | Numeric | Non-negative order amount |
| order_date | Date | Business date in YYYY-MM-DD format |
| event_timestamp | ISO-8601 timestamp | Event occurrence timestamp with timezone |

The pipeline also supports the original four-column legacy CSV format for backward compatibility.

### Database Entities

| Table | Primary key | Purpose |
|---|---|---|
| runs | run_id | Pipeline execution status and run-level metrics |
| task_runs | task_run_id | Individual task attempts, statuses, and errors |
| order_history | event_id | Append-only record of accepted events |
| target_orders | order_id | Latest accepted state for each order |
| rejects | reject_id | Quarantined records with source and rejection reason |

### Important Metrics

The run record tracks:

- `input_count`: Number of source records read.
- `output_count`: Number of records published to the target during the run.
- `rejected_count`: Number of rejected source records.
- `history_inserted_count`: Number of new event-history records inserted.
- `duplicate_skipped_count`: Number of already-seen events skipped.
- `target_upsert_count`: Number of target records inserted or updated.

These metrics help reconcile source processing with publication outcomes.

## 6. Data Quality and Business Rules

The validation stage checks the incoming records before publication.

Examples include:

- Required order and customer identifiers.
- Customer reference membership in the configured customer ID set.
- Valid numeric amounts.
- Rejection of negative and non-finite amounts.
- Valid business dates.
- Required event identifiers for event-schema records.
- Timezone-aware ISO-8601 event timestamps.
- Duplicate event detection.
- Schema compatibility.

Invalid records are recorded in the `rejects` table with their source file, CSV line number, raw record, and rejection reason.

The generated configuration contains these customer references by default:

- customer-001
- customer-002
- customer-003
- customer-004

These references are configurable through the Python `Config` object.

## 7. Orchestration, Retries, and Failure Handling

### Task Dependencies

The pipeline contains four dependent tasks:

1. `ingest` — reads the source CSV.
2. `validate` — checks records and separates valid records from rejects.
3. `transform` — normalizes accepted records.
4. `publish` — writes accepted records into SQLite.

### Retry Configuration

Default configuration:

| Setting | Default |
|---|---:|
| Maximum retries | 2 |
| Total attempts | 3 |
| Task timeout | 10 seconds |
| Retry delay | 0.1 seconds |

The initial execution is followed by up to two retries for retryable exceptions.

Permanent input and schema errors are treated as non-retryable.

### Timeout Handling

Tasks are executed with an asynchronous timeout using `asyncio.wait_for`.

A timeout is recorded as a task failure and follows the configured retry policy.

**Important limitation:** Asynchronous timeouts do not guarantee interruption of blocking synchronous operations such as file or database I/O.

### Failure Notification

On pipeline failure, the application:

- Logs the failure.
- Records the failed run and failure reason.
- Prints a console failure notification containing the run ID and reason.
- Raises the exception to the caller.

The current implementation uses application logging and console notification. External email, Slack, or PagerDuty integration is not included.

## 8. Idempotency and Event History

The pipeline is designed to support safe reruns.

- Event IDs are used as the event-history uniqueness key.
- Reprocessing an already accepted event does not insert it again.
- Conflicting payloads for an existing event ID are rejected.
- The target table maintains the latest accepted order state.
- Older late-arriving events do not overwrite a newer event timestamp.
- Publication uses a database transaction to prevent partial batch commits.

The legacy four-column input format uses a deterministic event identifier derived from the record content.

## 9. Installation and Setup

### Prerequisites

- Python 3.10 or newer.
- pip.
- Windows, Linux, or macOS.

### Clone the Repository

```bash
git clone https://github.com/manikantanarsampally-art/yottaflex-orchestrated-data-pipeline.git

cd yottaflex-orchestrated-data-pipeline
```

### Create a Virtual Environment

**Windows PowerShell:**

```powershell
python -m venv .venv

.venv\Scripts\Activate.ps1
```

**Linux or macOS:**

```bash
python3 -m venv .venv

source .venv/bin/activate
```

### Install Dependencies

```bash
python -m pip install -r requirements.txt
```

## 10. Running the Pipeline

### Generate Sample Data

Generate 30 reproducible sample records:

```bash
python -m src.pipeline --generate 30
```

The default output file is `data/orders.csv`.

### Run the Pipeline

```bash
python -m src.pipeline
```

The default input is `data/orders.csv`, and the default database is `pipeline.db`.

### Specify a Custom Input and Database

```bash
python -m src.pipeline --input data/orders.csv --db pipeline.db
```

### Configure Retries and Timeout

```bash
python -m src.pipeline --input data/orders.csv --db pipeline.db --retries 3 --timeout 15
```

### Generate Data at a Custom Path

```bash
python -m src.pipeline --input data/custom_orders.csv --generate 1000
```

The generator creates reproducible sample records using a fixed random seed.

## 11. Monitoring and Observability

Execution information is stored in SQLite.

The `runs` table provides run-level status, timestamps, source file, counts, and failure reason.

The `task_runs` table stores task-level execution details, including task name, attempt number, status, timestamps, and error information.

Application logs provide visibility into task execution and failures.

The pipeline also prints a run report after execution.

## 12. Automated Testing

Run the complete test suite:

```bash
python -m pytest -v
```

The tests cover:

- Successful pipeline execution and reconciliation.
- Invalid records and reject quarantine.
- Duplicate business keys.
- Empty input.
- Unexpected input schemas.
- Idempotent reruns.
- Critical validation failure blocking publication.
- Missing required fields.
- Valid boundary values and non-finite amounts.
- Invalid configuration.
- Timeout and retry behaviour.
- Publication rollback and recovery.
- Transient publication failure and retry.
- Publication metrics.
- Duplicate and conflicting event IDs.
- Late-arriving event handling.

### Latest Local Validation

The complete test suite was executed successfully:

```text
30 passed
```

Re-run the test command to verify the current checkout.

## 13. Limitations and Future Improvements

The current solution is intentionally self-contained and locally runnable.

Potential future improvements include:

- External failure notifications through email or messaging integrations.
- A configurable customer dimension table with database-enforced foreign keys.
- Cloud object storage and managed orchestration integration.
- Distributed execution for larger workloads.
- More advanced scheduling and backfill management.
- Dashboards for run and data-quality metrics.
- Stronger isolation of blocking I/O from asynchronous task execution.

These are potential extensions and are not represented as implemented features.

## 14. Design Principles

The implementation emphasizes:

- Modularity and separation of pipeline stages.
- Configurable execution behaviour.
- Deterministic sample generation.
- Data-quality enforcement before publication.
- Traceability of rejected records.
- Safe reruns and duplicate handling.
- Transactional publication.
- Run-level and task-level observability.
- Automated verification of failure scenarios.

---

**Project:** YottaFlex Orchestrated Data Pipeline

**Language:** Python

**Database:** SQLite

**Testing:** pytest
