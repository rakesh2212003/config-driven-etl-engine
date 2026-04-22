# RAW2DIS

A generic, config-driven PySpark CDC pipeline that reads raw files, applies data mapping transformations, and writes to a Hudi table with SCD Type 2 history tracking.

---

## Overview

```
RAW Layer (CSV / Parquet / JSON)
        │
        ▼
   Bookmark Check
   (skip already processed files)
        │
        ▼
   Format Detection
   (auto-detect from file extension)
        │
        ▼
   Data Mapping
   (rules: CAST, TRIM, UPPER, COALESCE, DERIVED ...)
        │
        ▼
   SCD Type 2
   (close old records, insert new versions)
        │
        ▼
  DIS Layer (Hudi / MERGE_ON_READ)
```

---

## Project Structure

```
raw2dis/
│
├── main.py                          # Entry point
├── requirements.txt
│
├── config/
│   ├── app.yaml                     # App-level config (Spark, paths, audit, logging)
│   ├── schemas/                     # DDL schema files per table
│   └── mappings/
│       ├── data-mapping-config/
│       │   └── customers.json       # Column mapping + Hudi config per table
│       └── bookmark-mapping-config/
│           └── customers.json       # (legacy — replaced by data/bookmarks/bookmark.json)
│
├── data/
│   ├── raw/
│   │   └── customers/
│   │       ├── cdc_20240101_120000.csv
│   │       └── cdc_20240102_120000.csv
│   ├── distilled/
│   │   └── customers/               # Hudi table output
│   ├── bookmarks/
│   │   └── bookmark.json            # Single bookmark file for all tables
│   ├── rejected/
│   │   └── customers/               # Bad records
│   └── audit/
│       └── customers/
│           └── audit_YYYYMMDD.jsonl # Daily audit log (newline-delimited JSON)
│
├── logs/
│   └── log_YYYYMMDD.log
│
└── src/
    ├── common/
    │   └── helpers.py               # format_duration, write_audit, ensure_dir
    ├── core/
    │   ├── config.py                # load_config, load_mapping, load/save bookmark
    │   ├── logger.py                # get_logger (file + console handlers)
    │   └── spark.py                 # get_spark (builds SparkSession from app.yaml)
    ├── io/
    │   ├── reader.py                # read_raw, get_pending_cdc_files
    │   └── writer.py                # write_hudi (SCD2 close + insert)
    ├── transforms/
    │   └── mapping.py               # apply_mapping, rule engine
    └── jobs/
        ├── __init__.py              # JOB_REGISTRY + get_job()
        └── raw2dis.py               # Main job orchestration
```

---

## Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### requirements.txt

```
pyspark==3.4.0
hudi-spark3.4-bundle_2.12
pyyaml
```

---

## Configuration

### `config/app.yaml`

App-level settings that apply to the whole pipeline — Spark, paths, retry, audit, logging. Nothing table-specific lives here.

```yaml
spark:
  app_name: "RAW2DIS"
  master: "local[*]"
  config:
    spark.driver.memory: "4g"
    spark.sql.shuffle.partitions: 2
    ...

raw_layer:
  path: "data/raw/"
  corrupt_record_handling: "PERMISSIVE"
  options:
    csv:
      header: true
      inferSchema: false
      ...

dis_layer:
  path: "data/distilled/"

bookmark:
  path: "data/bookmarks/bookmark.json"
```

### `config/mappings/data-mapping-config/{table}.json`

Per-table config — column mappings, transformation rules, Hudi settings, SCD2 config. One file per table.

```json
{
    "version": "1.0",
    "mapping": {
        "table_name": "customers",
        "primary_key": ["customer_id"],
        "precombine_field": "effective_from",
        "write_mode": "upsert"
    },
    "hudi": {
        "table_type": "MERGE_ON_READ",
        "options": { ... },
        "compaction": { "enabled": true, ... }
    },
    "scd": {
        "type": 2,
        "effective_from_field": "effective_from",
        "effective_to_field": "effective_to",
        "is_current_field": "is_current",
        "high_date": "9999-12-31 00:00:00"
    },
    "bookmark": {
        "file_pattern": "cdc_*.csv"
    },
    "columns": [
        {
            "seq": 1,
            "source": "customer_id",
            "target": "customer_id",
            "type": "STRING",
            "nullable": false,
            "rules": []
        },
        ...
    ]
}
```

---

## Transformation Rules

Rules are applied in sequence per column. Each rule is an `op` with optional parameters.

| Op                | Description                       | Parameters                                  |
| ----------------- | --------------------------------- | ------------------------------------------- |
| `TRIM`          | Strip leading/trailing whitespace | —                                          |
| `UPPER`         | Convert to uppercase              | —                                          |
| `LOWER`         | Convert to lowercase              | —                                          |
| `CAST`          | Cast to column `type`           | `format`(for DATE/TIMESTAMP),`on_error` |
| `COALESCE`      | Replace null with default         | `default`                                 |
| `REGEX_REPLACE` | Replace pattern with string       | `pattern`,`replacement`                 |
| `TRUNCATE`      | Limit string length               | `max_length`                              |
| `DERIVED`       | Compute from Spark SQL expression | `expr`                                    |

**Example — multi-rule column:**

```json
{
    "seq": 10,
    "source": "loyalty_tier",
    "target": "loyalty_tier",
    "type": "STRING",
    "nullable": true,
    "rules": [
        { "op": "COALESCE", "default": "STANDARD" },
        { "op": "TRIM" },
        { "op": "UPPER" }
    ]
}
```

**Example — derived column:**

```json
{
    "seq": 15,
    "source": null,
    "target": "effective_from",
    "type": "TIMESTAMP",
    "nullable": false,
    "rules": [
        { "op": "DERIVED", "expr": "COALESCE(updated_at, created_at)" }
    ]
}
```

---

## CDC & Bookmark

Raw CDC files must follow the naming pattern:

```
data/raw/{table_name}/cdc_<timestamp>.csv
```

Example:

```
data/raw/customers/cdc_20240101_120000.csv
data/raw/customers/cdc_20240102_120000.csv
```

The pipeline scans for files matching `cdc_*.csv`, compares against the bookmark, and processes only files newer than the last successful run — oldest to newest.

`data/bookmarks/bookmark.json` — single file tracking all tables:

```json
{
    "customers": {
        "last_processed_file": "cdc_20240101_120000.csv",
        "last_processed_at": "2024-01-01 12:05:00",
        "status": "SUCCESS"
    }
}
```

On failure the bookmark is saved with `"status": "FAILED"` at the failing file. On rerun the pipeline resumes from that file.

---

## SCD Type 2

Each CDC record with `op = I / U / D` is handled as follows:

| op    | Action                                                                                               |
| ----- | ---------------------------------------------------------------------------------------------------- |
| `I` | Insert new record with `effective_to = 9999-12-31`,`is_current = true`                           |
| `U` | Close existing record (`effective_to = effective_from`,`is_current = false`), insert new version |
| `D` | Close existing record,`is_current = false`                                                         |

---

## Running

```bash
python main.py --job raw2dis --table customers
```

---

## Audit

Each processed file produces an audit record written to:

```
data/audit/{table_name}/audit_YYYYMMDD.jsonl
```

Example record:

```json
{
    "run_id": "a1b2c3d4-...",
    "table_name": "customers",
    "cdc_file": "cdc_20240101_120000.csv",
    "rows_read": 1500,
    "rows_inserted": 800,
    "rows_updated": 600,
    "rows_deleted": 100,
    "rows_rejected": 0,
    "started_at": "2024-01-01 12:00:00",
    "finished_at": "2024-01-01 12:00:45",
    "status": "SUCCESS"
}
```

---

## Adding a New Table

1. Create mapping config at `config/mappings/data-mapping-config/{table}.json`
2. Drop CDC files at `data/raw/{table}/cdc_<timestamp>.csv`
3. Add table entry to `data/bookmarks/bookmark.json` with nulls
4. Run:

```bash
python main.py --job raw2dis --table {table}
```

No code changes needed.
