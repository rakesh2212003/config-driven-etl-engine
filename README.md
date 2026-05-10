# Config-Driven PySpark CDC + SCD2 Engine

A generic, modular, config-driven PySpark data engineering framework designed for:

- CDC (Change Data Capture) ingestion
- Incremental processing
- SCD Type 2 versioning
- Hudi-based storage
- Generic table onboarding
- Job-based execution architecture

The framework is designed to behave like a lightweight data platform instead of a collection of table-specific ETL scripts.

---

FEATURES

1. Generic Pipeline Architecture

No table-specific Python pipelines are required.

New tables can be onboarded by simply adding:

- raw source file
- mapping config
- bookmark config

---

2. Config-Driven Processing

Pipeline behavior is controlled through JSON/YAML configuration files.

Supports:

- column mapping
- datatype casting
- transformations
- Hudi options
- incremental processing

---

3. CDC Processing

Supports incremental CDC-style ingestion using bookmarks.

Example operations:

- Insert (I)
- Update (U)
- Delete (D)

---

4. SCD Type 2 Ready

Framework supports historical version tracking using:

- effective_from
- effective_to
- current_row

---

5. Bookmark Processing

Tracks previously processed records for incremental execution.

---

6. Hudi Integration

Supports Apache Hudi for:

- upserts
- versioned storage
- scalable lakehouse ingestion

---

7. Modular Design

Clean separation of concerns:

core        -> framework utilities
io          -> data reading/writing
pipelines   -> orchestration
services    -> business logic
transforms  -> reusable transformations
utils       -> helper utilities

---

PROJECT STRUCTURE

project/
│
├── config/
│   ├── app.yaml
│   └── mappings/
│       ├── data/
│       │   └── customer.json
│       └── bookmark/
│           └── customer.json
│
├── data/
│   ├── raw/
│   │   └── customer.csv
│   ├── processed/
│   └── tmp/
│
├── jobs/
│   ├── raw2dis.py
│   ├── dis2con.py
│   ├── postgres_load.py
│   └── index_load.py
│
├── logs/
│
├── src/
│   ├── core/
│   │   ├── config_loader.py
│   │   ├── logger.py
│   │   └── session.py
│   │
│   ├── io/
│   │   ├── reader.py
│   │   └── writer.py
│   │
│   ├── pipelines/
│   │   └── generic_pipeline.py
│   │
│   ├── services/
│   │   ├── bookmark_service.py
│   │   ├── mapping_service.py
│   │   └── scd_service.py
│   │
│   ├── transforms/
│   │   └── transformations.py
│   │
│   └── utils/
│
├── requirements.txt
├── .gitignore
└── README.md

---

INSTALLATION

1. Create Virtual Environment

python -m venv venv

---

2. Activate Environment

Windows:
venv\Scripts\activate

Linux / Mac:
source venv/bin/activate

---

3. Install Dependencies

pip install -r requirements.txt

---

DEPENDENCIES

pyspark==4.0.0
pyyaml==6.0.2
pandas==2.2.3
pyarrow==20.0.0

---

CONFIGURATION

1. app.yaml

Global framework settings.

File:
config/app.yaml

Example:

spark:
  app_name: raw2distilled-engine

paths:
  raw_dir: data/raw
  processed_dir: data/processed

formats:
  input: csv
  output: hudi

logging:
  level: INFO

---

2. Data Mapping Config

Controls:

- column mappings
- transformations
- datatype casting
- Hudi write options

File:
config/mappings/data/customer.json

Example:

{
  "table_name": "scd_customer",

  "write_mode": "upsert",

  "primary_key": ["customer_id"],

  "columns": [
    {
      "seq": 1,
      "source_field": "operation",
      "target_field": "op",
      "data_type": "string",
      "transformations": ["trim", "upper"]
    },
    {
      "seq": 2,
      "source_field": "customer_identifier",
      "target_field": "customer_id",
      "data_type": "int"
    }
  ],

  "hudi": {
    "table_type": "COPY_ON_WRITE",
    "options": {
      "hoodie.datasource.write.recordkey.field": "customer_id,effective_from",
      "hoodie.datasource.write.precombine.field": "effective_from"
    }
  }
}

---

3. Bookmark Config

Tracks incremental processing progress.

File:
config/mappings/bookmark/customer.json

Example:

{
  "table_name": "customer",

  "bookmark_column": "updated_timestamp",

  "last_processed_value": "1900-01-01 00:00:00",

  "last_run_status": "INITIALIZED",

  "last_run_started_at": null,

  "last_run_finished_at": null
}

---

RAW DATA EXAMPLE

File:
data/raw/customer.csv

Example:

operation,customer_identifier,customer_name,customer_email,updated_timestamp
I,1, John , JOHN@mail.com ,2026-04-25 10:00:00
U,1, John Updated , john_new@mail.com ,2026-04-25 11:00:00
D,1,, ,2026-04-25 12:00:00

---

RUNNING THE PIPELINE

Run Raw to Distilled Job:

python jobs/raw2dis.py --table customer

---

CURRENT PROCESSING FLOW

Raw File
   ↓
Read Data
   ↓
Apply Bookmark Filter
   ↓
Apply Mapping
   ↓
Apply Transformations
   ↓
Apply SCD2 Columns
   ↓
Write Hudi Table
   ↓
Update Bookmark

---

LOGGING

Logs are automatically written to:

logs/application.log

Example:

2026-04-25 18:11:23 | INFO  | GenericPipeline | Pipeline completed successfully

---

CURRENT CAPABILITIES

- CSV ingestion
- Config-driven mappings
- Generic pipeline execution
- Incremental CDC processing
- Bookmark tracking
- Transformation registry
- Hudi writes
- SCD2 metadata columns
- Structured logging

---

PLANNED ENHANCEMENTS

- Full SCD Type 2 merge logic
- Validation framework
- JDBC/Postgres loaders
- Elasticsearch/OpenSearch indexing
- Data quality checks
- Partition handling
- Multi-job orchestration
- Unit testing
- Airflow integration
- Streaming ingestion

---

DESIGN PHILOSOPHY

The framework is designed around:

Configuration controls behavior.
Code executes reusable logic.

Instead of building:

One pipeline per table

the framework focuses on:

One generic engine for all tables

---

AUTHOR

Rakesh
