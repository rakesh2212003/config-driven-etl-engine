# 🚀 Config-Driven PySpark ETL Engine

A modular, scalable **config-driven ETL framework** built with PySpark.
Designed to separate transformation logic from code using external configuration.

---

## 📌 Features

* ✅ Config-driven transformations (JSON-based)
* ✅ Modular architecture (reader, transformer, writer)
* ✅ Reusable pipeline framework
* ✅ Structured logging system
* ✅ Easy to extend for multiple datasets
* ✅ Clean project structure (production-ready)

---

## 🧱 Project Structure

```
.
├── main.py
├── config/
│   ├── app_config.yaml
│   ├── mappings/
│   │   └── customer.json
│   └── schemas/
│       └── customer_schema.json
│
├── data/
│   └── raw/
│       └── customer.csv
│
├── src/
│   ├── core/
│   │   ├── config.py
│   │   ├── logger.py
│   │   └── session.py
│   │
│   ├── io/
│   │   ├── reader.py
│   │   └── writer.py
│   │
│   ├── transforms/
│   │   ├── registry.py
│   │   └── common.py
│   │
│   ├── services/
│   │   └── mapping_service.py
│   │
│   └── pipelines/
│       ├── base_pipeline.py
│       └── customer_pipeline.py
│
└── logs/
```

---

## ⚙️ Setup

### 1️⃣ Create Virtual Environment

```
python -m venv .venv
source .venv/bin/activate   # Linux/Mac
.venv\Scripts\activate      # Windows
```

---

### 2️⃣ Install Dependencies

```
pip install pyspark pyyaml
```

---

### 3️⃣ Ensure Java is Installed

PySpark requires Java:

```
java -version
```

---

## ▶️ Run the Pipeline

```
python main.py --table customer
```

---

## 📥 Sample Input

**data/raw/customer.csv**

```
customer_id,name
1, john
2, alice
3,   bob
4,   charlie
```

---

## ⚙️ Mapping Config

**config/mappings/customer.json**

```json
{
  "columns": [
    {
      "source_field": "customer_id",
      "target_field": "customer_id",
      "transformations": ["trim"]
    },
    {
      "source_field": "name",
      "target_field": "name_clean",
      "transformations": ["trim", "upper"]
    }
  ]
}
```

---

## 📤 Output

Processed data is written to:

```
data/processed/customer/
```

Format: **Parquet**

---

## 🧠 How It Works

```
main.py
 → Pipeline Runner
    → Extract (Reader)
    → Transform (Mapping + Registry)
    → Load (Writer)
```

---

## 🔄 Transformation Engine

Transformations are defined in config:

```
["trim", "upper"]
```

Executed dynamically via:

* Transform Registry
* Column-level mapping

---

## 📝 Logging

* Structured logs with aligned formatting
* Console + file logging
* External logs (PySpark / py4j) suppressed

Example:

```
INFO  | MAIN               | Starting job
INFO  | READER             | Reading data
INFO  | TRANSFORM          | Mapping applied
INFO  | WRITER             | Write successful
```

---

## 🚀 Future Enhancements

* 🔹 Config-driven paths (remove hardcoding)
* 🔹 Multiple pipeline support
* 🔹 Data validation layer
* 🔹 Incremental / partition-based loading
* 🔹 Airflow integration
* 🔹 Schema enforcement

---

## 🤝 Contributing

Feel free to fork, extend, and improve the framework.

---

## 📄 License

MIT License
