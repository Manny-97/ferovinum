# 📦 ETL Pipeline Documentation

This document explains how to run the ETL pipeline using Docker Compose and outlines the extraction and transformation logic implemented in the pipeline.

---

## 🚀 How to Run the ETL Pipeline

### 🛠 Requirements
- Docker
- Docker Compose

### 📁 Project Structure
```bash
project-root/
├── data/
│   ├── logs/                # Log files: log_*.txt
│   ├── skus/                # SKU metadata: skus.json
│   └── market_prices/       # Parquet files: market_prices_*.parquet
├── outputs/                 # Output folder for generated CSVs and logs
├── main.py                  # Main ETL script
├── requirements.txt         # Python dependencies
├── Dockerfile
└── docker-compose.yml
```

### 🐳 Docker Setup


### ▶️ Run the Pipeline
```bash
git clone https://github.com/Manny-97/ferovinum
docker-compose up
```

---

## 🔍 ETL Logic Overview

### 1. 📥 Extraction Logic

#### a. Logs (`parse_logs`)
- Loads and merges all `log_*.txt` files in the `data/logs/` directory.
- Each log line is parsed using regex to extract:
  - Timestamp
  - Message type (`ORDER`, `TRANSACTION`, `RESULT`, `RESPONSE`)
  - Trace ID
  - Message details
- Incomplete entries are safely skipped and logged.

#### b. SKUs (`read_skus`)
- Reads nested JSON from `skus.json`.
- Uses `pd.json_normalize()` to flatten nested fields.
- Columns like grape variety, blend, ABV, etc. are renamed for clarity.

#### c. Market Prices (`read_market_prices`)
- Reads multiple `market_prices_*.parquet` files.
- Cleans the `quote_id` to extract SKU.
- Converts `timestamp` to datetime for accurate merging.

### 2. ♻️ Transformation Logic

This is where the real work happens. We're turning raw log entries and metadata into a unified dataset.

#### a. Match Orders to Transactions
- From `ORDER` log entries, we extract the SKU, quantity, and whether it's a `buy` or `sell`.
- If it's a `buy`, the quantity is negated. This way, `+` means `sell` and `-` means `buy`.
- We then look for a corresponding `TRANSACTION` log with the same `trace_id` and join them.

#### b. Add SKU Metadata
- The merged transaction dataset is then joined with the SKU metadata using the `sku` column.
- Now we know the type of product involved in each transaction: its alcohol content, bottle size, varietal, brand, etc.

#### c. Add Market Price
- We want to know the price of a product **at the time it was transacted**.
- For each transaction, we perform a **merge-as-of**: this finds the latest available price *before* the transaction timestamp.
- This gives a more realistic value of the transaction, reflecting market conditions.

#### d. Compute Transaction Value and Temporal Features
- We compute the dollar value of each transaction: `quantity * market_price`.
- Then, we extract time-based features: year, quarter, and week for temporal analysis.
- Unnecessary fields like raw `details` and `transaction_amount` are dropped.

At the end of this transformation, we have a fully joined, timestamped, and value-enriched transaction dataset.

### 3. 💾 Load
- Saves enriched dataset to `outputs/final_clean_dataset.csv`
- Generates and saves:
  - `top_region_per_sku.csv`: Most active region per SKU per quarter
  - `two_most_profitable_brands.csv`: Top 2 brands by sales value in early 2024
  - `market_data.csv`: Cleaned market data

---

## 🧪 Test Case Generation

The pipeline also includes a utility to generate inventory snapshots:
- Filters transactions before `2025-02-01`
- Aggregates inventory balances per SKU
- Filters to a set of selected SKUs

CSV is printed and available for inspection after the run.

---

## 📊 Summary of Outputs

| File                               | Description                                |
|------------------------------------|--------------------------------------------|
| `final_clean_dataset.csv`          | Enriched transactional dataset             |
| `top_region_per_sku.csv`           | Top performing region per SKU              |
| `two_most_profitable_brands.csv`   | Top 2 most profitable brands in early 2024 |
| `market_data.csv`                  | Cleaned and merged market price data       |
| `log_*.log`                        | Timestamped logs of pipeline execution     |

---

Nice working on this! 🎯

