# Databricks ETL Project

A PySpark-based ETL pipeline built for Databricks that ingests raw retail sales data (orders, customers, products), cleans and validates it through a **Bronze → Silver → Gold** medallion architecture, and produces a profit-analysis Gold table using Delta Lake.

## Overview

This project takes raw retail transaction data and progressively refines it into a business-ready aggregate table showing **profit by year, product category, product sub-category, and customer**. It follows the medallion (multi-hop) architecture pattern common in Databricks Lakehouse projects:

```
Bronze (raw)  →  Silver (cleaned, validated, quarantined)  →  Gold (aggregated, business-ready)
```

## Architecture

| Layer | Purpose | Format |
|---|---|---|
| **Bronze** | Raw ingestion of orders, customers, and products data, landed as-is for traceability | Delta |
| **Silver** | Cleaning, validation, standardization, and quarantine of bad records (invalid segments, malformed phone numbers, out-of-range discounts, etc.) | Delta |
| **Gold** | Business aggregates — profit summarized by year, category, sub-category, and customer | Delta |

### Gold Table Schema (`gold_sales`)

| Column | Type | Description |
|---|---|---|
| `year` | `IntegerType` | Calendar year of the order |
| `category` | `StringType` | Product category |
| `sub_category` | `StringType` | Product sub-category |
| `customer_id` | `StringType` | Customer identifier |
| `total_profit` | `DoubleType` | Sum of profit for the group |
| `order_line_count` | `LongType` | Count of order lines in the group |

The table is **partitioned by `year`**, with `OPTIMIZE ... ZORDER BY (category, sub_category, customer_id)` recommended for query performance on the other grouping dimensions.

## Project Structure

```
databricks-etl-project/
├── config/                # Environment/table configuration (catalog, schema, table names, paths)
├── docs/                  # Project documentation
├── notebooks/              # Databricks notebooks (exploration, orchestration, job entry points)
├── src/
│   ├── transformation/
│   │   └── transformers.py   # Pure transformation logic (transform_orders, transform_customers,
│   │                          # transform_products, transform_agg) — no I/O, fully unit-testable
│   └── utils/
│       └── schemas.py         # Explicit StructType schema definitions for all layers
├── tests/
│   └── unit/
│       ├── conftest.py        # Shared pytest fixtures (SparkSession, sample DataFrames)
│       ├── silver_test.py     # Unit tests for Silver-layer cleaning/validation logic
│       └── gold_test.py       # Unit tests for Gold-layer aggregation logic
├── .gitignore
├── requirements.txt
├── setup.py
└── README.md
```

## Data Flow

1. **Extract** — Raw orders, customers, and products data is ingested into Bronze Delta tables.
2. **Transform (Silver)**
   - `transform_orders` — parses dates, validates quantity/discount ranges, rejects invalid rows into a quarantine DataFrame instead of silently dropping them.
   - `transform_customers` — standardizes names, pads postal codes, splits phone extensions, strips country codes, quarantines invalid segments.
   - `transform_products` — normalizes product names (e.g., non-breaking spaces), validates pricing.
   - Each function returns a `(clean_df, rejected_df)` tuple so bad data is auditable rather than silently discarded.
3. **Transform (Gold)**
   - `transform_agg` — aggregates the enriched Silver data to `(year, category, sub_category, customer_id)` grain, computing `total_profit` and `order_line_count`.
   - `build_profit_summary` — orchestrates reading the enriched Silver table, calling `transform_agg`, and writing the result to the Gold Delta table with schema enforcement.
4. **Load** — Final aggregate is written to `gold_sales`, partitioned by `year`, ready for downstream SQL reporting.

## Reporting Queries

The Gold table supports the following required aggregate views via simple `GROUP BY` roll-ups (no separate tables needed, since profit is additive):

```sql
-- Profit by Year
SELECT year, SUM(total_profit) AS total_profit
FROM gold_sales
GROUP BY year;

-- Profit by Year + Product Category
SELECT year, category, SUM(total_profit) AS total_profit
FROM gold_sales
GROUP BY year, category;

-- Profit by Customer
SELECT customer_id, SUM(total_profit) AS total_profit
FROM gold_sales
GROUP BY customer_id;

-- Profit by Customer + Year
SELECT customer_id, year, SUM(total_profit) AS total_profit
FROM gold_sales
GROUP BY customer_id, year;
```

## Setup

### Prerequisites
- Python 3.9+
- Databricks workspace (or local PySpark + Delta Lake for testing)
- `pip`

### Installation

```bash
git clone https://github.com/akan310897/databricks-etl-project.git
cd databricks-etl-project
pip install -r requirements.txt
pip install -e .
```

### Configuration

Update `config/` with your target catalog, schema, and table names, and the raw data path. The pipeline reads these via `setup.load_config()`, `setup.get_all_tables()`, and `setup.get_raw_path()`.

## Running the Pipeline

On Databricks, run the notebooks under `notebooks/` in order (Bronze → Silver → Gold), or trigger the Gold build directly:

```python
from src.transformation.aggregations import build_profit_summary

build_profit_summary()
```

This reads the enriched Silver orders table, aggregates it, and writes/overwrites the `gold_sales` Delta table.

## Testing

Unit tests cover the pure transformation logic in `src/transformation/transformers.py` — no Delta/table I/O, so they run fast and locally.

```bash
pytest tests/unit/
```

**Coverage includes:**
- **Silver layer**: null/blank name handling, invalid segment quarantine, postal code normalization, phone number cleaning (extensions, country codes, error values), order date parsing, quantity/discount validation, product name normalization.
- **Gold layer**: correct output schema, profit summed correctly within a group, groups correctly separated by category/sub-category/year, negative and null profit handled correctly, zero-profit rows still counted in `order_line_count`.

> **Note:** Test files use the `*_test.py` naming convention. If running with default `pytest` discovery settings, ensure `python_files = *_test.py test_*.py` is set in `pytest.ini`/`pyproject.toml`, since pytest's default pattern (`test_*.py`) will not discover these files as-is.

## Data Quality Approach

Rather than silently dropping invalid rows, the Silver layer **quarantines** them into a separate `rejected` DataFrame with a `quarantine_reason` column (e.g. `invalid_segment`), preserving auditability of what was excluded and why — a rejected row is never simply lost.

## Tech Stack

- **PySpark** — distributed data processing
- **Delta Lake** — ACID-compliant table storage, schema enforcement, time travel
- **Databricks** — orchestration and notebook execution environment
- **pytest** — unit testing framework

## License

_Add license information here._
