# Medallion Architecture – Olist Data Platform

This folder contains the Databricks notebooks that implement the **Medallion Architecture** (Bronze → Silver → Gold) for the Olist e-commerce dataset, built on **Azure Data Lake Storage Gen2 (ADLS Gen2)** and **Delta Lake**.

## Overview

| Layer | Notebook | Purpose |
|---|---|---|
| 🥉 **Bronze** | `01_bronze_Layer.py` | Ingests all 9 raw CSV files from `Raw_Data/` using Auto Loader (`cloudFiles`) and writes them as Delta tables with no transformation — an exact copy of the source. |
| 🥈 **Silver** | `02_silver_Layer.py`  | Cleans, validates, standardizes and quality-checks each Bronze table. Invalid records are routed to `Quarantine/` Delta tables instead of being silently dropped. |
| 🥇 **Gold** | `03_gold_Layer.py` | Builds the final **Galaxy Schema (Fact Constellation)** — shared dimension tables + multiple fact tables — ready for BI/reporting (Power BI, dashboards, KPIs). |

---

## 🥉 Bronze Layer — Raw Ingestion

**What it does:**
- Reads each of the 9 raw source files from `Raw_Data/<Entity>/` using Spark **Structured Streaming + Auto Loader** (`cloudFiles`), with `trigger(availableNow=True)` for a batch-like run.
- Writes each stream to its own folder under `Bronze_Layer/<Entity>/` in **Delta format**, `outputMode("append")`.
- Registers each Delta path as a table in `Olist__Batch__Data.bronze.<entity>`.
- Uses ADLS Gen2 checkpoint & schema-inference locations under `checkpoints/streams/` and `checkpoints/schemas/` so re-runs are incremental and schema drift is tracked automatically.

**Files processed (1 folder = 1 entity = 1 schema):**
1. Customers
2. Geolocation
3. Order Items
4. Order Payments
5. Order Reviews *(read with `multiLine`, custom quote/escape to handle embedded review text)*
6. Orders
7. Products
8. Sellers
9. Category Name Translation

**Why no transformation happens here:** Bronze is meant to be a raw, replayable copy of the source. If something goes wrong downstream, we can always rebuild Silver/Gold from Bronze without re-touching the raw files.

---

## 🥈 Silver Layer — Cleaning, Validation & Quality

Each of the 9 tables goes through the same repeatable 8-step process:

1. **Read Table** from the Bronze layer.
2. **Schema Validation** — drop the Auto Loader `_rescued_data` column (present when all columns matched the inferred schema).
3. **Data Type Casting** — enforce correct types (int, double, timestamp, etc.).
4. **Remove Duplicates** — `dropDuplicates()` (kept in the pipeline even when 0 duplicates found today, since new weekly loads could introduce them).
5. **Handle Missing Values** — drop or impute nulls depending on business impact (e.g. `products` uses category-average imputation for weight instead of dropping rows, to preserve referential integrity for downstream sales joins).
6. **Data Standardization** — trim whitespace, normalize casing (e.g. states uppercased, categories title-cased), remove underscores/accents from text fields.
7. **Data Quality Checks & Business Logic** — row-level rule checks per table (e.g. no negative prices, valid ZIP codes, valid Brazilian state codes, review scores between 1–5, delivery date not before purchase date, "ghost delivery" detection, etc.).
   - **Bad records are never silently deleted** — they're written with an `error_reason` column to a **Quarantine Delta table** (`Olist__Batch__Data.quarantine.<entity>`) for audit and investigation.
8. **Write Silver Table** — clean, validated data is written to `Silver_Layer/<Entity>/` as Delta and registered as `Olist__Batch__Data.silver.<entity>`.

**Key data-quality decisions documented in the notebook:**
- `order_payments`: rows with `payment_type = 'NOT_DEFINED'` are dropped; credit card payments with 0 installments are flagged as illogical and quarantined.
- `order_reviews`: fixed a CSV mis-parsing bug (embedded newlines in review text shifted columns) by enabling `multiLine`/`quote`/`escape` options at the Bronze read step.
- `orders`: adds "ghost delivery" and "unapproved delivery" business-logic checks (status = DELIVERED but missing delivery/approval date).
- `sellers`: validates against the 27 official Brazilian state codes and normalizes/de-accents city names for BI-friendly grouping.
- `products`: imputes missing physical dimensions using the category average rather than dropping the product row.

---

## 🥇 Gold Layer — Galaxy Schema (Fact Constellation)

**Why a Galaxy Schema instead of a single Star Schema:**
The warehouse is organized around **business processes**, each with its own Fact table, all sharing common Dimension tables. This keeps each business process independently maintainable and lets us add new processes (returns, shipping, inventory) later without redesigning existing tables.

### Dimensions (shared across all facts)
- `dim_customers`
- `dim_products` *(joined with the English category translation; see the "Lesson Learned" note in the notebook about a key-casing mismatch between Silver `products` and `category_translation`)*
- `dim_sellers`
- `dim_orders`
- `dim_date` *(derived from `order_purchase_timestamp`: year, quarter, month, week, day, day name, is_weekend flag)*

### Facts (one per business process)
- `fact_orders` — order line items joined to orders (price, freight value, date key) → revenue/sales KPIs.
- `fact_payments` — payment transactions joined to orders (payment type, installments, value) → payment/method KPIs.
- `fact_reviews` — customer reviews joined to date dimension → satisfaction/review KPIs.

Each fact table has a set of ready-made SQL queries in the notebook for common BI visuals (KPI cards, line/bar/donut charts) — e.g. total revenue, revenue by month/state/product/seller, average review score, weekend vs weekday sales, top/bottom rated orders, etc. These map directly to Power BI visual types noted in the notebook comments.

---

## ADLS Gen2 Folder Structure Reference

```
olist-hybrid-data-platform/
└── Azure Storage Account/
    ├── Raw_Data/              # 9 folders, one per source file — see Raw_Data README
    ├── Bronze_Layer/          # 1:1 raw copy of Raw_Data, in Delta format
    ├── Silver_Layer/          # Cleaned, validated, deduplicated Delta tables
    ├── Quarantine/            # Rejected/invalid records with error_reason
    ├── checkpoints/
    │   ├── streams/           # Auto Loader streaming checkpoints
    │   └── schemas/           # Auto Loader inferred schema locations
    └── Gold_Layer/            # Star/Galaxy schema: dim_* and fact_* tables
```

## Databricks Catalog/Schema Naming

| Layer | Catalog.Schema |
|---|---|
| Bronze | `Olist__Batch__Data.bronze.(File Name)` |
| Silver | `olist_batch_data.silver.(File Name)` |
| Quarantine | `Olist__Batch__Data.quarantine.(File Name)` |
| Gold | `olist_batch_data.gold.(File Name)` |

## How to Run

1. Ensure new/updated raw files are uploaded into the matching subfolder under `Raw_Data/` (folder name must match the entity, e.g. new orders file → `Raw_Data/Orders/`).
2. Run the **Bronze** notebook first (ingests all 9 files via Auto Loader, `trigger(availableNow=True)`).
3. Run the **Silver** notebook (cleans/validates all 9 Bronze tables, writes Silver + Quarantine tables).
4. Run the **Gold** notebook (rebuilds dimensions and fact tables from Silver).
5. Pipeline is currently scheduled/intended to run weekly (referenced in-notebook as "Friday" runs).
