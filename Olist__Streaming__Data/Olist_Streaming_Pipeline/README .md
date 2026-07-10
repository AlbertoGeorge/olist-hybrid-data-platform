# Olist Streaming Pipeline — Event Hub → Bronze → Silver (Real-Time)

This folder contains the **real-time / streaming** counterpart to the batch Medallion pipeline. Instead of loading static CSVs from `Raw_Data/`, this pipeline simulates and processes **live order events** using **Azure Event Hub** + **Spark Structured Streaming** on Databricks.

## Architecture

```
Historical CSVs (Raw_Data/)
        │
        ▼
   Producer.py  ──────────►  Azure Event Hub (3 topics)
 (simulates real-time            │  orders
  order traffic)                 │  order_items
                                  │  payment
                                  ▼
                          Bronze_Layer.py
                    (EventHub consumer → raw Delta)
                                  │
                                  ▼
                          Silver_Layer.py
                  (8-step cleaning micro-batch engine)
                                  │
                                  ▼
                    olist_stream_data.silver.*  (Delta tables)
```

Storage container used for this pipeline: **`oliststreaming`** (separate from the batch container `olist`), under the same storage account `olistprojectdatalake`.

---

## 1. `Producer.py` — Event Simulator

**What it does:**
- Reads the original historical Olist CSVs (`orders`, `order_items`, `order_payments`) from `Raw_Data/` using PySpark, then converts them to Pandas for row-by-row iteration.
- Groups `order_items` and `order_payments` by `order_id` into dictionaries for fast lookup, so each order's related items/payments can be sent together.
- Opens 3 persistent `EventHubProducerClient` connections — one per topic (`orders`, `order_items`, `payment`).
- Iterates through every order and broadcasts:
  1. The order record → `orders` Event Hub topic.
  2. Its related line items → `order_items` Event Hub topic.
  3. Its related payments → `payment` Event Hub topic.
- Adds a `time.sleep(2)` between orders to simulate realistic, non-bulk arrival of live traffic.
- Gracefully closes all producer connections on completion or manual interrupt (`Ctrl+C`).

**Purpose:** since Olist's real dataset is historical/batch, this script turns it into a synthetic **live event stream** so the rest of the pipeline can be built and tested exactly as it would run against real production traffic.

---

## 2. `Bronze_Layer.py` — Raw Ingestion from Event Hub

**What it does:**
- Creates the `olist_stream_data` catalog with two schemas: `bronze` and `silver`.
- Connects to Event Hub **using the Kafka protocol** (Event Hubs is Kafka-compatible), via `spark.readStream.format("kafka")` with SASL/SSL auth.
- Subscribes to each topic (`orders`, `order_items`, `payment`) as its own independent stream.
- Casts the raw Kafka `value` (binary) to a string column named `Body` — this keeps the raw JSON payload untouched, matching Bronze's "no transformation" principle.
- Writes each stream to Delta with `outputMode("append")`, `trigger(processingTime="30 seconds")`, and a dedicated checkpoint path per topic (`Checkpoints/Bronze/<topic>`).
- Registers each Bronze path as a table: `olist_stream_data.bronze.orders`, `.order_items`, `.order_payments`.

**Bronze paths (ADLS Gen2, `oliststreaming` container):**
| Topic | Bronze Delta Path |
|---|---|
| orders | `Bronze_Layer/orders` |
| order_items | `Bronze_Layer/order_items` |
| payment | `Bronze_Layer/order_payments` |

---

## 3. `Silver_Layer.py` — Real-Time Cleaning Engine

**What it does:**
Applies the **same 8-step cleaning philosophy** as the batch Silver notebook, but adapted to run per micro-batch via `foreachBatch`:

1. Skip empty micro-batches.
2. **Schema Validation** — parse the raw JSON `Body` column using a strict `StructType` schema per topic (`orders_schema`, `items_schema`, `payments_schema`).
3. **Data Type Casting** — e.g. `order_purchase_timestamp` parsed with `try_to_timestamp` (fails safely to null instead of crashing the batch).
4. **Remove Duplicates** — `dropDuplicates(["order_id"])` for orders, `["order_id","order_item_id"]` for items, full-row for payments.
5. **Handle Missing Values** — `dropna` on business-critical key columns (`order_id`, `customer_id`, `price`, `payment_value`).
6. **Data Standardization** — trims/lowercases `order_status` and `payment_type`; normalizes payment-type synonyms (e.g. `"cc"`, `"creditcard"` → `"credit_card"`).
7. **Data Quality Checks** — filters out negative prices/freight/payment values and negative installment counts.
8. **Write Silver Table** — appends the cleaned micro-batch to the Silver Delta path.

**Stream orchestration (`start_silver_stream`):**
- Reads each Bronze Delta path as a stream (`spark.readStream.format("delta")`).
- Applies the cleaning function via `foreachBatch`.
- Runs on a `30 second` micro-batch trigger with its own checkpoint under `Checkpoints/Silver/<table>`.
- Three independent streams run in parallel: `orders`, `order_items`, `order_payments`.

Finally, registers the cleaned Delta paths as Unity Catalog tables under `olist_stream_data.silver.*`.

---

## Key Differences vs. the Batch Pipeline

| | Batch Pipeline (`Raw_Data/`) | Streaming Pipeline (`oliststreaming`) |
|---|---|---|
| Source | Static CSV files in ADLS | Live events from Azure Event Hub (Kafka protocol) |
| Trigger | `availableNow=True` (run once per upload) | `processingTime="30 seconds"` (continuous) |
| Ingestion tool | Auto Loader (`cloudFiles`) | Kafka connector (`format("kafka")`) |
| Files/topics | 9 entities | 3 entities (orders, order_items, payments) |
| Quarantine table | Yes — invalid rows kept with `error_reason` | Not yet implemented — invalid rows are filtered out silently |
| Catalog | `Olist__Batch__Data` | `olist_stream_data` |

---

## Azure Event Hub Setup (reference)

From the Azure Portal, namespace **`olist-streaming`** has 3 active Event Hubs, each with 1 partition and 1-hour message retention:

| Event Hub | Status | Retention | Partitions |
|---|---|---|---|
| `orders` | Active | 1 hour | 1 |
| `order_items` | Active | 1 hour | 1 |
| `payment` | Active | 1 hour | 1 |

---

## How to Run

1. **Rotate and secure** the Event Hub connection string (see security note above) before running anything.
2. Run `Bronze_Layer.py` first — starts 3 parallel streams consuming from Event Hub into Bronze Delta tables.
3. Run `Silver_Layer.py` — starts 3 parallel streams reading Bronze, cleaning, and writing to Silver Delta tables.
4. Run `Producer.py` to start simulating live order traffic into Event Hub — Bronze and Silver streams will pick up new events automatically within ~30 seconds.
5. Query `olist_stream_data.silver.orders`, `.order_items`, `.order_payments` to see live, cleaned data arriving.
