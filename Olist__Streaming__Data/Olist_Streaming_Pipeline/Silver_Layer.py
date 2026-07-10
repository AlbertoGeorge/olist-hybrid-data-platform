# Databricks notebook source
# MAGIC %md
# MAGIC ###(1) Import Required Packages and Functions

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, from_json, trim, lower, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

uc_base_path = "abfss://oliststreaming@olistprojectdatalake.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ##(2) Register Silver Streaming Tables
# MAGIC
# MAGIC Creates Silver Layer Delta tables in the `olist_stream_data` catalog and links them to their ADLS Gen2 storage locations.
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `orders`
# MAGIC - `order_items`
# MAGIC - `order_payments`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register Silver Streaming Tables
# MAGIC CREATE TABLE IF NOT EXISTS olist_stream_data.silver.orders
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://oliststreaming@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/orders';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS olist_stream_data.silver.order_items
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://oliststreaming@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/order_items';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS olist_stream_data.silver.order_payments
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://oliststreaming@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/order_payments';

# COMMAND ----------

# MAGIC %md
# MAGIC ##(3) Define Streaming Data Schemas
# MAGIC
# MAGIC Defines the expected schema for incoming streaming data to ensure proper data types and structure validation.
# MAGIC
# MAGIC **Schemas Defined:**
# MAGIC - `orders`
# MAGIC - `order_items`
# MAGIC - `order_payments`

# COMMAND ----------

orders_schema = StructType([

    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True)
])

# COMMAND ----------

items_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True)
])

# COMMAND ----------

payments_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##(4) Cleaning Pipeline
# MAGIC
# MAGIC Implements an 8-step data cleaning process to transform raw Bronze streaming data into validated Silver Delta tables.
# MAGIC
# MAGIC - Validates schema and parses JSON events.
# MAGIC - Casts data types and removes duplicates.
# MAGIC - Handles missing values and standardizes data.
# MAGIC - Applies data quality checks.
# MAGIC - Writes cleaned data into Silver Layer tables.

# COMMAND ----------

# ---------------------------------------------------------
# The 8-Step Micro-Batch Cleaning Engine
# ---------------------------------------------------------
def process_silver_microbatch(micro_df, batch_id, table_type, schema, silver_path):
    # Skip empty batches using the modern, native DataFrame API
    if micro_df.isEmpty():
        return

    # STEP 2: Schema Validation
    # Extracts the raw JSON 'Body' from Event Hubs and enforces the expected schema
    df = micro_df.withColumn("data", from_json(col("Body"), schema)).select("data.*")

    # Apply specific rules based on the table being processed
    if table_type == "orders":
        # STEP 3: Data Type Casting
        df = df.withColumn("order_purchase_timestamp", F.expr("try_to_timestamp(order_purchase_timestamp, 'yyyy-MM-dd HH:mm:ss')"))
        
        # STEP 4: Remove Duplicates
        df = df.dropDuplicates(["order_id"])
        
        # STEP 5: Handle Missing Values
        df = df.dropna(subset=["order_id", "customer_id"])
        
        # STEP 6: Data Standardization
        df = df.withColumn("order_status", trim(lower(col("order_status"))))
        
        # STEP 7: Data Quality Checks
        df = df.filter(col("order_purchase_timestamp").isNotNull())

    elif table_type == "items":
        # STEP 3: Data Type Casting (Handled implicitly by the StructType schema)
        
        # STEP 4: Remove Duplicates
        df = df.dropDuplicates(["order_id", "order_item_id"])
        
        # STEP 5: Handle Missing Values
        df = df.dropna(subset=["order_id", "price"])
        
        # STEP 6: Data Standardization (N/A for these numeric item fields)
        
        # STEP 7: Data Quality Checks (No negative pricing)
        df = df.filter((col("price") >= 0) & (col("freight_value") >= 0))

    elif table_type == "payments":
        # STEP 3: Data Type Casting (Handled implicitly)
        
        # STEP 4: Remove Duplicates
        df = df.dropDuplicates()
        
        # STEP 5: Handle Missing Values
        df = df.dropna(subset=["order_id", "payment_value"])
        
        # STEP 6: Data Standardization
        df = df.withColumn("payment_type", trim(lower(col("payment_type"))))
        # Example of categorical normalization (if applicable to your data)
        df = df.withColumn("payment_type", 
                           when(col("payment_type").isin("credit card", "creditcard", "cc"), "credit_card")
                           .otherwise(col("payment_type")))
        
        # STEP 7: Data Quality Checks
        df = df.filter((col("payment_value") >= 0) & (col("payment_installments") >= 0))

    # STEP 8: Write Silver Table
    # Appends the fully cleaned micro-batch into the Silver Delta Table
    df.write.format("delta").mode("append").save(silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## (5) Silver Stream Orchestrator
# MAGIC
# MAGIC Starts the Silver Layer streaming jobs by reading Bronze Delta streams, applying the cleaning pipeline, and writing processed data into Silver tables.
# MAGIC
# MAGIC - Reads data from Bronze Layer.
# MAGIC - Processes micro-batches every 30 seconds.
# MAGIC - Uses checkpoints for streaming reliability.

# COMMAND ----------

import sys

def start_silver_stream(table_type, schema, bronze_path, silver_path, checkpoint_path):
    bronze_stream = spark.readStream.format("delta").load(bronze_path)
    
    query = (
        bronze_stream.writeStream
        .foreachBatch(lambda df, epoch_id: process_silver_microbatch(df, epoch_id, table_type, schema, silver_path))
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    # Force the buffer to output the text before Databricks intercepts the UI
    print(f"Silver stream started for: {table_type}")
    sys.stdout.flush() 
    
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ##(6) Start Silver Streaming Jobs
# MAGIC
# MAGIC Starts three parallel Silver Layer streams to process Bronze data and write cleaned records into Silver Delta tables.
# MAGIC
# MAGIC **Streams Started:**
# MAGIC - `orders`
# MAGIC - `order_items`
# MAGIC - `order_payments`
# MAGIC
# MAGIC Each stream uses its own checkpoint location for reliable processing.

# COMMAND ----------

silver_items_query = start_silver_stream(
    "items", 
    items_schema, 
    f"{uc_base_path}/Bronze_Layer/order_items", 
    f"{uc_base_path}/Silver_Layer/order_items", 
    f"{uc_base_path}/Checkpoints/Silver/order_items"
)

silver_orders_query = start_silver_stream(
    "orders", 
    orders_schema, 
    f"{uc_base_path}/Bronze_Layer/orders", 
    f"{uc_base_path}/Silver_Layer/orders", 
    f"{uc_base_path}/Checkpoints/Silver/orders"
)

silver_payments_query = start_silver_stream(
    "payments", 
    payments_schema, 
    f"{uc_base_path}/Bronze_Layer/order_payments", 
    f"{uc_base_path}/Silver_Layer/order_payments", 
    f"{uc_base_path}/Checkpoints/Silver/order_payments"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(7) Register Silver Tables in Unity Catalog
# MAGIC
# MAGIC Registers Silver Delta tables in the `olist_stream_data.silver` schema and connects them to their ADLS Gen2 storage locations.
# MAGIC
# MAGIC **Tables Registered:**
# MAGIC - `orders`
# MAGIC - `order_items`
# MAGIC - `order_payments`

# COMMAND ----------

print("INFO: Registering Silver tables to Unity Catalog...")

# Register Orders
spark.sql(f"""
CREATE TABLE IF NOT EXISTS olist_stream_data.silver.orders
USING DELTA
LOCATION '{uc_base_path}/Silver_Layer/orders'
""")

# Register Order Items
spark.sql(f"""
CREATE TABLE IF NOT EXISTS olist_stream_data.silver.order_items
USING DELTA
LOCATION '{uc_base_path}/Silver_Layer/order_items'
""")

# Register Order Payments
spark.sql(f"""
CREATE TABLE IF NOT EXISTS olist_stream_data.silver.order_payments
USING DELTA
LOCATION '{uc_base_path}/Silver_Layer/order_payments'
""")

print("INFO: Silver tables successfully registered in 'olist_stream_data.silver'!")