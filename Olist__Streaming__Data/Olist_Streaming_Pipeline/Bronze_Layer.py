# Databricks notebook source
# MAGIC %md
# MAGIC ###(1) Import Required Packages and Functions

# COMMAND ----------

from pyspark.sql.functions import col, from_json, trim, lower
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BinaryType,
    MapType, IntegerType, DoubleType)
import os
from dotenv import load_dotenv

# COMMAND ----------

# MAGIC %md
# MAGIC ###(2) Create Data Schemas in the `olist_stream_data` Catalog
# MAGIC - **Bronze:** Stores raw ingested data from the source systems with minimal transformations.
# MAGIC - **Silver:** Stores cleaned, validated, and transformed data ready for further processing and analysis. 

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG olist_stream_data;
# MAGIC
# MAGIC -- 3. Create the Medallion schemas inside the new catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Raw streaming JSON payloads from Event Hubs';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Cleaned, deduplicated, and upserted streaming data';

# COMMAND ----------

# MAGIC %md
# MAGIC ###(3) Configure Storage Connection and Unity Catalog Paths

# COMMAND ----------

#(1) Storage Account: Defines the Azure Storage Account that hosts the data lake containers.
storage_account_name = "olistprojectdatalake"

#(2) Storage Account: Defines the Azure Storage Account that hosts the data lake containers.
streaming_container = "oliststreaming"

#(3) Batch Container: Dedicated container for storing batch-processed datasets and historical data.
batch_container = "olist"

#(4) Unity Catalog Stream Path: Defines the root location of the streaming container using the `abfss` protocol for secure access and data management
uc_stream_path = f"abfss://{streaming_container}@{storage_account_name}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ##(4) Ingest Raw Streaming Data into Bronze Layer
# MAGIC
# MAGIC This section reads real-time data from **Azure Event Hub** using Spark Structured Streaming and stores the raw JSON messages in the **Bronze Layer**.
# MAGIC
# MAGIC - Reads events directly from Event Hub topics.
# MAGIC - Converts incoming binary messages into JSON strings.
# MAGIC - Writes raw data into Bronze Delta tables.
# MAGIC - Uses checkpoints to ensure fault tolerance and streaming reliability.
# MAGIC
# MAGIC **Flow:**  
# MAGIC `Azure Event Hub → Spark Structured Streaming → Bronze Delta Layer`

# COMMAND ----------

load_dotenv()

eh_connection_string = os.getenv('EH_CONNECTION_STRING')

eh_sasl_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{eh_connection_string}";'

eh_namespace = os.getenv('EH_NAMESPACE')

# COMMAND ----------

def ingest_to_bronze_direct(eventhub_topic_name, bronze_path, bronze_checkpoint):
    
    # Read DIRECTLY from the live Event Hub socket
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", eh_namespace)
        .option("subscribe", eventhub_topic_name)  # e.g., "orders", "order_items", "payment"
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", eh_sasl_config)
        .option("startingOffsets", "latest") # Start reading new events from the moment the stream starts
        .option("failOnDataLoss", "false")
        .load()
    )

    # In EventHubs, the actual JSON message is stored in the "value" column as binary
    # We cast it to a string and rename it to "Body" to match your downstream Silver logic
    formatted_stream = raw_stream.selectExpr("CAST(value AS STRING) as Body")

# Write the raw JSON string to the Bronze Delta table
    query = (
        formatted_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", bronze_checkpoint) # CRITICAL: Tells Spark where to save state
        .trigger(processingTime="30 seconds")
        .start(bronze_path)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## (5) Start Bronze Streaming Jobs
# MAGIC
# MAGIC This section starts the streaming ingestion jobs for the Bronze Layer.
# MAGIC
# MAGIC - Connects each Event Hub topic to its corresponding Bronze Delta table.
# MAGIC - Creates independent streaming pipelines for:
# MAGIC   - `order_items`
# MAGIC   - `orders`
# MAGIC   - `payment`
# MAGIC - Stores checkpoints separately to maintain streaming state and ensure recovery.
# MAGIC
# MAGIC **Flow:**  
# MAGIC `Event Hub Topics → Bronze Delta Tables`

# COMMAND ----------

# Start Order Items Stream
bronze_items = ingest_to_bronze_direct(
    eventhub_topic_name="order_items", 
    bronze_path=f"{uc_stream_path}/Bronze_Layer/order_items", 
    bronze_checkpoint=f"{uc_stream_path}/Checkpoints/Bronze/order_items"
)

# Start Orders Stream
bronze_orders = ingest_to_bronze_direct(
    eventhub_topic_name="orders", 
    bronze_path=f"{uc_stream_path}/Bronze_Layer/orders", 
    bronze_checkpoint=f"{uc_stream_path}/Checkpoints/Bronze/orders"
)

# Start Payments Stream
bronze_payments = ingest_to_bronze_direct(
    eventhub_topic_name="payment", 
    bronze_path=f"{uc_stream_path}/Bronze_Layer/order_payments", 
    bronze_checkpoint=f"{uc_stream_path}/Checkpoints/Bronze/order_payments"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(6) Create Bronze Streaming Tables
# MAGIC
# MAGIC Registers Bronze Delta tables in the `olist_stream_data` catalog and links them to their ADLS Gen2 storage locations.
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `orders`
# MAGIC - `order_items`
# MAGIC - `order_payments`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the Orders Streaming Table
# MAGIC CREATE TABLE IF NOT EXISTS olist_stream_data.bronze.orders
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://oliststreaming@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/orders';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the Order Items Streaming Table
# MAGIC CREATE TABLE IF NOT EXISTS olist_stream_data.bronze.order_items
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://oliststreaming@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/order_items';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the Order Payments Streaming Table
# MAGIC CREATE TABLE IF NOT EXISTS olist_stream_data.bronze.order_payments
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://oliststreaming@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/order_payments';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`abfss://oliststreaming@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/orders`;

# COMMAND ----------

