# Databricks notebook source
# MAGIC %md
# MAGIC ## (1) Install Azure Event Hub Package
# MAGIC
# MAGIC Installs the `azure-eventhub` Python package required to interact with Azure Event Hub services.

# COMMAND ----------

# MAGIC %pip install azure-eventhub

# COMMAND ----------

# MAGIC %restart_python
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## (2) Import Required Libraries

# COMMAND ----------

import time
import json
import numpy as np
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData
import os
from dotenv import load_dotenv

# COMMAND ----------

# MAGIC %md
# MAGIC ##(3) Load Batch Data for Streaming Simulation
# MAGIC
# MAGIC Loads historical data from ADLS Gen2 using PySpark and prepares it for real-time streaming simulation.

# COMMAND ----------

# orders__File
spark_df_orders = spark.read.csv("abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Orders/olist_orders_dataset.csv", 
header=True, inferSchema=True)
# order_items__File
spark_df_items = spark.read.csv("abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Order_Items/olist_order_items_dataset.csv", header=True, inferSchema=True)
# Order_Payments__File
spark_df_payments = spark.read.csv("abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Order_Payments/olist_order_payments_dataset.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(4) Convert to DataFrames and Handle Null Values
# MAGIC
# MAGIC Converts Spark DataFrames into Pandas DataFrames and replaces missing values with `None` to ensure JSON-compatible formatting for Event Hub streaming.

# COMMAND ----------

df_orders = spark_df_orders.toPandas().replace({np.nan: None})
df_items = spark_df_items.toPandas().replace({np.nan: None})
df_payments = spark_df_payments.toPandas().replace({np.nan: None})

# COMMAND ----------

# Standardized logging output
print(f"INFO: Data successfully loaded into memory. Total orders to process: {len(df_orders)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##(5) Optimize Data Lookup Structure
# MAGIC
# MAGIC Converts DataFrames into Python dictionaries and groups related records by `order_id` for faster event generation and streaming simulation.

# COMMAND ----------

from collections import defaultdict

print("INFO: Building optimized dictionaries...")

# 1. Convert entire DataFrames to pure Python lists in one fast operation
items_list = df_items.to_dict(orient='records')
payments_list = df_payments.to_dict(orient='records')

# 2. Group them using native Python dictionaries (Extremely fast)
# defaultdict automatically creates a new list if the order_id hasn't been seen yet
items_dict_temp = defaultdict(list)
for item in items_list:
    items_dict_temp[item['order_id']].append(item)

payments_dict_temp = defaultdict(list)
for payment in payments_list:
    payments_dict_temp[payment['order_id']].append(payment)

# 3. Convert back to standard dictionaries to match your existing code perfectly
items_dict = dict(items_dict_temp)
payments_dict = dict(payments_dict_temp)

print("INFO: Dictionaries built successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ##(6) Initialize Event Hub Connection
# MAGIC
# MAGIC Creates the connection configuration required to initialize Event Hub producer clients for streaming data transmission.

# COMMAND ----------

load_dotenv()

CONNECTION_STR = os.getenv('CONNECTION_STR')
# COMMAND ----------

producer_orders = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name="orders")
producer_items = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name="order_items")
producer_payments = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name="payment")

# COMMAND ----------

# MAGIC %md
# MAGIC ##(7) Broadcast Events to Event Hub
# MAGIC
# MAGIC Sends order events with their related items and payments to separate Event Hub topics, simulating real-time e-commerce streaming traffic.
# MAGIC
# MAGIC - Publishes events in batches.
# MAGIC - Adds delays to mimic live data arrival.
# MAGIC - Handles graceful shutdown and closes Event Hub connections.

# COMMAND ----------

try:
    for index, order_row in df_orders.iterrows():
        order_id = order_row['order_id']
        
        # 3A. Broadcast Primary Order Event
        # Note: 'default=str' prevents JSON serialization crashes on PySpark/Pandas Timestamp objects
        order_data = json.dumps(order_row.to_dict(), default=str)
        batch_orders = producer_orders.create_batch()
        batch_orders.add(EventData(order_data))
        producer_orders.send_batch(batch_orders)
        
        # 3B. Broadcast Associated Order Items
        if order_id in items_dict:
            batch_items = producer_items.create_batch()
            for item in items_dict[order_id]:
                batch_items.add(EventData(json.dumps(item, default=str)))
            producer_items.send_batch(batch_items)
            
        # 3C. Broadcast Associated Order Payments
        if order_id in payments_dict:
            batch_payments = producer_payments.create_batch()
            for payment in payments_dict[order_id]:
                batch_payments.add(EventData(json.dumps(payment, default=str)))
            producer_payments.send_batch(batch_payments)
            
        # Log standard output for monitoring metrics
        print(f"INFO: [Transaction {index}] Successfully broadcasted Order ID: {order_id} + child records.")
        
        # Throttle output to simulate realistic network traffic and micro-batching behavior
        time.sleep(2) 

except KeyboardInterrupt:
    print("\nWARNING: Streaming generator gracefully terminated by manual user intervention.")
finally:
    # Guarantee network connection closure to prevent memory and socket leaks
    print("INFO: Closing Event Hub client connections...")
    producer_orders.close()
    producer_items.close()
    producer_payments.close()
    print("INFO: Connections successfully closed. Stream ended.")