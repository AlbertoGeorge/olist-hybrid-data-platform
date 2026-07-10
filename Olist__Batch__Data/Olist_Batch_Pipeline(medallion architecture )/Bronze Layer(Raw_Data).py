# Databricks notebook source
#import modules we need
import pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC # load all files (9 Files And Convert To Delta Table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## (1)Customer_File

# COMMAND ----------

# Paths : (Customers__File)

cus_raw_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Customers/"

cus_bronze_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Customers/"

cus_schema_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/schemas/Customers/"

cus_checkpoint_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/streams/Customers/"


# COMMAND ----------

# Read from Raw_Data (ADLS Gen2)
df_customers = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", cus_schema_path)
        .load(cus_raw_path)
)

# COMMAND ----------

## Write to Bronze (Delta__Table)
cus_query = (
    df_customers.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", cus_checkpoint_path)
        .trigger(availableNow=True)
        .start(cus_bronze_path)
)

cus_query.awaitTermination()

# COMMAND ----------

df_cus = spark.read.format("delta").load(cus_bronze_path)

df_cus.show(5)

df_cus.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.bronze.customers
# MAGIC using delta
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Customers/';

# COMMAND ----------

spark.table("Olist__Batch__Data.bronze.customers").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## (2)Geolocation_File

# COMMAND ----------

# Paths : (Geolocation__File)

geo_raw_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Geolocation/"

geo_bronze_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Geolocation/"

geo_schema_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/schemas/Geolocation/"

geo_heckpoint_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/streams/Geolocation/"

# COMMAND ----------

# Read from Raw_Data (ADLS Gen2)
df_geolocation = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", geo_schema_path)
        .load(geo_raw_path)
)

# COMMAND ----------

## Write to Bronze (Delta__Table)
geo_query = (
    df_geolocation.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", geo_heckpoint_path)
        .trigger(availableNow=True)
        .start(geo_bronze_path)
)

geo_query.awaitTermination()

# COMMAND ----------

#Load   
df_geo = spark.read.format('delta').load(geo_bronze_path)
df_geo.show(5)

df_geo.printSchema() 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.bronze.geolocation
# MAGIC using delta
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Geolocation/';

# COMMAND ----------

spark.table("Olist__Batch__Data.bronze.geolocation").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(3)Order_Items_File

# COMMAND ----------

# Paths : (Order_Items_File)

oi_raw_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Order_Items/"

oi_bronze_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Order_Items/"

oi_schema_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/schemas/Order_Items/"

oi_checkpoint_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/streams/Order_Items/"

# COMMAND ----------

# Read from Raw_Data (ADLS Gen2)
df_order_items = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", oi_schema_path)
        .load(oi_raw_path)
)

# COMMAND ----------

## Write to Bronze (Delta__Table)
oi_query = (
    df_order_items.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", oi_checkpoint_path)
        .trigger(availableNow=True)
        .start(oi_bronze_path)
)

oi_query.awaitTermination()

# COMMAND ----------

#Load   
df_order_items = spark.read.format('delta').load(oi_bronze_path)
df_order_items.show(5)

df_order_items.printSchema() 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.bronze.order_items
# MAGIC using delta
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Order_Items/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.bronze.order_items").show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ##(4)Order_Payments_File

# COMMAND ----------

# Paths : (Order_Payments_File)

op_raw_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Order_Payments/"

op_bronze_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Order_Payments/"

op_schema_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/schemas/Order_Payments/"

op_checkpoint_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/streams/Order_Payments/"

# COMMAND ----------

# Read from Raw_Data (ADLS Gen2)
df_order_payments = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", op_schema_path)
        .load(op_raw_path)
)

# COMMAND ----------

## Write to Bronze (Delta__Table)
op_query = (
    df_order_payments.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", op_checkpoint_path)
        .trigger(availableNow=True)
        .start(op_bronze_path)
)

op_query.awaitTermination()

# COMMAND ----------

#Load   
df_order_payments = spark.read.format('delta').load(op_bronze_path)
df_order_payments.show(5)

df_order_payments.printSchema() 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.bronze.order_payments
# MAGIC using delta
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Order_Payments/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.bronze.order_payments").show(5)



# COMMAND ----------

# MAGIC %md
# MAGIC ## (5)Order_Reviews_File

# COMMAND ----------

# Paths : (Order_Reviews_File)

or_raw_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Order_Reviews/"

or_bronze_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Order_Reviews/"

or_schema_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/schemas/Order_Reviews/"

or_checkpoint_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/streams/Order_Reviews/"

# COMMAND ----------

# Read from Raw_Data (ADLS Gen2)
df_order_reviews = (
     spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("cloudFiles.schemaLocation", or_schema_path)
        .load(or_raw_path)
)

# COMMAND ----------

## Write to Bronze (Delta__Table)
or_query = (
    df_order_reviews.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", or_checkpoint_path)
        .trigger(availableNow=True)
        .start(or_bronze_path)
)

or_query.awaitTermination()

# COMMAND ----------

#Load   
df_order_reviews = spark.read.format('delta').load(or_bronze_path)
df_order_reviews.show(5)

df_order_reviews.printSchema() 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.bronze.order_reviews
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Order_Reviews/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.bronze.order_reviews").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(6)Orders_File

# COMMAND ----------

# Paths : (Orders_File)

o_raw_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Orders/"

o_bronze_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Orders/"

o_schema_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/schemas/Orders/"

o_checkpoint_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/streams/Orders/"

# COMMAND ----------

# Read from Raw_Data (ADLS Gen2)
df_orders = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", o_schema_path)
        .load(o_raw_path)
)

# COMMAND ----------

## Write to Bronze (Delta__Table)
o_query = (
    df_orders.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", o_checkpoint_path)
        .trigger(availableNow=True)
        .start(o_bronze_path)
)

o_query.awaitTermination()

# COMMAND ----------

#Load   
df_orders = spark.read.format('delta').load(o_bronze_path)
df_orders.show(5)

df_orders.printSchema() 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.bronze.orders
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Orders/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.bronze.orders").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(7)Products_File

# COMMAND ----------

# Paths : (Produts_File)

p_raw_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Products/"

p_bronze_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Products/"

p_schema_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/schemas/Products/"

p_checkpoint_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/streams/Products/"

# COMMAND ----------

# Read from Raw_Data (ADLS Gen2)
df_products = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", p_schema_path)
        .load(p_raw_path)
)

# COMMAND ----------

## Write to Bronze (Delta__Table)
pro_query = (
    df_products.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", p_checkpoint_path)
        .trigger(availableNow=True)
        .start(p_bronze_path)
)

pro_query.awaitTermination()

# COMMAND ----------

#Load   
df_products = spark.read.format('delta').load(p_bronze_path)
df_products.show(5)

df_products.printSchema() 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.bronze.products
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Products/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.bronze.products").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(8)Sellers_File

# COMMAND ----------

# Paths : (Sellers_File)

s_raw_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Sellers/"

s_bronze_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Sellers/"

s_schema_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/schemas/Sellers/"

s_checkpoint_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/streams/Sellers/"

# COMMAND ----------

# Read from Raw_Data (ADLS Gen2)
df_sellers = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", s_schema_path)
        .load(s_raw_path)
)

# COMMAND ----------

## Write to Bronze (Delta__Table)
sel_query = (
    df_sellers.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", s_checkpoint_path)
        .trigger(availableNow=True)
        .start(s_bronze_path)
)

sel_query.awaitTermination()

# COMMAND ----------

#Load   
df_sellers = spark.read.format('delta').load(s_bronze_path)
df_sellers.show(5)

df_sellers.printSchema() 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.bronze.sellers
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Sellers/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.bronze.sellers").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(9)Category_Name_Translation_File

# COMMAND ----------

# Paths : (Category_Name_Translation_File)

cnt_raw_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Raw_Data/Category_Name_Translation/"

cnt_bronze_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Category_Name_Translation/"

cnt_schema_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/schemas/Category_Name_Translation/"

cnt_checkpoint_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/checkpoints/streams/Category_Name_Translation/"

# COMMAND ----------

# Read from Raw_Data (ADLS Gen2)
df_category_name_translation = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", cnt_schema_path)
        .load(cnt_raw_path)
)

# COMMAND ----------

## Write to Bronze (Delta__Table)
cnt_query = (
    df_category_name_translation.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", cnt_checkpoint_path)
        .trigger(availableNow=True)
        .start(cnt_bronze_path)
)

cnt_query.awaitTermination()

# COMMAND ----------

#Load   
df_category_name_translation = spark.read.format('delta').load(cnt_bronze_path)
df_category_name_translation.show(5)

df_category_name_translation.printSchema() 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.bronze.category_name_translation
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Bronze_Layer/Category_Name_Translation/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.bronze.category_name_translation").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test All Files Done !!

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in Olist__Batch__Data.bronze;

# COMMAND ----------

