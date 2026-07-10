# Databricks notebook source
# MAGIC %md
# MAGIC # The flow I will make in each table 
# MAGIC ###1. Read Table (Bronze Layer)
# MAGIC ###2. Schema Validation
# MAGIC  - Check the col. match in schema data whether it comes from source or api 
# MAGIC ###3. Data Type Casting
# MAGIC  - Ensure each column has the correct data type (Integer, Double, Date, String....)
# MAGIC ###4. Remove Duplicates
# MAGIC  - Remove duplicate records based on business keys or full-row duplicates
# MAGIC ###5. Handle Missing Values
# MAGIC   - Remove or Handel it
# MAGIC
# MAGIC ###6. Data Standardization
# MAGIC  - String Cleaning
# MAGIC     - like from " Alberto " To "Alberto"
# MAGIC - Standardizing the format and values of data 
# MAGIC   - Normalize categorical values:
# MAGIC   - "Male", "M", "male" → "male"
# MAGIC   - "Female", "F", "female" → "female"
# MAGIC ###7. Data Quality Checks & Logic
# MAGIC  - Check the col. have a value in good range
# MAGIC     - like count = -50 'issue'
# MAGIC ###8. Write Silver Table

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema To Validate
# MAGIC
# MAGIC ![Schema Validation](/Volumes/olist_pipeline_v2/image_schema/image/Schema of the database.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(1)Customer_File

# COMMAND ----------

#(1)(2) Read Table & Schema Validation
import pyspark
df_customers = spark.table('Olist__Batch__Data.bronze.customers')
df_customers.show(5)

# COMMAND ----------

### validation step is done , compere with the schema , and outo loader will take all columns in right way , _rescued_data == null , then it not need to be in the table
df_customers = df_customers.drop("_rescued_data")

# COMMAND ----------

df_customers.show(2)

# COMMAND ----------

num_of_cus = df_customers.count()
f"Number of customers is: {num_of_cus}"

# COMMAND ----------

#(3) Data Type Casting
df_customers.printSchema()

# COMMAND ----------

### Edit "customer_zip_code_prefix" : type cast to "int"
df_customers = df_customers.withColumn("customer_zip_code_prefix", df_customers["customer_zip_code_prefix"].cast("int"))

df_customers.printSchema()

# COMMAND ----------

## (4)Remove Duplicates

# Check Number Of Duplicates
num_of_dup_cus = df_customers.count() - df_customers.dropDuplicates().count()
print(num_of_dup_cus)

# COMMAND ----------

### I know we don't have a duplicates but we will do it, Because the project run all files in "friday", if data come have duplicates we will delete it
# Delete Duplicates
df_customers = df_customers.dropDuplicates()
df_customers.count()

# COMMAND ----------

## (5)Handel missing value (Remove or Handel it)

# check number of nulls (Null Profiling)
import pyspark.sql.functions as F
nulls_df_cus = df_customers.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_customers.columns
])

display(nulls_df_cus)


# COMMAND ----------

### If data comes with nulls we will delete it also this is by default in the project , But if this project have human to monter the data we will handel it --->> this is recomended , because we don't know what is the null value

# Delete Nulls
df_customers= df_customers.dropna()
df_customers.count()

# COMMAND ----------

df_customers.show(2)

# COMMAND ----------


##(6) Data Standardization

###(1) check if data have space in the string columns
from pyspark.sql.functions import col, trim

string_columns = [
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state"
]

df_customers_check = df_customers

for column in string_columns:
    df_customers_check = df_customers_check.withColumn(
        column,
        trim(col(column))
    )

# COMMAND ----------

from pyspark.sql.functions import col, trim

df_customers.filter(
    (col("customer_id") != trim(col("customer_id"))) |
    (col("customer_unique_id") != trim(col("customer_unique_id"))) |
    (col("customer_city") != trim(col("customer_city"))) |
    (col("customer_state") != trim(col("customer_state")))
).show(truncate=False)

# COMMAND ----------

###(2) Normalize categorical values
from pyspark.sql.functions import col, lower, trim, upper
df_customers = df_customers.withColumn("customer_state", upper(trim(col("customer_state"))))
df_customers.show()

# COMMAND ----------

##(7) Data Quality Checks & Logic

# --> Understand The Column --> "customer_state" What Is The Value Have
unique_states = df_customers.select("customer_state").distinct()
unique_states.show()

# COMMAND ----------

# --> Understand The Column --> :"customer_city" what the value have
unique_cities = df_customers.select("customer_city").distinct()
unique_cities.show()

# COMMAND ----------

from pyspark.sql.functions import col
##(1)customer_zip_code_prefix
invalid_customer_zip_code_prefix = df_customers.filter(col("customer_zip_code_prefix") < 0)
invalid_customer_zip_code_prefix.display()

##(2)customer_state
invalid_customer_state = (df_customers.filter(col("customer_state").isNull() |(trim(col("customer_state")) == "")))
invalid_customer_state.display()

## (3) customer_id
invalid_customer_id = (df_customers.filter(col("customer_id").rlike("^[0-9]+$")))
invalid_customer_id.display()

## (4) customer_unique_id
invalid_customer_unique_id = (df_customers.filter(col("customer_unique_id").rlike("^[0-9]+$")))
invalid_customer_unique_id.display()

## (5) customer_city
invalid_customer_city = (df_customers.filter(col("customer_city").rlike("^[0-9]+$")))
invalid_customer_city.display()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Save all errors in Quarantine Table
invalid_customers = (
    df_customers.filter(
        (col("customer_zip_code_prefix") < 0) |

        # Customer ID
        (col("customer_id").isNull()) |
        (trim(col("customer_id")) == "") |
        (col("customer_id").rlike("^[0-9]+$")) |

        # Customer Unique ID
        (col("customer_unique_id").isNull()) |
        (trim(col("customer_unique_id")) == "") |
        (col("customer_unique_id").rlike("^[0-9]+$")) |

        # Customer City
        (col("customer_city").isNull()) |
        (trim(col("customer_city")) == "") |
        (col("customer_city").rlike("^[0-9]+$")) |

        # Customer State
        (col("customer_state").isNull()) |
        (trim(col("customer_state")) == "") |
        (col("customer_state").rlike("^[0-9]+$"))
    )
)

display(invalid_customers)

# COMMAND ----------

from pyspark.sql.functions import (col,trim,when,lit,concat_ws)

invalid_customers = (
    invalid_customers.withColumn(
        "error_reason",
        concat_ws(
            " | ",

            when(col("customer_zip_code_prefix") < 0,
                 lit("Negative ZIP Code")),

            when(col("customer_id").isNull(),
                 lit("Customer ID is NULL")),

            when(trim(col("customer_id")) == "",
                 lit("Customer ID is Empty")),

            when(col("customer_id").rlike("^[0-9]+$"),
                 lit("Customer ID contains only numbers")),

            when(col("customer_unique_id").isNull(),
                 lit("Customer Unique ID is NULL")),

            when(trim(col("customer_unique_id")) == "",
                 lit("Customer Unique ID is Empty")),

            when(col("customer_unique_id").rlike("^[0-9]+$"),
                 lit("Customer Unique ID contains only numbers")),

            when(col("customer_city").isNull(),
                 lit("Customer City is NULL")),

            when(trim(col("customer_city")) == "",
                 lit("Customer City is Empty")),

            when(col("customer_city").rlike("^[0-9]+$"),
                 lit("Customer City contains only numbers")),

            when(col("customer_state").isNull(),
                 lit("Customer State is NULL")),

            when(trim(col("customer_state")) == "",
                 lit("Customer State is Empty")),

            when(col("customer_state").rlike("^[0-9]+$"),
                 lit("Customer State contains only numbers"))
        )
    )
)

# COMMAND ----------

display(invalid_customers)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

invalid_customers = (
    invalid_customers
    .withColumn("quarantine_timestamp", current_timestamp())
    .withColumn("source_table", lit("customers"))
)

# COMMAND ----------

## Save Quarantine Table

quarantine_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Customers/"

(
    invalid_customers.write.format("delta").mode("append").option("overwriteSchema", "true").save(quarantine_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Olist__Batch__Data.quarantine.customers
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Customers/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.quarantine.customers").show()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Keep only valid ZIP Code
df_customers = df_customers.filter(col("customer_zip_code_prefix") >= 0)

### Keep only valid Customer State
df_customers = df_customers.filter(col("customer_state").isNotNull() &(trim(col("customer_state")) != ""))

### Keep only valid Customer ID
df_customers = df_customers.filter(col("customer_id").isNotNull() &(trim(col("customer_id")) != "") &(~col("customer_id").rlike("^[0-9]+$")))

### Keep only valid Customer Unique ID
df_customers = df_customers.filter(col("customer_unique_id").isNotNull() &(trim(col("customer_unique_id")) != "") &
(~col("customer_unique_id").rlike("^[0-9]+$")))

### Keep only valid Customer City
df_customers = df_customers.filter(col("customer_city").isNotNull() &(trim(col("customer_city")) != "") &
(~col("customer_city").rlike("^[0-9]+$"))
)

print("Valid Customers:", df_customers.count())

# COMMAND ----------

## (8) write silver Table

## Path of Silver_Layer ... in ADLS Gen2
cus_silver_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Customers/"

# COMMAND ----------

## Write this Table in Silver Layer Folder
df_customers.write.mode("overwrite").format("delta").save(cus_silver_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.silver.customers
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Customers/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.silver.customers").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(2)Geolocation_File

# COMMAND ----------

#(1)(2) Read Table & Schema Validation
import pyspark
df_Geolocation = spark.table('Olist__Batch__Data.bronze.geolocation')
df_Geolocation.show(5)

# COMMAND ----------

### validation step is done , compere with the schema , and auto loader will take all columns in right way , _rescued_data == null , then it not need to be in the table
df_Geolocation = df_Geolocation.drop("_rescued_data")

# COMMAND ----------

df_Geolocation.show(2)

# COMMAND ----------

num_of_geo = df_Geolocation.count()
f"Number of Geolocation is: {num_of_geo}"

# COMMAND ----------

#(3) Data Type Casting
df_Geolocation.printSchema()

# COMMAND ----------

## Edit "geolocation_zip_code_prefix" , "geolocation_lat" ,and "geolocation_lng" data type to "double" and "int"

df_Geolocation = df_Geolocation.withColumn("geolocation_zip_code_prefix", df_Geolocation["geolocation_zip_code_prefix"].cast("int"))
df_Geolocation = df_Geolocation.withColumn("geolocation_lat", df_Geolocation["geolocation_lat"].cast("double"))
df_Geolocation = df_Geolocation.withColumn("geolocation_lng", df_Geolocation["geolocation_lng"].cast("double"))
df_Geolocation.printSchema()

# COMMAND ----------

## (4)Remove Duplicates

# Check Number Of Duplicates
num_of_dup_geo = df_Geolocation.count() - df_Geolocation.dropDuplicates().count()
print(num_of_dup_geo)

# COMMAND ----------

df_Geolocation.groupBy("geolocation_zip_code_prefix").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Handling Duplicate `geolocation_zip_code_prefix` Values
# MAGIC
# MAGIC - Duplicate `geolocation_zip_code_prefix` values are expected because a single ZIP code prefix can correspond to multiple geographic coordinates.
# MAGIC - To avoid **row explosion** during joins with the `customers` table in the Gold layer, the geolocation data was deduplicated to retain a single representative record for each `zip_code_prefix`.
# MAGIC

# COMMAND ----------

# Delete Duplicates
df_Geolocation = df_Geolocation.dropDuplicates()
df_Geolocation.count()

# COMMAND ----------

## (5)Handle missing value (Remove or Handel it)

# check number of nulls (Null Profiling)
import pyspark.sql.functions as F
nulls_df_geo = df_Geolocation.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_Geolocation.columns
])

display(nulls_df_geo)

# COMMAND ----------

### If data comes with nulls we will delete it also this is by default in the project , But if this project have human to monter the data we will handle it --->> this is recomended , because we don't know what is the null value

# Delete Nulls
df_Geolocation = df_Geolocation.dropna()
df_Geolocation.count()

# COMMAND ----------

df_Geolocation.show(2)

# COMMAND ----------


##(6) Data Standardization

###(1) check if data have space in the string columns
from pyspark.sql.functions import trim

df_Geolocation = (
    df_Geolocation
    .withColumn("geolocation_city", trim("geolocation_city"))
    .withColumn("geolocation_state", trim("geolocation_state"))
)

# COMMAND ----------

from pyspark.sql.functions import col, trim
df_Geolocation.filter(

    (col("geolocation_city") != trim(col("geolocation_city"))) |
    (col("geolocation_state") != trim(col("geolocation_state")))
).show(truncate=False)

# COMMAND ----------

###(2) Normalize categorical values
from pyspark.sql.functions import col, lower, trim, upper
df_Geolocation = df_Geolocation.withColumn("geolocation_state", upper(trim(col("geolocation_state"))))
df_Geolocation.show(2)

# COMMAND ----------

##(7) Data Quality Checks & Logic

###(1) Check if the col. "geolocation_zip_code_prefix" have valid values or negative values
from pyspark.sql.functions import col
df_Geolocation.filter((col("geolocation_zip_code_prefix") == 0) | (col("geolocation_zip_code_prefix") < 0)).show()


# COMMAND ----------

###(2) Now we need to check the col.: "geolocation_lat" , "geolocation_lng" have valid values
from pyspark.sql.functions import col

df_Geolocation.filter((col("geolocation_lat") < -90) |(col("geolocation_lat") > 90)).show()
df_Geolocation.filter((col("geolocation_lng") < -180) |(col("geolocation_lng") > 180)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Good Result : 
# MAGIC - No invalid latitude or longitude values were found
# MAGIC - All geographic coordinates fall within the expected ranges

# COMMAND ----------

###(3) Ensure in Col. : "geolocation_city" , "geolocation_state" have valid values
from pyspark.sql.functions import col

df_Geolocation.filter((col("geolocation_city") == "") | (col("geolocation_state") == "")).count()


# COMMAND ----------

from pyspark.sql.functions import col, trim

### Save all errors in Quarantine Table
invalid_geolocation = (
    df_Geolocation.filter(
        (col("geolocation_zip_code_prefix") < 0) |

        # Geolocation City
        (col("geolocation_city").isNull()) |
        (trim(col("geolocation_city")) == "") |
        (col("geolocation_city").rlike("^[0-9]+$")) |

        # Geolocation State
        (col("geolocation_state").isNull()) |
        (trim(col("geolocation_state")) == "") |
        (col("geolocation_state").rlike("^[0-9]+$")) |

        # Invalid Spatial Coordinates
        (col("geolocation_lat") < -90) | (col("geolocation_lat") > 90) |
        (col("geolocation_lng") < -180) | (col("geolocation_lng") > 180)
    )
)

# COMMAND ----------

display(invalid_geolocation)

# COMMAND ----------

from pyspark.sql.functions import (col,trim,when,lit,concat_ws)

invalid_geolocation = (
    invalid_geolocation.withColumn(
        "error_reason",
        concat_ws(
            " | ",

            when(col("geolocation_zip_code_prefix") < 0,
                 lit("Negative ZIP Code")),

            when(col("geolocation_city").isNull(),
                 lit("Geolocation City is NULL")),

            when(trim(col("geolocation_city")) == "",
                 lit("Geolocation City is Empty")),

            when(col("geolocation_city").rlike("^[0-9]+$"),
                 lit("Geolocation City contains only numbers")),

            when(col("geolocation_state").isNull(),
                 lit("Geolocation State is NULL")),

            when(trim(col("geolocation_state")) == "",
                 lit("Geolocation State is Empty")),

            when(col("geolocation_state").rlike("^[0-9]+$"),
                 lit("Geolocation State contains only numbers")),
                 
            when((col("geolocation_lat") < -90) | (col("geolocation_lat") > 90),
                 lit("Latitude is out of valid bounds [-90, 90]")),
                 
            when((col("geolocation_lng") < -180) | (col("geolocation_lng") > 180),
                 lit("Longitude is out of valid bounds [-180, 180]"))
        )
    )
)

# COMMAND ----------

display(invalid_geolocation)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

invalid_geolocation = (
    invalid_geolocation
    .withColumn("quarantine_timestamp", current_timestamp())
    .withColumn("source_table", lit("geolocation"))
)

# COMMAND ----------

## Save Quarantine Table
quarantine_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Geolocation/"

(
    invalid_geolocation.write.format("delta").mode("append").option("overwriteSchema", "true").save(quarantine_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Olist__Batch__Data.quarantine.geolocation
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Geolocation/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.quarantine.geolocation").show()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Keep only valid ZIP Code
df_geolocation = df_Geolocation.filter(col("geolocation_zip_code_prefix") >= 0)

### Keep only valid Geolocation State
df_geolocation = df_geolocation.filter(col("geolocation_state").isNotNull() &(trim(col("geolocation_state")) != ""))

### Keep only valid Geolocation City
df_geolocation = df_geolocation.filter(col("geolocation_city").isNotNull() &(trim(col("geolocation_city")) != "") &
(~col("geolocation_city").rlike("^[0-9]+$"))
)

### Keep only valid Spatial Coordinates
df_geolocation = df_geolocation.filter(
    (col("geolocation_lat") >= -90) & (col("geolocation_lat") <= 90) &
    (col("geolocation_lng") >= -180) & (col("geolocation_lng") <= 180)
)

# COMMAND ----------

print("Valid Geolocation Records:", df_geolocation.count())

# COMMAND ----------

## (8) write silver Table

## Path of Silver_Layer ... in ADLS Gen2
geo_silver_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Geolocation/"

## Write this Table in Silver Layer Folder
df_geolocation.write.mode("overwrite").format("delta").save(geo_silver_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.silver.geolocation
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Geolocation/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.silver.geolocation").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(3)Order_Items_File

# COMMAND ----------

#(1)(2) Read Table & Schema Validation
import pyspark
df_order_items = spark.table('Olist__Batch__Data.bronze.order_items')
df_order_items.show(5)

# COMMAND ----------

### validation step is done , compere with the schema , and outo loader will take all columns in right way , _rescued_data == null , then it not need to be in the table
df_order_items = df_order_items.drop("_rescued_data")
df_order_items.show(2)

# COMMAND ----------

num_of_items = df_order_items.count()
print(f"Number of order items is: {num_of_items}")

# COMMAND ----------

#(3) Data Type Casting
df_order_items.printSchema()

# COMMAND ----------

### Edit columns : type cast to correct data types
df_order_items = df_order_items.withColumn("order_item_id", df_order_items["order_item_id"].cast("int"))
df_order_items = df_order_items.withColumn("shipping_limit_date", df_order_items["shipping_limit_date"].cast("timestamp"))
df_order_items = df_order_items.withColumn("price", df_order_items["price"].cast("double"))
df_order_items = df_order_items.withColumn("freight_value", df_order_items["freight_value"].cast("double"))

df_order_items.printSchema()

# COMMAND ----------

## (4)Remove Duplicates

# Check Number Of Duplicates
num_of_dup_items = df_order_items.count() - df_order_items.dropDuplicates().count()
print(num_of_dup_items)

# COMMAND ----------

# Delete Duplicates when present
df_order_items = df_order_items.dropDuplicates()
df_order_items.count()

# COMMAND ----------

## (5)Handle missing value (Remove or Handle it)

# check number of nulls (Null Profiling)
import pyspark.sql.functions as F
nulls_df_items = df_order_items.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_order_items.columns
])

display(nulls_df_items)

# COMMAND ----------

### If data comes with nulls we will delete it also this is by default in the project , But if this project have human to monter the data we will handel it --->> this is recomended , because we don't know what is the null value

# Delete Nulls
df_order_items= df_order_items.dropna()
df_order_items.count()

# COMMAND ----------

##(6) Data Standardization

###(1) check if data have space in the string columns
from pyspark.sql.functions import col, trim

string_columns = [
    "order_id",
    "product_id",
    "seller_id"
]

df_order_items_check = df_order_items

for column in string_columns:
    df_order_items_check = df_order_items_check.withColumn(
        column,
        trim(col(column))
    )

# COMMAND ----------

from pyspark.sql.functions import col, trim

df_order_items.filter(
    (col("order_id") != trim(col("order_id"))) |
    (col("product_id") != trim(col("product_id"))) |
    (col("seller_id") != trim(col("seller_id")))
).show(truncate=False)

# COMMAND ----------

##(7) Data Quality Checks & Logic

from pyspark.sql.functions import col

##(1)order_item_id
invalid_item_id = df_order_items.filter(col("order_item_id") <= 0)
invalid_item_id.display()

##(2)price
invalid_price = df_order_items.filter(col("price") < 0)
invalid_price.display()

##(3)freight_value
invalid_freight = df_order_items.filter(col("freight_value") < 0)
invalid_freight.display()

##(4)order_id, product_id, seller_id
invalid_ids = df_order_items.filter(
    (col("order_id").isNull()) | (trim(col("order_id")) == "") |
    (col("product_id").isNull()) | (trim(col("product_id")) == "") |
    (col("seller_id").isNull()) | (trim(col("seller_id")) == "")
)

# COMMAND ----------

invalid_ids.display()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Save all errors in Quarantine Table
invalid_order_items = (
    df_order_items.filter(
        (col("order_item_id") <= 0) |
        (col("price") < 0) |
        (col("freight_value") < 0) |

        # Order ID
        (col("order_id").isNull()) |
        (trim(col("order_id")) == "") |

        # Product ID
        (col("product_id").isNull()) |
        (trim(col("product_id")) == "") |

        # Seller ID
        (col("seller_id").isNull()) |
        (trim(col("seller_id")) == "")
    )
)

# COMMAND ----------

display(invalid_order_items)

# COMMAND ----------

from pyspark.sql.functions import (col,trim,when,lit,concat_ws)

invalid_order_items = (
    invalid_order_items.withColumn(
        "error_reason",
        concat_ws(
            " | ",

            when(col("order_item_id") <= 0,
                 lit("Order Item ID is 0 or negative")),

            when(col("price") < 0,
                 lit("Negative Price")),

            when(col("freight_value") < 0,
                 lit("Negative Freight Value")),

            when(col("order_id").isNull(),
                 lit("Order ID is NULL")),

            when(trim(col("order_id")) == "",
                 lit("Order ID is Empty")),

            when(col("product_id").isNull(),
                 lit("Product ID is NULL")),

            when(trim(col("product_id")) == "",
                 lit("Product ID is Empty")),

            when(col("seller_id").isNull(),
                 lit("Seller ID is NULL")),

            when(trim(col("seller_id")) == "",
                 lit("Seller ID is Empty"))
        )
    )
)

# COMMAND ----------

display(invalid_order_items)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

invalid_order_items = (
    invalid_order_items
    .withColumn("quarantine_timestamp", current_timestamp())
    .withColumn("source_table", lit("order_items"))
)

# COMMAND ----------

## Save Quarantine Table
quarantine_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Order_Items/"

(
    invalid_order_items.write.format("delta").mode("append").option("overwriteSchema", "true").save(quarantine_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Olist__Batch__Data.quarantine.order_items
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Order_Items/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.quarantine.order_items").show()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Keep only valid Order Item ID
df_order_items = df_order_items.filter(col("order_item_id") > 0)

### Keep only valid Price & Freight
df_order_items = df_order_items.filter(col("price") >= 0)
df_order_items = df_order_items.filter(col("freight_value") >= 0)

### Keep only valid Order ID
df_order_items = df_order_items.filter(col("order_id").isNotNull() & (trim(col("order_id")) != ""))

### Keep only valid Product ID
df_order_items = df_order_items.filter(col("product_id").isNotNull() & (trim(col("product_id")) != ""))

### Keep only valid Seller ID
df_order_items = df_order_items.filter(col("seller_id").isNotNull() & (trim(col("seller_id")) != ""))

print("Valid Order Items:", df_order_items.count())

# COMMAND ----------

## (8) write silver Table

## Path of Silver_Layer ... in ADLS Gen2
items_silver_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Order_Items/"

## Write this Table in Silver Layer Folder
df_order_items.write.mode("overwrite").format("delta").save(items_silver_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.silver.order_items
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Order_Items/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.silver.order_items").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##(4)Order_Payments_File

# COMMAND ----------

# (1)(2) Read Table and Schema Validation
from pyspark.sql.functions import col, trim, lower, current_timestamp, lit
import pyspark
df_order_payments = spark.table('Olist__Batch__Data.bronze.order_payments')
df_order_payments.show(5)

# COMMAND ----------

### validation step is done , compere with the schema , and outo loader will take all columns in right way , _rescued_data == null , then it not need to be in the table
df_order_payments = df_order_payments.drop("_rescued_data")
df_order_payments.show(2)

# COMMAND ----------

num_of_payments = df_order_payments.count()
print(f"Number of order payments is: {num_of_payments}")

# COMMAND ----------

# (3) Data Type Casting 
# Check data types
df_order_payments.printSchema()

# COMMAND ----------

### Edit columns : type cast to correct data types
df_order_payments = df_order_payments.withColumn("payment_sequential", df_order_payments["payment_sequential"].cast("int"))
df_order_payments = df_order_payments.withColumn("payment_installments", df_order_payments["payment_installments"].cast("int"))
df_order_payments = df_order_payments.withColumn("payment_value", df_order_payments["payment_value"].cast("double"))

# COMMAND ----------

df_order_payments.printSchema()

# COMMAND ----------

## (4)Remove Duplicates

# Check Number Of Duplicates
num_of_dup_payments = df_order_payments.count() - df_order_payments.dropDuplicates().count()
print(num_of_dup_payments)

# COMMAND ----------

### I know we don't have a duplicates but we will do it, Because the project run all files in "friday", if data come have duplicates we will delete it
# Delete Duplicates
df_order_payments = df_order_payments.dropDuplicates()
df_order_payments.count()

# COMMAND ----------

## (5)Handle missing value (Remove or Handel it)

# check number of nulls (Null Profiling)
import pyspark.sql.functions as F
nulls_df_payments = df_order_payments.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_order_payments.columns
])

# COMMAND ----------

display(nulls_df_payments)

# COMMAND ----------

### If data comes with nulls we will delete it also this is by default in the project , But if this project have human to monter the data we will handel it --->> this is recomended , because we don't know what is the null value

# Delete Nulls
df_order_payments= df_order_payments.dropna()
df_order_payments.count()

# COMMAND ----------

##(6) Data Standardization

###(1) check if data have space in the string columns
from pyspark.sql.functions import col, trim

string_columns = [
    "order_id",
    "payment_type"
]

df_payments_check = df_order_payments

for column in string_columns:
    df_payments_check = df_payments_check.withColumn(
        column,
        trim(col(column))
    )

# COMMAND ----------

df_order_payments.filter(
    (col("order_id") != trim(col("order_id"))) |
    (col("payment_type") != trim(col("payment_type")))
).show(truncate=False)

# COMMAND ----------

###(2) Normalize categorical values
from pyspark.sql.functions import col, lower, trim, upper
# Standardize payment_type ('credit_card' -> 'CREDIT_CARD')
df_order_payments = df_order_payments.withColumn("payment_type", upper(trim(col("payment_type"))))
df_order_payments.show()

# COMMAND ----------

##(7) Data Quality Checks & Logic

# --> Understand The Column --> "payment_type" What Is The Value Have
unique_payment_types = df_order_payments.select("payment_type").distinct()
unique_payment_types.show()

# COMMAND ----------

df_order_payments.filter(col("payment_type") == "NOT_DEFINED").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###good there is only three recourds, then delete them

# COMMAND ----------

# Filtering OUT the bad records 'NOT_DEFINED', and if in future we have more bad records we will delete (Pipeline run in friday 12 PM)
df_order_payments = df_order_payments.filter(F.col("payment_type") != "NOT_DEFINED")

# COMMAND ----------

# Verify the fix
df_order_payments.select("payment_type").distinct().show()
df_order_payments.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Validation
# MAGIC
# MAGIC To ensure data quality, validation checks are performed during each pipeline run to verify that the ingested data is complete, consistent, and free from invalid or corrupted records before proceeding to the next layer.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

##(1)payment_sequential
invalid_payment_sequential = df_order_payments.filter(col("payment_sequential") < 1)
invalid_payment_sequential.display()

##(2)payment_installments
# Installments should generally be >= 1, but we quarantine strictly negative impossible values
invalid_payment_installments = df_order_payments.filter(col("payment_installments") < 0)
invalid_payment_installments.display()

##(3)payment_value
invalid_payment_value = df_order_payments.filter(col("payment_value") < 0)
invalid_payment_value.display()

##(4)order_id
invalid_order_id = (df_order_payments.filter(col("order_id").isNull() | (trim(col("order_id")) == "")))
invalid_order_id.display()

##(5)payment_type
invalid_payment_type = (df_order_payments.filter(col("payment_type").isNull() | (trim(col("payment_type")) == "")))
invalid_payment_type.display()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Save all errors in Quarantine Table
invalid_order_payments = (
    df_order_payments.filter(
        (col("payment_sequential") < 1) |
        (col("payment_installments") < 0) |
        (col("payment_value") < 0) |

        # Order ID
        (col("order_id").isNull()) |
        (trim(col("order_id")) == "") |

        # Payment Type
        (col("payment_type").isNull()) |
        (trim(col("payment_type")) == "") |
        
        # Cross-Column Business Logic: Credit Cards cannot have 0 installments
        ((col("payment_type") == "CREDIT_CARD") & (col("payment_installments") == 0))
    )
)

# COMMAND ----------

display(invalid_order_payments)

# COMMAND ----------

from pyspark.sql.functions import (col,trim,when,lit,concat_ws)

invalid_order_payments = (
    invalid_order_payments.withColumn(
        "error_reason",
        concat_ws(
            " | ",

            when(col("payment_sequential") < 1,
                 lit("Payment Sequential is less than 1")),

            when(col("payment_installments") < 0,
                 lit("Negative Payment Installments")),

            when(col("payment_value") < 0,
                 lit("Negative Payment Value")),

            when(col("order_id").isNull(),
                 lit("Order ID is NULL")),

            when(trim(col("order_id")) == "",
                 lit("Order ID is Empty")),

            when(col("payment_type").isNull(),
                 lit("Payment Type is NULL")),

            when(trim(col("payment_type")) == "",
                 lit("Payment Type is Empty")),
            
            when(trim(col("payment_type")) == "",
                 lit("Payment Type is Empty")),
                 
            when((col("payment_type") == "CREDIT_CARD") & (col("payment_installments") == 0),
                 lit("Illogical Payment: Credit Card with 0 Installments"))
           )
        )
    )    

# COMMAND ----------

display(invalid_order_payments)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

invalid_order_payments = (
    invalid_order_payments
    .withColumn("quarantine_timestamp", current_timestamp())
    .withColumn("source_table", lit("order_payments"))
)

# COMMAND ----------

## Save Quarantine Table
quarantine_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Order_Payments/"

(
    invalid_order_payments.write.format("delta").mode("append").option("overwriteSchema", "true").save(quarantine_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Olist__Batch__Data.quarantine.order_payments
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Order_Payments/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.quarantine.order_payments").show()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Keep only valid Payment Sequential
df_order_payments = df_order_payments.filter(col("payment_sequential") >= 1)

### Keep only valid Payment Installments
df_order_payments = df_order_payments.filter(col("payment_installments") >= 0)

### Keep only valid Payment Value
df_order_payments = df_order_payments.filter(col("payment_value") >= 0)

### Keep only valid Order ID
df_order_payments = df_order_payments.filter(col("order_id").isNotNull() & (trim(col("order_id")) != ""))

### Keep only valid Payment Type
df_order_payments = df_order_payments.filter(col("payment_type").isNotNull() & (trim(col("payment_type")) != ""))

### Keep only valid Payment Installments (and remove the 0-installment credit cards)
df_order_payments = df_order_payments.filter(
    (col("payment_installments") >= 0) & 
    ~((col("payment_type") == "CREDIT_CARD") & (col("payment_installments") == 0))
)


print("Valid Order Payments:", df_order_payments.count())

# COMMAND ----------

## (8) write silver Table

## Path of Silver_Layer ... in ADLS Gen2
payments_silver_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Order_Payments/"

## Write this Table in Silver Layer Folder
df_order_payments.write.mode("overwrite").format("delta").save(payments_silver_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.silver.order_payments
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Order_Payments/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.silver.order_payments").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(5)Order_Reviews_File

# COMMAND ----------

#(1)(2) Read Table & Schema Validation
import pyspark
df_order_reviews = spark.table('Olist__Batch__Data.bronze.order_reviews')
df_order_reviews.show(5)

# COMMAND ----------

df_order_reviews.select("review_score").distinct().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV Parsing Issue Fix
# MAGIC
# MAGIC **Problem:**
# MAGIC `review_score` contained timestamps and review text instead of values `1–5`, causing casting errors.
# MAGIC
# MAGIC **Cause:**
# MAGIC The CSV file contains quoted/multiline review comments, but it was read without multiline parsing options, resulting in column misalignment.
# MAGIC
# MAGIC **Fix:**
# MAGIC
# MAGIC ```python
# MAGIC .option("multiLine", "true")
# MAGIC .option("quote", '"')
# MAGIC .option("escape", '"')
# MAGIC ```
# MAGIC
# MAGIC **Result:**
# MAGIC The CSV was parsed correctly, `review_score` contained only valid values (`1–5`), and type casting completed successfully.
# MAGIC

# COMMAND ----------

### validation step is done , compere with the schema , and outo loader will take all columns in right way , _rescued_data == null , then it not need to be in the table
df_order_reviews = df_order_reviews.drop("_rescued_data")
df_order_reviews.show(5)

# COMMAND ----------

from pyspark.sql.functions import col

df_order_reviews.filter(
    col("review_answer_timestamp").contains('b396ba75350276a6cc1993b6627dceea')).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
# 2. CRITICAL NLP NORMALIZATION: Replace carriage returns and newlines with a single space
# This prevents downstream CSV exports from breaking.
df_order_reviews = df_order_reviews \
    .withColumn("review_comment_message", regexp_replace(col("review_comment_message"), "[\r\n]", " ")) \
    .withColumn("review_comment_title", regexp_replace(col("review_comment_title"), "[\r\n]", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Text Normalization
# MAGIC
# MAGIC To improve data quality and prevent downstream processing issues, carriage returns (`\r`) and newline characters (`\n`) were replaced with a single space in the review title and review message. This ensures consistent text formatting and avoids errors during CSV export and subsequent data processing.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

df_order_reviews.filter(
    col("review_answer_timestamp").contains('b396ba75350276a6cc1993b6627dceea')).show(truncate=False)

# COMMAND ----------

num_of_reviews = df_order_reviews.count()
print(f"Number of order reviews is: {num_of_reviews}")

# COMMAND ----------

# (3) Data Type Casting 
# Check data types
df_order_reviews.printSchema()

# COMMAND ----------

    ### Edit columns : type cast to correct data types
df_order_reviews = df_order_reviews.withColumn("review_score", df_order_reviews["review_score"].try_cast("double"))
df_order_reviews = df_order_reviews.withColumn("review_creation_date", df_order_reviews["review_creation_date"].try_cast("timestamp"))
df_order_reviews = df_order_reviews.withColumn("review_answer_timestamp", df_order_reviews["review_answer_timestamp"].try_cast("timestamp"))

# COMMAND ----------

df_order_reviews.printSchema()

# COMMAND ----------

## (4)Remove Duplicates
from pyspark.sql.functions import col
order_reviews_duplicates = df_order_reviews.select("order_id", "review_id").groupBy("order_id", "review_id").count().filter(col("count") > 1)

display(order_reviews_duplicates)


# COMMAND ----------

# Check Number Of Duplicates
num_of_dup_reviews = order_reviews_duplicates.count()
print(num_of_dup_reviews)

# COMMAND ----------

# Delete Duplicates
df_order_reviews = df_order_reviews.dropDuplicates()
df_order_reviews.count()

# COMMAND ----------

## (5) Handle missing value (Remove or Handle it)

# check number of nulls (Null Profiling)
import pyspark.sql.functions as F
nulls_df_reviews = df_order_reviews.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_order_reviews.columns
])
display(nulls_df_reviews)

# COMMAND ----------

### 1. Fill non-critical text columns so Power BI doesn't show "(Blank)"
df_order_reviews = df_order_reviews \
    .fillna("No Title", subset=["review_comment_title"]) \
    .fillna("No Comment", subset=["review_comment_message"])

# COMMAND ----------

### 2. Drop the row entirely ONLY if critical master keys or the score are missing
df_order_reviews = df_order_reviews.dropna(subset=["review_id", "order_id", "review_score"])

print("Records after handling nulls:", df_order_reviews.count())

# COMMAND ----------

##(6) Data Standardization

# 1. check if data have space in the string columns
from pyspark.sql.functions import col, trim

string_columns = [
    "review_id",
    "order_id",
    "review_comment_title",
    "review_comment_message"
]

df_reviews_check = df_order_reviews

for column in string_columns:
    df_reviews_check = df_reviews_check.withColumn(
        column,
        trim(col(column))
    )

# COMMAND ----------

# 3. Validation Check
from pyspark.sql.functions import col, trim

df_order_reviews.filter(
    (col("review_id") != trim(col("review_id"))) |
    (col("order_id") != trim(col("order_id")))
).show(truncate=False)

# COMMAND ----------

##(7) Data Quality Checks & Logic

# --> Understand The Column --> "review_score" What Is The Value Have
unique_scores = df_order_reviews.select("review_score").distinct()
unique_scores.show()

# COMMAND ----------

from pyspark.sql.functions import col

##(1)review_score (Olist scores must be between 1 and 5)
invalid_review_score = df_order_reviews.filter((col("review_score") < 1) | (col("review_score") > 5))
invalid_review_score.display()

##(2)review_id
invalid_review_id = (df_order_reviews.filter(col("review_id").isNull() | (trim(col("review_id")) == "")))
invalid_review_id.display()

##(3)order_id
invalid_order_id = (df_order_reviews.filter(col("order_id").isNull() | (trim(col("order_id")) == "")))
invalid_order_id.display()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Save all errors in Quarantine Table
invalid_order_reviews = (
    df_order_reviews.filter(
        (col("review_score") < 1) | 
        (col("review_score") > 5) |

        # Review ID
        (col("review_id").isNull()) |
        (trim(col("review_id")) == "") |

        # Order ID
        (col("order_id").isNull()) |
        (trim(col("order_id")) == "")
    )
)

display(invalid_order_reviews)

# COMMAND ----------

from pyspark.sql.functions import (col,trim,when,lit,concat_ws)

invalid_order_reviews = (
    invalid_order_reviews.withColumn(
        "error_reason",
        concat_ws(
            " | ",

            when((col("review_score") < 1) | (col("review_score") > 5),
                 lit("Review Score out of bounds (must be 1-5)")),

            when(col("review_id").isNull(),
                 lit("Review ID is NULL")),

            when(trim(col("review_id")) == "",
                 lit("Review ID is Empty")),

            when(col("order_id").isNull(),
                 lit("Order ID is NULL")),

            when(trim(col("order_id")) == "",
                 lit("Order ID is Empty"))
        )
    )
)

display(invalid_order_reviews)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

invalid_order_reviews = (
    invalid_order_reviews
    .withColumn("quarantine_timestamp", current_timestamp())
    .withColumn("source_table", lit("order_reviews"))
)

## Save Quarantine Table
quarantine_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Order_Reviews/"

(
    invalid_order_reviews.write.format("delta").mode("append").option("overwriteSchema", "true").save(quarantine_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Olist__Batch__Data.quarantine.order_reviews
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Order_Reviews/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.quarantine.order_reviews").show()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Keep only valid Review Scores
df_order_reviews = df_order_reviews.filter((col("review_score") >= 1) & (col("review_score") <= 5))

### Keep only valid Review ID
df_order_reviews = df_order_reviews.filter(col("review_id").isNotNull() & (trim(col("review_id")) != ""))

### Keep only valid Order ID
df_order_reviews = df_order_reviews.filter(col("order_id").isNotNull() & (trim(col("order_id")) != ""))


print("Valid Order Reviews:", df_order_reviews.count())

# COMMAND ----------

## (8) write silver Table

## Path of Silver_Layer ... in ADLS Gen2
reviews_silver_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Order_Reviews/"

## Write this Table in Silver Layer Folder
df_order_reviews.write.mode("overwrite").format("delta").save(reviews_silver_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.silver.order_reviews
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Order_Reviews/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.silver.order_reviews").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(6)Orders_File

# COMMAND ----------

#(1)(2) Read Table & Schema Validation
import pyspark
df_orders = spark.table('Olist__Batch__Data.bronze.orders')
df_orders.show(5)

# COMMAND ----------

### validation step is done , compere with the schema , and outo loader will take all columns in right way , _rescued_data == null , then it not need to be in the table
df_orders = df_orders.drop("_rescued_data")
df_orders.show(2)

# COMMAND ----------

num_of_orders = df_orders.count()
print(f"Number of orders is: {num_of_orders}")

# COMMAND ----------

# (3)Data Type Casting
# Check data types
df_orders.printSchema()

# COMMAND ----------

### Edit columns : type cast dates to "timestamp"
df_orders = df_orders.withColumn("order_purchase_timestamp", df_orders["order_purchase_timestamp"].cast("timestamp"))
df_orders = df_orders.withColumn("order_approved_at", df_orders["order_approved_at"].cast("timestamp"))
df_orders = df_orders.withColumn("order_delivered_carrier_date", df_orders["order_delivered_carrier_date"].cast("timestamp"))
df_orders = df_orders.withColumn("order_delivered_customer_date", df_orders["order_delivered_customer_date"].cast("timestamp"))
df_orders = df_orders.withColumn("order_estimated_delivery_date", df_orders["order_estimated_delivery_date"].cast("timestamp"))

df_orders.printSchema()

# COMMAND ----------

## (4)Remove Duplicates

# Check Number Of Duplicates
num_of_dup_orders = df_orders.count() - df_orders.dropDuplicates().count()
print(num_of_dup_orders)

# COMMAND ----------

### I know we don't have a duplicates but we will do it, Because the project run all files on "friday", if data come have duplicates we will delete it
# Delete Duplicates
df_orders = df_orders.dropDuplicates()
df_orders.count()

# COMMAND ----------

## (5)Handle missing value (Remove or Handel it)

# check number of nulls (Null Profiling)
import pyspark.sql.functions as F
nulls_df_orders = df_orders.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_orders.columns
])

display(nulls_df_orders)

# COMMAND ----------

df_orders.select("order_status").distinct().show()

# COMMAND ----------

### Now We Want to know if the null values in 'order_approved_at' , 'order_delivered_carrier_date' , 'order_delivered_customer_date' 
## the status == processing , canceled , unavailable , created  -->> i think that row 'nulls is accept 

df_orders.select("order_status", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date").filter(
    (col("order_status") == "processing") |
    (col("order_status") == "canceled") |
    (col("order_status") == "unavailable") |
    (col("order_status") == "created")
).show()

# COMMAND ----------

### NOTE: It is perfectly normal for delivery timestamps to be NULL if the order is still "processing" or "shipped". 
### We will ONLY drop rows if the primary identifiers are missing.

# Delete Nulls on critical columns only
df_orders = df_orders.dropna(subset=["order_id", "customer_id", "order_status"])
df_orders.count()

# COMMAND ----------

####(6) Data Standardization

###(1) check if data have space in the string columns
from pyspark.sql.functions import col, trim

string_columns = [
    "order_id",
    "customer_id",
    "order_status"
]

# COMMAND ----------

df_orders_check = df_orders

for column in string_columns:
    df_orders_check = df_orders_check.withColumn(
        column,
        trim(col(column))
    )

# COMMAND ----------

from pyspark.sql.functions import col, trim

df_orders.filter(
    (col("order_id") != trim(col("order_id"))) |
    (col("customer_id") != trim(col("customer_id"))) |
    (col("order_status") != trim(col("order_status")))
).show(truncate=False)

# COMMAND ----------

###(2) Normalize categorical values
from pyspark.sql.functions import col, lower, trim, upper
# Standardize order_status (e.g., 'delivered' -> 'DELIVERED')
df_orders = df_orders.withColumn("order_status", upper(trim(col("order_status"))))
df_orders.show()

# COMMAND ----------

##(7) Data Quality Checks & Logic

# --> Understand The Column --> "order_status" What Is The Value Have
unique_status = df_orders.select("order_status").distinct()
unique_status.show()

# COMMAND ----------

from pyspark.sql.functions import col

##(1)order_id
invalid_order_id = (df_orders.filter(col("order_id").isNull() | (trim(col("order_id")) == "")))
invalid_order_id.display()

##(2)customer_id
invalid_customer_id = (df_orders.filter(col("customer_id").isNull() | (trim(col("customer_id")) == "")))
invalid_customer_id.display()

##(3) Logical Date Anomaly (Delivered before purchased)
invalid_timeline = df_orders.filter(col("order_delivered_customer_date") < col("order_purchase_timestamp"))
invalid_timeline.display()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Save all errors in Quarantine Table
invalid_orders = (
    df_orders.filter(
        # Order ID & Customer ID Checks
        (col("order_id").isNull()) | (trim(col("order_id")) == "") |
        (col("customer_id").isNull()) | (trim(col("customer_id")) == "") |
        
        # Order Status
        (col("order_status").isNull()) | (trim(col("order_status")) == "") |
        
        # Temporal Logic Failure
        (col("order_delivered_customer_date") < col("order_purchase_timestamp")) |
        
        # Operational Logic: Ghost Deliveries & Unapproved Deliveries
        ((col("order_status") == "DELIVERED") & col("order_delivered_customer_date").isNull()) |
        ((col("order_status") == "DELIVERED") & col("order_approved_at").isNull())
    )
)
display(invalid_orders)

# COMMAND ----------

from pyspark.sql.functions import (col,trim,when,lit,concat_ws)

invalid_orders = (
    invalid_orders.withColumn(
        "error_reason",
        concat_ws(
            " | ",

            when(col("order_id").isNull(),
                 lit("Order ID is NULL")),

            when(trim(col("order_id")) == "",
                 lit("Order ID is Empty")),

            when(col("customer_id").isNull(),
                 lit("Customer ID is NULL")),

            when(trim(col("customer_id")) == "",
                 lit("Customer ID is Empty")),
                 
            when(col("order_status").isNull(),
                 lit("Order Status is NULL")),

            when(trim(col("order_status")) == "",
                 lit("Order Status is Empty")),

            when(col("order_delivered_customer_date") < col("order_purchase_timestamp"),
                 lit("Temporal Anomaly: Delivered before Purchase Date")),
            
            when((col("order_status") == "DELIVERED") & col("order_delivered_customer_date").isNull(),
                 lit("Ghost Delivery: Status is Delivered but missing delivery date")),
                 
            when((col("order_status") == "DELIVERED") & col("order_approved_at").isNull(),
                 lit("Unapproved Delivery: Status is Delivered but missing payment approval date"))
        )
    )
)

display(invalid_orders)

# COMMAND ----------

## Save Quarantine Table
quarantine_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Orders/"

(
    invalid_orders.write.format("delta").mode("append").option("overwriteSchema", "true").save(quarantine_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Olist__Batch__Data.quarantine.orders
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Orders/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.quarantine.orders").show()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Keep only valid Order ID
df_orders = df_orders.filter(col("order_id").isNotNull() & (trim(col("order_id")) != ""))

### Keep only valid Customer ID
df_orders = df_orders.filter(col("customer_id").isNotNull() & (trim(col("customer_id")) != ""))

### Keep only valid Order Status
df_orders = df_orders.filter(col("order_status").isNotNull() & (trim(col("order_status")) != ""))

### Keep only records with valid physical timelines (or where delivery date is still safely null)
df_orders = df_orders.filter(
    col("order_delivered_customer_date").isNull() | 
    (col("order_delivered_customer_date") >= col("order_purchase_timestamp"))
)

### Keep only records with valid physical timelines
df_orders = df_orders.filter(
    col("order_delivered_customer_date").isNull() | 
    (col("order_delivered_customer_date") >= col("order_purchase_timestamp"))
)

### Remove Ghost & Unapproved Deliveries
df_orders = df_orders.filter(
    ~((col("order_status") == "DELIVERED") & col("order_delivered_customer_date").isNull()) &
    ~((col("order_status") == "DELIVERED") & col("order_approved_at").isNull())
)


print("Valid Orders:", df_orders.count())

# COMMAND ----------

## (8) write silver Table

## Path of Silver_Layer ... in ADLS Gen2
orders_silver_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Orders/"

## Write this Table in Silver Layer Folder
df_orders.write.mode("overwrite").format("delta").save(orders_silver_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.silver.orders
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Orders/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.silver.orders").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # (7)Products_File

# COMMAND ----------

#(1)(2) Read Table & Schema Validation
import pyspark
df_products = spark.table('Olist__Batch__Data.bronze.products')
df_products.show(5)

# COMMAND ----------

### validation step is done , compere with the schema , and outo loader will take all columns in right way , _rescued_data == null , then it not need to be in the table
df_products = df_products.drop("_rescued_data")
df_products.show(2)
num_of_prod = df_products.count()
print(f"Number of products is: {num_of_prod}")

# COMMAND ----------

#(3) Data Type Casting
df_products.printSchema()

# COMMAND ----------

### Edit physical and metadata columns : type cast to "int"
from pyspark.sql.functions import col

columns_to_cast = [
    "product_name_lenght", "product_description_lenght", "product_photos_qty", 
    "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
]

for c in columns_to_cast:
    df_products = df_products.withColumn(c, col(c).cast("int"))

df_products.printSchema()

# COMMAND ----------

## (4)Remove Duplicates

# Check Number Of Duplicates
num_of_dup_prod = df_products.count() - df_products.dropDuplicates().count()
print(num_of_dup_prod)

# COMMAND ----------

### I know we don't have a duplicates but we will do it, Because the project run all files in "friday", if data come have duplicates we will delete it
# Delete Duplicates
df_products = df_products.dropDuplicates()
df_products.count()

# COMMAND ----------

## (5)Handle missing value (Remove or Handle it)

# check number of nulls (Null Profiling)
import pyspark.sql.functions as F
nulls_df_prod = df_products.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_products.columns
])

display(nulls_df_prod)

# COMMAND ----------

### ==============================================================================
### APPROACH: Imputation Over Deletion
###    Metadata & Categories: Filled with '0' or 'Unknown'. 
###    Why? To preserve referential integrity. Dropping these rows would cause 
###    downstream sales records to fail their joins, resulting in lost revenue data.
### ==============================================================================
#  Fill missing Categorical and Metadata values (The 610 records)
df_products = df_products.fillna({
    "product_category_name": "Unknown",
    "product_name_lenght": 0,
    "product_description_lenght": 0,
    "product_photos_qty": 0
})

# COMMAND ----------

##(6) Data Standardization

###(1) check if data have space in the string columns
from pyspark.sql.functions import col, trim, regexp_replace, initcap

string_columns = [
    "product_id",
    "product_category_name"
]

# COMMAND ----------

df_products_check = df_products

for column in string_columns:
    df_products_check = df_products_check.withColumn(
        column,
        trim(col(column))
    )

# COMMAND ----------

df_products.filter(
    (col("product_id") != trim(col("product_id"))) |
    (col("product_category_name") != trim(col("product_category_name")))
).show(truncate=False)

# COMMAND ----------

###(2) Normalize categorical values (Remove underscores and Title Case categories)
df_products = df_products.withColumn(
    "product_category_name", 
    lower(initcap(regexp_replace(trim(col("product_category_name")), "_", " ")))
)
df_products.show()

# COMMAND ----------

##(7) Data Quality Checks & Logic

# --> Understand The Column --> "product_category_name" What Is The Value Have
unique_categories = df_products.select("product_category_name").distinct()
unique_categories.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col

##(1) Physical Dimensions (must be strictly > 0)
invalid_product_dimensions = df_products.filter(
    (col("product_weight_g") <= 0) | 
    (col("product_length_cm") <= 0) | 
    (col("product_height_cm") <= 0) | 
    (col("product_width_cm") <= 0)
)
invalid_product_dimensions.display()

# COMMAND ----------

from pyspark.sql.functions import col, mean, round, when
### ==============================================================================
###    APPROACH: Imputation Over Deletion
###    Imputed using the average of the same category.
###    Why? A weight of '0' skews logistics/freight calculations, and dropping 
###    the row loses the product entirely. Category average is the safest estimate.
### ==============================================================================
# 1. Calculate the average weight for each category (ONLY looking at valid weights > 0)
category_avg_weights = df_products.filter(col("product_weight_g") > 0) \
    .groupBy("product_category_name") \
    .agg(round(mean("product_weight_g")).alias("category_avg_weight"))

# 2. Join the averages back to the main DataFrame
df_products = df_products.join(category_avg_weights, on="product_category_name", how="left")

# 3. Replace weights that are <= 0 with the calculated category average
df_products = df_products.withColumn(
    "product_weight_g",
    when(
        (col("product_weight_g").isNull()) | (col("product_weight_g") <= 0), 
        col("category_avg_weight")
    ).otherwise(col("product_weight_g"))
)

# 4. Drop the temporary average column to keep the schema clean
df_products = df_products.drop("category_avg_weight")



# COMMAND ----------

##(2) product_id
invalid_product_id = (df_products.filter(col("product_id").rlike("^[0-9]+$")))
invalid_product_id.display()

# COMMAND ----------

##(3) Metadata limits (must be >= 0)
invalid_product_metadata = df_products.filter(
    (col("product_name_lenght") < 0) | 
    (col("product_description_lenght") < 0) | 
    (col("product_photos_qty") < 0)
)
invalid_product_metadata.display()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Save all errors in Quarantine Table
invalid_products = (
    df_products.filter(
        # Product ID
        (col("product_id").isNull()) |
        (trim(col("product_id")) == "") |
        (col("product_id").rlike("^[0-9]+$")) |

        # Category Name
        (col("product_category_name").isNull()) |
        (trim(col("product_category_name")) == "") |

        # Physical Dimensions
        (col("product_weight_g") <= 0) |
        (col("product_length_cm") <= 0) |
        (col("product_height_cm") <= 0) |
        (col("product_width_cm") <= 0) |

        # Metadata
        (col("product_name_lenght") < 0) |
        (col("product_description_lenght") < 0) |
        (col("product_photos_qty") < 0)
    )
)

display(invalid_products)

# COMMAND ----------

from pyspark.sql.functions import (col,trim,when,lit,concat_ws)

invalid_products = (
    invalid_products.withColumn(
        "error_reason",
        concat_ws(
            " | ",
            
            when(col("product_id").isNull(),
                 lit("Product ID is NULL")),
                 
            when(trim(col("product_id")) == "",
                 lit("Product ID is Empty")),
                 
            when(col("product_id").rlike("^[0-9]+$"),
                 lit("Product ID contains only numbers")),

            when(col("product_category_name").isNull() | (trim(col("product_category_name")) == ""),
                 lit("Product Category is NULL or Empty")),

            when(col("product_weight_g") <= 0,
                 lit("Zero or Negative Product Weight")),

            when(col("product_length_cm") <= 0,
                 lit("Zero or Negative Product Length")),

            when(col("product_height_cm") <= 0,
                 lit("Zero or Negative Product Height")),

            when(col("product_width_cm") <= 0,
                 lit("Zero or Negative Product Width")),

            when((col("product_name_lenght") < 0) | (col("product_description_lenght") < 0) | (col("product_photos_qty") < 0),
                 lit("Negative Metadata lengths/qty"))
        )
    )
)
display(invalid_products)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

invalid_products = (
    invalid_products
    .withColumn("quarantine_timestamp", current_timestamp())
    .withColumn("source_table", lit("products"))
)

# COMMAND ----------

## Save Quarantine Table
quarantine_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Products/"

(
    invalid_products.write.format("delta").mode("append").option("overwriteSchema", "true").save(quarantine_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Olist__Batch__Data.quarantine.products
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Products/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.quarantine.products").show()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Keep only valid Product ID
df_products = df_products.filter(
    col("product_id").isNotNull() & 
    (trim(col("product_id")) != "") & 
    (~col("product_id").rlike("^[0-9]+$"))
)

### Keep only valid Category Name
df_products = df_products.filter(
    col("product_category_name").isNotNull() & 
    (trim(col("product_category_name")) != "")
)

### Keep only valid Dimensions
df_products = df_products.filter(
    (col("product_weight_g") > 0) &
    (col("product_length_cm") > 0) &
    (col("product_height_cm") > 0) &
    (col("product_width_cm") > 0)
)

### Keep only valid Metadata
df_products = df_products.filter(
    (col("product_name_lenght") >= 0) &
    (col("product_description_lenght") >= 0) &
    (col("product_photos_qty") >= 0)
)

print("Valid Products:", df_products.count())

# COMMAND ----------

## (8) write silver Table

## Path of Silver_Layer ... in ADLS Gen2
prod_silver_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Products/"

## Write this Table in Silver Layer Folder
df_products.write.mode("overwrite").format("delta").save(prod_silver_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.silver.products
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Products/';

# COMMAND ----------

    
spark.read.table("Olist__Batch__Data.silver.products").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # (8)Sellers_File

# COMMAND ----------

#(1)(2) Read Table & Schema Validation
import pyspark
df_sellers = spark.table('Olist__Batch__Data.bronze.sellers')
df_sellers.show(5)

# COMMAND ----------

### validation step is done , compere with the schema , and outo loader will take all columns in right way , _rescued_data == null , then it not need to be in the table
df_sellers = df_sellers.drop("_rescued_data")
df_sellers.show(2)
num_of_sel = df_sellers.count()
f"Number of sellers is: {num_of_sel}"

# COMMAND ----------

#(3) Data Type Casting
df_sellers.printSchema()

# COMMAND ----------

### Edit "seller_zip_code_prefix" : type cast to "int"
df_sellers = df_sellers.withColumn("seller_zip_code_prefix", df_sellers["seller_zip_code_prefix"].cast("int"))

# COMMAND ----------

df_sellers.printSchema()

# COMMAND ----------

## (4)Remove Duplicates

# Check Number Of Duplicates
num_of_dup_sel = df_sellers.count() - df_sellers.dropDuplicates().count()
print(num_of_dup_sel)

# COMMAND ----------

### I know we don't have a duplicates but we will do it, Because the project run all files in "friday", if data come have duplicates we will delete it
# Delete Duplicates
df_sellers = df_sellers.dropDuplicates()
df_sellers.count()

# COMMAND ----------

## (5)Handle missing value (Remove or Handle it)

# check number of nulls (Null Profiling)
import pyspark.sql.functions as F
nulls_df_sel = df_sellers.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_sellers.columns
])

display(nulls_df_sel)

# COMMAND ----------

### If data comes with nulls we will delete it also this is by default in the project , But if this project have human to monter the data we will handel it --->> this is recomended , because we don't know what is the null value

# Delete Nulls
df_sellers = df_sellers.dropna()
df_sellers.count()

# COMMAND ----------

##(6) Data Standardization

###(1) check if data have space in the string columns
from pyspark.sql.functions import col, trim

string_columns = [
    "seller_id",
    "seller_city",
    "seller_state"
]

# COMMAND ----------

df_sellers_check = df_sellers

for column in string_columns:
    df_sellers_check = df_sellers_check.withColumn(
        column,
        trim(col(column))
    )

# COMMAND ----------

from pyspark.sql.functions import col, trim

df_sellers.filter(
    (col("seller_id") != trim(col("seller_id"))) |
    (col("seller_city") != trim(col("seller_city"))) |
    (col("seller_state") != trim(col("seller_state")))
).show(truncate=False)

# COMMAND ----------

###(2) Normalize categorical values
from pyspark.sql.functions import col, lower, trim, upper
df_sellers = df_sellers.withColumn("seller_state", upper(trim(col("seller_state"))))
df_sellers.show()

# COMMAND ----------

##(7) Data Quality Checks & Logic

# --> Understand The Column --> "seller_state" What Is The Value Have
unique_states = df_sellers.select("seller_state").distinct()
unique_states.show()

# COMMAND ----------

# --> Understand The Column --> :"seller_city" what the value have
unique_cities = df_sellers.select("seller_city").distinct()
unique_cities.show()

# COMMAND ----------

from pyspark.sql.functions import col

# --> Define the 27 valid federative units of Brazil
valid_br_states = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", 
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", 
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]

### Update Quarantine Logic: Flag states not in the valid list
invalid_seller_state = (
    df_sellers.filter(
        col("seller_state").isNull() | 
        (trim(col("seller_state")) == "") | 
        (~col("seller_state").isin(valid_br_states))
    )
)

### Keep only valid Brazilian states for the Silver Table
df_sellers = df_sellers.filter(col("seller_state").isin(valid_br_states))

# COMMAND ----------

from pyspark.sql.functions import col, initcap, regexp_replace, translate

###  Standardize City Names for BI: Remove accents, fix extra spaces, and Title Case
df_sellers = df_sellers.withColumn(
    "seller_city",
    initcap(
        regexp_replace(
            translate(col("seller_city"), "áàãâäéèêëíìîïóòõôöúùûüç", "aaaaaeeeeiiiiooooouuuuc"),
            "\\s+", " " # Replace multiple spaces with a single space
        )
    )
)

# COMMAND ----------

from pyspark.sql.functions import col
##(1)seller_zip_code_prefix
invalid_seller_zip_code_prefix = df_sellers.filter(col("seller_zip_code_prefix") < 0)
invalid_seller_zip_code_prefix.display()

# COMMAND ----------

##(2)seller_state
invalid_seller_state = (df_sellers.filter(col("seller_state").isNull() | (trim(col("seller_state")) == "") | (~col("seller_state").rlike("^[A-Z]{2}$"))))
invalid_seller_state.display()

# COMMAND ----------

## (3) seller_id
invalid_seller_id = (df_sellers.filter(col("seller_id").isNull() | (trim(col("seller_id")) == "") | (~col("seller_id").rlike("^[a-fA-F0-9]{32}$"))))
invalid_seller_id.display()

# COMMAND ----------

## (4) seller_city
invalid_seller_city = (df_sellers.filter(col("seller_city").rlike("^[0-9]+$")))
invalid_seller_city.display()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Save all errors in Quarantine Table
invalid_sellers = (
    df_sellers.filter(
        (col("seller_zip_code_prefix") < 0) |

        # Seller ID
        (col("seller_id").isNull()) |
        (trim(col("seller_id")) == "") |
        (~col("seller_id").rlike("^[a-fA-F0-9]{32}$")) |

        # Seller City
        (col("seller_city").isNull()) |
        (trim(col("seller_city")) == "") |
        (col("seller_city").rlike("^[0-9]+$")) |

        # Seller State
        (col("seller_state").isNull()) |
        (trim(col("seller_state")) == "") |
        (~col("seller_state").rlike("^[A-Z]{2}$"))
    )
)

# COMMAND ----------

display(invalid_sellers)

# COMMAND ----------

from pyspark.sql.functions import (col,trim,when,lit,concat_ws)

invalid_sellers = (
    invalid_sellers.withColumn(
        "error_reason",
        concat_ws(
            " | ",

            when(col("seller_zip_code_prefix") < 0,
                 lit("Negative ZIP Code")),

            when(col("seller_id").isNull(),
                 lit("Seller ID is NULL")),

            when(trim(col("seller_id")) == "",
                 lit("Seller ID is Empty")),

            when(~col("seller_id").rlike("^[a-fA-F0-9]{32}$"),
                 lit("Seller ID is not a valid 32-char hex string")),

            when(col("seller_city").isNull(),
                 lit("Seller City is NULL")),

            when(trim(col("seller_city")) == "",
                 lit("Seller City is Empty")),

            when(col("seller_city").rlike("^[0-9]+$"),
                 lit("Seller City contains only numbers")),

            when(col("seller_state").isNull(),
                 lit("Seller State is NULL")),

            when(trim(col("seller_state")) == "",
                 lit("Seller State is Empty")),

            when(~col("seller_state").rlike("^[A-Z]{2}$"),
                 lit("Seller State is not exactly 2 uppercase letters"))
        )
    )
)
display(invalid_sellers)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

invalid_sellers = (
    invalid_sellers
    .withColumn("quarantine_timestamp", current_timestamp())
    .withColumn("source_table", lit("sellers"))
)

# COMMAND ----------

## Save Quarantine Table
quarantine_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Sellers/"

(
    invalid_sellers.write.format("delta").mode("append").option("overwriteSchema", "true").save(quarantine_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Olist__Batch__Data.quarantine.sellers
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/Sellers/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.quarantine.sellers").show()

# COMMAND ----------

from pyspark.sql.functions import col, trim

### Keep only valid ZIP Code
df_sellers = df_sellers.filter(col("seller_zip_code_prefix") >= 0)

### Keep only valid Seller State (Must be exactly 2 characters)
df_sellers = df_sellers.filter(col("seller_state").isNotNull() & (trim(col("seller_state")) != "") & (col("seller_state").rlike("^[A-Z]{2}$")))

### Keep only valid Seller ID (Must be 32-character hex)
df_sellers = df_sellers.filter(col("seller_id").isNotNull() & (trim(col("seller_id")) != "") & (col("seller_id").rlike("^[a-fA-F0-9]{32}$")))

### Keep only valid Seller City
df_sellers = df_sellers.filter(col("seller_city").isNotNull() & (trim(col("seller_city")) != "") & (~col("seller_city").rlike("^[0-9]+$")))

print("Valid Sellers:", df_sellers.count())

# COMMAND ----------

## (8) write silver Table

## Path of Silver_Layer ... in ADLS Gen2
sel_silver_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Sellers/"

## Write this Table in Silver Layer Folder
df_sellers.write.mode("overwrite").format("delta").save(sel_silver_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.silver.sellers
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/Sellers/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.silver.sellers").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # (9)Category_Name_Translation_File

# COMMAND ----------

#(1)(2) Read Table & Schema Validation
import pyspark
df_category = spark.table('Olist__Batch__Data.bronze.category_name_translation')
df_category.show(5)

# COMMAND ----------

### validation step is done , compere with the schema , and auto loader will take all columns in right way , _rescued_data == null , then it not need to be in the table
df_category = df_category.drop("_rescued_data")
df_category.show(2)

# COMMAND ----------

num_of_cat = df_category.count()
f"Number of categories is: {num_of_cat}"

# COMMAND ----------

#(3) Data Type Casting
### Both columns "product_category_name" and "product_category_name_english" should naturally be inferred as strings.
### No explicit casting needed here, but we verify the schema.
df_category.printSchema()

# COMMAND ----------

## (4)Remove Duplicates

# Check Number Of Duplicates
num_of_dup_cat = df_category.count() - df_category.dropDuplicates().count()
print(num_of_dup_cat)

# COMMAND ----------

### I know we don't have a duplicates but we will do it, Because the project run all files in "friday", if data come have duplicates we will delete it
# Delete Duplicates
df_category = df_category.dropDuplicates()
df_category.count()

# COMMAND ----------

## (5)Handle missing value (Remove or Handle it)

# check number of nulls (Null Profiling)
import pyspark.sql.functions as F
nulls_df_cat = df_category.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_category.columns
])

display(nulls_df_cat)

# COMMAND ----------

### If data comes with nulls we will delete it also this is by default in the project , But if this project have human to monter the data we will handel it --->> this is recomended , because we don't know what is the null value

# Delete Nulls
df_category = df_category.dropna()
df_category.count()

# COMMAND ----------

##(6) Data Standardization

###(1) check if data have space in the string columns
from pyspark.sql.functions import col, trim, lower, regexp_replace

string_columns = [
    "product_category_name",
    "product_category_name_english"
]

# COMMAND ----------

df_category_check = df_category

for column in string_columns:
    df_category_check = df_category_check.withColumn(
        column,
        trim(col(column))
    )

# COMMAND ----------

df_category.filter(
    (col("product_category_name") != trim(col("product_category_name"))) |
    (col("product_category_name_english") != trim(col("product_category_name_english")))
).show(truncate=False)

# COMMAND ----------

###(2) Normalize categorical values (Standardize formats to lowercase and replace spaces/hyphens with underscores)
from pyspark.sql.functions import lower, trim, regexp_replace, col

df_category = df_category.withColumn(
    "product_category_name",
    lower(regexp_replace(trim(col("product_category_name")), "_", " "))
)

df_category = df_category.withColumn(
    "product_category_name_english",
    lower(regexp_replace(trim(col("product_category_name_english")), "_", " "))
)
df_category.show()

# COMMAND ----------

##(7) Data Quality Checks & Logic

# --> Understand The Column --> "product_category_name" What Is The Value Have
unique_cat_pt = df_category.select("product_category_name").distinct()
unique_cat_pt.show()

# COMMAND ----------

# --> Understand The Column --> :"product_category_name_english" what the value have
unique_cat_en = df_category.select("product_category_name_english").distinct()
unique_cat_en.show()

# COMMAND ----------

from pyspark.sql.functions import col, length

## (1) product_category_name
invalid_cat_pt = (df_category.filter(
    col("product_category_name").isNull() | 
    (trim(col("product_category_name")) == "") | 
    (col("product_category_name").rlike("^[0-9]+$")) |
    (length(col("product_category_name")) < 3)
))
invalid_cat_pt.display()

# COMMAND ----------

## (2) product_category_name_english
invalid_cat_en = (df_category.filter(
    col("product_category_name_english").isNull() | 
    (trim(col("product_category_name_english")) == "") | 
    (col("product_category_name_english").rlike("^[0-9]+$")) |
    (length(col("product_category_name_english")) < 3)
))
invalid_cat_en.display()

# COMMAND ----------

### Save all errors in Quarantine Table
invalid_categories = (
    df_category.filter(
        # Portuguese Name
        (col("product_category_name").isNull()) |
        (trim(col("product_category_name")) == "") |
        (col("product_category_name").rlike("^[0-9]+$")) |
        (length(col("product_category_name")) < 3) |

        # English Name
        (col("product_category_name_english").isNull()) |
        (trim(col("product_category_name_english")) == "") |
        (col("product_category_name_english").rlike("^[0-9]+$")) |
        (length(col("product_category_name_english")) < 3)
    )
)

display(invalid_categories)

# COMMAND ----------

from pyspark.sql.functions import (col,trim,when,lit,concat_ws,length)

invalid_categories = (
    invalid_categories.withColumn(
        "error_reason",
        concat_ws(
            " | ",

            when(col("product_category_name").isNull(),
                 lit("PT Category Name is NULL")),

            when(trim(col("product_category_name")) == "",
                 lit("PT Category Name is Empty")),

            when(col("product_category_name").rlike("^[0-9]+$"),
                 lit("PT Category Name contains only numbers")),

            when(length(col("product_category_name")) < 3,
                 lit("PT Category Name is too short (junk data)")),

            when(col("product_category_name_english").isNull(),
                 lit("EN Category Name is NULL")),

            when(trim(col("product_category_name_english")) == "",
                 lit("EN Category Name is Empty")),

            when(col("product_category_name_english").rlike("^[0-9]+$"),
                 lit("EN Category Name contains only numbers")),

            when(length(col("product_category_name_english")) < 3,
                 lit("EN Category Name is too short (junk data)"))
        )
    )
)
display(invalid_categories)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

invalid_categories = (
    invalid_categories
    .withColumn("quarantine_timestamp", current_timestamp())
    .withColumn("source_table", lit("category_translation"))
)

# COMMAND ----------

## Save Quarantine Table
quarantine_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/CategoryTranslation/"

(
    invalid_categories.write.format("delta").mode("append").option("overwriteSchema", "true").save(quarantine_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Olist__Batch__Data.quarantine.category_translation
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Quarantine/CategoryTranslation/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.quarantine.category_translation").show()

# COMMAND ----------

from pyspark.sql.functions import col, trim, length

### Keep only valid Portuguese Category Name
df_category = df_category.filter(
    col("product_category_name").isNotNull() & 
    (trim(col("product_category_name")) != "") & 
    (~col("product_category_name").rlike("^[0-9]+$")) &
    (length(col("product_category_name")) >= 3)
)

### Keep only valid English Category Name
df_category = df_category.filter(
    col("product_category_name_english").isNotNull() & 
    (trim(col("product_category_name_english")) != "") & 
    (~col("product_category_name_english").rlike("^[0-9]+$")) &
    (length(col("product_category_name_english")) >= 3)
)

print("Valid Categories:", df_category.count())

# COMMAND ----------

## (8) write silver Table

## Path of Silver_Layer ... in ADLS Gen2
cat_silver_path = "abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/CategoryTranslation/"

## Write this Table in Silver Layer Folder
df_category.write.mode("overwrite").format("delta").save(cat_silver_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Olist__Batch__Data.silver.category_translation
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://olist@olistprojectdatalake.dfs.core.windows.net/Silver_Layer/CategoryTranslation/';

# COMMAND ----------

spark.read.table("Olist__Batch__Data.silver.category_translation").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #(10) Test All Files Done (Sliver Tables) !!

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in Olist__Batch__Data.silver;

# COMMAND ----------

