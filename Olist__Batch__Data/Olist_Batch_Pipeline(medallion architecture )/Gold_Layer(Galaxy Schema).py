# Databricks notebook source
# MAGIC %md
# MAGIC # Why We Choose a Galaxy Schema
# MAGIC
# MAGIC Before designing the Gold Layer, we evaluated both the **Star Schema** and the **Galaxy Schema (Fact Constellation)**.
# MAGIC
# MAGIC ## Why Galaxy Schema?
# MAGIC
# MAGIC A Galaxy Schema organizes the data warehouse around **business processes** rather than placing everything in one large Fact Table.
# MAGIC
# MAGIC Each business process has its own Fact Table, while all Fact Tables share the same Dimension Tables.
# MAGIC
# MAGIC For example:
# MAGIC
# MAGIC - **Fact_Orders**
# MAGIC - **Fact_Payments**
# MAGIC - **Fact_Reviews**
# MAGIC
# MAGIC All of them share common dimensions such as:
# MAGIC
# MAGIC - **Dim_Customers**
# MAGIC - **Dim_Products**
# MAGIC - **Dim_Sellers**
# MAGIC - **dim_order**
# MAGIC - **Dim_Date**
# MAGIC
# MAGIC ## Business Benefits
# MAGIC
# MAGIC - **Better Organization**
# MAGIC   - Each business process has its own independent Fact Table.
# MAGIC   - Similar to organizing files in **ADLS Gen2**, where data is separated into folders (Raw, Bronze, Silver, Gold) instead of storing everything in one place.
# MAGIC
# MAGIC - **Scalability**
# MAGIC   - As the business grows, new business processes can be added by creating new Fact Tables without redesigning the existing warehouse.
# MAGIC   - Example:
# MAGIC     - Today: Orders, Payments, Reviews
# MAGIC     - Tomorrow: Returns, Inventory, Shipping
# MAGIC
# MAGIC - **Independent Development**
# MAGIC   - Each Fact Table represents one business domain.
# MAGIC   - Changes to the Payments process do not affect Orders or Reviews.
# MAGIC
# MAGIC - **Easier Maintenance**
# MAGIC   - Bugs or business logic changes are isolated within one Fact Table.
# MAGIC   - This reduces the risk of breaking other analytical processes.
# MAGIC
# MAGIC - **Reusable Dimensions**
# MAGIC   - Dimension tables are shared across multiple Fact Tables.
# MAGIC   - This eliminates duplication and keeps the warehouse consistent.
# MAGIC
# MAGIC ## Trade-offs
# MAGIC
# MAGIC The main disadvantage of a Galaxy Schema is that analytical queries may require **more joins** because data is distributed across multiple Fact Tables.
# MAGIC
# MAGIC However, modern Data Warehouse technologies such as **Databricks**, **Delta Lake**, and **Apache Spark** are designed to efficiently process these joins, making the scalability and maintainability benefits outweigh this drawback in enterprise environments.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![Schema Galaxy (DWH)](/Volumes/olist_batch_data/image_schema/image/olist_gold_galaxy_schema_drawio__version__2.drawio.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##(1)Dim Customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_batch_data.gold.dim_customers AS
# MAGIC
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_unique_id,
# MAGIC     customer_city,
# MAGIC     customer_state
# MAGIC FROM olist_batch_data.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *FROM olist_batch_data.gold.dim_customers LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ##(2)Dim_products

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_batch_data.gold.dim_products AS
# MAGIC
# MAGIC SELECT
# MAGIC     p.product_id,
# MAGIC     p.product_category_name,
# MAGIC     c.product_category_name_english,
# MAGIC     p.product_name_lenght,
# MAGIC     p.product_description_lenght,
# MAGIC     p.product_photos_qty,
# MAGIC     p.product_weight_g,
# MAGIC     p.product_length_cm,
# MAGIC     p.product_height_cm,
# MAGIC     p.product_width_cm
# MAGIC
# MAGIC FROM olist_batch_data.silver.products p
# MAGIC
# MAGIC LEFT JOIN olist_batch_data.silver.category_translation c
# MAGIC ON p.product_category_name = c.product_category_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ Lesson Learned
# MAGIC ****If you're reading my notebook, be careful with this small issue****
# MAGIC
# MAGIC While building the Gold Layer, my join returned **NULL** for `product_category_name_english`.
# MAGIC
# MAGIC The issue was not in the Gold Layer—it originated in the Silver Layer.
# MAGIC
# MAGIC A small transformation changed the business key from:
# MAGIC
# MAGIC ```text
# MAGIC esporte_lazer
# MAGIC ```
# MAGIC
# MAGIC to
# MAGIC
# MAGIC ```text
# MAGIC Esporte Lazer
# MAGIC ```
# MAGIC
# MAGIC Since:
# MAGIC
# MAGIC ```text
# MAGIC esporte_lazer ≠ Esporte Lazer
# MAGIC ```
# MAGIC
# MAGIC the join failed and returned `NULL`.
# MAGIC
# MAGIC Instead of fixing the problem in the Gold Layer, I went back to the Silver Layer and restored the original business key. This is the correct approach because the **Gold Layer should focus only on business reporting and analytics**, not on fixing data quality or key mismatches.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *FROM olist_batch_data.gold.dim_products LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##(3)Dim_sellers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_batch_data.gold.dim_sellers AS
# MAGIC
# MAGIC SELECT
# MAGIC     seller_id,
# MAGIC     seller_zip_code_prefix,
# MAGIC     seller_city,
# MAGIC     seller_state
# MAGIC
# MAGIC FROM olist_batch_data.silver.sellers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olist_batch_data.gold.dim_sellers;

# COMMAND ----------

# MAGIC %md
# MAGIC ##(4)Dim_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_batch_data.gold.dim_orders AS
# MAGIC
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_status,
# MAGIC     order_purchase_timestamp,
# MAGIC     order_approved_at,
# MAGIC     order_delivered_carrier_date,
# MAGIC     order_delivered_customer_date,
# MAGIC     order_estimated_delivery_date
# MAGIC
# MAGIC FROM olist_batch_data.silver.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olist_batch_data.gold.dim_orders limit 2;

# COMMAND ----------

# MAGIC %md
# MAGIC ##(5)Dim_date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_batch_data.gold.dim_date AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC
# MAGIC     CAST(date_format(order_purchase_timestamp, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC
# MAGIC     DATE(order_purchase_timestamp) AS full_date,
# MAGIC
# MAGIC     YEAR(order_purchase_timestamp) AS year,
# MAGIC
# MAGIC     QUARTER(order_purchase_timestamp) AS quarter,
# MAGIC
# MAGIC     MONTH(order_purchase_timestamp) AS month,
# MAGIC
# MAGIC     date_format(order_purchase_timestamp, 'MMMM') AS month_name,
# MAGIC
# MAGIC     WEEKOFYEAR(order_purchase_timestamp) AS week,
# MAGIC
# MAGIC     DAY(order_purchase_timestamp) AS day,
# MAGIC
# MAGIC     date_format(order_purchase_timestamp, 'EEEE') AS day_name,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN dayofweek(order_purchase_timestamp) IN (1,7)
# MAGIC         THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_weekend
# MAGIC
# MAGIC FROM olist_batch_data.silver.orders
# MAGIC
# MAGIC WHERE order_purchase_timestamp IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olist_batch_data.gold.dim_date limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *FROM olist_batch_data.gold.dim_date
# MAGIC ORDER BY full_date;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##(1) Fact_Orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_batch_data.gold.fact_orders AS
# MAGIC
# MAGIC SELECT
# MAGIC     oi.order_item_id,
# MAGIC     oi.order_id,
# MAGIC     o.customer_id,
# MAGIC     oi.product_id,
# MAGIC     oi.seller_id,
# MAGIC
# MAGIC     CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC
# MAGIC     oi.price,
# MAGIC     oi.freight_value
# MAGIC
# MAGIC FROM olist_batch_data.silver.order_items AS oi
# MAGIC
# MAGIC INNER JOIN olist_batch_data.silver.orders AS o
# MAGIC     ON oi.order_id = o.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olist_batch_data.gold.fact_orders limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.Total Revenue KPI

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(price) AS total_revenue
# MAGIC FROM olist_batch_data.gold.fact_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.Total Freight Cost (Card)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(freight_value) AS total_freight
# MAGIC FROM olist_batch_data.gold.fact_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ###3.Number of Order Items Sold (Card)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_items_sold
# MAGIC FROM olist_batch_data.gold.fact_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.Average Product Price (Card)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(AVG(price),2) AS average_price
# MAGIC FROM olist_batch_data.gold.fact_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ###5.Revenue by Month (Line Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     d.year,
# MAGIC     d.month_name,
# MAGIC
# MAGIC     SUM(f.price) AS revenue
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_orders f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_date d
# MAGIC ON f.date_key = d.date_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name
# MAGIC
# MAGIC ORDER BY
# MAGIC     d.year,
# MAGIC     d.month;

# COMMAND ----------

# MAGIC %md
# MAGIC ###6.Revenue by State (Chart / Filled Map)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     c.customer_state,
# MAGIC
# MAGIC     SUM(f.price) AS revenue
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_orders f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_customers c
# MAGIC ON f.customer_id = c.customer_id
# MAGIC
# MAGIC GROUP BY
# MAGIC     c.customer_state
# MAGIC
# MAGIC ORDER BY
# MAGIC     revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ###7.Top 10 Products(Horizontal Bar Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     p.product_category_name_english,
# MAGIC
# MAGIC     SUM(f.price) AS revenue
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_orders f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_products p
# MAGIC ON f.product_id = p.product_id
# MAGIC
# MAGIC GROUP BY
# MAGIC     p.product_category_name_english
# MAGIC
# MAGIC ORDER BY
# MAGIC     revenue DESC
# MAGIC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ###8.Top 10 Sellers (Bar Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     s.seller_city,
# MAGIC
# MAGIC     SUM(f.price) AS revenue
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_orders f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_sellers s
# MAGIC ON f.seller_id = s.seller_id
# MAGIC
# MAGIC GROUP BY
# MAGIC     s.seller_city
# MAGIC
# MAGIC ORDER BY
# MAGIC     revenue DESC
# MAGIC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ###9.Revenue by Weekday (Column Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     d.day_name,
# MAGIC
# MAGIC     SUM(f.price) AS revenue
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_orders f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_date d
# MAGIC ON f.date_key = d.date_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.day_name
# MAGIC
# MAGIC ORDER BY
# MAGIC     revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ###10.Weekend vs Weekday Sales (Pie Chart / Donut Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     d.is_weekend,
# MAGIC
# MAGIC     SUM(f.price) AS revenue
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_orders f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_date d
# MAGIC ON f.date_key = d.date_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.is_weekend;

# COMMAND ----------

# MAGIC %md
# MAGIC ##(2) fact_payments

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_batch_data.gold.fact_payments AS
# MAGIC
# MAGIC SELECT
# MAGIC     p.order_id,
# MAGIC     p.payment_sequential,
# MAGIC
# MAGIC     CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC
# MAGIC     p.payment_type,
# MAGIC     p.payment_installments,
# MAGIC     p.payment_value
# MAGIC
# MAGIC FROM olist_batch_data.silver.order_payments p
# MAGIC
# MAGIC INNER JOIN olist_batch_data.silver.orders o
# MAGIC     ON p.order_id = o.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olist_batch_data.gold.fact_payments limit 2;

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.Total Payment Value (KPI)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(SUM(payment_value), 2) AS total_payment_value
# MAGIC FROM olist_batch_data.gold.fact_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.Revenue by Payment Type (Bar Chart / Donut Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     payment_type,
# MAGIC     ROUND(SUM(payment_value), 2) AS total_revenue
# MAGIC FROM olist_batch_data.gold.fact_payments
# MAGIC GROUP BY payment_type
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ###3.Payment Trend by Month (Line Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC     ROUND(SUM(f.payment_value), 2) AS total_revenue
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_payments f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_date d
# MAGIC ON f.date_key = d.date_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name
# MAGIC
# MAGIC ORDER BY
# MAGIC     d.year,
# MAGIC     d.month;

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.Average Payment Value (KPI Card)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(AVG(payment_value), 2) AS average_payment
# MAGIC FROM olist_batch_data.gold.fact_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC ###5.Average Installments (KPI Card)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(AVG(payment_installments), 2) AS average_installments
# MAGIC FROM olist_batch_data.gold.fact_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC ###6.Installment Distribution (Column Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     payment_installments,
# MAGIC     COUNT(*) AS total_transactions
# MAGIC FROM olist_batch_data.gold.fact_payments
# MAGIC GROUP BY payment_installments
# MAGIC ORDER BY payment_installments;

# COMMAND ----------

# MAGIC %md
# MAGIC ###7.Top Payment Methods (Horizontal Bar Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     payment_type,
# MAGIC     COUNT(*) AS total_transactions
# MAGIC FROM olist_batch_data.gold.fact_payments
# MAGIC GROUP BY payment_type
# MAGIC ORDER BY total_transactions DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.Highest Payment Transactions (Table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     payment_type,
# MAGIC     payment_value
# MAGIC FROM olist_batch_data.gold.fact_payments
# MAGIC ORDER BY payment_value DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.Payment Method Share (Pie / Donut Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     payment_type,
# MAGIC     ROUND(SUM(payment_value), 2) AS revenue
# MAGIC FROM olist_batch_data.gold.fact_payments
# MAGIC GROUP BY payment_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.Weekend vs Weekday Payments (Donut Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.is_weekend,
# MAGIC     ROUND(SUM(f.payment_value), 2) AS total_revenue
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_payments f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_date d
# MAGIC ON f.date_key = d.date_key
# MAGIC
# MAGIC GROUP BY d.is_weekend;

# COMMAND ----------

# MAGIC %md
# MAGIC ## (3)Fact_Reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_batch_data.gold.fact_reviews AS
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     review_id,
# MAGIC
# MAGIC     order_id,
# MAGIC
# MAGIC     CAST(date_format(review_creation_date, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC
# MAGIC     review_score,
# MAGIC
# MAGIC     review_creation_date,
# MAGIC
# MAGIC     review_answer_timestamp
# MAGIC
# MAGIC FROM olist_batch_data.silver.order_reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olist_batch_data.gold.fact_reviews limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Average Review Score (KPI Card)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(AVG(review_score),2) AS average_review_score
# MAGIC FROM olist_batch_data.gold.fact_reviews;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Review Score Distribution(Column Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     review_score,
# MAGIC
# MAGIC     COUNT(*) AS total_reviews
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_reviews
# MAGIC
# MAGIC GROUP BY review_score
# MAGIC
# MAGIC ORDER BY review_score;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Reviews by Month (Line Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC
# MAGIC     COUNT(*) AS total_reviews
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_reviews f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_date d
# MAGIC ON f.date_key = d.date_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name
# MAGIC
# MAGIC ORDER BY
# MAGIC     d.year,
# MAGIC     d.month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Average Review Score by Month(Line Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC
# MAGIC     ROUND(AVG(f.review_score),2) AS average_review
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_reviews f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_date d
# MAGIC ON f.date_key = d.date_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name
# MAGIC
# MAGIC ORDER BY
# MAGIC     d.year,
# MAGIC     d.month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Weekend vs Weekday Reviews(Donut Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     d.is_weekend,
# MAGIC
# MAGIC     COUNT(*) AS total_reviews
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_reviews f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_date d
# MAGIC ON f.date_key = d.date_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.is_weekend;

# COMMAND ----------

# MAGIC %md
# MAGIC ###6. Top 10 Orders with Lowest Reviews (Table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     order_id,
# MAGIC
# MAGIC     review_score
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_reviews
# MAGIC
# MAGIC ORDER BY
# MAGIC     review_score ASC
# MAGIC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Top 10 Orders with Highest Reviews (Table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     order_id,
# MAGIC
# MAGIC     review_score
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_reviews
# MAGIC
# MAGIC ORDER BY
# MAGIC     review_score DESC
# MAGIC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Review Score Percentage (Pie / Donut Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     review_score,
# MAGIC
# MAGIC     COUNT(*) AS total_reviews
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_reviews
# MAGIC
# MAGIC GROUP BY
# MAGIC     review_score
# MAGIC
# MAGIC ORDER BY
# MAGIC     review_score;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Reviews by Day of Week (Bar Chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     d.day_name,
# MAGIC
# MAGIC     COUNT(*) AS total_reviews
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_reviews f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_date d
# MAGIC ON f.date_key = d.date_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.day_name
# MAGIC
# MAGIC ORDER BY
# MAGIC     total_reviews DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Monthly Average Review Trend (Combo Chart (Line + Column))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC     d.year,
# MAGIC     d.month_name,
# MAGIC
# MAGIC     ROUND(AVG(review_score),2) AS average_score,
# MAGIC
# MAGIC     COUNT(*) AS total_reviews
# MAGIC
# MAGIC FROM olist_batch_data.gold.fact_reviews f
# MAGIC
# MAGIC JOIN olist_batch_data.gold.dim_date d
# MAGIC ON f.date_key = d.date_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name
# MAGIC
# MAGIC ORDER BY
# MAGIC     d.year,
# MAGIC     d.month;

# COMMAND ----------

