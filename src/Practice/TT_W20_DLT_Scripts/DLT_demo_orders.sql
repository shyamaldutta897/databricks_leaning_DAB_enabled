-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE skd_personal_bronze.new_schema.orders_raw_new AS
SELECT *,
_metadata.file_name as input_file_name,
current_timestamp() as ingest_time
FROM cloud_files('abfss://retaildata@stgsdpersonaldev.dfs.core.windows.net/landing/orders','csv',map('inferColumnTypes','true'));

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.orders_cleaned_new_table
(
    CONSTRAINT valid_order EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT 
orderid AS order_id,
orderdate AS order_date,
customerid AS customer_id,
totalamount AS total_amount,
status,
input_file_name,
ingest_time
FROM STREAM( skd_personal_bronze.new_schema.orders_raw); --STREAM indicates that the load happens incrementally

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.orders_merged_new_table;

CREATE FLOW orders_silver_scd1 AS
AUTO CDC INTO skd_personal_silver.cleaned.orders_merged_new_table
FROM STREAM(skd_personal_silver.cleaned.orders_cleaned_new_table)
KEYS(order_id)
SEQUENCE BY ingest_time;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW skd_personal_gold.curated.city_wise_sales_new
AS
SELECT city, SUM(total_amount) AS total_Sales
FROM skd_personal_silver.cleaned.orders_cleaned_new_table o
JOIN skd_personal_silver.cleaned.customers_cleaned_new_table c
ON o.customer_id=c.customer_id
GROUP BY city