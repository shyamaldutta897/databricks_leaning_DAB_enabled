-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 1. Load data from landing zone and move it to bronze layer
-- MAGIC
-- MAGIC Quick pointers - 
-- MAGIC
-- MAGIC 1. SQL version of spark.readStream.
-- MAGIC 2. Comment property is used to add description of the table in the frontend.
-- MAGIC 3. Quality is another property that is visible under the properties section. Whatever we deine inside the parameter TBLPROPERTIES would be visible in the frontend.
-- MAGIC 4. cloud_files is same as readStream autoloader. First parameter is about the file location, second is about data format and 3rd is about inferSchema (only inferColumns is used here). Also, map function is used in place of option (readStream).

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE skd_personal_bronze.new_schema.orders_raw 
COMMENT 'Loading orders data from Landing to Bronze Layer'
TBLPROPERTIES('quality'='bronze')
AS
SELECT *,
_metadata.file_name as input_file_name,
current_timestamp() as ingest_time
FROM cloud_files('abfss://retaildata@stgsdpersonaldev.dfs.core.windows.net/landing/orders','csv',map('inferColumnTypes','true'));

CREATE STREAMING TABLE skd_personal_bronze.new_schema.customers_raw 
COMMENT 'Loading customers data from Landing to Bronze Layer'
TBLPROPERTIES('quality'='bronze')
AS
SELECT *,
_metadata.file_name as input_file_name,
current_timestamp() as ingest_time
FROM cloud_files('abfss://retaildata@stgsdpersonaldev.dfs.core.windows.net/landing/customers','csv',map('inferColumnTypes','true'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Load data to Silver from Bronze
-- MAGIC
-- MAGIC Quick pointers - 
-- MAGIC 1. Constraint types (or expectations) - \
-- MAGIC     a. none - Only warns but no action\
-- MAGIC     b. ON VIOLATION DROP ROW - Drops all rows whichever doesn't meet the expectation\
-- MAGIC     c. ON VIOLATION FAIL UPDATE - Pipeline run will be terminated if there is a violation
-- MAGIC 2. The STREAM keyword is used to notify Databricks that we want to read incrementally from a location.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.orders_cleaned_new
(
    CONSTRAINT valid_order EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT correct_amount EXPECT (total_amount>=0) ON VIOLATION FAIL UPDATE,
    CONSTRAINT no_missing_status EXPECT (status IS NOT NULL)
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

CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.customers_cleaned_new
(
    CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT customer_since_check EXPECT(customer_since IS NOT NULL)
) AS
SELECT
customerid AS customer_id,
customername AS customer_name,
address AS city,
dateofbirth AS dob,
registrationdate AS customer_since,
input_file_name,
ingest_time
FROM STREAM (skd_personal_bronze.new_schema.customers_raw)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.Take the data from the cleaned customers table and apply SCD2 to maintain history
-- MAGIC
-- MAGIC Quick pointers - 
-- MAGIC 1. The AUTO CDC function expects the parameters in a specific order - 
-- MAGIC     a. FROM
-- MAGIC     b. KEYS
-- MAGIC     c. SEQUENCE BY
-- MAGIC     d. COLUMNS * EXCEPT
-- MAGIC     e. STORED AS SCD
-- MAGIC 2. COLUMNS * EXCEPT - Denotes which are the fields that we don't want as a part of the new SCD table.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Cell 6
    
CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.customers_cleaned_history_new;

CREATE FLOW customers_silver_scd2 AS
AUTO CDC INTO skd_personal_silver.cleaned.customers_cleaned_history_new
FROM STREAM(skd_personal_silver.cleaned.customers_cleaned_new)
KEYS(customer_id)
SEQUENCE BY ingest_time
COLUMNS * EXCEPT
(customer_name)
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Apply only merge to cleaned Orders table and not SCD2 and load back to Silver layer

-- COMMAND ----------

-- DBTITLE 1,Cell 8
    
CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.orders_merged_new;

CREATE FLOW orders_silver_scd1 AS
AUTO CDC INTO skd_personal_silver.cleaned.orders_merged_new
FROM STREAM(skd_personal_silver.cleaned.orders_cleaned_new)
KEYS(order_id)
SEQUENCE BY ingest_time;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Load the processed data with buisiness logics to gold layer
-- MAGIC
-- MAGIC Quick pointers - 
-- MAGIC 1. We can use CONSTRAINTS with Materialized Views as well.
-- MAGIC 2. Refer to the py version of the same notebook why Materialized Views are preferred option of gold layer.

-- COMMAND ----------

CREATE MATERIALIZED VIEW orders_customers_mapped
(CONSTRAINT no_missing_customer EXPECT(customer_id IS NOT NULL))
AS
SELECT o.order_id,o.order_date,o.total_amount,c.customer_id,c.city
FROM skd_personal_silver.cleaned.orders_cleaned_new o
JOIN skd_personal_silver.cleaned.customers_cleaned_new c ON
o.customer_id=c.customer_id

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW skd_personal_gold.curated.city_wise_sales
AS
SELECT city, SUM(total_amount) AS total_Sales
FROM skd_personal_silver.cleaned.orders_cleaned_new o
JOIN skd_personal_silver.cleaned.customers_cleaned_new c
ON o.customer_id=c.customer_id
GROUP BY city