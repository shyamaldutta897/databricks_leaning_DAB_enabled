-- Databricks notebook source
CREATE STREAMING TABLE skd_personal_bronze.new_schema.customers_raw_new AS
SELECT *,
_metadata.file_name as input_file_name,
current_timestamp() as ingest_time
FROM cloud_files('abfss://retaildata@stgsdpersonaldev.dfs.core.windows.net/landing/customers','csv',map('inferColumnTypes','true'))

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.customers_cleaned_new_table
(
    CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT
customerid AS customer_id,
customername AS customer_name,
address AS city,
dateofbirth AS dob,
registrationdate AS customer_since,
input_file_name,
ingest_time
FROM STREAM (skd_personal_bronze.new_schema.customers_raw_new)


-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE skd_personal_silver.cleaned.customers_cleaned_history_new_table;

CREATE FLOW customers_silver_scd2 AS
AUTO CDC INTO skd_personal_silver.cleaned.customers_cleaned_history_new_table
FROM STREAM(skd_personal_silver.cleaned.customers_cleaned_new_table)
KEYS(customer_id)
SEQUENCE BY ingest_time
STORED AS SCD TYPE 2;