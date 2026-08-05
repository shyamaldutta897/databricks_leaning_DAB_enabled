# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Load data from landing zone and move it to bronze layer
# MAGIC
# MAGIC Quick pointers - 
# MAGIC
# MAGIC 1. The read code is exactly same as readStream code, no difference
# MAGIC 2. Only difference is with the @dlt.table decorator. We need to define the new table name inside it. Default behavior is, if we don't provide the table name the decorator will consider the function name as table name.
# MAGIC 3. The use of decorator is - it converts a normal function into a Spark UDF. But in case of DLT pipelines, dlt.table creates a streaming table.
# MAGIC 4. Also, while creating the pipeline we need to mention the default catalog and schema - which in this case is the bronze catalog. Hence in the name parameter we don't need to create the table with three level naming convention.

# COMMAND ----------

# DBTITLE 1,Cell 2
import dlt
from pyspark.sql.functions import current_timestamp, col

@dlt.table(
    name="orders_raw"
)
def orders_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://retaildata@stgsdpersonaldev.dfs.core.windows.net/landing/orders")
        .withColumn("input_file_name", col("_metadata.file_path"))
        .withColumn("ingest_time", current_timestamp())
    )

# 2. Customers Raw Streaming Table
@dlt.table(
    name="customers_raw"
)
def customers_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://retaildata@stgsdpersonaldev.dfs.core.windows.net/landing/customers")
        .withColumn("input_file_name", col("_metadata.file_path"))
        .withColumn("ingest_time", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Load data to Silver from Bronze
# MAGIC
# MAGIC Quick pointers - 
# MAGIC
# MAGIC 1. Three level naming convention since Silver Catalog is not registered as default catalog.
# MAGIC 2. Expectation Matrix - Equivalent to constraints in SQL, checks against a particular condition - if doesn't meet then perform certain operation. The expression is passed as Key:Value pair - \
# MAGIC     a. `@dlt.expect('name','expr')` - Only logs alert, doesn't drop row.\
# MAGIC     b. `@dlt.expect_all_or_drop('name','expr')` - Checks against the provided expression, if doesn't meet then drops row.\
# MAGIC     c. `@dlt.expect_or_fail('name','expr')` - Fails the pipeline.
# MAGIC 3. The function-\
# MAGIC     a. Why is it written this way - In DLT Python, there is no execution loops. A standard function is defined that acts as the blueprint for the pipeline's execution graph.\
# MAGIC     b. `dlt.read_stream` - This is an indication to Spark to read incrementally from the mentioned table inside the function.\
# MAGIC     c. `selectExpr` is nothing but the SQL expression.

# COMMAND ----------

# DBTITLE 1,Cell 4
# 1. Orders Cleaned Streaming Table
@dlt.table(
    name="skd_personal_silver.cleaned.orders_cleaned"
)
@dlt.expect_all_or_drop({
    "valid_order": "order_id IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL"
})
def orders_cleaned():
    return (
        dlt.read_stream("orders_raw")
        .selectExpr(
            "orderid AS order_id",
            "orderdate AS order_date",
            "customerid AS customer_id",
            "totalamount AS total_amount",
            "status",
            "input_file_name",
            "ingest_time"
        )
    )

# 2. Customers Cleaned Streaming Table
@dlt.table(
    name="skd_personal_silver.cleaned.customers_cleaned"
)
@dlt.expect_all_or_drop({
    "valid_customer": "customer_id IS NOT NULL"
})
def customers_cleaned():
    return (
        dlt.read_stream("customers_raw")
        .selectExpr(
            "customerid AS customer_id",
            "customername AS customer_name",
            "address AS city",
            "dateofbirth AS dob",
            "registrationdate AS customer_since",
            "input_file_name",
            "ingest_time"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.Take the data from the cleaned customers table and apply SCD2 to maintain history
# MAGIC
# MAGIC Quick pointers - 
# MAGIC
# MAGIC 1. SCD2 implementation in declarative style
# MAGIC 2. First we create the table using dlt.create_streaming_table command, creates a completely empty target table.
# MAGIC 3. `dlt.create_auto_cdc_flow` - It is an API in declarative Lakeflow Pipelines used to cimplify Change Data Capture processing.
# MAGIC 4. Same like Merge Into, it is simplified a lot.\
# MAGIC     a. `name` - CDC flow name\
# MAGIC     b. `target` - The target table name where SCD2 should be applied, same as target in Merge/ Merge Into\
# MAGIC     c. `source` - The table against which change of data should be checked.\
# MAGIC     d. `keys` - Same as SCD2 keys\
# MAGIC     e.  `stored_as_scd_type` - Supports numeric values, 1 means SCD type 1, 2 means SCD type 2 and so on 

# COMMAND ----------

# DBTITLE 1,Cell 6
# 1. Declare the empty target streaming table in the metastore
dlt.create_streaming_table(
    name="skd_personal_silver.cleaned.customers_history"
)

# 2. Define the flow to automatically capture changes and apply SCD Type 2 logic
dlt.create_auto_cdc_flow(
    name="customers_silver_scd2",
    target="skd_personal_silver.cleaned.customers_history",
    source="skd_personal_silver.cleaned.customers_cleaned", 
    keys=["customer_id"],
    sequence_by="ingest_time",
    stored_as_scd_type=2
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Apply only merge to cleaned Orders table and not SCD2 and load back to Silver layer
# MAGIC
# MAGIC Quick pointers - 
# MAGIC
# MAGIC 1. Here we are using `dlt.apply_changes` API which is a legacy feature. But it is backward compatible, so it can be used in modern Lakeflow Pipelines.
# MAGIC 2. It uses the same parameters like `dlt.auto_cdc_flow`. Just internal differneces, external remains the same.
# MAGIC 3. Main differnece is apply_changes is built for DLT, but auto_cdc_flow is built for LakeFlow Pipelines.
# MAGIC 4. Which one to use - Databricks recommends using `auto_cdc_flow` for all new Lakeflow Pipelines.

# COMMAND ----------

# DBTITLE 1,Cell 8
# 1. Declare the empty target streaming table in the metastore
dlt.create_streaming_table(
    name="skd_personal_silver.cleaned.orders_silver"
)

# 2. Apply CDC tracking logic (Defaults to SCD Type 1)
dlt.apply_changes(
    target="skd_personal_silver.cleaned.orders_silver",
    source="skd_personal_silver.cleaned.orders_cleaned", 
    keys=["order_id"],
    sequence_by="ingest_time",
    stored_as_scd_type=1      # Explicitly defaults to Type 1 since Type 2 wasn't specified
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Load the processed data with buisiness logics to gold layer
# MAGIC
# MAGIC Quick pointers - 
# MAGIC 1. `@dlt_read` without streaming input(read_stream) creates a materialized view. So in the below case, dlt_read is used.
# MAGIC 2. Same approach is being used - Python function to create a blueprint for LakeFlow Pipeline.
# MAGIC 3. Reading data from Silver tables.
# MAGIC 4. Why use materialized view over streaming tables in Gold layer - In DLT & UC, materialized view is a physical table, but it has an automated, smart engine attached to it. It is basically a way of choosing automated Data Engineering over manual pipeline maintenance
# MAGIC 5. Materialized view benefits - 
# MAGIC
# MAGIC     a. Automated Incremental Refresh - Normal table needs Merge Into, but in case of materialized view Databricks tracks all Silver Tables related to the view and also locates all types of changes, so that the time and effort is minimal in terms of processing.\
# MAGIC     b. Precomputed results for BI Dashboards - If we rely on normal tables, Databricks has to scan through millions of rows. But Materialzed View is built on top of predefined metrics which helps to fetch the exact results to the dashboards.\
# MAGIC     c. Declarative Governance & Lineage - If normal table's schema change, then downstream tables can be affected. But in case of a ,aterialized view there is a predefined query. As long as the fields related to the query are not changed or impacted it doesn't matter at all about schema changes in case of materialized views.

# COMMAND ----------

# DBTITLE 1,Cell 10
# 1. City-Wise Sales Materialized View
@dlt.table(
    name="skd_personal_gold.curated.city_wise_sales"
)
def city_wise_sales():
    # Read the silver layer tables as static views to calculate aggregations
    orders = dlt.read("skd_personal_silver.cleaned.orders_cleaned")
    customers = dlt.read("skd_personal_silver.cleaned.customers_cleaned")
    
    # Join and aggregate using PySpark DataFrame operations
    return (
        orders.join(customers, on="customer_id", how="inner")
        .groupBy("city")
        .sum("total_amount")
        .withColumnRenamed("sum(total_amount)", "total_Sales")
    )