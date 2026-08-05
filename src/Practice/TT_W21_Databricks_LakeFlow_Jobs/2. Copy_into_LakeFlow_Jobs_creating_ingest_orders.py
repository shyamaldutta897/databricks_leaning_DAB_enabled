# Databricks notebook source
bronze_catalog=dbutils.widgets.get('bronze')
silver_catalog=dbutils.widgets.get('silver')
gold_catalog=dbutils.widgets.get('gold')


ingest_schema=dbutils.widgets.get('ingest')
clean_schema=dbutils.widgets.get('clean')
enriched_schema=dbutils.widgets.get('enriched')
curated_schema=dbutils.widgets.get('curated')



# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_catalog}.{ingest_schema}")

raw_location='abfss://lakeflowjobcontainer@stgsdpersonaldev.dfs.core.windows.net/'

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {bronze_catalog}.{ingest_schema}.orders_raw_copy_into
          (
              order_id int,
              order_date date,
              customer_id int,
              total_amount decimal(10,2),
              order_status string
          )
          USING DELTA
          TBLPROPERTIES('enableChangeDataFeed'='true')
          """)

spark.sql(f"""
          COPY INTO {bronze_catalog}.{ingest_schema}.orders_raw_copy_into
          FROM
          (
              SELECT 
              try_cast(OrderID AS int) AS order_id,
              try_cast(OrderDate AS date) AS order_date,
              try_cast(CustomerID AS int) AS customer_id,
              try_cast(TotalAmount AS decimal(10,2)) AS total_amount,
              try_cast(Status AS string) AS order_status
              FROM '{raw_location}orders'
          )
          FILEFORMAT=CSV
          FORMAT_OPTIONS
          (
              'header'='true',
              'mergeSchema'='true',
              'delimiter'=','
          )
          COPY_OPTIONS
          (
              'mergeSchema'='true'
          )


          """)


