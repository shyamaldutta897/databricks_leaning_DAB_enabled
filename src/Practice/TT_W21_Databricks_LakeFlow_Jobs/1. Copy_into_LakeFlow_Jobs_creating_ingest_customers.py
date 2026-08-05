# Databricks notebook source
#bronze_cat=dbutils.widgets.text('catalog_name','skd_personal_bronze')

bronze_catalog=dbutils.widgets.get("bronze")
silver_catalog=dbutils.widgets.get('silver')
gold_catalog=dbutils.widgets.get('gold')


ingest_schema=dbutils.widgets.get('ingest')
clean_schema=dbutils.widgets.get('clean')
enriched_schema=dbutils.widgets.get('enriched')
curated_schema=dbutils.widgets.get('curated')



# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS skd_personal_bronze.ingestion_schema")

raw_location='abfss://lakeflowjobcontainer@stgsdpersonaldev.dfs.core.windows.net/'




#Creating the table and setting CDF as true because down the line we'll be relying upon it to apply merge
spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {bronze_catalog}.{ingest_schema}.customers_raw_copy_into(
          customer_id int,
          customer_name string,
          contact_number string,
          email string,
          city string,
          DOB date,
          reg_date date,
          start_date date,
          end_date date
          )
          USING DELTA
          TBLPROPERTIES ('enableChangeDataFeed'='true')
          
          
          """)

#Copy into - only for ingest purpose. First set of options are for read and second set of options are for write
#Without the second set of options it is not going to execute.
spark.sql(f"""
          COPY INTO {bronze_catalog}.{ingest_schema}.customers_raw_copy_into
          FROM

          (
              SELECT 
              try_cast(CustomerID AS int) AS customer_id,
              CustomerName AS customer_name,
              ContactNumber AS contact_number,
              Email AS email,
              Address AS city,
              try_cast(DateOfBirth AS date) AS DOB,
              try_cast(RegistrationDate AS date) AS reg_date,
              try_cast(EffectiveStartDate AS date) AS start_date ,
              try_cast(EffectiveEndDate AS date) AS end_date
              FROM '{raw_location}customers'
          )
          FILEFORMAT=CSV
          FORMAT_OPTIONS(
              'header'='true',
              'delimiter'=',',
              'mergeSchema'='true'
          )

          COPY_OPTIONS
          (
              'mergeSchema'='true'
          )
          """)




