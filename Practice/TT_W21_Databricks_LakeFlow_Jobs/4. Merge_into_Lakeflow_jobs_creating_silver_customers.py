# Databricks notebook source
# MAGIC %run ./8.helper_exclude_columns_table_changes

# COMMAND ----------

bronze_catalog=dbutils.widgets.get('bronze')
silver_catalog=dbutils.widgets.get('silver')
gold_catalog=dbutils.widgets.get('gold')


ingest_schema=dbutils.widgets.get('ingest')
clean_schema=dbutils.widgets.get('clean')
enriched_schema=dbutils.widgets.get('enriched')
curated_schema=dbutils.widgets.get('curated')



# COMMAND ----------

from pyspark.sql.functions import *

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {silver_catalog}.{enriched_schema}')

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {silver_catalog}.{enriched_schema}.customers_copy_into_silver(
          customer_id int,
          customer_name string,
          contact_number string,
          email string,
          city string,
          state string,
          DOB date,
          reg_date date,
          start_date date,
          end_date date
          )
          USING DELTA
          TBLPROPERTIES ('enableChangeDataFeed'='true')
          
          """)

json_path='abfss://lakeflowjobcontainer@stgsdpersonaldev.dfs.core.windows.net/'

json_cities=(spark.
             read.
             format('json').
             load(json_path))

max_version=spark.sql(f"SELECT MAX(version) from (describe history {silver_catalog}.{clean_schema}.customers_copy_into_cleaned)").first()[0]

customers_silver=spark.sql(f"select * from table_changes('{silver_catalog}.{clean_schema}.customers_copy_into_cleaned',{max_version})  where _change_type='update_postimage'")

customers_silver=select_clean(customers_silver)

customers_processed=(customers_silver.alias('cs').
                     join(json_cities.alias('js'), on= col('cs.city')==col('js.city'),how='left' ).
                     drop(col('js.city'))
                     
                   )

customers_processed.createOrReplaceTempView('cust_silver')

cols_silver=spark.table(f'{silver_catalog}.{enriched_schema}.customers_copy_into_silver').columns
cols_cleaned=customers_processed.columns

common_cols=set(cols_cleaned) & set(cols_silver)


update_condition = ', '.join([f'tgt.{c}=src.{c}' for c in common_cols if c!='customer_id'])

insert_cols=','.join(common_cols)
insert_val=', '.join([f'src.{c}' for c in common_cols])


spark.sql(f"""
          MERGE INTO {silver_catalog}.{enriched_schema}.customers_copy_into_silver as tgt
          USING cust_silver as src
          ON src.customer_id = tgt.customer_id
          WHEN MATCHED THEN 
          update set {update_condition}

          WHEN NOT MATCHED THEN
          insert({insert_cols}) values ({insert_val})
          """)
