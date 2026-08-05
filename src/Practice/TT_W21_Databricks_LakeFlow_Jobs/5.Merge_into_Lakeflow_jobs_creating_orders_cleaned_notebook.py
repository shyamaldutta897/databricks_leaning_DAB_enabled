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
from pyspark.sql import Window

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {silver_catalog}.{clean_schema}.orders_cleaned_merge_into
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

max_version=spark.sql(f"SELECT MAX(version) FROM (DESCRIBE HISTORY {bronze_catalog}.{ingest_schema}.orders_raw_copy_into)").first()[0]
orders_raw=spark.sql(f"SELECT * FROM table_changes('{bronze_catalog}.{ingest_schema}.orders_raw_copy_into',{max_version})  where _change_type='update_postimage'")

orders_bronze=select_clean(orders_raw)

common_cols=set(spark.table(f'{silver_catalog}.{clean_schema}.orders_cleaned_merge_into').columns) & set(orders_bronze.columns)

insert_cols=','.join(common_cols)
insert_vals=','.join([f'src.{c}' for c in common_cols])

update_condition=','.join([f'tgt.{c}=src.{c}' for c in common_cols if c!='order_id'])

orders_bronze.createOrReplaceTempView('orders_bronze')

win=Window.partitionBy('order_id').orderBy(col('order_id').desc())

total_count=orders_bronze.count()

total_count_dedups=(orders_bronze.
                    withColumn('rn',row_number().over(win)).
                    filter(col('rn')==1).count())

duplication_exists=total_count!=total_count_dedups

dbutils.jobs.taskValues.set(key='has_duplicates',value=duplication_exists)


spark.sql(f"""
          MERGE INTO {silver_catalog}.{clean_schema}.orders_cleaned_merge_into as tgt
          USING orders_bronze as src 
          ON src.order_id=tgt.order_id

          WHEN MATCHED THEN
          UPDATE SET {update_condition}

          WHEN NOT MATCHED THEN
          INSERT ({insert_cols}) VALUES ({insert_vals})

          """)


# COMMAND ----------

