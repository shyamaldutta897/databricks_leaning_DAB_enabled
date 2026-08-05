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

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {silver_catalog}.{enriched_schema}')

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {silver_catalog}.{enriched_schema}.orders_silver_merge_into
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

max_version=spark.sql(f'select max(version) from (describe history {silver_catalog}.{clean_schema}.orders_cleaned_merge_into)').first()[0]

orders_cleaned=spark.sql(f"select * from table_changes('{silver_catalog}.{clean_schema}.orders_cleaned_merge_into',{max_version}) where _change_type='update_postimage'")


common_cols=set(spark.table(f'{silver_catalog}.{enriched_schema}.orders_silver_merge_into').columns) & set(orders_cleaned.columns)

update_condition=','.join([f'tgt.{c}=src.{c}' for c in common_cols if c!='order_id' ])

insert_cols=','.join(common_cols)
insert_vals=','.join([f'src.{c}' for c in common_cols])



win=Window.partitionBy('order_id').orderBy(col('order_id').desc())

orders_deduped=(orders_cleaned.
                    withColumn('rn',row_number().over(win)).
                    filter(col('rn')==1).
                    drop('rn')
                    )

orders_deduped.createOrReplaceTempView('orders_clean')


spark.sql(f"""
          MERGE INTO {silver_catalog}.{enriched_schema}.orders_silver_merge_into as tgt
          USING orders_clean as src
          ON src.order_id=tgt.order_id

          WHEN MATCHED THEN 
          UPDATE SET {update_condition}

          WHEN NOT MATCHED THEN
          INSERT({insert_cols}) VALUES({insert_vals})
          
          """)

