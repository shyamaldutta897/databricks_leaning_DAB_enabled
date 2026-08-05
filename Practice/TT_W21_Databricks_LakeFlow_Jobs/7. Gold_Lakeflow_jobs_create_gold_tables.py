# Databricks notebook source
bronze_catalog=dbutils.widgets.get('bronze')
silver_catalog=dbutils.widgets.get('silver')
gold_catalog=dbutils.widgets.get('gold')


ingest_schema=dbutils.widgets.get('ingest')
clean_schema=dbutils.widgets.get('clean')
enriched_schema=dbutils.widgets.get('enriched')
curated_schema=dbutils.widgets.get('curated')


# COMMAND ----------

from pyspark.sql.functions import *

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {gold_catalog}.{curated_schema}')

orders_silver=spark.table(f'{silver_catalog}.{enriched_schema}.orders_silver_merge_into')
customers_silver=spark.table(f'{silver_catalog}.{enriched_schema}.customers_copy_into_silver')

joined_df=orders_silver.alias('o').join(customers_silver.alias('c'),on=(col('o.customer_id')==col('c.customer_id')),how='left')\
                       .drop('c.customer_id','c.Address')
filtered_df=joined_df.filter((col('city').isNotNull()) | (col('state').isNotNull()))

groupped_df=filtered_df.groupBy('city','state','order_status').agg(sum('total_amount').alias('total_sales'))

statuses_df=groupped_df.select('order_status').distinct()
statuses_list=[row['order_status'] for row in statuses_df.collect()]

dbutils.jobs.taskValues.set(key='status_list',value=statuses_list)


(groupped_df.
 write.
 format('delta').
 mode('overwrite').
 option('mergeSchema','true').
 saveAsTable(f'{gold_catalog}.{curated_schema}.retail_data_new_pipeline'))



# COMMAND ----------

