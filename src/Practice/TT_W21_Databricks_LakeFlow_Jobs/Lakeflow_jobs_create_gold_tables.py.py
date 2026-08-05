# Databricks notebook source
from pyspark.sql.functions import *

spark.sql('CREATE SCHEMA IF NOT EXISTS skd_personal_gold.curated')

orders_silver=spark.table('skd_personal_silver.silver.orders_silver')
customers_silver=spark.table('skd_personal_silver.silver.customers_silver')

joined_df=orders_silver.alias('o').join(customers_silver.alias('c'),on=(col('o.customer_id')==col('c.customer_id')),how='left')\
                       .drop('c.customer_id','c.Address')
filtered_df=joined_df.filter((col('city').isNotNull()) | (col('state').isNotNull()))

grouped_df=filtered_df.groupBy('city','state','status').agg(sum('total_amount').alias('total_sales'))

statuses_df=grouped_df.select('status').distinct()
statuses_list=[row['status'] for row in statuses_df.collect()]

dbutils.jobs.taskValues.set(key='status_list',value=statuses_list)


(grouped_df.
 write.
 format('delta').
 mode('overwrite').
 option('mergeSchema','true').
 saveAsTable('skd_personal_gold.curated.retail_data'))



# COMMAND ----------

print(statuses_list)

# COMMAND ----------

