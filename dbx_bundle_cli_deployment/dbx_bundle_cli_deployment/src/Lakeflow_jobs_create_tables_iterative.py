# Databricks notebook source
from pyspark.sql.functions import *

#dbutils.widgets.text(name='status',defaultValue='completed',label='status_value')

status_param=dbutils.widgets.get('status')

gold_df=spark.table('skd_personal_gold.curated.retail_data')

gold_df_filtered=gold_df.filter(col('status')==f"{status_param}")

(gold_df_filtered.
 write.
 format('delta').
 option('mergeSchema','true').
 mode('overwrite').
 saveAsTable(f'skd_personal_gold.curated.{status_param.lower()}_sales'))

# COMMAND ----------

