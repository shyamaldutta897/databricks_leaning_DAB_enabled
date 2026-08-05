# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import Window

orders_bronze=spark.table("skd_personal_bronze.ingestion_schema.orders_raw")

renamed={'OrderID':'order_id',
         'OrderDate':'order_date',
         'CustomerID':'customer_id',
         'TotalAmount':'total_amount',
         'Status':'status'
         }

for old_name,new_name in renamed.items():
    orders_bronze=orders_bronze.withColumnRenamed(old_name,new_name)


win=Window.partitionBy('order_id').orderBy(col('order_id').desc())

total_count=orders_bronze.count()

total_count_dedups=(orders_bronze.
                    withColumn('rn',row_number().over(win)).
                    filter(col('rn')==1).count())

duplication_exists=total_count!=total_count_dedups

dbutils.jobs.taskValues.set(key='has_duplicates',value=duplication_exists)


(orders_bronze.
 write.
 format('delta').
 option('mergeSchema','true').
 mode('overwrite').
 saveAsTable('skd_personal_silver.cleaned.orders_cleaned'))
