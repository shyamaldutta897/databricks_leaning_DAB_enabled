# Databricks notebook source
from pyspark.sql.functions import *

customers_bronze=spark.table("skd_personal_bronze.ingestion_schema.customers_raw")

customers_clean=(customers_bronze.
                 replace(to_replace='-',value=None,subset=['EffectiveEndDate']).
                 withColumn('EffectiveEndDate_clean',col('EffectiveEndDate').cast('date')).
                 filter(col('EffectiveEndDate').isNull()).
                 drop('EffectiveEndDate')
                 
                 )

# COMMAND ----------

display(customers_clean)

# COMMAND ----------

orders_bronze=spark.table('skd_personal_bronze.ingestion_schema.orders_raw')

orders_bronze.printSchema()

# COMMAND ----------

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
    orders_enriched=orders_enriched.withColumnRenamed(old_name,new_name)


win=Window.partitionBy('order_id').orderBy(col('order_id').desc())

total_count=orders_enriched.count()

total_count_dedups=(orders_enriched.
                    withColumn('rn',row_number().over(win)).
                    filter(col('rn')==1).count())

duplication_exists=total_count!=total_count_dedups

# COMMAND ----------

orders_enriched.show(5)

# COMMAND ----------

print(total_count)
print(total_count_dedups)
print(duplication_exists)

# COMMAND ----------

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
    orders_enriched=orders_enriched.withColumnRenamed(old_name,new_name)

# COMMAND ----------

orders_enriched.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS skd_personal_bronze.ingestion_schema.customers_raw_copy_into
# MAGIC select * from skd_personal_bronze.ingestion_schema.customers_raw_copy_into limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from skd_personal_bronze.ingestion_schema.orders_raw_copy_into limit 10

# COMMAND ----------


max_version=spark.sql(f"""
select max(version) as max_ver from (describe history skd_personal_bronze.ingestion_schema.orders_raw_copy_into)
""").first()



# COMMAND ----------

max_version[0]

# COMMAND ----------

df=spark.table('skd_personal_bronze.ingestion_schema.customers_raw_copy_into')

df.show(5)

# COMMAND ----------

df=spark.table('skd_personal_bronze.ingestion_schema.orders_raw_copy_into')
df.columns

# COMMAND ----------

# MAGIC %run ./helper_exclude_columns_table_changes

# COMMAND ----------


from pyspark.sql.functions import *
json_path='abfss://lakeflowjobcontainer@stgsdpersonaldev.dfs.core.windows.net/'

json_cities=(spark.
             read.
             format('json').
             load(json_path))

max_version=spark.sql("SELECT MAX(version) from (describe history skd_personal_silver.cleaned.customers_copy_into_cleaned)").first()[0]

customers_silver=spark.sql(f"select * from table_changes('skd_personal_silver.cleaned.customers_copy_into_cleaned',{max_version}) where _change_type='update_postimage'")

customers_silver=select_clean(customers_silver)

customers_processed=(customers_silver.alias('cs').
                     join(json_cities.alias('js'), on= col('cs.city')==col('js.city'),how='left' ).
                     drop(col('js.city'))
                     
                   )

# COMMAND ----------

max_version

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists skd_personal_silver.silver.customers_copy_into_silver
# MAGIC select customer_id, count(*) from skd_personal_silver.silver.orders_silver_merge_into group by all having count(*)>1

# COMMAND ----------

