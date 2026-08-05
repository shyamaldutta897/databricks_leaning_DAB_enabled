# Databricks notebook source
# MAGIC %fs mkdirs /Volumes/skd_personal_bronze/new_schema/delta_volume_new/checkpoint

# COMMAND ----------

landing_zone='/Volumes/skd_personal_bronze/new_schema/delta_volume_new'
emp_data=landing_zone+'/emp_data'
checkpoint=landing_zone+'/checkpoint'

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

emp_schema=StructType([
    StructField('id', IntegerType()),
    StructField('name',StringType()),
    StructField('department',StringType()),
    StructField('salary',IntegerType()),
    StructField('hire_date',DateType())
])

# COMMAND ----------

employee_data=(spark.readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format','csv')
                    .schema(emp_schema)
                    .option('header','true')
                    .option('rescuedDataColumn','_rescued_data')
                    .option('cloudFiles.schemaLocation',checkpoint)
                    .load(emp_data))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.emp_autoloader;
# MAGIC
# MAGIC CREATE TABLE skd_personal_bronze.new_schema.emp_autoloader

# COMMAND ----------

op=(employee_data.writeStream
              .format('delta')
              .option('checkpointLocation',checkpoint)
              .option('mergeSchema','true')
              .outputMode('append')
              .trigger(availableNow=True)
              .toTable('skd_personal_bronze.new_schema.emp_autoloader'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.emp_autoloader

# COMMAND ----------

