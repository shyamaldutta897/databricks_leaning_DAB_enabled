# Databricks notebook source
# MAGIC %fs mkdirs /Volumes/skd_personal_bronze/new_schema/delta_volume_new/checkpoint

# COMMAND ----------

landing_zone='/Volumes/skd_personal_bronze/new_schema/delta_volume_new'
emp_data=landing_zone+'/emp_data'
checkpoint=landing_zone+'/checkpoint'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution Mode - none

# COMMAND ----------

employee_data=(spark.readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format','csv')
                    .option('cloudFiles.inferSchema','true')
                    .option('cloudFiles.inferColumnTypes','true')
                    .option('cloudFiles.schemaLocation',checkpoint)
                    .option('cloudFiles.schemaEvolutionMode','none')
                    .load(emp_data))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution Mode - Fail on new columns

# COMMAND ----------

employee_data=(spark.readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format','csv')
                    .option('cloudFiles.inferSchema','true')
                    .option('cloudFiles.inferColumnTypes','true')
                    .option('cloudFiles.schemaLocation',checkpoint)
                    .option('cloudFiles.schemaEvolutionMode','failOnNewColumns')
                    .load(emp_data))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution Mode - Rescue
# MAGIC

# COMMAND ----------

employee_data=(spark.readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format','csv')
                    .option('cloudFiles.inferSchema','true')
                    .option('cloudFiles.inferColumnTypes','true')
                    .option('cloudFiles.schemaLocation',checkpoint)
                    .option('cloudFiles.schemaEvolutionMode','rescue')
                    .load(emp_data))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution Mode - Add new columns

# COMMAND ----------

employee_data=(spark.readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format','csv')
                    .option('cloudFiles.inferSchema','true')
                    .option('cloudFiles.inferColumnTypes','true')
                    .option('cloudFiles.schemaLocation',checkpoint)
                    .option('cloudFiles.schemaEvolutionMode','addNewColumns')
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

