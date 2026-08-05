# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.emp_from_volume;
# MAGIC
# MAGIC CREATE TABLE skd_personal_bronze.new_schema.emp_from_volume

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO skd_personal_bronze.new_schema.emp_from_volume
# MAGIC FROM '/Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data'
# MAGIC FILEFORMAT=CSV
# MAGIC FORMAT_OPTIONS
# MAGIC (
# MAGIC     'inferSchema'='true',
# MAGIC     'header'='true',
# MAGIC     'mergeSchema'='true',
# MAGIC     'delimiter'=',' --optional when delimiter is ',' otherwise need to provide e.g. 'delimiter'='|'
# MAGIC )
# MAGIC COPY_OPTIONS
# MAGIC (
# MAGIC     'mergeSchema'='true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.emp_from_volume

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.emp_from_volume

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED skd_personal_bronze.new_schema.emp_from_volume