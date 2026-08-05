# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.emp_from_volume;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE skd_personal_bronze.new_schema.emp_from_volume
# MAGIC (
# MAGIC    id int,
# MAGIC    name string,
# MAGIC    department string,
# MAGIC    salary int,
# MAGIC    hire_date date,
# MAGIC    _file_name,
# MAGIC    _load_date
# MAGIC )

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO skd_personal_bronze.new_schema.emp_from_volume
# MAGIC FROM 
# MAGIC (SELECT CAST(id as int),name,department,CAST(salary as int),CAST(hire_date as date),
# MAGIC _metadata.file_name as _file_name, current_timestamp() as _load_date
# MAGIC FROM
# MAGIC '/Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data')
# MAGIC FILEFORMAT=CSV
# MAGIC FORMAT_OPTIONS
# MAGIC (
# MAGIC     'header'='true'
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

