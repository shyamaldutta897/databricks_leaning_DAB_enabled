# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG skd_personal_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA new_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME delta_volume_new

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/skd_personal_bronze/new_schema/delta_volume_new'

# COMMAND ----------

# MAGIC %fs mkdirs /Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data

# COMMAND ----------

# MAGIC %fs ls /Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data

# COMMAND ----------

# MAGIC %fs ls /Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CSV.`/Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files
# MAGIC (
# MAGIC     '/Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data',
# MAGIC     format => 'CSV'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files
# MAGIC (
# MAGIC     '/Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data',
# MAGIC     format => 'CSV'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files
# MAGIC (
# MAGIC     '/Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data',
# MAGIC     format => 'CSV'
# MAGIC )
# MAGIC limit (10)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files
# MAGIC (
# MAGIC     '/Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data',
# MAGIC     format => 'CSV'
# MAGIC )
# MAGIC limit (10)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC read_files
# MAGIC (
# MAGIC     '/Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data',
# MAGIC     format=> 'csv',
# MAGIC     header=> 'true',
# MAGIC     schema=>'id int, name string, department string, salary int, hire_date date, remote_status string, _rescued_data string',
# MAGIC     mode=>'permissive',
# MAGIC     rescuedDataColumn=> '_rescued_data'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC read_files
# MAGIC (
# MAGIC     '/Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data',
# MAGIC     format=> 'csv',
# MAGIC     header=> 'true',
# MAGIC     schema=>'id int, name string, department string, salary int, hire_date date, remote_status string, _rescued_data string',
# MAGIC     mode=>'permissive',
# MAGIC     columnNameofCorruptRecord=> '_rescued_data'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.emp_from_volume;
# MAGIC
# MAGIC CREATE TABLE skd_personal_bronze.new_schema.emp_from_volume AS
# MAGIC SELECT * FROM
# MAGIC read_files
# MAGIC (
# MAGIC     '/Volumes/skd_personal_bronze/new_schema/delta_volume_new/emp_data',
# MAGIC     format=> 'csv',
# MAGIC     header=> 'true',
# MAGIC     schema=>'id int, name string, department string, salary int, hire_date date, remote_status string, _rescued_data string',
# MAGIC     mode=>'permissive',
# MAGIC     rescuedDataColumn=> '_rescued_data'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.emp_from_volume