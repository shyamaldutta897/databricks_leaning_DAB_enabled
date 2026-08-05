# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.emp_new;
# MAGIC
# MAGIC CREATE TABLE skd_personal_bronze.new_schema.emp_new (
# MAGIC     employee_id INT,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     department STRING,
# MAGIC     salary FLOAT,
# MAGIC     hire_date DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES
# MAGIC (
# MAGIC 'delta.columnMapping.mode'='name',
# MAGIC 'delta.enableIcebergCompatV2'='true',
# MAGIC 'delta.universalFormat.enabledFormats'='iceberg'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.emp_new (employee_id, first_name, last_name, department, salary, hire_date) 
# MAGIC VALUES 
# MAGIC (101, 'Alice', 'Smith', 'Engineering', 85000.00, '2022-03-15'),
# MAGIC (102, 'Bob', 'Johnson', 'Marketing', 62000.50, '2023-01-10'),
# MAGIC (103, 'Charlie', 'Brown', 'Engineering', 91000.00, '2021-06-22'),
# MAGIC (104, 'Diana', 'Prince', 'Finance', 78500.00, '2024-11-01'),
# MAGIC (105, 'Evan', 'Wright', 'HR', 55000.00, '2025-05-18');

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES skd_personal_bronze.new_schema.emp_new

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED skd_personal_bronze.new_schema.emp_new

# COMMAND ----------

