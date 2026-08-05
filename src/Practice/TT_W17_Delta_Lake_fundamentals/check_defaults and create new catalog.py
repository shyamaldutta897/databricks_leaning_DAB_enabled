# Databricks notebook source
# MAGIC %md
# MAGIC ### Check current catalog, metastore and schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog()

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_metastore()

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_schema()

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog samples

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas

# COMMAND ----------

# MAGIC %sql 
# MAGIC use catalog skd_personal_dbx_dev

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS employees (
# MAGIC     employee_id INT PRIMARY KEY,
# MAGIC     first_name VARCHAR(50) NOT NULL,
# MAGIC     last_name VARCHAR(50) NOT NULL,
# MAGIC     department VARCHAR(50),
# MAGIC     salary DECIMAL(10, 2),
# MAGIC     hire_date DATE NOT NULL
# MAGIC );
# MAGIC
# MAGIC
# MAGIC INSERT INTO employees (employee_id, first_name, last_name, department, salary, hire_date) 
# MAGIC VALUES 
# MAGIC (101, 'Alice', 'Smith', 'Engineering', 85000.00, '2022-03-15'),
# MAGIC (102, 'Bob', 'Johnson', 'Marketing', 62000.50, '2023-01-10'),
# MAGIC (103, 'Charlie', 'Brown', 'Engineering', 91000.00, '2021-06-22'),
# MAGIC (104, 'Diana', 'Prince', 'Finance', 78500.00, '2024-11-01'),
# MAGIC (105, 'Evan', 'Wright', 'HR', 55000.00, '2025-05-18');
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from skd_personal_dbx_dev.default.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create catalog skd_personal_bronze 
# MAGIC managed location 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/unity_catalog'

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog skd_personal_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.employees (
# MAGIC     employee_id INT PRIMARY KEY,
# MAGIC     first_name VARCHAR(50) NOT NULL,
# MAGIC     last_name VARCHAR(50) NOT NULL,
# MAGIC     department VARCHAR(50),
# MAGIC     salary DECIMAL(10, 2),
# MAGIC     hire_date DATE NOT NULL
# MAGIC );
# MAGIC
# MAGIC
# MAGIC INSERT INTO employees (employee_id, first_name, last_name, department, salary, hire_date) 
# MAGIC VALUES 
# MAGIC (101, 'Alice', 'Smith', 'Engineering', 85000.00, '2022-03-15'),
# MAGIC (102, 'Bob', 'Johnson', 'Marketing', 62000.50, '2023-01-10'),
# MAGIC (103, 'Charlie', 'Brown', 'Engineering', 91000.00, '2021-06-22'),
# MAGIC (104, 'Diana', 'Prince', 'Finance', 78500.00, '2024-11-01'),
# MAGIC (105, 'Evan', 'Wright', 'HR', 55000.00, '2025-05-18');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating external location using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION stg_sd_personal_dev_schema_ext_loc URL 
# MAGIC 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/schema' 
# MAGIC WITH(CREDENTIAL uc_credentials_dev) 
# MAGIC COMMENT "This is a schema external location"

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog skd_personal_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema skd_personal_bronze.new_schema
# MAGIC managed location "abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/schema"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS new_schema.employees (
# MAGIC     employee_id INT PRIMARY KEY,
# MAGIC     first_name VARCHAR(50) NOT NULL,
# MAGIC     last_name VARCHAR(50) NOT NULL,
# MAGIC     department VARCHAR(50),
# MAGIC     salary DECIMAL(10, 2),
# MAGIC     hire_date DATE NOT NULL
# MAGIC );
# MAGIC
# MAGIC
# MAGIC INSERT INTO employees (employee_id, first_name, last_name, department, salary, hire_date) 
# MAGIC VALUES 
# MAGIC (101, 'Alice', 'Smith', 'Engineering', 85000.00, '2022-03-15'),
# MAGIC (102, 'Bob', 'Johnson', 'Marketing', 62000.50, '2023-01-10'),
# MAGIC (103, 'Charlie', 'Brown', 'Engineering', 91000.00, '2021-06-22'),
# MAGIC (104, 'Diana', 'Prince', 'Finance', 78500.00, '2024-11-01'),
# MAGIC (105, 'Evan', 'Wright', 'HR', 55000.00, '2025-05-18');

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE new_schema.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC UNDROP TABLE new_schema.employees

# COMMAND ----------

