# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.employee_records;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS skd_personal_bronze.new_schema.employee_records (
# MAGIC     emp_id INT NOT NULL,
# MAGIC     emp_name STRING,
# MAGIC     department STRING,
# MAGIC     salary DOUBLE,
# MAGIC     hire_date DATE
# MAGIC ) 
# MAGIC USING delta;
# MAGIC
# MAGIC
# MAGIC ALTER TABLE skd_personal_bronze.new_schema.employee_records
# MAGIC ADD CONSTRAINT check_salary CHECK (salary > 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.employee_records VALUES 
# MAGIC (101, 'Alice Smith', 'HR', 55000.0, '2022-01-15'),
# MAGIC (102, 'Bob Jones', 'IT', 75000.0, '2021-06-20'),
# MAGIC (103, 'Charlie Brown', 'Finance', 85000.0, '2023-03-10'),
# MAGIC (104, 'Diana Prince', 'IT', 95000.0, '2020-11-05'),
# MAGIC (105, 'Evan Wright', 'Marketing', 48000.0, '2024-02-18');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.employee_records VALUES 
# MAGIC (106, 'Fiona Gallagher', 'HR', 52000.0, '2023-08-22'),
# MAGIC (107, 'George Clark', 'Finance', 90000.0, '2019-04-12'),
# MAGIC (108, 'Hannah Abbott', 'IT', 68000.0, '2025-01-05'),
# MAGIC (109, 'Ian Malcolm', 'Marketing', 61000.0, '2022-10-30');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.employee_records VALUES 
# MAGIC (110, 'Julia Roberts', 'Sales', 58000.0, '2021-07-14'),
# MAGIC (111, 'Kevin Bacon', 'Sales', 62000.0, '2023-11-01'),
# MAGIC (112, 'Laura Croft', 'IT', 105000.0, '2018-05-25');

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE skd_personal_bronze.new_schema.employee_records
# MAGIC SET salary = salary * 1.05 
# MAGIC WHERE department = 'IT';

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE skd_personal_bronze.new_schema.employee_records
# MAGIC SET department = 'Finance' 
# MAGIC WHERE emp_id = 110;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM skd_personal_bronze.new_schema.employee_records
# MAGIC WHERE emp_id = 105;
# MAGIC
# MAGIC DELETE FROM skd_personal_bronze.new_schema.employee_records
# MAGIC WHERE emp_name = 'Ian Malcolm';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.employee_records VALUES 
# MAGIC (113, 'Miles Morales', 'IT', 72000.0, '2026-03-01'),
# MAGIC (114, 'Natasha Romanoff', 'Sales', 67000.0, '2025-09-15');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.employee_records@v10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_ronze.new_schema.emp_records_sclone;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE
# MAGIC skd_personal_bronze.new_schema.emp_records_sclone 
# MAGIC SHALLOW CLONE skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.emp_records_sclone 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.emp_records_sclone 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.emp_records_sclone

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.employee_records VALUES 
# MAGIC (115, 'Oliver Queen', 'Finance', 89000.0, '2026-06-22');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.employee_records ORDER BY emp_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.emp_records_sclone ORDER BY emp_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.emp_records_sclone

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.emp_records_sclone VALUES 
# MAGIC (116, 'Penelope Featherington', 'Marketing', 54000.0, '2026-06-22');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.emp_records_sclone

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.emp_records_sclone WHERE emp_id=116

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.employee_records WHERE emp_id=116

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM skd_personal_bronze.new_schema.emp_records_sclone where emp_id=114

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.emp_records_sclone

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.emp_records_sclone

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM skd_personal_bronze.new_schema.employee_records RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE skd_personal_bronze.new_schema.employee_records
# MAGIC SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 168 hours')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM skd_personal_bronze.new_schema.employee_records DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.emp_records_sclone

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.emp_records_sclone

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.employee_records@v11

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.employee_records@v11

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.employee_records@v12

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE skd_personal_bronze.new_schema.emp_records_sclone
# MAGIC SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 hours')

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM skd_personal_bronze.new_schema.emp_records_sclone DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE skd_personal_bronze.new_schema.employee_records TO VERSION AS OF 11

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.employee_records

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM skd_personal_bronze.new_schema.emp_records_sclone

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.emp_records_sclone

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.emp_records_sclone

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.emp_records_sclone

# COMMAND ----------

