# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog skd_personal_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS skd_personal_bronze.new_schema.students (
# MAGIC     student_id INT,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     major STRING,
# MAGIC     gpa DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/students';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.students VALUES
# MAGIC (101, 'Alex', 'Smith', 'Data Science', 3.8),
# MAGIC (102, 'Maria', 'Garcia', 'Computer Science', 3.9),
# MAGIC (103, 'Liam', 'Johnson', 'Mathematics', 3.5),
# MAGIC (104, 'Chloe', 'Wang', 'Engineering', 3.7),
# MAGIC (105, 'Omar', 'Sy', 'Physics', 3.6);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.students VALUES
# MAGIC ('one thousand', 'Alex', 'Smith', 'Data Science', 3.8)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.students VALUES
# MAGIC ('1000', 'Alex', 'Smith', 'Data Science', 3.8)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from skd_personal_bronze.new_schema.students

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.students VALUES
# MAGIC ('1001', 'Bob', 'Smith', 'Data Science', 5)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.students
# MAGIC (student_id,first_name,last_name,major,gpa,grade)
# MAGIC VALUES
# MAGIC ('1000', 'Alex', 'Smith', 'Data Science', 3.8, 'A')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW students_new_col AS
# MAGIC SELECT * FROM (VALUES
# MAGIC     (106, 'Emma', 'Brown', 'Chemistry', 3.4,'A'),
# MAGIC     (107, 'Lucas', 'Silva', 'Statistics', 3.8,'A'))
# MAGIC AS (student_id, first_name, last_name, major, gpa, grade)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE WITH SCHEMA EVOLUTION INTO skd_personal_bronze.new_schema.students as tgt
# MAGIC USING students_new_col as src 
# MAGIC ON src.student_id=tgt.student_id
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from skd_personal_bronze.new_schema.students

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE from skd_personal_bronze.new_schema.students where gpa=5

# COMMAND ----------

dml_data = [
    (111, 'Zoe', 'Miller', 'Biology', 3.4, 2025, 'Active'),
    (112, 'Ryan', 'Davis', 'Chemistry', 3.1, 2024, 'Academic Probation'),
    (113, 'Priya', 'Patel', 'Data Science', 4.0, 2025, 'Active')
]

schema = "student_id int, first_name string, last_name string, major string, gpa double, enrollment_year int, status string"
students_new_df = spark.createDataFrame(dml_data,schema)


# COMMAND ----------

students=spark.read.table('skd_personal_bronze.new_schema.students')

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

students.alias('t').merge(students_new_df.alias('s'),'s.student_id=t.student_id')\
                   .whenMatchedUpdateAll()\
                   .whenNotMatchedInsertAll()\
                   .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE skd_personal_bronze.new_schema.students
# MAGIC SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');

# COMMAND ----------

path='abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/students'

students_new_df\
    .write\
    .format('delta')\
    .mode('append')\
    .option('mergeSchema','true')\
    .save(path)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from skd_personal_bronze.new_schema.students

# COMMAND ----------

