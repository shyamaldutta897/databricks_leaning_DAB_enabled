# Databricks notebook source
from pyspark.sql.types import *

schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("major", StringType(), True),
    StructField("gpa", DoubleType(), True),
    StructField("enrollment_year", IntegerType(), True),  
    StructField("status", StringType(), True)            
])

data = [

    (101, 'Alex', 'Smith', 'Data Science', 3.8, 2024, 'Active'),
    (102, 'Maria', 'Garcia', 'Computer Science', 3.9, 2023, 'Active'),
    (103, 'Liam', 'Johnson', 'Mathematics', 3.5, 2024, 'On Leave'),
    (104, 'Chloe', 'Wang', 'Engineering', 3.7, 2022, 'Graduated'),
    (105, 'Omar', 'Sy', 'Physics', 3.6, 2023, 'Active'),
    (111, 'Zoe', 'Miller', 'Biology', 3.4, 2025, 'Active'),
    (112, 'Ryan', 'Davis', 'Chemistry', 3.1, 2024, 'Academic Probation'),
    (113, 'Priya', 'Patel', 'Data Science', 4.0, 2025, 'Active')
]

df = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

path='/Volumes/skd_personal_bronze/default/delta_volume/students/'

df.write.format('delta').mode('overwrite').save(path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`/Volumes/skd_personal_bronze/default/delta_volume/students/`

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE DELTA.`/Volumes/skd_personal_bronze/default/delta_volume/students/`
# MAGIC RENAME COLUMN gpa to grade_pt_avg

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE DELTA.`/Volumes/skd_personal_bronze/default/delta_volume/students/`
# MAGIC SET TBLPROPERTIES ('delta.columnMapping.mode'='name')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE DELTA.`/Volumes/skd_personal_bronze/default/delta_volume/students/`
# MAGIC RENAME COLUMN gpa to grade_pt_avg

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE DELTA.`/Volumes/skd_personal_bronze/default/delta_volume/students/`
# MAGIC DROP COLUMN enrollment_year

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY DELTA.`/Volumes/skd_personal_bronze/default/delta_volume/students/`

# COMMAND ----------

