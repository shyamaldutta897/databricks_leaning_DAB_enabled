# Databricks notebook source
data = [
    (101, "Alice", "Engineering", "New York", 85000),
    (102, "Bob", "Marketing", "Chicago", 72000),
    (103, "Charlie", "Finance", "San Francisco", 95000),
    (104, "David", "HR", "Austin", 64000),
    (105, "Eva", "Operations", "Miami", 68000)
]

columns = ["Employee_ID", "Name", "Department", "City", "Salary"]

df = spark.createDataFrame(data, schema=columns)


# COMMAND ----------

path='/Volumes/skd_personal_bronze/default/delta_volume/merge_demo/'

df.write.mode("overwrite").format("delta").save(path)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo`

# COMMAND ----------

data = [
    (101, "Alice", "Engineering", "Boston", 92000),      # Updated City & Salary
    (102, "Bob", "Product Management", "Chicago", 78000), # Updated Dept & Salary
    (106, "Frank", "Sales", "Seattle", 80000)             # New Row
]

columns = ["Employee_ID", "Name", "Department", "City", "Salary"]

df_updates = spark.createDataFrame(data, schema=columns)

# COMMAND ----------

df_updates.createOrReplaceTempView('source')

# COMMAND ----------

from delta.tables import DeltaTable

path='/Volumes/skd_personal_bronze/default/delta_volume/merge_demo/'
delta_table=DeltaTable.forPath(spark,path)

delta_table.alias('t')\
           .merge(df_updates.alias('s'), 't.Employee_ID = s.Employee_ID')\
           .whenMatchedUpdateAll()\
           .whenNotMatchedInsertAll()\
           .execute()


           


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo` AS t
# MAGIC USING source as s
# MAGIC on s.Employee_ID=t.Employee_ID
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC t.Name=s.Name,
# MAGIC t.Department=s.Department,
# MAGIC t.City=s.City,
# MAGIC t.Salary=s.Salary
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT (Employee_ID,Name,Department,City,Salary)
# MAGIC VALUES (s.Employee_ID,s.Name,s.Department,s.City,s.Salary)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo`

# COMMAND ----------

display(delta_table.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo`

# COMMAND ----------

data = [
    (101, "Alice", "Engineering", "Boston", 98000, False),      # Updated City & Salary
    (104, "Bob", "Product Management", "Chicago", 78000,True), # Updated Dept & Salary
]

columns = ["Employee_ID", "Name", "Department", "City", "Salary",'is_deleted']

df_updates_new = spark.createDataFrame(data, schema=columns)

# COMMAND ----------

from delta.tables import DeltaTable

path='/Volumes/skd_personal_bronze/default/delta_volume/merge_demo/'
delta_table=DeltaTable.forPath(spark,path)

delta_table.alias('t').merge(df_updates_new.alias('s'),'s.Employee_ID=t.Employee_ID')\
                      .whenMatchedDelete(condition='s.is_deleted=True')\
                      .whenMatchedUpdate(
                          set={
                              "Name": "s.Name",
                              "Department": "s.Department",
                              "City": "s.City", 
                              "Salary": "s.Salary"
                          }
                      )\
                      .whenNotMatchedInsertAll()\
                      .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo`

# COMMAND ----------

df_updates_new.createOrReplaceTempView('source')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo` AS t
# MAGIC USING source as s on s.Employee_ID=t.Employee_ID
# MAGIC WHEN MATCHED and s.is_deleted=True
# MAGIC THEN DELETE
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT *
# MAGIC     
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo`

# COMMAND ----------

data = [
    (107, "Grace", "Legal", "Los Angeles", 105000),
    (108, "Henry", "Customer Support", "Denver", 58000)
]

columns = ["Employee_ID", "Name", "Department", "City", "Salary"]

df_new_rows = spark.createDataFrame(data, schema=columns)

# COMMAND ----------

df_new_rows.createOrReplaceTempView('source')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo` AS t
# MAGIC USING source as s on s.EMPLOYEE_ID=t.EMPLOYEE_ID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE THEN DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo`

# COMMAND ----------

from delta.tables import DeltaTable
path='/Volumes/skd_personal_bronze/default/delta_volume/merge_demo'

delta_df=DeltaTable.forPath(spark,path)

delta_df.alias('t').merge(df_new_rows.alias('s'), 's.Employee_ID=t.Employee_ID')\
                   .whenMatchedUpdateAll()\
                   .whenNotMatchedInsertAll()\
                   .whenNotMatchedBySourceDelete()\
                   .execute()



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from delta.`/Volumes/skd_personal_bronze/default/delta_volume/merge_demo`

# COMMAND ----------

