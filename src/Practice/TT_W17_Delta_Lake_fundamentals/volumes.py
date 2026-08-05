# Databricks notebook source
# MAGIC %md
# MAGIC **1. First selecting the catalog in which we  want to create the volume.**

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog skd_personal_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Next we shall create a new volume inside a DB. In this case for example sake I have considered default, but we should stick to more specific Databases or Schemas**

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists skd_personal_bronze.default.delta_volume

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Then creating a new path where we shall be keeping all dataframes related files. Again, there should be spearate folders for separate dataframes, just for example purpose I have created the folder name as dataframes.**

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/skd_personal_bronze/default/delta_volume/dataframes')

# COMMAND ----------

# MAGIC %md
# MAGIC **We can also use the magic command %fs for DBFS related commands. Behind the scenes it runs the command as `dbutils.fs.command_name`**

# COMMAND ----------

# MAGIC %fs mkdirs '/Volumes/skd_personal_bronze/default/delta_volume/test'

# COMMAND ----------

# MAGIC %md
# MAGIC **Another test - we can't remove a directory using the universal rmdir command, but we need to use the rm command with -r**

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %fs rm -r '/Volumes/skd_personal_bronze/default/delta_volume/test' 

# COMMAND ----------

# MAGIC %md
# MAGIC **4. Coming back to the topic, now creating a new DF and writing to the location( dataframes folder) that was created in the volume**

# COMMAND ----------

data = [
    (1, "Alice", "HR", 3000),
    (2, "Bob", "Finance", 3500),
    (3, "Cathy", "Engineering", 4000),
    (4, "David", "Marketing", 3200),
    (5, "Eva", "Sales", 3800)
]
columns = ["id", "name", "department", "salary"]

df = spark.createDataFrame(data, schema=columns)

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('/Volumes/skd_personal_bronze/default/delta_volume/dataframes')

# COMMAND ----------

# MAGIC %md
# MAGIC **5. Since volume keeps all raw files in it, we can read all of them in the notebook. In the below example, we can see that we can query the entire folder which is nothing but a delta table** 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DELTA.`/Volumes/skd_personal_bronze/default/delta_volume/dataframes`

# COMMAND ----------

# MAGIC %md
# MAGIC **6. Similarly using DBFS commands we can list all the files that the volume currently stores. We have just created the dataframe, so from the output we see that there is a parquet file along with a folder called `_delta_log` - which together builds the delta table.**

# COMMAND ----------

# MAGIC %fs ls '/Volumes/skd_personal_bronze/default/delta_volume/dataframes'

# COMMAND ----------

# MAGIC %md
# MAGIC **7. Similarly, we can create DFs out of the metadata files that are stored inside the _delta_log folder. Had we created a normal table, first we couldn't see anything related to _delta_log folder in teh catalog. Secondly, even if we would try to create a DF based on the JSON data stored in the underlying storage account - that is not allowed in Databricks notebooks. That's how volume is different from a normal table**

# COMMAND ----------

json_path='/Volumes/skd_personal_bronze/default/delta_volume/dataframes/_delta_log/00000000000000000000.json'

df=spark.read.format('json').load(json_path)


# COMMAND ----------

# MAGIC %md
# MAGIC **As we see in the output, entire content of the JSON file is visible as a DF, which is easier to read here, compared to the raw file.**

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **We can also use SQL on top of the JSON file and the output would be similar**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM JSON.`/Volumes/skd_personal_bronze/default/delta_volume/dataframes/_delta_log/00000000000000000000.json`

# COMMAND ----------

# MAGIC %md
# MAGIC **8. Now, an important thing - We could make a DF out of the JSON file stored in _delta_log folder. Similarly, we should be able to make a DF out of the parquet file which holds the actual data. But that doesn't happen in reality - because we have created the table as DELTA - which is nothing but `Parquet+Transaction Logs`. So we just can't analyse the underlying parquet files alone.**

# COMMAND ----------

df=spark.read.format('parquet').load('/Volumes/skd_personal_bronze/default/delta_volume/dataframes//Volumes/skd_personal_bronze/default/delta_volume/dataframes/part-00000-a9f784bf-be5a-471c-bee9-d83059f52934.c000.snappy.parquet')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Similarly, even if we try to load the pqrquet files calling them as Delta - it still wouldn't work, because there is no transaction log. So there has to be complete directory which holds data + transaction logs, only then we can create a DF**

# COMMAND ----------

df=spark.read.format('delta').load('/Volumes/skd_personal_bronze/default/delta_volume/dataframes/part-00000-a9f784bf-be5a-471c-bee9-d83059f52934.c000.snappy.parquet')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Here it works, becaue we are reading the entire directory and not individual files**

# COMMAND ----------

df=spark.read.format('delta').load('/Volumes/skd_personal_bronze/default/delta_volume/dataframes')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **9. Now, let's say we want to add new row items to our DF. Meaning we will create a new DF and append to the existing location**

# COMMAND ----------

new_data = [
    (6, "Frank", "Operations", 3100),
    (7, "Grace", "Legal", 4500)
]


columns = ["id", "name", "department", "salary"]

df = spark.createDataFrame(new_data, schema=columns)

df.write.format('delta').mode('append').save('/Volumes/skd_personal_bronze/default/delta_volume/dataframes/')

# COMMAND ----------

# MAGIC %md
# MAGIC **Post that, when we list files in the dataframes location, we'll see that there's a new parquet file added that holds the new data. Prior to that there was only one parquet file and _delta_log folder**

# COMMAND ----------

# MAGIC %fs ls '/Volumes/skd_personal_bronze/default/delta_volume/dataframes/'

# COMMAND ----------

# MAGIC %md
# MAGIC **Now there will be two json files - the one looking like 000.json was created when we created the DF for the first time. Post that, the file 001.json got created when we wanted to append the data**

# COMMAND ----------

# MAGIC %fs ls '/Volumes/skd_personal_bronze/default/delta_volume/dataframes/_delta_log'

# COMMAND ----------

# MAGIC %md
# MAGIC **So now we can query the latest json file as well.**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM JSON.`/Volumes/skd_personal_bronze/default/delta_volume/dataframes/_delta_log/00000000000000000001.json`

# COMMAND ----------

# MAGIC %md
# MAGIC **Another important feature of Volume is - Since it holds all raw data, we can see the `entire history`. Wich means, we can perform `time travel` as well. As we see from the below output - there are two versions - 0 and 1. Version 0 has the mode Overwrite and version 1 has the mode Append. This way we can see each and every change that we performed in each and every version - whcih helps immensely whenever we want to use an old version or compare data outputs.**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY DELTA.`/Volumes/skd_personal_bronze/default/delta_volume/dataframes/`

# COMMAND ----------

# MAGIC %md
# MAGIC **Last but not the list, similar to how we can call describe command on top of a regular table in databricks, we can do the same in volumes as well. Just that we need to call the specific location where the data is stored.**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED DELTA.`/Volumes/skd_personal_bronze/default/delta_volume/dataframes/`