# Databricks notebook source
spark

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/databricks-datasets/'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/databricks-datasets/airlines/'))

# COMMAND ----------

airlines_df=spark.read.format('csv').load('dbfs:/databricks-datasets/airlines/')

# COMMAND ----------

display(airlines_df.limit(10))

# COMMAND ----------


dbutils.fs.read('dbfs:/databricks-datasets/airlines/README.md')

# COMMAND ----------

