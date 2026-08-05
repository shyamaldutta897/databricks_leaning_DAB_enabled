# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE skd_personal_bronze.new_schema.orders (
# MAGIC   order_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   order_date TIMESTAMP,
# MAGIC   total_amount DECIMAL(10, 2),
# MAGIC   status STRING
# MAGIC )
# MAGIC USING delta
# MAGIC location 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext'

# COMMAND ----------

for i in range(100):
    order_id=f'ORD-{i}'
    customer_id=f'CID-{i}'
    order_date='2022-01-01'
    total_amount=i*100
    if i<50:
        order_status='open'
    else:
        order_status='closed'
    sql=f"""
    INSERT INTO skd_personal_bronze.new_schema.orders (order_id, customer_id, order_date, total_amount, status) VALUES ('{order_id}', '{customer_id}', '{order_date}', {total_amount}, '{order_status}')
    """

    spark.sql(sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.orders

# COMMAND ----------

parquet_path='abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext/_delta_log/00000000000000000036.checkpoint.parquet'

df=spark.read.parquet(parquet_path)
display(df)


# COMMAND ----------

