# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.orders_auto_optimize;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS skd_personal_bronze.new_schema.orders_auto_optimize (
# MAGIC   order_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   order_date TIMESTAMP,
# MAGIC   total_amount DECIMAL(10, 2),
# MAGIC   status STRING
# MAGIC )
# MAGIC USING delta

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
    INSERT INTO skd_personal_bronze.new_schema.orders_auto_optimize (order_id, customer_id, order_date, total_amount, status) VALUES ('{order_id}', '{customer_id}', '{order_date}', {total_amount}, '{order_status}')
    """

    spark.sql(sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.orders_auto_optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.orders_auto_optimize limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.orders_auto_optimize where order_id='ORD-55'

# COMMAND ----------

df=spark.sql('select avg(total_amount) from skd_personal_bronze.new_schema.orders_auto_optimize')

print(df.first()[0])