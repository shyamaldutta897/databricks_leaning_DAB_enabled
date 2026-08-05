# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS skd_personal_bronze.new_schema.product_catalog (
# MAGIC   product_id INT,
# MAGIC   product_name STRING,
# MAGIC   category STRING,
# MAGIC   price DECIMAL(10,2),
# MAGIC   stock_quantity INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO skd_personal_bronze.new_schema.product_catalog
# MAGIC VALUES 
# MAGIC   (101, 'Wireless Mouse', 'Electronics', 24.99, 150),
# MAGIC   (102, 'Mechanical Keyboard', 'Electronics', 89.99, 45),
# MAGIC   (103, 'Ergonomic Desk Chair', 'Furniture', 199.50, 20),
# MAGIC   (104, 'Stainless Steel Water Bottle', 'Home & Kitchen', 15.00, 300),
# MAGIC   (105, 'Noise-Cancelling Headphones', 'Electronics', 149.99, 75);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.product_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.product_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE skd_personal_bronze.new_schema.product_catalog
# MAGIC SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('skd_personal_bronze.new_schema.product_catalog',1)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. INSERT: Add 2 new products to the catalog
# MAGIC INSERT INTO skd_personal_bronze.new_schema.product_catalog
# MAGIC VALUES 
# MAGIC   (106, 'Bluetooth Speaker', 'Electronics', 45.00, 120),
# MAGIC   (107, 'LED Desk Lamp', 'Furniture', 29.99, 60);
# MAGIC
# MAGIC
# MAGIC -- 2. UPDATE: Change prices and restock quantities
# MAGIC -- Modifies 'Mechanical Keyboard' (102) and 'Ergonomic Desk Chair' (103)
# MAGIC UPDATE skd_personal_bronze.new_schema.product_catalog
# MAGIC SET 
# MAGIC   price = CASE 
# MAGIC     WHEN product_id = 102 THEN 79.99  -- Price drop
# MAGIC     WHEN product_id = 103 THEN 210.00 -- Price increase
# MAGIC   END,
# MAGIC   stock_quantity = CASE 
# MAGIC     WHEN product_id = 102 THEN 60     -- Restocked
# MAGIC     WHEN product_id = 103 THEN 15     -- Sold items
# MAGIC   END
# MAGIC WHERE product_id IN (102, 103);
# MAGIC
# MAGIC
# MAGIC -- 3. DELETE: Remove discontinued or out-of-stock items
# MAGIC -- Removes 'Wireless Mouse' (101) and 'Stainless Steel Water Bottle' (104)
# MAGIC DELETE FROM skd_personal_bronze.new_schema.product_catalog
# MAGIC WHERE product_id IN (101, 104);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.product_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('skd_personal_bronze.new_schema.product_catalog',2)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.product_catalog_delete_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE skd_personal_bronze.new_schema.product_catalog_delete_logs
# MAGIC (
# MAGIC     product_id int,
# MAGIC     product_name string,
# MAGIC     category string,
# MAGIC     price float,
# MAGIC     stock_qty int,
# MAGIC     change_type string,
# MAGIC     commit_version int,
# MAGIC     commit_timestamp timestamp
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.product_catalog_delete_logs
# MAGIC  (SELECT * FROM table_changes('skd_personal_bronze.new_schema.product_catalog',2,5) where _change_type='delete')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.product_catalog_delete_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.cdf_delete_streaming

# COMMAND ----------

from pyspark.sql.functions import col

(spark.readStream
      .format('delta')
      .option('readChangeFeed','true')
      .option('startingVersion',2)
      .table('skd_personal_bronze.new_schema.product_catalog')
      .filter(col('_change_type')=='delete')
      .select('*')
      .writeStream
      .outputMode('append')
      .option('checkpointLocation','/Volumes/skd_personal_bronze/default/delta_volume/stream_checkpoint/')
      .trigger(availableNow=True)
      .table('skd_personal_bronze.new_schema.cdf_delete_streaming'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.cdf_delete_streaming

# COMMAND ----------

from pyspark.sql.functions import *

(spark.readStream
      .format('delta')
      .option('readChangeFeed','true')
      .option('startingVersion','2')
      .table('skd_personal_bronze.new_schema.product_catalog')
      .filter(col('_change_type')=='delete')
      .select('*')
      .writeStream
      .outputMode('append')
      .option('checkpointLocation','/Volumes/skd_personal_bronze/default/delta_volume/stream_checkpoint1/')
      .trigger(processingTime='2 seconds')
      .table('skd_personal_bronze.new_schema.cdf_delete_streaming_auto'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.cdf_delete_streaming_auto

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM skd_personal_bronze.new_schema.product_catalog
# MAGIC WHERE product_id = 105;

# COMMAND ----------

