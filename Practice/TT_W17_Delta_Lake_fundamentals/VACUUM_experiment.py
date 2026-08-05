# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.sensor_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS skd_personal_bronze.new_schema.sensor_logs (
# MAGIC   log_id LONG,
# MAGIC   device_id STRING,
# MAGIC   temperature DOUBLE,
# MAGIC   humidity DOUBLE,
# MAGIC   reading_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
# MAGIC LOCATION 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/sensor_logs';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES skd_personal_bronze.new_schema.sensor_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.sensor_logs VALUES
# MAGIC   (1001, 'THERM-01', 21.5, 45.2, '2026-06-15 19:30:00'),
# MAGIC   (1002, 'THERM-02', 22.1, 40.8, '2026-06-15 19:30:05'),
# MAGIC   (1003, 'THERM-01', 21.4, 45.3, '2026-06-15 19:31:00'),
# MAGIC   (1004, 'THERM-03', 19.8, 50.1, '2026-06-15 19:31:12'),
# MAGIC   (1005, 'THERM-02', 22.2, 40.7, '2026-06-15 19:31:15');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from skd_personal_bronze.new_schema.sensor_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.sensor_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM skd_personal_bronze.new_schema.sensor_logs WHERE device_id = 'THERM-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from skd_personal_bronze.new_schema.sensor_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE skd_personal_bronze.new_schema.sensor_logs VALUES
# MAGIC   (1006, 'THERM-01', 20.8, 44.1, '2026-06-15 19:35:00'),
# MAGIC   (1007, 'THERM-02', 22.5, 39.9, '2026-06-15 19:35:10'),
# MAGIC   (1008, 'THERM-03', 19.5, 51.2, '2026-06-15 19:36:00');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.sensor_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.sensor_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.sensor_logs@v0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.sensor_logs@v1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path from skd_personal_bronze.new_schema.sensor_logs@v2

# COMMAND ----------

spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM skd_personal_bronze.new_schema.sensor_logs RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM skd_personal_bronze.new_schema.sensor_logs RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.sensor_logs VERSION AS OF 0

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

print(spark.conf.get("spark.databricks.io.cache.enabled"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.sensor_logs VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.sensor_logs@v2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.sensor_logs@v4