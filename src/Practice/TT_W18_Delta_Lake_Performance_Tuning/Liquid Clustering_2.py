# Databricks notebook source
# MAGIC %fs 
# MAGIC ls /databricks-datasets/nyctaxi/tables/nyctaxi_yellow/

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /databricks-datasets/nyctaxi/tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED DELTA.`dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/`

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc (
# MAGIC     vendor_id           STRING,
# MAGIC     pickup_datetime     TIMESTAMP,
# MAGIC     dropoff_datetime    TIMESTAMP,
# MAGIC     passenger_count     INT,
# MAGIC     trip_distance       DOUBLE,
# MAGIC     pickup_longitude    DOUBLE,
# MAGIC     pickup_latitude     DOUBLE,
# MAGIC     rate_code_id        INT,
# MAGIC     store_and_fwd_flag  STRING,
# MAGIC     dropoff_longitude   DOUBLE,
# MAGIC     dropoff_latitude    DOUBLE,
# MAGIC     payment_type        STRING,
# MAGIC     fare_amount         DOUBLE,
# MAGIC     extra               DOUBLE,
# MAGIC     mta_tax             DOUBLE,
# MAGIC     tip_amount          DOUBLE,
# MAGIC     tolls_amount        DOUBLE,
# MAGIC     total_amount        DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (vendor_id)
# MAGIC

# COMMAND ----------

table_location='/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/'
file_path=[p.path for p in dbutils.fs.ls(table_location)[:10] if p.path.endswith('.parquet')]
print(file_path)

# COMMAND ----------

taxi_df=spark.read.format('parquet').load(file_path)
(taxi_df.repartition(200)
 .write
 .format('delta')
 .mode('overwrite')
 .option('repartitionTotalByKeys','true')
 .saveAsTable('skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc where trip_distance>=100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_name,min(trip_distance) as min_distinace, max(trip_distance) as max_distance FROM skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc
# MAGIC GROUP BY _metadata.file_name

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc
# MAGIC ZORDER BY (trip_distance)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc where trip_distance>=100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_name,min(trip_distance) as min_distinace, max(trip_distance) as max_distance FROM skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc
# MAGIC GROUP BY _metadata.file_name
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Liquid Clustering part with multiple fields

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc (
# MAGIC     vendor_id           STRING,
# MAGIC     pickup_datetime     TIMESTAMP,
# MAGIC     dropoff_datetime    TIMESTAMP,
# MAGIC     passenger_count     INT,
# MAGIC     trip_distance       DOUBLE,
# MAGIC     pickup_longitude    DOUBLE,
# MAGIC     pickup_latitude     DOUBLE,
# MAGIC     rate_code_id        INT,
# MAGIC     store_and_fwd_flag  STRING,
# MAGIC     dropoff_longitude   DOUBLE,
# MAGIC     dropoff_latitude    DOUBLE,
# MAGIC     payment_type        STRING,
# MAGIC     fare_amount         DOUBLE,
# MAGIC     extra               DOUBLE,
# MAGIC     mta_tax             DOUBLE,
# MAGIC     tip_amount          DOUBLE,
# MAGIC     tolls_amount        DOUBLE,
# MAGIC     total_amount        DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (vendor_id,trip_distance)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc where trip_distance>=100

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_name,min(trip_distance) as min_distinace, max(trip_distance) as max_distance FROM skd_personal_bronze.new_schema.nyctaxi_yellow_trips_lc
# MAGIC GROUP BY _metadata.file_name
# MAGIC

# COMMAND ----------

