spark.sql("CREATE SCHEMA IF NOT EXISTS skd_personal_bronze.ingestion_schema")

raw_location='abfss://lakeflowjobcontainer@stgsdpersonaldev.dfs.core.windows.net/'

customers_raw=(spark.
               read.
               format('csv').
               option('inferSchema','true').
               option('header','true').
               load(raw_location+'customers'))

(customers_raw.
 write.
 format('delta').
 mode('overwrite').
 saveAsTable('skd_personal_bronze.ingestion_schema.customers_raw'))




