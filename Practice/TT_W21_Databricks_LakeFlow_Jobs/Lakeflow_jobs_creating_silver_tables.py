from pyspark.sql.functions import *

spark.sql('CREATE SCHEMA IF NOT EXISTS skd_personal_silver.silver')

json_path='abfss://lakeflowjobcontainer@stgsdpersonaldev.dfs.core.windows.net/'

json_cities=(spark.
             read.
             format('json').
             load(json_path))

customers_silver=(spark.table("skd_personal_silver.cleaned.customers_cleaned").alias('cs').
                  join(json_cities.alias('js'), on= col('cs.Address')==col('js.city'),how='left' ).
                   selectExpr(*"""CustomerID AS customer_id,
                          CustomerName AS name,
                          ContactNumber,
                          Email,
                          Address AS city,
                          state,
                          DateOfBirth AS DOB,
                          RegistrationDate
                          """.strip().split(','))
                   )



(customers_silver.
 write.
 format('delta').
 mode('overwrite').
 option('mergeSchema','true').
 saveAsTable('skd_personal_silver.silver.customers_silver'))
