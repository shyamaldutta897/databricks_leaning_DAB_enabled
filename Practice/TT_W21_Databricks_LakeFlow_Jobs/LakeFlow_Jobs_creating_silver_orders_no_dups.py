
from pyspark.sql.functions import *
from pyspark.sql import Window

spark.sql('CREATE SCHEMA IF NOT EXISTS skd_personal_silver.silver')

orders_cleaned=spark.table('skd_personal_silver.cleaned.orders_cleaned')
(orders_cleaned.
 write.
 format('delta').
 option('mergeSchema','true').
 mode('overwrite').
 saveAsTable('skd_personal_silver.silver.orders_silver'))

