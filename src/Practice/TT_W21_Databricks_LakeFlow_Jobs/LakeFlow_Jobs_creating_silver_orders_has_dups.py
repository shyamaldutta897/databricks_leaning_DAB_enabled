
from pyspark.sql.functions import *
from pyspark.sql import Window

spark.sql('CREATE SCHEMA IF NOT EXISTS skd_personal_silver.silver')

orders_cleaned=spark.table('skd_personal_silver.cleaned.orders_cleaned')

win=Window.partitionBy('order_id').orderBy(col('order_id').desc())

orders_deduped=(orders_cleaned.
                    withColumn('rn',row_number().over(win)).
                    filter(col('rn')==1).
                    drop('rn')
                    )


(orders_deduped.
 write.
 format('delta').
 option('mergeSchema','true').
 mode('overwrite').
 saveAsTable('skd_personal_silver.silver.orders_silver'))

