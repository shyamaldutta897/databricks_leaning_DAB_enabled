from pyspark.sql.functions import *

customers_bronze=spark.table("skd_personal_bronze.ingestion_schema.customers_raw")

customers_clean=(customers_bronze.
                 replace(to_replace='-',value=None,subset=['EffectiveEndDate']).
                 withColumn('EffectiveEndDate_clean',col('EffectiveEndDate').cast('date')).
                 filter(col('EffectiveEndDate').isNull()).
                 drop('EffectiveEndDate')
                 
                 )

(customers_clean.
 write.
 format('delta').
 mode('overwrite').
 saveAsTable('skd_personal_silver.cleaned.customers_cleaned'))

