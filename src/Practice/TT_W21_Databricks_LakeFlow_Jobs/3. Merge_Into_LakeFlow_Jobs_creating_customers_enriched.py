# Databricks notebook source
# MAGIC %md
# MAGIC below is a helper UDF, used to exclude certain fields. 
# MAGIC Wanted to reuse the code in multiple places so created a separate notebook and running it here
# MAGIC True globalisation would be to exclude the fields at Spark cluster level, but for this use case not required.

# COMMAND ----------

# MAGIC %run ./8.helper_exclude_columns_table_changes
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

bronze_catalog=dbutils.widgets.get('bronze')
silver_catalog=dbutils.widgets.get('silver')
gold_catalog=dbutils.widgets.get('gold')


ingest_schema=dbutils.widgets.get('ingest')
clean_schema=dbutils.widgets.get('clean')
enriched_schema=dbutils.widgets.get('enriched')
curated_schema=dbutils.widgets.get('curated')



# COMMAND ----------



from pyspark.sql.functions import *

#Creating the table structure first because merge into needs a properly defined structure
#Also, sticking to SQL style of merge here, it can be implemented through merge command available in pySpark as well.
spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {silver_catalog}.{clean_schema}.customers_copy_into_cleaned(
          customer_id int,
          customer_name string,
          contact_number string,
          email string,
          city string,
          DOB date,
          reg_date date,
          start_date date,
          end_date date
          )
          USING DELTA
          TBLPROPERTIES ('enableChangeDataFeed'='true')
          
          """)

#Now, how CDF helps - It keeps track of each and every change in a table version wise.
#So, our aim here is to pick up the latest version info and apply it to our target table.

#To pick the max version, we can rely on the DESCRIBE HISTORY clause and apply max on top of the version field.
max_version=spark.sql(f"SELECT MAX(version) FROM(DESCRIBE HISTORY {bronze_catalog}.{ingest_schema}.customers_raw_copy_into)").first()[0]

#Then comes the table_changes function. As it takes two params, table name and version number - in place of version number we can place the max version var
customers_bronze=spark.sql(f"SELECT *  FROM table_changes('{bronze_catalog}.{ingest_schema}.customers_raw_copy_into',{max_version}) where _change_type='update_postimage'")

customers_bronze=select_clean(customers_bronze)

customers_clean=(customers_bronze.
                 replace(to_replace='-',value=None,subset=['end_date']).
                 withColumn('end_date',col('end_date').cast('date')).
                 filter(col('end_date').isNull())
                 
                 )

#Now, for the merge operation, we are bringing in a bit of dynamic code

#Common columns - Let's say in the source table there is a field which we don't want to include in target table.
#Also, there could be some hidden fields which could also create problem.
#So, we take the intersecation between the fields available in the new table and processed df.
common_cols=set(customers_clean.columns) & set(spark.table(f'{silver_catalog}.{clean_schema}.customers_copy_into_cleaned').columns)

#All the fields that remain will be used for insert and update.
insert_fields=', '.join(common_cols)
insert_values=', '.join([f"src.{c}" for c in common_cols])

update_condition=','.join(f'tgt.{c}=src.{c}' for c in common_cols if c != "customer_id")


customers_clean.createOrReplaceTempView('cust_clean')


spark.sql(f"""
          MERGE INTO {silver_catalog}.{clean_schema}.customers_copy_into_cleaned tgt
          USING cust_clean src
          ON tgt.customer_id=src.customer_id
          WHEN MATCHED THEN
          UPDATE SET {update_condition}

          WHEN NOT MATCHED THEN
          INSERT ({insert_fields}) VALUES ({insert_values})
        """)

