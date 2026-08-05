# Databricks notebook source
# MAGIC %md
# MAGIC ### Decorator in Python
# MAGIC
# MAGIC 1. Decorators in Python are used when there is a requirement to add extra functionality to an existing function.
# MAGIC 2. In the below example, we first define a function that reads data from a location and then retuns a df.
# MAGIC 3. But the extra functionality needed here was to write the DF as a delta table to a catalog as well
# MAGIC 4. We didn't want to alter the actual function, so we created an extra decorator function along with a wrapper function inside
# MAGIC 5. Now, we call the decorator function as @decoractor_func on top of the actual function. So the flow will be, first the actual function will execute and then the output will be passed to the decorator function
# MAGIC 6. From that point onward, the decorator will take things forward and execute the extra functionality, i.e. to write the output to a specific location.

# COMMAND ----------

def write_delta(func):
    def wrapper():
        df=func()
        table_name=func.__name__
        df.write.format('delta').mode('overwrite').saveAsTable(f"skd_personal_bronze.new_schema.{table_name}")
        print('delta table created')
    return wrapper

# COMMAND ----------

@write_delta
def read_orders_w20():
    df= (
        spark.read.
        format('csv').
        option('header','true').
        option('inferSchema','true').
        load('abfss://week20assignment@stgsdpersonaldev.dfs.core.windows.net/landingzone/orders')
    )
    return df


# COMMAND ----------

df=read_orders_w20()

# COMMAND ----------

df=(spark.read.
        format('csv').
        option('header','true').
        option('inferSchema','true').
        load('abfss://week20assignment@stgsdpersonaldev.dfs.core.windows.net/landingzone/orders'))

# COMMAND ----------

display(df)

# COMMAND ----------

