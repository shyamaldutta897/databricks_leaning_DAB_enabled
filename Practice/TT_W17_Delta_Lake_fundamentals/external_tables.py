# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating an external location using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION stg_sd_personal_dev_external_location URL 
# MAGIC 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external' 
# MAGIC WITH(CREDENTIAL uc_credentials_dev) 
# MAGIC COMMENT "This is a schema external location"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Case 1 - Creating the table to an empty directory
# MAGIC ###### Databricks will dump the data for the first time to the shared location
# MAGIC ###### From next time onward, we can directly read from the location.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS skd_personal_bronze.new_schema.employees (
# MAGIC     employee_id INT PRIMARY KEY,
# MAGIC     first_name VARCHAR(50) NOT NULL,
# MAGIC     last_name VARCHAR(50) NOT NULL,
# MAGIC     department VARCHAR(50),
# MAGIC     salary DECIMAL(10, 2),
# MAGIC     hire_date DATE NOT NULL
# MAGIC )
# MAGIC location 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.employees (employee_id, first_name, last_name, department, salary, hire_date) 
# MAGIC VALUES 
# MAGIC (101, 'Alice', 'Smith', 'Engineering', 85000.00, '2022-03-15'),
# MAGIC (102, 'Bob', 'Johnson', 'Marketing', 62000.50, '2023-01-10'),
# MAGIC (103, 'Charlie', 'Brown', 'Engineering', 91000.00, '2021-06-22'),
# MAGIC (104, 'Diana', 'Prince', 'Finance', 78500.00, '2024-11-01'),
# MAGIC (105, 'Evan', 'Wright', 'HR', 55000.00, '2025-05-18');

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from skd_personal_bronze.new_schema.employees

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Even if we drop an external table, only schema gets dropped but data is always secured in it's position.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE skd_personal_bronze.new_schema.employees

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Which means, if we create the schema again, data can be fetched as if table was never deleted.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS skd_personal_bronze.new_schema.employees (
# MAGIC     employee_id INT PRIMARY KEY,
# MAGIC     first_name VARCHAR(50) NOT NULL,
# MAGIC     last_name VARCHAR(50) NOT NULL,
# MAGIC     department VARCHAR(50),
# MAGIC     salary DECIMAL(10, 2),
# MAGIC     hire_date DATE NOT NULL
# MAGIC )
# MAGIC location 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from skd_personal_bronze.new_schema.employees

# COMMAND ----------

# MAGIC %md
# MAGIC ### Case 2 - Creating external table with a directory having data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION stg_sd_personal_devexternal_loc_with_data URL 
# MAGIC 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_with_data' 
# MAGIC WITH(CREDENTIAL uc_credentials_dev) 
# MAGIC COMMENT "This is an external location"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS skd_personal_bronze.new_schema.bookstore_inventory (
# MAGIC     book_id VARCHAR(50),
# MAGIC     title VARCHAR(255),
# MAGIC     author VARCHAR(255),
# MAGIC     genre VARCHAR(100),
# MAGIC     price DECIMAL(10, 2),
# MAGIC     stock INT
# MAGIC )
# MAGIC USING CSV
# MAGIC location 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_with_data'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from skd_personal_bronze.new_schema.bookstore_inventory

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Now if we add new data to the directory which is already having data, Databricks can't sense that in terms of external table.
# MAGIC
# MAGIC ###### So in that case we need to explictely refresh the table, so that the new data can be fetched in.

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from skd_personal_bronze.new_schema.bookstore_inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE skd_personal_bronze.new_schema.bookstore_inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from skd_personal_bronze.new_schema.bookstore_inventory

# COMMAND ----------

# MAGIC %md
# MAGIC **Similar to Volumes, in case of External locations as well we can access all the files and folder stored in that specific location and build dfs out of them as well analyze**

# COMMAND ----------

# MAGIC %fs ls 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external'

# COMMAND ----------

# MAGIC %fs ls 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external/_delta_log/'

# COMMAND ----------

json_path='abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external/_delta_log/00000000000000000001.json'
df=spark.read.format('json').load(json_path)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Similarly, we can check the history of the table as well. As we see from the below examples, we can call history command either on the location where the table is created or on the table, the result will be similar**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY DELTA.`abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.employees

# COMMAND ----------

# MAGIC %sql DESCRIBE EXTENDED skd_personal_bronze.new_schema.employees

# COMMAND ----------

# MAGIC %md
# MAGIC The **INSERT OVERWRITE** command is used to replace all existing data in a table with the newly provided data. This command can be used with both **managed and external** tables

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE skd_personal_bronze.new_schema.employees (employee_id, first_name, last_name, department, salary, hire_date) 
# MAGIC VALUES 
# MAGIC (108, 'Jack', 'Smith', 'Engineering', 85000.00, '2022-03-15')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from skd_personal_bronze.new_schema.employees

# COMMAND ----------

# MAGIC %md
# MAGIC When we check history - we see 3 versions - 
# MAGIC
# MAGIC 1. When the table was first created, **Operation: CREATE TABLE**
# MAGIC 2. When data was first entered using *INSERT INTO*, **Operation:WRITE,mode:Append**
# MAGIC 3. When data was entered for the second time using *INSERT OVERWRITE*, **Operation:WRITE,mode:Overwrite**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.employees

# COMMAND ----------

# MAGIC %md
# MAGIC #Advanced Operations With External Tables and Delta Lake Behavior

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a new table and inserting data two times - the goal is to make sure that there should be two parquet files in the external location.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE skd_personal_bronze.new_schema.employee_external (
# MAGIC   employee_id INT,
# MAGIC   employee_name STRING,
# MAGIC   department STRING
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/emp_ext';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.employee_external (employee_id, employee_name, department)
# MAGIC VALUES 
# MAGIC   (101, 'Alice Smith', 'Engineering'),
# MAGIC   (102, 'Bob Jones', 'Marketing'),
# MAGIC   (103, 'Charlie Brown', 'Finance');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.employee_external (employee_id, employee_name, department)
# MAGIC VALUES 
# MAGIC   (104, 'Diana Prince', 'Operations'),
# MAGIC   (105, 'Evan Wright', 'Human Resources');

# COMMAND ----------

# MAGIC %md
# MAGIC When we check history -it is as expected, there will be two write operations and one create table operation

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.employee_external

# COMMAND ----------

# MAGIC %md
# MAGIC **1st Experiment** - 
# MAGIC
# MAGIC 1. Let's delete a row from the table.
# MAGIC 2. When we check the history, there will be two new operations - **DELETE** and **OPTIMIZE**
# MAGIC 3. In the History output, if we look in the **operationMetrics** field against DELETE row, we'll see an entry like **numDeletionVectorsAdded: "1"**
# MAGIC 4. Now this is an important concept in terms of Delete opearations in External Tables. 
# MAGIC     a. What is a Deleteion Vector - Whenever we delete rows from a table, there will be a .bin file added to the external location that holds delete information.\
# MAGIC     b. Then, on top of that, if we look inside the __delta_log JSON files - there will be two new JSON files added. One related to *DELETE* operations and the other related to *OPTIMIZE* operation.\
# MAGIC     c. In the JSON file, there are 3 fields - add, CommitInfo and remove.\
# MAGIC     d. If we check properly, we can see an entry like **numDeletionVectorsAdded: "1"** under CommitInfo field. At the same time, under **Remove** field we can see that the field from which we deleted the item is listed and all information related to that file is also listed. Which means that this file is being removed.\
# MAGIC     e. But at the same time, if we check under **Add** field, we'll see that the same file is listed again. But one important distinction is,there will be a section called **deletionVector** and it will hold items like **cardinality**, **offset** etc. This section is basically designed to notify the reader that read all the info from the specified file, except for the information that is registered in **deletionVector**. So that's how deletes are managed in terms of Delta Files.
# MAGIC 5. The role of **OPTIMIZE** operation - It bundles all file together into a new file, excluding the row items that are deleted. At the same time, the old files are logged under **Remove** and the new file is logged under **Add**. So this is a way to get rid of the too many small file problems.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM skd_personal_bronze.new_schema.employee_external WHERE employee_id = 105

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.employee_external

# COMMAND ----------

# MAGIC %fs ls 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/emp_ext'

# COMMAND ----------

# MAGIC %fs ls 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/emp_ext/_delta_log/'

# COMMAND ----------

json_path='abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/emp_ext/_delta_log/00000000000000000003.json'

df=spark.read.format('json').load(json_path)

# COMMAND ----------

display(df)

# COMMAND ----------

json_path='abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/emp_ext/_delta_log/00000000000000000004.json'

df=spark.read.format('json').load(json_path)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **2nd Experiment** - 
# MAGIC
# MAGIC Let's delete multiple rows from the same table that belongs to multiple files.
# MAGIC
# MAGIC In this case the behavior remains more of less the same like single delete, but as expected, under **Remove** field there will be multiple entries notifying that multiple files are impacted.
# MAGIC
# MAGIC At the same time, under the **Add** field as well, there will be equal amount of rows and every entry will have a **deletionVector** section to notify the reader which info is not supposed to be read.
# MAGIC
# MAGIC Finally, **OPTIMIZE** will tag the old files as removed and club all the remaining data into a new parquet file.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE skd_personal_bronze.new_schema.orders (
# MAGIC   order_id STRING,
# MAGIC   customer_id INT,
# MAGIC   order_date TIMESTAMP,
# MAGIC   total_amount DECIMAL(10, 2),
# MAGIC   status STRING
# MAGIC )
# MAGIC USING delta
# MAGIC location 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.orders (order_id, customer_id, order_date, total_amount, status)
# MAGIC VALUES 
# MAGIC   ('ORD-2026-001', 101, '2026-06-12 09:30:00', 150.50, 'Completed'),
# MAGIC   ('ORD-2026-002', 103, '2026-06-12 11:15:00', 45.00, 'Pending'),
# MAGIC   ('ORD-2026-003', 104, '2026-06-12 14:00:00', 299.99, 'Shipped');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.orders (order_id, customer_id, order_date, total_amount, status)
# MAGIC VALUES 
# MAGIC   ('ORD-2026-004', 102, '2026-06-12 16:45:00', 89.25, 'Completed'),
# MAGIC   ('ORD-2026-005', 105, '2026-06-12 18:20:00', 12.50, 'Cancelled');

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM skd_personal_bronze.new_schema.orders WHERE order_id IN ('ORD-2026-001','ORD-2026-005')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.orders

# COMMAND ----------

# MAGIC %fs ls abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext

# COMMAND ----------

# MAGIC %fs ls abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext/_delta_log/

# COMMAND ----------

json_path='abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext/_delta_log/00000000000000000003.json'

df=spark.read.format('json').load(json_path)
display(df)

# COMMAND ----------

json_path='abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext/_delta_log/00000000000000000004.json'

df=spark.read.format('json').load(json_path)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **3rd Experiment** - To update items in an external table
# MAGIC
# MAGIC Since update involves both deletion and addition, we get to see a similar behavior like DELETE but a bit advanced.
# MAGIC
# MAGIC So when we update a row in a table, similar to DELETE, there will be two operation in History Output - UPDATE and OPTIMIZE. Plus, even if we are not deleting any info, if we check upnder **operationMetrics** we can see an entry called **numDeletionVectorsAdded**. Which indicates that, Update is basically a combination of **DELETE** and **INSERT**.
# MAGIC
# MAGIC Next, upon checking the delta logs, we can see two new JSON files - similar tow DELETE.
# MAGIC
# MAGIC Let's say our updates impacted only one file - In that case, in the first JSON file, the **Remove** section will have only one row indicating the file that is impacted and of course the data that is changed as well.
# MAGIC
# MAGIC However the **Add** section will behave a bit differently. First, it will have the same file that is listed under **remove** section, but with a **deletionVector** to notify the reader which information not to be read. But there will be a new row item that will hold metadata of a new file as well. This file holds info that was updated.
# MAGIC
# MAGIC Next, **OPTIMIZE** will pick up both the files from **ADD** section from the previous JSON file and push to **REMOVE** section. Post that, it will club info from both the files into one parquet file and push under **ADD** section of the current JSON file.
# MAGIC
# MAGIC In short, we can sisualize the behavior of optimize in the way - 
# MAGIC
# MAGIC Let's say our original file is called **file1.parquet**. We update this file.
# MAGIC
# MAGIC So at first the delta log json files will be like - 
# MAGIC
# MAGIC **Remove - file1.parquet\
# MAGIC Add - file1. parquet with deletion vector\
# MAGIC Add - file2.parquet with updated info**
# MAGIC
# MAGIC Now comes **OPTIMIZE**. It will collate all information together in the new logs -  
# MAGIC
# MAGIC **Add file1.parquet\
# MAGIC Remove file1.parquet\
# MAGIC Remove file1.parquet with deletion vector\
# MAGIC Remove file2.parquet\
# MAGIC Add file3.parquet collating info from file1 with dv and file2**
# MAGIC
# MAGIC From the above operations, **first two operations gets nullified** right away since we are **adding and removing the same file**. Then remains 3rd and 4th operation. So eventually **OPTIMIZE** clubs both of these operations together and converts to a final file, let's say file3.parquet. In the meantime, it also removes the old files since they are not anymore required.
# MAGIC
# MAGIC Now when we do a select query on the table the information will be fetched from the new file.
# MAGIC
# MAGIC That's basically how UPDATE works.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE skd_personal_bronze.new_schema.orders
# MAGIC SET status = 'Cancelled'
# MAGIC WHERE order_id = 'ORD-2026-002'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.orders

# COMMAND ----------

# MAGIC %fs ls abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext

# COMMAND ----------

# MAGIC %fs ls abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext/_delta_log/

# COMMAND ----------

json_path='abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext/_delta_log/00000000000000000005.json'

df=spark.read.format('json').load(json_path)
display(df)

# COMMAND ----------

json_path='abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext/_delta_log/00000000000000000006.json'

df=spark.read.format('json').load(json_path)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Hidden command to see which file is holding **latest compute state** in a table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata from skd_personal_bronze.new_schema.orders

# COMMAND ----------

# MAGIC %md
# MAGIC The process with **INSERT OVERWRITE** is straightforward. All we do here is, overwrite the existing file.
# MAGIC
# MAGIC So there is only one operation **WRITE, with mode:Overwrite**
# MAGIC
# MAGIC What happens in the background is, **current file will be pushed to Remove state and a new file will be added to Add state.**

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE skd_personal_bronze.new_schema.orders 
# MAGIC
# MAGIC VALUES('ORD-2026-001', 101, '2026-06-12 09:30:00', 150.50, 'Completed'),
# MAGIC ('ORD-2026-002', 103, '2026-06-12 11:15:00', 45.00, 'Pending')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.orders

# COMMAND ----------

json_path='abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/orders_ext/_delta_log/00000000000000000007.json'

df=spark.read.format('json').load(json_path)
display(df)