# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog skd_personal_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS skd_personal_bronze.new_schema.book_inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS skd_personal_bronze.new_schema.book_inventory (
# MAGIC     book_id INT,
# MAGIC     title STRING,
# MAGIC     author STRING,
# MAGIC     price DECIMAL(5, 2),
# MAGIC     stock_quantity INT
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://bronze@stgsdpersonaldev.dfs.core.windows.net/external_new/books_inventory';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO skd_personal_bronze.new_schema.book_inventory VALUES
# MAGIC     (1, 'The Great Gatsby', 'F. Scott Fitzgerald', 10.99, 45),
# MAGIC     (2, 'To Kill a Mockingbird', 'Harper Lee', 12.50, 30),
# MAGIC     (3, '1984', 'George Orwell', 9.99, 85),
# MAGIC     (4, 'The Hobbit', 'J.R.R. Tolkien', 14.95, 20),
# MAGIC     (5, 'The Catcher in the Rye', 'J.D. Salinger', 11.25, 15);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skd_personal_bronze.new_schema.book_inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE skd_personal_bronze.new_schema.book_inventory
# MAGIC ALTER COLUMN book_id SET NOT NULL,
# MAGIC              stock_quantity SET NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE skd_personal_bronze.new_schema.book_inventory
# MAGIC ADD CONSTRAINT correct_num CHECK ((price>0) AND (stock_quantity>=0))

# COMMAND ----------

from decimal import Decimal

invalid_book_data = [
    # 1. Fails: book_id is None (NULL)
    (8, "Brave New World", "Aldous Huxley", Decimal("14.99"), 50),
    
    # 2. Fails: price is negative (-2.50)
    (6, "Fahrenheit 451", "Ray Bradbury", Decimal("-2.50"), 40),
    
    # 3. Fails: stock_quantity is negative (-15)
    (7, "Animal Farm", "George Orwell", Decimal("8.99"), -15)
]

book_schema="""book_id INT,
    title STRING,
    author STRING,
    price DECIMAL(5, 2),
    stock_quantity INT"""

invalid_book_df=spark.createDataFrame(invalid_book_data,book_schema)
invalid_book_df.createOrReplaceTempView('invalid_book_details')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY skd_personal_bronze.new_schema.book_inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO skd_personal_bronze.new_schema.book_inventory as tgt
# MAGIC USING invalid_book_details as src
# MAGIC ON src.book_id=tgt.book_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *