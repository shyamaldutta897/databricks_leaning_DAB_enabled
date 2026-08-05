-- Databricks notebook source
use catalog skd_personal_silver

-- COMMAND ----------

drop table cleaned.customers_cleaned;
drop table cleaned.orders_cleaned;
drop table 

-- COMMAND ----------

select * from skd_personal_silver.cleaned.orders_cleaned_current where order_id=400