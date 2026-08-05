# Databricks notebook source
my_catalog='skd_personal_bronze'
my_schema='new_schema'
my_volume='delta_volume_new'

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {my_catalog}.{my_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {my_catalog}.{my_schema}.{my_volume}")

volume_path=f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"
download_url="https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"

filename="baby_names.csv"

dbutils.fs.cp(download_url,volume_path + filename)