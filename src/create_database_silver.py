# Databricks notebook source
# MAGIC %run ./file_access

# COMMAND ----------

spark.conf.set('da.container', 'silver')
spark.conf.set('da.people_table_name', 'people_test_dynamic')
spark.conf.set('da.storage_account', storage_account)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists db_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema db_silver;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create external table if not exists ${da.people_table_name}
# MAGIC  (id STRING, name STRING, surname STRING)
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://${da.container}@${da.storage_account}.dfs.core.windows.net/${da.people_table_name}';

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------


