# Databricks notebook source
# MAGIC %run ./file_access

# COMMAND ----------

spark.conf.set('da.container', 'bronze')
spark.conf.set('da.people_table_name', 'people_test_dynamic')
spark.conf.set('da.storage_account', storage_account)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists db_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema db_bronze;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create external table if not exists ${da.people_table_name}
# MAGIC  (id INT, name STRING, surname STRING)
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://${da.container}@${da.storage_account}.dfs.core.windows.net/${da.people_table_name}';

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended people_test_dynamic

# COMMAND ----------


