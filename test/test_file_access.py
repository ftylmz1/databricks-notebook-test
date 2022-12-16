# Databricks notebook source
# MAGIC %run ../src/file_access

# COMMAND ----------

container="bronze"
display(dbutils.fs.ls(f"abfss://{container}@{storage_account}.dfs.core.windows.net/people/_delta_log"))

# COMMAND ----------

container="ingestion"

display(spark.read.json(f'abfss://{container}@{storage_account}.dfs.core.windows.net/people'))|


# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

container="ingestion"
spark.read.json(f'abfss://{container}@{storage_account}.dfs.core.windows.net/people').printSchema()

# COMMAND ----------


