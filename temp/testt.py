# Databricks notebook source
# MAGIC %python
# MAGIC display(spark.sql('show tables'))

# COMMAND ----------

storage_account="strgftterraformstate"
container="bronze"

dbutils.fs.ls(f"abfss://{container}@{storage_account}.dfs.core.windows.net/people")

# COMMAND ----------

spark.read.json('abfss://bronze@strgftterraformstate.dfs.core.windows.net/people').show()

# COMMAND ----------


