# Databricks notebook source
# MAGIC %run ../src/file_access

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop schema db_silver cascade;

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop schema db_bronze cascade;

# COMMAND ----------

containers = f"abfss://{container}@{storage_account}.dfs.core.windows.net/people"

# COMMAND ----------

containers = ['bronze', 'silver', 'gold']
folders = ['people', 'schemas', 'checkpoints']


for container in containers:
    for folder in folders:
        dbutils.fs.rm(f"abfss://{container}@{storage_account}.dfs.core.windows.net/{folder}", True)



# COMMAND ----------


