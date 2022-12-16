# Databricks notebook source
# MAGIC %run ../src/file_access

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

df = spark.sql("show tables")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema bronze_pii_person;  

# COMMAND ----------

# MAGIC %sql
# MAGIC use default;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

def get_table_names(df):
    return set(map(lambda x: x.tableName, df.select('tableName').collect()))


# COMMAND ----------

get_table_names(df)

# COMMAND ----------

for x in get_table_names(df):
    spark.sql(f"drop table {x}")

# COMMAND ----------

container="bronze"

dbutils.fs.ls(f"abfss://{container}@{storage_account}.dfs.core.windows.net/people")

# COMMAND ----------

container="ingestion"

display(spark.read.json('abfss://ingestion@{storage_account}.dfs.core.windows.net/people'))


# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------


