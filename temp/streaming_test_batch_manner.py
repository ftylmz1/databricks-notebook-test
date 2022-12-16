# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC create table bronze_person_json
# MAGIC (id int, name string, surname string)

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls('dbfs:/local_disk0/data/user/part1.json')

# COMMAND ----------

def process_bronze():
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "json")
                  .option("cloudFiles.schemaLocation", f"/local_disk0/schema/bronze_schema")
                  .load('/local_disk0/data/user')
                  .writeStream
                  .option("checkpointLocation", f"/local_disk0/checkpoint/bronze_test2")
                  .trigger(availableNow=True)
                  .table("bronze_test2"))
    
    query.awaitTermination()

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from bronze_test2 order by id desc

# COMMAND ----------

# IF DELETE FILES FROM SOURCE LOCATION AND UPDATE TABLE, THE DATA LOADED INTO THE TABLE IS NOT DELETED
