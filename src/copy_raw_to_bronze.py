# Databricks notebook source
# MAGIC %run ./file_access

# COMMAND ----------

# MAGIC %run ./common_functions

# COMMAND ----------

# MAGIC %run ./create_database_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema db_bronze;

# COMMAND ----------

source_table_name="people"
target_table_name="people"

source_container="ingestion"
target_container="bronze"
source_path = f"abfss://{source_container}@{storage_account}.dfs.core.windows.net"
target_path = f"abfss://{target_container}@{storage_account}.dfs.core.windows.net"

target_path_checkpoint = f"{target_path}/checkpoints/{target_container}/{target_table_name}"
target_path_schema = f"{target_path}/schemas/{target_table_name}"

source_path_people = f"{source_path}/{source_table_name}"

# COMMAND ----------

def process_people():
    query = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaHints", "id INT")
        .option("cloudFiles.schemaLocation", target_path_schema)
        .load(source_path_people)
        .writeStream
        .option("checkpointLocation", target_path_checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .table(target_table_name))

    query.awaitTermination()

# COMMAND ----------

process_people()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended people;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended db_bronze.people;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_bronze.people;

# COMMAND ----------


