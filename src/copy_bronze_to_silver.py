# Databricks notebook source
# MAGIC %run ./file_access

# COMMAND ----------

# MAGIC %run ./common_functions

# COMMAND ----------

# MAGIC %run ./create_database_silver

# COMMAND ----------

source_table_name="db_bronze.people"
target_table_name="db_silver.people"

container="silver"
target_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
target_path_checkpoint = f"{target_path}/checkpoints/{container}/{target_table_name}"
target_path_schema = f"{target_path}/schemas/{target_table_name}"

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe table extended db_bronze.people;

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe table extended db_silver.people;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_silver.people;

# COMMAND ----------

def process_people(df):
    query = (df
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", target_path_checkpoint)
      .option("mergeSchema", True)
      .trigger(availableNow=True)
      .table(target_table_name))
    
    query.awaitTermination()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType



df = spark.readStream.table(source_table_name)\
.withColumn('id', F.sha2(F.col('id').cast(StringType()), 256))\
.withColumn('name', F.sha2('name', 256))\
.withColumn('surname', F.substring('surname', 0, 3))


# COMMAND ----------

process_people(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_silver.people;

# COMMAND ----------


