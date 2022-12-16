# Databricks notebook source
# MAGIC %run ../src/file_access

# COMMAND ----------

# MAGIC %sql
# MAGIC Show schemas;
# MAGIC use db_silver;
# MAGIC show tables;

# COMMAND ----------

df = spark.table('db_bronze.people')
df.show()

# COMMAND ----------

from spark_transformer.tranformations.executor import Executor
from spark_transformer.tranformations.drop import Drop
from spark_transformer.tranformations.hash import Hash

executor = Executor(spark)
hash_column = Hash('surname')
drop_column = Drop('name')
drop_column_rd = Drop('_rescued_data')

transformation_list = [hash_column, drop_column, drop_column_rd]
executor.apply_bulk(df, transformation_list).show()


# COMMAND ----------

df.show()

# COMMAND ----------


