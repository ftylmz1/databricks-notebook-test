# Databricks notebook source
# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

my_param = "db_bronze"
print(my_param)

# COMMAND ----------

spark.conf.set("da.user_db", f"db_silver")


# COMMAND ----------

# MAGIC %sql 
# MAGIC use ${custom_user_db};

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------


