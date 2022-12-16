# Databricks notebook source
# MAGIC %sql
# MAGIC create table bronze_person
# MAGIC (id int, name string, surname string);

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC INSERT INTO bronze_person
# MAGIC VALUES
# MAGIC (1, 'fuat', 'yilmaz')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from bronze_person

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC INSERT INTO bronze_person
# MAGIC VALUES
# MAGIC (2, 'john', 'smith'),
# MAGIC (3, 'adrina', 'lima')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from bronze_person

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database extended default

# COMMAND ----------

# MAGIC %python 
# MAGIC # default checkpoint location
# MAGIC dbutils.fs.ls('/local_disk0/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/local_disk0/checkpoint/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/local_disk0/data/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/local_disk0/schema/')

# COMMAND ----------

dbutils.fs.rm('dbfs:/local_disk0/checkpoint/', True)
dbutils.fs.rm('dbfs:/local_disk0/data/', True)
dbutils.fs.rm('dbfs:/local_disk0/schema/', True)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import functions as F
# MAGIC 
# MAGIC spark.table('bronze_person').withColumn('name', F.sha2('name', 256)).show()

# COMMAND ----------

# use default location
# .option("checkpointLocation", f"{DA.paths.checkpoints}/joined")


from pyspark.sql import functions as F

query = (spark.readStream.table("bronze_person")
                  .withColumn('name', F.sha2('name', 256))
                  .withColumn('surname', F.sha2('name', 256))
                  .writeStream
                  .option("checkpointLocation", "/local_disk0/tmp/default_db/bronze_person_hashing")
                  .queryName("bronze_person_hashing")
                  .table("silver_person"))

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls('/local_disk0/tmp/default_db/bronze_person_hashing')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_person order by id desc

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC INSERT INTO bronze_person
# MAGIC VALUES
# MAGIC (4, 'mark', 'smith'),
# MAGIC (5, 'joana', 'lima'),
# MAGIC (6, 'camila', 'lima')

# COMMAND ----------

def block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")
    
block_until_stream_is_ready(query)

# COMMAND ----------

for stream in spark.streams.active:
    print(stream.name)

# COMMAND ----------

for stream in spark.streams.active:
    print(f"Stopping {stream.name}")
    stream.stop()
    stream.awaitTermination()

# COMMAND ----------


