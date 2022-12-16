# Databricks notebook source
def stop_all_streams():
    for stream in spark.streams.active:
      print(f"Stopping {stream.name}")
      stream.stop()
      stream.awaitTermination()

# COMMAND ----------

def block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")

# COMMAND ----------


