# Databricks notebook source
assert spark.table("beans").filter("name='pinto'").count() == 1, "There should only be 1 entry for pinto beans"
row = spark.table("beans").filter("name='pinto'").first()
assert row["color"] == "brown", "The pinto bean should be labeled as the color brown"
assert row["grams"] == 1500, "Make sure you correctly specified the `grams` as 1500"
assert row["delicious"] == True, "The pinto bean is a delicious bean"


assert spark.table("beans"), "Table named `beans` does not exist"
assert spark.table("beans").columns == ["name", "color", "grams", "delicious"], "Please name the columns in the order provided above"
assert spark.table("beans").dtypes == [("name", "string"), ("color", "string"), ("grams", "float"), ("delicious", "boolean")], "Please make sure the column types are identical to those provided above"


assert spark.table("beans").count() == 6, "The table should have 6 records"
assert spark.conf.get("spark.databricks.delta.lastCommitVersionInSession") == "2", "Only 3 commits should have been made to the table"
assert set(row["name"] for row in spark.table("beans").select("name").collect()) == {'beanbag chair', 'black', 'green', 'jelly', 'lentils', 'pinto'}, "Make sure you have not modified the data provided"


# COMMAND ----------

# MAGIC %run ../src/file_access

# COMMAND ----------

tbl_bronze_people = spark.table("db_bronze.people")
tbl_silver_people = spark.table("db_silver.people")

# COMMAND ----------

tbl_bronze_people.show()

# COMMAND ----------

display(tbl_silver_people)

# COMMAND ----------

assert tbl_bronze_people.count() == tbl_silver_people.count(), "Row count is not equal"
assert tbl_bronze_people.dtypes == [("id", "int"), ("name", "string"), ("surname", "string"), ("_rescued_data", "string")], "Schema does not match"
assert tbl_silver_people.dtypes == [("id", "string"), ("name", "string"), ("surname", "string"), ("_rescued_data", "string")], "Schema does not match"
