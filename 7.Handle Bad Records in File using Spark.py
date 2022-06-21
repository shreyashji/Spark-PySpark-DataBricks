# Databricks notebook source
# MAGIC %md
# MAGIC ##Display content in JSON

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/ford.json

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read the json File

# COMMAND ----------

in_df = spark.read.json("/FileStore/tables/ford.json")
display(in_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Modes  used in Reading
# MAGIC * permissive : default
# MAGIC * drop malformed  : remove corupted records,show valid only
# MAGIC * failfast : throw error

# COMMAND ----------

in_df = spark.read.option("mode","failfast") \
            .json("/FileStore/tables/ford.json")
display(in_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##fEATURE AVILABLE IN DATABRICS TO SAVE Bad Records

# COMMAND ----------

in_df = spark.read.option("badRecordsPath", "/FileStore/tables/badrecord_ford") \
            .json("/FileStore/tables/ford.json")
display(in_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###View content in bad record paths

# COMMAND ----------

bad_df = spark.read.json("/FileStore/tables/badrecord_ford/*/bad_records/*")
display(bad_df)

# COMMAND ----------

