# Databricks notebook source
# MAGIC %sql
# MAGIC Describe history testdelta

# COMMAND ----------

df = spark.sql("""select * from testdelta version as of 16 where Name ="arun" and Branch is null""")


# COMMAND ----------

