# Databricks notebook source
# MAGIC %md
# MAGIC - 1 permisive : include corrupt records in seprate column
# MAGIC - 2 Drop Malformed : ignore corrupt records
# MAGIC - 3 Fail fast : throw exception if corrupt record

# COMMAND ----------

#DEFINE SCEHAM

# COMMAND ----------

df = spark.read.format('csv').option('mode','DROPMALFORMED').option('header','true').schema(schema).load(path)

# COMMAND ----------

df = spark.read.format('csv').option('mode','PERMISSIVE').option('header','true').schema(schema).load(path)

# COMMAND ----------

df = spark.read.format('csv').option('mode','FAILFAST').option('header','true').schema(schema).load(path)