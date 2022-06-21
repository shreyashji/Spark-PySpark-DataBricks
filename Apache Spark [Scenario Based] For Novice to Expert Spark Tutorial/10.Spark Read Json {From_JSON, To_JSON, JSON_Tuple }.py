# Databricks notebook source
df1 =  spark.read.option("header",True).csv("/FileStore/tables/jsonto_fromjson.csv")
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####METHOD 1: Using JSON Tuple

# COMMAND ----------

from pyspark.sql.functions import col,json_tuple,to_json,from_json
#when we know the coln names only

# COMMAND ----------

df1.select("*",json_tuple("request","Response")).show()

# COMMAND ----------

df1.select("*",json_tuple("request","Response")).drop("request").select("*",json_tuple("c0","MesssageId","Latitude","longitude").alias("MesssageId","Latitude","longitude").drop("c0").show()

# COMMAND ----------

