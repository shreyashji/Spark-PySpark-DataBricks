# Databricks notebook source
# MAGIC %md
# MAGIC - pyspark functions that split a single column into multiple columns based on certain logic

# COMMAND ----------

# MAGIC %md
# MAGIC ######## pyspark.sql.functions.split(str,pattern,limit=-1)
# MAGIC - str - a string expression to split
# MAGIC - pattern - a string representing a regular expression
# MAGIC - limit - optional ; an integer that controls the number of times pattern is applied 

# COMMAND ----------

outputdf= inputdf.withColumn('first_name',split(inputdf['Name'],' ').getitem(0))\
                  .withColumn('last_name',split(inputdf['Name'],' ').getitem(1))

# COMMAND ----------

