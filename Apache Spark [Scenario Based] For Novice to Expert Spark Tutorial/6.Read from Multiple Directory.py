# Databricks notebook source
in_df = spark.read.option("header',True).csv(["Data1/*.csv","Data2/*.csv","Data3/*.csv"])

# COMMAND ----------

# MAGIC %md
# MAGIC ####By regex pattern,drawback regex is continuous 

# COMMAND ----------

in_df = spark.read.option("header',True).csv(["Data1[1-3]*/*.csv"])

# COMMAND ----------

# MAGIC %md
# MAGIC ####curly braces with selected items

# COMMAND ----------

in_df = spark.read.option("header',True).csv(["Data{1,2,3}*/*.csv"])