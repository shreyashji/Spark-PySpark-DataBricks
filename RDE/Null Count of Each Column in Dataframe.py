# Databricks notebook source
# MAGIC %md
# MAGIC - find null occurence of each column in dataframe

# COMMAND ----------

from pyspark.sql.functions import col,count,when
#logic
result = df.select([count( when(col(c).isNull(),c)).alias(c) for c in df.column ])

dispaly(result)