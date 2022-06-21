# Databricks notebook source
# MAGIC %md
# MAGIC - single & multiple condition
# MAGIC - starts with
# MAGIC - end with
# MAGIC - contains
# MAGIC - Like
# MAGIC - NULL values
# MAGIC - isin

# COMMAND ----------

filter(df.column != 50)
filter((f.col('column1') > 50) && (f.col('column2') > 50))
filter((f.col('column1') > 50) | (f.col('column2') > 50))
filter(df.column.isNull())
filter(df.column.isNotNull())
filter(df.column.like('%%'))
filter(df.name.isin)
filter(df.column.contains(''))
filter(df.column.startsWith(''))
filter(df.column.endsWith(''))