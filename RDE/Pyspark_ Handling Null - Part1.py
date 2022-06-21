# Databricks notebook source
#filter all records with null value
#STANDARD WAY
display(df.filter(df.columnname.isNull()))

# COMMAND ----------

#filter all records with null value
display(df.filter('column_name IS NULL '))

# COMMAND ----------

#filter all records withOUT null value
display(df.filter(df.columnname.isNotNull()))

# COMMAND ----------

#filter all records withOUT null value
display(df.filter(df.columnname.isNotNull()) & (df.columnname2.isNotNull()))