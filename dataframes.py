# Databricks notebook source
df = spark.read.csv("/FileStore/tables/weather-1.csv")

# COMMAND ----------

df.printSchema(header=true)

# COMMAND ----------

df.show()