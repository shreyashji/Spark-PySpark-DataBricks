# Databricks notebook source
df1 = spark.read.option("header",True).option("inferSchema",True).csv("/FileStore/tables/dates.csv")

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df1.show()

# COMMAND ----------

from pyspark.sql.functions import date_add,to_date,col,expr

# COMMAND ----------

df1.select(to_date(col("RechargeDate").cast("string"),"yyyyMMdd")).show()

# COMMAND ----------

df2 = df1.withColumn("Date_S",to_date(col("RechargeDate").cast("string"),"yyyyMMdd"))

# COMMAND ----------

df2.show()

# COMMAND ----------

df2.select("*",expr("date_add(Date_S,Remaining_days)")).show()

# COMMAND ----------

