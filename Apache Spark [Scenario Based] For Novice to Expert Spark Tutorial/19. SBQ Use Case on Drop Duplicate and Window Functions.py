# Databricks notebook source
# MAGIC %md
# MAGIC ###max amount debited  from each customer on the latest date

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").option('inferSchema',True).csv("/FileStore/tables/personal_transaction.csv")

# COMMAND ----------

display(df1)


# COMMAND ----------

df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import date_format,to_date

df_cast = df1.withColumn("Date",to_date("Date","MM/dd/yyyy"))

# COMMAND ----------

display(df_cast)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Solution by changing conf to Legacy

# COMMAND ----------

spark.conf.get("spark.sql.legacy.timeParserPolicy") 
#by default exception

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

df_cast = df1.withColumn("Date",to_date("Date","MM/dd/yyyy"))

# COMMAND ----------

display(df_cast)

# COMMAND ----------

df_cast.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Filter transcation type on Debit alone

# COMMAND ----------

from pyspark.sql.functions import col

df_filter = df_cast.filter(col("Transaction Type")=="debit")

# COMMAND ----------

 display(df_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 1 : byUsing groupBy & Joins

# COMMAND ----------

# MAGIC %md
# MAGIC ####Using window 7 row_num : used in production

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# COMMAND ----------

windowSpec = Window.partitionBy('Customer_No').orderBy(col('Date').desc(),col('Amount').desc())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Assign row_num with Window spec

# COMMAND ----------

from pyspark.sql.functions import row_number
df_wind = df_filter.withColumn("row_num",row_number().over(windowSpec))

df_output2 = df_wind.filter("row_num=1").drop("row_num")

# COMMAND ----------

display(df_wind)

print("Final Output")
display(df_output2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Using order by & drop duplicates

# COMMAND ----------

from pyspark.sql.functions import col

df_output3 = df_filter.select("Customer_No","Date","Amount").orderBy('Customer_No',col('Date').desc(),col('Amount').desc()).drop_duplicates(['Customer_No'])

# COMMAND ----------

display(df_output3)

# COMMAND ----------

