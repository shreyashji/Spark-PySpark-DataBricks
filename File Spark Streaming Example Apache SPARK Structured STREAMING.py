# Databricks notebook source
sch = "ID int,Name string,Age int"
df_read = spark.readStream.option('header',True).schema(sch).csv('/FileStore/tables/')
display(df)

# COMMAND ----------

df_read.writeStream.format('memory').queryName('streaming_data').trigger(processingTime="10 seconds").outputMode('append').start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_data

# COMMAND ----------

