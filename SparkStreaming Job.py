# Databricks notebook source
#dbutils.fs.rm('/FileStore/tables/',True)

# COMMAND ----------

###Spark Streaming Example with PySpark | Apache SPARK Structured STREAMING
sch = "ID int,Name string,Age int"
df = spark.readStream.option('header',True).schema(sch).csv('/FileStore/tables/')
display(df)


# COMMAND ----------

#append or complete or update,WRITING IN CONSOLE
df.writeStream.format('console').option('maxfilespertrigger',1).outputMode('append').start()

# COMMAND ----------

df.writeStream.format('console').trigger(processingTime="10 seconds").outputMode('append').start()

# COMMAND ----------

display(df)


# COMMAND ----------

