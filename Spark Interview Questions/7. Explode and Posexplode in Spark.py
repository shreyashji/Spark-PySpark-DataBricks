# Databricks notebook source
df1 = spark.read.option('header',True).option('delimiter','|').csv('/FileStore/tables/explodepose.csv')


# COMMAND ----------

from pyspark.sql.functions import explode_outer,split
df1.withColumn("Qualification",explode_outer(split('EDUCATION',',')))

# COMMAND ----------

from pyspark.sql.functions import posexplode_outer,split
df1.select('*',posexplode_outer(split('EDUCATION',','))).withColumnRenamed('col','Qualification').withColumnRenamed('pos','Index').drop('EDUCATION').show()

# COMMAND ----------

