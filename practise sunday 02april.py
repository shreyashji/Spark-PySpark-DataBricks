# Databricks notebook source
# import pyspark class Row from module sql
from pyspark.sql import *
df = spark.read.json("/FileStore/tables/people-1.json")


# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt

# COMMAND ----------

##broadcast join 
val df1= spark.range(1000).as("a")
val df2= spark.range(1000).as("b")
val df3= spark.range(1000).as("c")

# COMMAND ----------

#broadcast join
val q =  df1.join(df2).where($"a.id" === $"b.id")

# COMMAND ----------

#broadcast join with sql 
df4.createOrReplaceTempView("df4")
df5.createOrReplaceTempView("df5")

# COMMAND ----------

spark.sql("Select * from df4.inner join df5 on df4.id == df5.id").explain 