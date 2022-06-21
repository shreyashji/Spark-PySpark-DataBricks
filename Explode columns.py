# Databricks notebook source
from pyspark.sql.functions import col, explode
simpleData = [(1,["sagar","choudhary"]),\
              (2,["shivam","Gupta"]),
              (3,["kunal","khemu"])   
]
columns = ["ID","Name"]
df_1 = spark.createDataFrame(data = simpleData,schema = columns)
df_1.show()

# COMMAND ----------

df_output = df_1.select(col("ID"),explode(col("Name")).alias("Name"))
df_output.show()

# COMMAND ----------

