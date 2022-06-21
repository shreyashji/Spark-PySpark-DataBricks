# Databricks notebook source
df1 = spark.read.format("csv").option("header", "true").option('inferSchema',True).option("escape","\"").option("multiline",True).csv("/FileStore/tables/complex_json.csv")

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Using json_tuple

# COMMAND ----------

from pyspark.sql.functions import json_tuple,col,split
import json
import ast

# COMMAND ----------

df1.select(json_tuple("Type","fruits")).alias("jason_split").display()

# COMMAND ----------

df = df1.select(json_tuple("Type","fruits")).rdd\
                                            .map(lambda x:ast.literal_eval(x[0]))\
                                            .map(lambda x: x[0]['fruit']+','+x[1]['fruit']+','+x[2]['fruit'])

df= spark.read.option("escape","\"").option("multiline",True).csv(df)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##From Json

# COMMAND ----------

in_sch = spark.read.json(df1.select(col('Type').alias('jsoncol')).rdd.map(lambda x: x.jsoncol)).schema

# COMMAND ----------

in_sch

# COMMAND ----------

from pyspark.sql.functions import from_json,size

inp_json = df1.select('*', from_json("Type",in_sch).alias('jsonstr'))

# COMMAND ----------

display(inp_json)from pyspark.sql.functions import from_json,size

# COMMAND ----------

from pyspark.sql.functions import col
inp_json.select(
    col("jsonstr.fruits.fruit")[0].alais("fruit1"),
    col("jsonstr.fruits.fruit")[1].alais("fruit2"),
    col("jsonstr.fruits.fruit")[2].alais("fruit3")
).display()

# COMMAND ----------

