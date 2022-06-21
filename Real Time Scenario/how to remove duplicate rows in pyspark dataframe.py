# Databricks notebook source
# MAGIC %md
# MAGIC * Removing duplicate rows based on updated date

# COMMAND ----------

dbutils.fs.put("/scenarios/duplicates.csv","""id,name,loc,updated_date
1,ravi,bangalore,2021-01-01
1,ravi,chennai,2022-02-02
1,ravi,Hyderabad,2022-06-10
2,Raj,bangalore,2021-01-01
2,Raj,chennai,2022-02-02
3,Raj,Hyderabad,2022-06-10
4,Prasad,bangalore,2021-01-01
5,Mahesh,chennai,2022-02-02
4,Prasad,Hyderabad,2022-06-10
""")

# COMMAND ----------

df= spark.read.csv("/scenarios/duplicates.csv",header=True,inferSchema=True)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC * dropduplicates & distinct will not work

# COMMAND ----------

display(df.dropDuplicates()) 

# COMMAND ----------

display(df.distinct()) 

# COMMAND ----------

#dropduplicates on id basis
display(df.dropDuplicates(["id"])) 

# COMMAND ----------

#we need to consider lates data

# COMMAND ----------

from pyspark.sql.functions import col
display(df.orderBy(col("updated_date").desc()).dropDuplicates(["id"]))

# COMMAND ----------

# MAGIC %md
# MAGIC * Window function with row_number()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
df = df.withColumn("rowid",row_number().over(Window.partitionBy("id").orderBy(col("updated_date").desc())))

# COMMAND ----------

df_uniq = df.filter("rowid=1")

# COMMAND ----------

df_baddata = df.filter("rowid>1")

# COMMAND ----------

display(df_uniq)

# COMMAND ----------

