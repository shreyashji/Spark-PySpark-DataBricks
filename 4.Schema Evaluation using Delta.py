# Databricks notebook source
# MAGIC %md
# MAGIC ##Day 1 file

# COMMAND ----------

lst= [["arun","33"], ["sainin","34"],["rajat","35"],["peter","33"]]

# COMMAND ----------

 df = spark.sparkContext.parallelize(lst).toDF(["Name","Age"])

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.format("delta").mode("append").option("mergeSchema",True).saveAsTable("testDelta")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Day 2 file

# COMMAND ----------

lst1 = [["arun","33","BE"], ["sainin","34","ME"],["rajat","35","CSE"],["peter","33","ME"]]

# COMMAND ----------

 df1 = spark.sparkContext.parallelize(lst1).toDF(["Name","Age","Branch"])

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.write.format("delta").mode("append").option("mergeSchema",True).saveAsTable("testDelta")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select data from table

# COMMAND ----------

df2 = spark.sql("SELECT * FROM testDelta")

# COMMAND ----------

df2.show()

# COMMAND ----------

