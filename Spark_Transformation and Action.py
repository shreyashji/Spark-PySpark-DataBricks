# Databricks notebook source
# MAGIC %md
# MAGIC ####Source Files by Names

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")


# COMMAND ----------

# MAGIC %md
# MAGIC ####read the source dtaa to create dataframes

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema",True).option("header",True).option("sep",',').load("/FileStore/tables/auto_loan.csv")
display(df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Narrrow Transformation :Filter

# COMMAND ----------

df1 = df.filter(df.car_model == "sports cars")

# COMMAND ----------

df2 = df.filter(df.car_model == "luxury cars")

# COMMAND ----------

df3 = df1.union(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Wide Transformation : Group By

# COMMAND ----------

df4 = df3.groupBy("loan_status").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Calling Action : Collect

# COMMAND ----------

df4.collect()

# COMMAND ----------

