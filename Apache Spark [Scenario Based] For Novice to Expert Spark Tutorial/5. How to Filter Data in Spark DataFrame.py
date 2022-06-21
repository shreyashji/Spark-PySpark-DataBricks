# Databricks notebook source
df = spark.read.csv("/FileStore/tables/student.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##method -1 Using TempView

# COMMAND ----------

df= spark.read.option('delimiter',',').csv('/FileStore/tables/student.csv',header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.createOrReplaceTempView("Filter_view")

# COMMAND ----------

df_filter = spark.sql(""" SELECT * FROM Filter_view WHERE Age > 23""")
df_filter.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 2 : Using Filter() api

# COMMAND ----------

df_filter1 = df.filter("Age >24" and "ID>2")
df_filter1.show()

# COMMAND ----------

