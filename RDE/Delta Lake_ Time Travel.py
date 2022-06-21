# Databricks notebook source
# MAGIC %md
# MAGIC - Table history

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history scd2demo

# COMMAND ----------

# MAGIC %md
# MAGIC - pyspark appraches

# COMMAND ----------

# MAGIC %md
# MAGIC - method 1 : pyspark - timestamp + table

# COMMAND ----------

df = spark.read \
    .format("delta") \
    .option("timeStampAsOf","2022-05-21T09:55:53.000*000") \
    .table("scd2demo")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - method 2 : pyspark - timestamp + path

# COMMAND ----------

df = spark.read \
    .format("delta") \
    .option("timeStampAsOf","2022-05-21T09:55:53.000*000") \
    .load("/FileStore/tables/scd2demo")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - method 3 : pyspark - version + path

# COMMAND ----------

df = spark.read \
    .format("delta") \
    .option("versionAsOf",2) \
    .load("/FileStore/tables/scd2demo")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - method 4 : pyspark - version + table

# COMMAND ----------

df = spark.read \
    .format("delta") \
    .option("versionAsOf",2) \
    .table("scd2demo")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - method 5 LSQL : Version + table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM scd2demo VERSION AS OF 2

# COMMAND ----------

# MAGIC %md
# MAGIC - method 6 LSQL : Version + PATH

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM "/FileStore/tables/scd2demo" VERSION AS OF 2

# COMMAND ----------

# MAGIC %md
# MAGIC - method 7 LSQL : timestamp + TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM scd2demo TIMESTAMP AS OF "2022-05-21T09:55:53.000*000"

# COMMAND ----------

# MAGIC %md
# MAGIC - method 8 LSQL : timestamp + PATH

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM "/FileStore/tables/scd2demo" TIMESTAMP AS OF "2022-05-21T09:55:53.000*000"