# Databricks notebook source
# MAGIC %md
# MAGIC - Numnber of records in dataframe

# COMMAND ----------

print(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC - Default partiton count

# COMMAND ----------

print(df.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC - Number of records per partition

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
df.withColumn('partitonId',spark_partition_id()).groupBy('partitonId').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - repartition the dataframe to 5

# COMMAND ----------

df_5 = df.select(df.Year,df.country,df.sex,df.count).repartition(5)

# COMMAND ----------

# MAGIC %md
# MAGIC - get number of partition in the dataframe

# COMMAND ----------

print(df_5.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC - get number of records per partition

# COMMAND ----------

df.withColumn('partitonId',spark_partition_id()).groupBy('partitonId').count().show()