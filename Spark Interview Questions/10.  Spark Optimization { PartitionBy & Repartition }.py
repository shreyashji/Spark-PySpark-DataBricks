# Databricks notebook source
path = '/FileStore/tables/personal_transaction.csv'

# COMMAND ----------

df = spark.read.format('csv').option('header',True).load(path)

# COMMAND ----------

display(df)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.repartition(3).rdd.getNumPartitions()

# COMMAND ----------

#optimized by spark itself
df.repartition(3,'Customer_No').rdd.getNumPartitions()

# COMMAND ----------

df.write.partitionBy('Customer_No').rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC #### repartiton by : data resides in memory
# MAGIC #### partitionby  data in disk