# Databricks notebook source
# MAGIC %md
# MAGIC ####How to change Default Partition - 200 ?

# COMMAND ----------

df1 = spark.read.format("csv") \
                .option("header",True) \
                .load("dbfs:/FileStore/shared_uploads/shreyashchoudhary7789@gmail.com/googleplaystore.csv")\
                .repartition(3)

# COMMAND ----------

display(df1)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Get number of Partitions

# COMMAND ----------

df1.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformation & Actions with Shuffle

# COMMAND ----------

df2 = df1.groupBy("Category").count()

# COMMAND ----------

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Number of parts after transformation

# COMMAND ----------

df2.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Spark shuffle default config

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ##change the setting

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",100)

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

display(df2)

# COMMAND ----------

