# Databricks notebook source
lst =[]
for i in range (10000):
    lst.append([str(i),"name"+str(i)])

# COMMAND ----------

in_data = spark.sparkContext.parallelize(lst)

# COMMAND ----------

in_df = spark.createDataFrame(in_data,["id","Name"])

# COMMAND ----------

in_df.show(10)

# COMMAND ----------

in_df.rdd.getNumPartitions()

# COMMAND ----------

in_df.coalesce(1).write.mode("overwrite").saveAsTable("unbucket_demo1")

# COMMAND ----------

in_df.coalesce(1).write.bucketBy(4,"id").sortBy("id").mode("overwrite").saveAsTable("bucket_demo1")

# COMMAND ----------

df_read1 = spark.table("unbucket_demo1")
df_read1.write("id='1000'").show()

# COMMAND ----------

df_read = spark.table("bucket_demo1")
df_read.write("id='1000'").show()

# COMMAND ----------

spark.sql("Describe Extended bucket_demo1").show()

# COMMAND ----------

