# Databricks notebook source
# MAGIC %md
# MAGIC - PartitionBy - its a function used to write the dataframe into disc partitioned by specific key(s)

# COMMAND ----------

#df.write.PartitionBy(key).csv('path')

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/baby_names.csv")


# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).option('sep',',').load('/FileStore/tables/baby_names.csv')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - distinct year list & count for each year

# COMMAND ----------

df.groupBy('Year').count().show(truncate=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC - partiton by one key column

# COMMAND ----------

df.write.option('header',True) \
        .partitionBy('Year') \
        .mode('overwrite') \
        .csv('/FileStore/tables/baby_names_output')


# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/baby_names_output/',True)

dbutils.fs.mkdirs('/FileStore/tables/baby_names_output')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### partition by multiple keys

# COMMAND ----------

df.write.option('header',True) \
        .partitionBy('Year','Sex') \
        .mode('overwrite') \
        .csv('/FileStore/tables/baby_names_output')

# COMMAND ----------

# MAGIC %md
# MAGIC - partition by key column along with maximumm number of record for each partition

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/baby_names_output/',True)

dbutils.fs.mkdirs('/FileStore/tables/baby_names_output')

# COMMAND ----------

df.write.option('header',True) \
        .option('maxRecordsPerFile',4200) \
        .partitionBy('Year') \
        .mode('overwrite') \
        .csv('/FileStore/tables/baby_names_output')

# COMMAND ----------

