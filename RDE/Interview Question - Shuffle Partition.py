# Databricks notebook source
# MAGIC %md
# MAGIC ###### Shuffling parameter
# MAGIC - what is : decides no. of partitions to be created during shuffle operation
# MAGIC - default : 200 is default value but configurable
# MAGIC - Get : spark.conf.get("spark.sql.shuffle.partitions")
# MAGIC - Set : spark.conf.set("spark.sql.shuffle.partitions",<value>)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### How to decide shuffle parameter
# MAGIC - there is no magic formula for the number of shuffle partitions to set for the shufffle stage
# MAGIC - the number may vary depending on your use case,data set,number of cores, and the amount of executor memory available
# MAGIC - its a trial & error approach

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Factors to consider
# MAGIC - USE CASE : For smaller data set 200 partition would split the data into much smaller chunks which adds disk & network overhead
# MAGIC - USE CASE : For large data set 200 partiton wouldgreat bigger chunks of data which leads to poor parallelism
# MAGIC 
# MAGIC - Factor : make sure the partition size would be atleast 128mb to 200 mb
# MAGIC - Factor : make sure number of partitons are multiples of number of cores (1xcores,2xcores,3xcores etc)

# COMMAND ----------

