# Databricks notebook source
# MAGIC %md
# MAGIC #### non uniform distribution of data across  the partitions in cluster is called skewness
# MAGIC #### skewness affect parallelism,processing will be late ,inconsistency in processing time 
# MAGIC ##### partition on wrong coln
# MAGIC ##### adaptive querry execution framework in spark 3.0

# COMMAND ----------

# MAGIC %md
# MAGIC ##### in spark 2.0 we can use Repartiton : if only single dominant partitionid it will fail
# MAGIC ##### salting technique :we have a partition which added with random values & end result will be another partiton
# MAGIC ##### Reduce data skew effect before uploading :like if we know we have a dominant partiton id then its better to select other coln along with skew partition id
# MAGIC ##### Bump up spark.sql.autoBroadcastJoinThreshold : incr its Threshold value of BroadcastJoin in spark config which will incre liklihood of spark engine to chosse broadcast join in preference with sortmerge join,which will be usefull for skewness of data
# MAGIC ##### Iterative (chuncked) broadcast join : larger dataset is sliced into smaller chunk 7 this junk again broadcasted to join with huge volume of data & finally all the resource will be unioned to a single dataset

# COMMAND ----------

