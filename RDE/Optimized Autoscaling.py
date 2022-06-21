# Databricks notebook source
# MAGIC %md
# MAGIC - databricks CHOSSES DYNAMICALLY the appropriate NUMBER OF WORKERS required to run the jobs based on RANGE OF NUMBER of workers
# MAGIC - its one of the performace optimization techniques
# MAGIC - it is also one of the cost saving technique
# MAGIC ##### autoscale is of two types OPTIMIZED & STANDARD
# MAGIC - two types of cluster : automated or job cluster(OPTIMIZED AUTOSCALING APPLIED) & all purpose or interactive cluster(depends on environment)
# MAGIC - premium pricing tier of environment :optimized auto scaling applied on that environment
# MAGIC - standard pricing tier : standard autoscaling applicable for that enviroment

# COMMAND ----------

# MAGIC %md
# MAGIC ##### OPTIMIZED
# MAGIC - scales up from min to max in 2 steps
# MAGIC - can scale down even if the cluster is not idle by looking at shuffle file state
# MAGIC - scales down based on percentage of current nodes
# MAGIC - on job cluster,scales down if the cluster is underutilized over the last 40 sec
# MAGIC - on all purpose cluster, scales down if the cluster is underutlized over the last 150 seconds
# MAGIC 
# MAGIC ##### STANDARD
# MAGIC - Starts with adding 8 nodes thereafter scales up exponentialy but can take many steps to reach the max
# MAGIC - but can be customized by  spark.databricks.autoscaling.standardFirstStepUp
# MAGIC - scales down only when the cluster is completely idle and ithas been underutilized for the last 10 minutes
# MAGIC - scales down exponentialy ,starting with 1 node

# COMMAND ----------

