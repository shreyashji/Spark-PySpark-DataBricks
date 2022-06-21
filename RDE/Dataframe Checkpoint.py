# Databricks notebook source
# MAGIC %md
# MAGIC - checkpointing is a process of materializing the dataframe resultant data into storage directory
# MAGIC - its one of the performance optimization techniques
# MAGIC - though its is diffrerent from cache,looks similar in some aspects
# MAGIC - it boosts the performance for use-cases where particular dataframe gets iterated multiple times,it is useful in ML iterative algorithim where the plan may grow exponentialy

# COMMAND ----------

# MAGIC %md
# MAGIC - Checkpoint : writes into Disc,Truncates DAG
# MAGIC - Cache : Writes into memory,Retains DAG 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - Computation takes a long time :  there are 10 df, 3 df taking to long time  ,create checkpoint for that 3 df transformation,so time consuming compuation done only once and resultant data written to disc further subsequent transformation can refer the checkpointed data into disc
# MAGIC - Computation chain is too long : in our notebook there are 100s of transformation ,finally we are calling an action ,that action would traverse across all transformation in order to cutshot we can go for checkpointing

# COMMAND ----------

