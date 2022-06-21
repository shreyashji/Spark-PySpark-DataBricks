# Databricks notebook source
# MAGIC %md
# MAGIC - What is data skew
# MAGIC - Data Skew is a condition in which a table's data unevenly distributed among partitions in the cluster
# MAGIC - data skew can certains downgrades performace of queries, especially those with joins
# MAGIC - joins with big tables require shuffling data and the skew can lead to an extreme imbalance of work in the cluster
# MAGIC - its  likely that data skew is affecting a query,if a query appears to be stuck finishing very few tasks (for example,the last 3 tasks out of 200)

# COMMAND ----------

# MAGIC %md
# MAGIC - SALT THE SKEWED COLUMN with a random number creating a better distribution across each partition
# MAGIC - APPLY SKEW HINT,with the information from these hins, spark can construct a better query plan,one that does not suffer from data skew
# MAGIC - use BROADCAST JOIN for smaller tables
# MAGIC - enable ADAPTIVE QUERY EXECUTIUON if you are using spark 3 which will balance out the partitions for us automtically

# COMMAND ----------

# MAGIC %md
# MAGIC - AQE
# MAGIC - dynamically coalace shuffle partitions
# MAGIC - dynamically switch join strategies
# MAGIC - dynamically optimize skew joins

# COMMAND ----------

