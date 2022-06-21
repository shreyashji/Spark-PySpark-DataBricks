# Databricks notebook source
# MAGIC %md
# MAGIC ##### RBO(RULE BASED OPTIMIZER) VS CBO(COST BASED OPTIMIZER)
# MAGIC * RBO - Set of predefined rules is used to design the logical plan,(blindly follow predefined rule)
# MAGIC * CBO - (best)based on available statistics,collection of underlying data,cost is estimated and best efficient execution approach is decided based on cost

# COMMAND ----------

# MAGIC %md
# MAGIC * front end->sql api,dataframe,dataset
# MAGIC * optimizer->unresolved logical plan->logical plan->optimized logical plan->
# MAGIC * backend-> physicla plan->cost model ->selected physical plan->execute Native RDD

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cache vs Persist
# MAGIC - cache is programming mechanism which gives an option to store data in memory across nodes
# MAGIC - persist is programming mechanism which gives an option to store data either in memory or in disc across nodes

# COMMAND ----------

