# Databricks notebook source
# MAGIC %md
# MAGIC - All this are APi provided by spark for developers fro data processing & analytics,in terms of functionality all are same and returns same output fro provided input data, but they differ in the way of handling and processing data, so there is a differenc ein terms of performance,user convenience & language support,User can choss any of the api while working with spark
# MAGIC Dataset=best of RDD(programming control OOPs,Type safety) & best of dataframe(relational format,optimization,memory mgmt)

# COMMAND ----------

# MAGIC %md
# MAGIC - Dataset = best of RDD(programming control OOPs,Type safety) & best of dataframe(relational format,optimization,memory mgmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### similarities
# MAGIC - fault tolerant : create logical plan ,based on that create lineage graph DAG(KEEP all flow in dag), IN CASE OF FAILURE CAN REBUILD IT BY dag
# MAGIC - Distributed : incoming data distributed between multiple executors in the form of partitions
# MAGIC - In memory parallel processing : 
# MAGIC - immutable : sourc3ee data cannot be modified,if we apply transform on source rdd ,it will create another rdd
# MAGIC - lazy evaluation : it won't process data immediately, construct logical plan or dag
# MAGIC - internally processing as RDD for all api's

# COMMAND ----------

# MAGIC %md
# MAGIC - RDD need to tell whatt to do &  how to do programmatically, DF &DS has optimizer so just tell what to do based on that it will create multiple logical plans & based on that it will chosse the best plan to construct physical plan
# MAGIC - RDD provide oops style api,DF provide SQL style api, DS provide oops style api
# MAGIC - RDD NO OPTIMIZER,DF catalyst optimizer,DS optimization
# MAGIC - RDD strong typesafety, less type safety, strong typesafety
# MAGIC - RDD compile time error,DF run time error, DS compile time error
# MAGIC - RDD-> java ,scala,python,r ,DF->java ,scala,python   ,DS -> JAVA & SCALA
# MAGIC - RDD no schema,DF->schema structured,DS->schema structured

# COMMAND ----------

