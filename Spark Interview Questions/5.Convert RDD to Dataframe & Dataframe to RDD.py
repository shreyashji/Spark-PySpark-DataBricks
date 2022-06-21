# Databricks notebook source
inp_list = list[['Azad'],['pandu'],['bangu']]

# COMMAND ----------

rdd1 = sc.parallelize(inp_list)

# COMMAND ----------

type(rdd1)

# COMMAND ----------

rdd1.collect()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Convert RDD to DF

# COMMAND ----------

df1= rdd1.toDF()
#or
#df1= spark.createDataframe(rdd1,['Name'])

# COMMAND ----------

type(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Convert df to rdd

# COMMAND ----------

rdd2 = df1.rdd()

# COMMAND ----------

type(rdd2)

# COMMAND ----------

#will be of row type ,not of not similar to rdd1 which is list type
rdd2.collect()