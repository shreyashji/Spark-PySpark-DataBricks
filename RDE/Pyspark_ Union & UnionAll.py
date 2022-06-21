# Databricks notebook source
# MAGIC %md
# MAGIC - UNION - to combine two dataframes.the schema of both dataframe should match.removes the resultant record from dataframe until spark version 2.0.0,
# MAGIC - so duplicate can be removed manually by using dropDuplicates()
# MAGIC - unionaAll - same as union but retians duplicate records as well in resultant dataframe 

# COMMAND ----------

#syntax 
df_union = df1.union(DF2)

# COMMAND ----------

df_unionall = df1.unionAll(DF2)