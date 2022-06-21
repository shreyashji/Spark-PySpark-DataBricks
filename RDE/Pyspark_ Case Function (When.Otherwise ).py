# Databricks notebook source
# MAGIC %md
# MAGIC - To evaluate list of conditions and chosse a result path according to the matching condition,when().otherwise() function in pyspark can be used
# MAGIC - this is similar to case or switch stmt in other prog. language
# MAGIC - when no condition is matching ,otherwise result path would be chosen

# COMMAND ----------

df.withColumn('new or existing column' ,when(condition1, Result1)
                                       .when(condition2, Result2)
                                       .when(conditionN, ResultN)
                                       .otherwise(Result))

# COMMAND ----------

df.withColumn('new or existing column' ,expr("CASE WHEN condition1 THEN Result1"+
                                                   "WHEN condition2 THEN Result2"+
                                                   "WHEN conditionN THEN ResultN"+
                                                   "ELSE Result END" ))

# COMMAND ----------

# MAGIC %md
# MAGIC -  to combine multiple conditios : '&' for AND ; '|' for OR

# COMMAND ----------

