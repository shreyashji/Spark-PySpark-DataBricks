# Databricks notebook source
# MAGIC %md
# MAGIC - UDF : user defined function is a piece of code which perform certain task and can be reused to perform the same task across multiple scenario
# MAGIC - udf - black box (NOT RECOMMENDED),spark cannot applied any optimization on UDF,its very expensive
# MAGIC - try to minimize the usage of UDF and apply built in functions wherever possible
# MAGIC - UDF are created in python or scala,but dataframes are in JVM format,so when we call UDF to execute certain task it would happen through java api which require data serialization/deserialization to perform the task and as UDF is black box to spark (as not in JVM), it cant apply optimization techniques by default

# COMMAND ----------

#SYNTAX
def UDF_NAME(parameters):
    --code to perform the task
    return RETURN_OUTPUT