# Databricks notebook source
# MAGIC %md
# MAGIC - pthon functions that returns a string by joining all tthe elments of an iterable data,such as list,tuple etc seprated by string seprator 

# COMMAND ----------

# MAGIC %md
# MAGIC ###### string_seprator.join(iterable data)
# MAGIC - seprator string using which iterable element get joined 
# MAGIC - join- keyword of join function
# MAGIC - iterable data - data such as list or tuple which can be iterated for joining

# COMMAND ----------

joined_streing - ",".join(column_list)