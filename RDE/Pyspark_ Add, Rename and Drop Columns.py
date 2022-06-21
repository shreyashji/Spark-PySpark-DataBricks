# Databricks notebook source
# MAGIC %md
# MAGIC #### ADD COLN

# COMMAND ----------

#syntax
df.withColumn('new_column_name',value)
df.withColumn('city',lit('mumbai'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename Column

# COMMAND ----------

df.withColumnRenamed('old_name','new_name')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop Column

# COMMAND ----------

df.drop("Column_name")