# Databricks notebook source
# MAGIC %md
# MAGIC ###### Map : if give 4 inputs we will get 4 outputs,applied on record/row level
# MAGIC 
# MAGIC ###### FlatMap : if give 1 input,we will get 4 ouput ,if single record give it will flatten the data & based on function we apply it will give more than single record in output

# COMMAND ----------

path ='/FileStore/tables/flatmap.csv'

# COMMAND ----------

in_rdd = sc.textFile(path)

# COMMAND ----------

in_rdd.collect()

# COMMAND ----------

in_rdd.count()

# COMMAND ----------

map_rdd = in_rdd.map(lambda x:x.split(','))

# COMMAND ----------

map_rdd.collect()

# COMMAND ----------

map_rdd.count()

# COMMAND ----------

flatmap_rdd = in_rdd.flatMap(lambda x:x.split(','))

# COMMAND ----------

flatmap_rdd.collect()

# COMMAND ----------

flatmap_rdd.count()

# COMMAND ----------

