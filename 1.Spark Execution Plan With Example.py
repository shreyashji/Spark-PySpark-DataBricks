# Databricks notebook source
in_part = spark.read.csv("/FileStore/tables/student.csv",header=True).toDF("ID","Name","Age")

# COMMAND ----------

# MAGIC %md
# MAGIC #####spark is lazy evaluated but Execution plan is not lazy evaluated

# COMMAND ----------

#we didnt trigger any action with the help of execution plan this stmt throw error msg 
#in_part.select('Name1')

# COMMAND ----------

in_part.select('Name').explain()

# COMMAND ----------

in_part.display()

# COMMAND ----------

from pyspark.sql.functions import col
in_part.filter("Age >=23 ").groupby('ID').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Explain
# MAGIC * simple
# MAGIC * extended
# MAGIC * cost
# MAGIC * codegen
# MAGIC * formated 

# COMMAND ----------

in_part.filter("Age >=23 ").groupby('ID').count().explain(mode='simple')

# COMMAND ----------

in_part.filter("Age >=23 ").groupby('ID').count().explain(mode='extended')

# COMMAND ----------

in_part.filter("Age >=23 ").groupby('ID').count().explain(mode='cost')

# COMMAND ----------

in_part.filter("Age >=23 ").groupby('ID').count().explain(mode='formatted')

# COMMAND ----------

in_part.filter("Age >=23 ").groupby('ID').count().explain(mode='simple')

# COMMAND ----------

in_part.filter("Age >=23 ").groupby('ID').count().explain(mode='codegen')