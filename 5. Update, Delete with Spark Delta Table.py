# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM testDelta where Name='arun'

# COMMAND ----------

# MAGIC %md
# MAGIC ##UPDATE/DELETE USING sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM testDelta where Name='arun' and Branch is null
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE testDelta SET Age=23  where Name='arun' and Branch is null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM testDelta where Name='arun' and Branch is null

# COMMAND ----------

# MAGIC %sql
# MAGIC ---UPDATE delta, "/user/hive/warehouse/testdelta" SET Age=43  where Name='arun' and Branch is null

# COMMAND ----------

# MAGIC %md
# MAGIC ##pythonic  way to Update

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, "/user/hive/warehouse/testdelta")

# COMMAND ----------

deltaTable.update("Name='arun' and Branch is null",{"Age" : "24" })

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM testDelta where Name='arun' and Branch is null

# COMMAND ----------

# MAGIC %sql
# MAGIC delete FROM testDelta where Name='arun' and Branch is null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM testDelta where Name='arun' 

# COMMAND ----------

