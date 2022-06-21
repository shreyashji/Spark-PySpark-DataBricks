# Databricks notebook source
df = spark.read.option("header","true").option("delimiter","|").csv("/FileStore/tables/uspop.csv")


# COMMAND ----------

lookup = dict({
    "TX":"Texas",
    "NY":"New York",
    "OH":"Ohio",
    "CA":"California",
    "IL":"Illinois",
    "CO":"Colorado",
    "AZ":"Arizona"
})

# COMMAND ----------

df.show(5)

# COMMAND ----------

broad = sc.broadcast(lookup)

# COMMAND ----------

broad.value["NY"]

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import *

# COMMAND ----------

def broadval(col):
    return broad.value[col]

# COMMAND ----------

funcreg = udf(broadval)

# COMMAND ----------

df.withColumn("state",funcreg("state_code")).show()

# COMMAND ----------

