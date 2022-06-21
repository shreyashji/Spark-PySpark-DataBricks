# Databricks notebook source
path = '/FileStore/tables/uspop.csv'

# COMMAND ----------

df = spark.read.option('header',True).option('inferSchema',True).option('delimiter','|').csv(path)

# COMMAND ----------

lookup = ({
    'TX':'Texas',
    'NY':'New York',
    'OH':'Ohio',
    'CA':'California',
    'AZ':'Arizona',
    'CO':'Colorado',
    'IL':'Illinois'
})

# COMMAND ----------

df.show(5)

# COMMAND ----------

broad = sc.broadcast(lookup)

# COMMAND ----------

broad.value['NY']

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import *

# COMMAND ----------

def broadval(col):
    return broad.value[col]

# COMMAND ----------

funcreg = udf(broadval)

# COMMAND ----------

df.withColumn('state',funcreg('state_code')).show()

# COMMAND ----------

