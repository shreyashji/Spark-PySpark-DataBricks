# Databricks notebook source
spark.read.format("csv").load(path)    
spark.read.format("csv").option("inferSchema",True).load(path)
spark.read.format("csv").schema(schema).load(path)

# COMMAND ----------

# MAGIC %md
# MAGIC - 1st method will create only 1 job : attach the data(residing in a file), scan the file,fetch 1 record out of that & calculate no. of columns
# MAGIC - 2nd method will create opnly 2 job : scan the file only once 7 fetch only 1 record out of that & calculate no of columns that is 1 job, after this it perform inferschema get datatype for each column so it scan entire data once again & get datatype
# MAGIC - 3rd mathod will create 0 job : explicitly specifying schema

# COMMAND ----------

df_no_option = spark.read.format("csv").load("/FileStore/tables/baby_names.csv")

# COMMAND ----------

df_inferschema = spark.read.format("csv").option("inferSchema",True).load("/FileStore/tables/baby_names.csv")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

schema = StructType([ \
                      StructField("Year",IntegerType(),False), \
                     StructField("FirstName",StringType(),True), \
                     StructField("Country",StringType(),True), \
                     StructField("Sex",StringType(),True), \
                     StructField("Count",IntegerType(),True)
                     ])

# COMMAND ----------

df_definedschema = spark.read.format("csv").schema(schema).load("/FileStore/tables/baby_names.csv")

# COMMAND ----------

