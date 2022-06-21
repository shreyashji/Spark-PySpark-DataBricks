# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import * 

schema=StructType((StructField("emp_id",IntegerType(),True),StructField("name",StringType(),True),StructField("city",StringType(),True),StructField("country",StringType(),True),StructField("contact_no",IntegerType(),True) ))

# COMMAND ----------

data = [(1000,"michel","columbus","usa",21123123)]

df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

#create delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dte_employees(
# MAGIC   emp_id int,
# MAGIC   name string,
# MAGIC   city string,
# MAGIC   country string,
# MAGIC   contact_no int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/FileStore/tables/delta_merge"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dte_employees

# COMMAND ----------

# MAGIC %md
# MAGIC - METHOD 1 :spark SQL

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dte_employees

# COMMAND ----------

# MAGIC %md
# MAGIC - syntax for performing merge operation

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dte_employees AS target
# MAGIC USING source_view as source
# MAGIC   ON target.emp_id  = source.emp_id
# MAGIC   WHEN MATCHED
# MAGIC   THEN UPDATE SET
# MAGIC   target.name = source.name,
# MAGIC   target.city = source.city,
# MAGIC   target.country = source.country,
# MAGIC   target.contact_no = source.contact_no
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT (emp_id,name,city,country,contact_no) VALUES (emp_id,name,city,country,contact_no)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dte_employees

# COMMAND ----------

data = [(1000,"michel","chicago","usa",21123123),(2000,"nancy","new york","usa",21123123)]

df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dte_employees

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dte_employees AS target
# MAGIC USING source_view as source
# MAGIC   ON target.emp_id  = source.emp_id
# MAGIC   WHEN MATCHED
# MAGIC   THEN UPDATE SET
# MAGIC   target.name = source.name,
# MAGIC   target.city = source.city,
# MAGIC   target.country = source.country,
# MAGIC   target.contact_no = source.contact_no
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT (emp_id,name,city,country,contact_no) VALUES (emp_id,name,city,country,contact_no)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dte_employees

# COMMAND ----------

# MAGIC %md
# MAGIC - METHOD 2 : Pyspark

# COMMAND ----------

data = [(2000,"Sarah","new york","usa",7922911),(3000,"David","atlanta","usa",21123123)]

df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

from delta.tables import *
delta_df = DeltaTable.forPath(spark,"/FileStore/tables/delta_merge")

# COMMAND ----------

delta_df.alias("target").merge(
    source = df.alias("source"),
    condition = "target.emp_id = source.emp_id" 
).whenMatchedUpdate(set =
    { "name":"source.name",
      "city":"source.city",
      "country":"source.country",
      "contact_no":"source.contact_no"
    }
  ).whenNotMatchedInsert(values =
    {
     "emp_id":"source.emp_id",
     "name":"source.name",
     "city":"source.city",
     "country":"source.country",
     "contact_no":"source.contact_no"
    }
  ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dte_employees

# COMMAND ----------

