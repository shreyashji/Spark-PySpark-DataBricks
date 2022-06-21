# Databricks notebook source
# MAGIC %md
# MAGIC - scd type 1 : overwritten ,no history maintained
# MAGIC - scd type 2 : storing previous version of record as inactive & new as active,new record for each change of attributes, maintaining history, at the same time creating duplicate records of primary keys
# MAGIC - scd type 3 : instead of overwriting , we are going to create new column for each updated value

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE scd2demo (
# MAGIC                                   pk1 int,
# MAGIC                                   pk2 string,
# MAGIC                                   dim1 int,
# MAGIC                                   dim2 int,
# MAGIC                                   dim3 int,
# MAGIC                                   dim4 int,
# MAGIC                                   active_status string,
# MAGIC                                   start_date TIMESTAMP,
# MAGIC                                   end_date TIMESTAMP)
# MAGIC                                   USING DELTA
# MAGIC                                   LOCATION '/FileStore/tables/scd2Demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO scd2demo VALUES (111,'Unit1',200,500,800,400,'Y',current_timestamp(),'9999-12-31');
# MAGIC INSERT INTO scd2demo VALUES (222,'Unit2',900,Null,700,100,'Y',current_timestamp(),'9999-12-31');
# MAGIC INSERT INTO scd2demo VALUES (333,'Unit3',300,900,250,650,'Y',current_timestamp(),'9999-12-31');

# COMMAND ----------

from delta import *
targetTable = DeltaTable.forPath(spark,'/FileStore/tables/scd2Demo')
targetDF = targetTable.toDF()
display(targetDF)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
schema = StructType([StructField("pk1",StringType(),True),\
                     StructField("pk2",StringType(),True),\
                     StructField("dim1",IntegerType(),True),\
                     StructField("dim2",IntegerType(),True),\
                     StructField("dim3",IntegerType(),True),\
                     StructField("dim4",IntegerType(),True)])


# COMMAND ----------

data = [(111,'Unit1',200,500,800,400),
       (222,'Unit2',800,1300,800,500),
       (333,'Unit3',100,None,700,300)]

sourceDF = spark.createDataFrame(data=data,schema = schema)
display(sourceDF)

# COMMAND ----------

joindf = sourceDF.join(targetDF, (sourceDF.pk1==targetDF.pk1) & \
                      (sourceDF.pk2 == targetDF.pk2) & \
                      (targetDF.active_status =="Y"),"leftouter") \
                   .select(sourceDF["*"],\
                          targetDF.pk1.alias("target_pk1"),\
                          targetDF.pk2.alias("target_pk2"),\
                          targetDF.dim1.alias("target_dim1"),\
                          targetDF.dim2.alias("target_dim2"),\
                          targetDF.dim3.alias("target_dim3"),\
                          targetDF.dim4.alias("target_dim4"))

display(joindf)

# COMMAND ----------

filterDF=joindf.filter(xxhash64(joindf.dim1,joindf.dim2,joindf.dim3,joindf.dim4)!= xxhash64(joindf.target_dim1,joindf.target_dim2,joindf.target_dim3,joindf.target_dim4))
display(filterDF)

# COMMAND ----------

mergeDF = filterDF.withColumn("MERGEKEY",concat(filterDF.pk1,filterDF.pk2))

display(mergeDF)

# COMMAND ----------

dummydf = filterDF.filter("target_pk1 is not null").withColumn("MERGEKEY",lit(None))

display(dummydf)

# COMMAND ----------

scDF= mergeDF.union(dummydf)

display(scDF)

# COMMAND ----------

# MAGIC %md
# MAGIC - https://www.youtube.com/watch?v=GhBlup-8JbE&list=PLgPb8HXOGtsQeiFz1y9dcLuXjRh8teQtw&index=50&t=756s

# COMMAND ----------

