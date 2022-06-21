# Databricks notebook source
# MAGIC %md
# MAGIC Data partitioning is critical to data processing performance especially for large volume of data processing in Spark.
# MAGIC pyspark : How to write dataframe partition by year/month/ sub-directory?

# COMMAND ----------

df_emp_csv  = spark.read.option("nullValue","null").csv("/FileStore/tables/emp1.csv",header=True,inferSchema=True)
display(df_emp_csv)

# COMMAND ----------

from pyspark.sql.functions import to_date
#Change string to Date DataType
df_emp_csv = df_emp_csv.withColumn("HIREDATE",to_date("HIREDATE",'dd-MM-yyyy')).fillna({"HIREDATE":'9999-12-31'})
df_emp_csv.show()

# COMMAND ----------

from pyspark.sql.functions import date_format
#creating two YEAR and MONTH new columns based on hiredate date field
df_emp_csv = df_emp_csv.withColumn("YEAR",date_format("HIREDATE",'yyyy')).withColumn("MONTH",date_format("HIREDATE",'MM'))
df_emp_csv.show()

# COMMAND ----------

# using PartitionBy with two columns creating partitions
df_emp_csv.write.format("delta").partitionBy("YEAR","MONTH").mode("overwrite").saveAsTable("emp_part")

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/emp_part/YEAR=1980/MONTH=12/

# COMMAND ----------

# MAGIC %sql
# MAGIC explain select * from emp_part where year='1980'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_part where year='1980'

# COMMAND ----------

