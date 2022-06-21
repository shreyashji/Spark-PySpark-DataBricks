# Databricks notebook source
# MAGIC %md
# MAGIC Importing text file with varying number of columns in PySpark?
# MAGIC variable size of columns reading in pyspark?
# MAGIC How to create dynamic columns with variable size of columns in pyspark dataframe?

# COMMAND ----------

dbutils.fs.put("/scenario_files/dynamicolumns.csv",
"""
id,name,loc,age,sex
1,ravi,bangalore,33,m
2,raj,chennai
3,mohan
4,prasad,hyderabad,35
5,sridhar,chennai
""",True)

# COMMAND ----------

df = spark.read.csv("/scenario_files/dynamicolumns.csv",header=True)
display(df)

# COMMAND ----------

dbutils.fs.put("/scenario_files/dynamicolumns_withoutheader.csv",
"""1,ravi,bangalore
2,raj,chennai,33,m
3,mohan
4,prasad,hyderabad,35,m,787878987
5,sridhar,chennai
""",True)

# COMMAND ----------

df1 = spark.read.csv("/scenario_files/dynamicolumns_withoutheader.csv")
display(df1)

# COMMAND ----------

# Create Dataframe reading csv file using spark.read.text api
df1 = spark.read.text("/scenario_files/dynamicolumns_withoutheader.csv")

# COMMAND ----------

from pyspark.sql.functions import split,length,col,max,size
# Split text data using split function with comma delimieter
df3 =df1.select(split("value",",").alias("splitted_col"))

# COMMAND ----------

# Get Length of each row using size function then find max length of row for generating no of columns dynamically
df3.select('splitted_col',size('splitted_col')).show(truncate=False)

# COMMAND ----------

# Verify no of columns is going to generate this from data.
df3.select(max(size('splitted_col'))).collect()[0][0]

# COMMAND ----------

# Getting Max Index value for generating dynamic columns using max size of items at each row.
for i in range(df3.select(max(size('splitted_col'))).collect()[0][0]):
    # Dynamically Add Columns using WithColumn 
    df3=df3.withColumn('col'+str(i),df3["splitted_col"][i])
# Drop splitted_Col which is not required after splitting into individual columns
df3 = df3.drop("splitted_col")
df3.show()

# COMMAND ----------

