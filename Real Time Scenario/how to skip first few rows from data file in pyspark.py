# Databricks notebook source
dbutils.fs.put("/scenario_files/empty_header.csv","""sampleline
smapleline2
sampleline3
id,name,location
1,ravi,bangalore
2,raj,chennai
3,prasad,pune
4,mahesh,hyderabad
5,sridhar,mumbai
""",True)

# COMMAND ----------

df=spark.read.csv("/scenario_files/empty_header.csv",header=True)
display(df)

# COMMAND ----------

rdd = sc.textFile("/scenario_files/empty_header.csv")
rdd.collect()

# COMMAND ----------

final_rdd = rdd.zipWithIndex().filter(lambda x: x[1] > 2).map(lambda a :a[0].split(","))
final_rdd.collect()

# COMMAND ----------

# get columns into list to create DataFrame
columns=final_rdd.collect()[0]
columns

# COMMAND ----------

# skip header/columns from data 
columns=final_rdd.first() #extract header for filter that row
data = final_rdd.filter(lambda col: col != columns)   #filter out header
data.collect()

# COMMAND ----------

data.toDF(columns).show()

# COMMAND ----------

