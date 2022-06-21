# Databricks notebook source
#Create input Spark Dataframe
list_data=[["Babu",20],["Raja",8],["Mani",75],["Kalam",100],["Zoin",7],["Kal",53]]
df1=spark.createDataFrame(list_data,["name","score"])
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Solution;
# MAGIC ######Apache Spark offers us with the three different approach to handle this scenario. One can use any of the below method as per their business requirement and get the required output. 
# MAGIC * Using Format String
# MAGIC * Using lpad
# MAGIC * Using Concat and Substring

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method  1 -  Using LPad Function:

# COMMAND ----------

from pyspark.sql.functions import lpad
#lpad(col,len,pad)
df2=df1.withColumn("score_000",lpad("score",3,"0"))
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 2 - Using Format String

# COMMAND ----------

#format_string(format,*cols)
from pyspark.sql.functions import format_string
df2=df1.withColumn("score_000",format_string("%03d","score"))
df2.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 3 - Using Concat and SubString

# COMMAND ----------

#concat & substring
from pyspark.sql.functions import concat,substring,lit

df2=df1.withColumn("score_000",concat(lit("00"),"score"))
df3=df2.withColumn("score_000",substring("score_000",-3,3))

df2.display()

# COMMAND ----------

df3.display()

# COMMAND ----------

