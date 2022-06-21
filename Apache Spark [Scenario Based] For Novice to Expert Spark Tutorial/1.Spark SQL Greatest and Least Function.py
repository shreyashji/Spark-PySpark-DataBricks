# Databricks notebook source
list_data = [
    ['Babu',20,33,60,44],
    ['Raja',58,33,78,83],
    ['mani',75,81,63,67],
    ['harish',100,100,93,87],
    ['Zoin',7,32,44,18],
    ['Raja',53,25,55,41],
]
list_schema = ["name","term_1","term_2","term_3","term_4"]

# COMMAND ----------

#create dataframe from the list
df1 =  spark.createDataFrame(list_data,list_schema)
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Methods
# MAGIC * Using Spark sql Functions - Greatest() & least()  
# MAGIC * Using Spark UDF (User defined functions)

# COMMAND ----------

from pyspark.sql.functions import greatest,least

df2 = df1.withColumn("GREATEST",greatest("term_1","term_2","term_3","term_4"))\
        .withColumn("LEAST",least("term_1","term_2","term_3","term_4"))

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Method -2 UDF

# COMMAND ----------

@udf
def great_least(type,cols:list):
    if type == "greatest":
        return max(cols)
    else:
        return min(cols)
    

# COMMAND ----------

from pyspark.sql.functions import array,lit

df3 = df1.withColumn("GREATEST",great_least(lit("greatest"),array(["term_1","term_2","term_3","term_4"]))) \
                     .withColumn("LEAST",great_least(lit("Least"),array(["term_1","term_2","term_3","term_4"])))

# COMMAND ----------

df3.show()

# COMMAND ----------

