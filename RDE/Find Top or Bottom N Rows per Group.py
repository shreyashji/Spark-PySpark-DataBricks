# Databricks notebook source
data_students = [("Michel","physics",80,"p",90),
                 ("Michel","chemistry",67,"p",90),
                 ("Michel","maths",78,"p",90),
                 ("Nancy","physics",30,"p",80),
                 ("Nancy","chemistry",59,"p",80),
                 ("Nancy","maths",75,"p",80),
                 ("David","physics",90,"p",70),
                 ("David","chemistry",87,"p",70),
                 ("David","maths",97,"p",70),
                 ("John","physics",33,"p",60),
                 ("John","chemistry",28,"p",60),
                 ("John","maths",52,"p",60),
                 ("Blessy","physics",89,"p",75),
                 ("Blessy","chemistry",76,"p",75),
                 ("Blessy","maths",63,"p",75)]
Schema = ["name","subjects","marks","status","attendence"]

df = spark.createDataFrame(data=data_students,schema=Schema)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - create rank within each group of name

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col,row_number

windowDept =  Window.partitionBy("name").orderBy(col("marks").desc())
                                 
df2 =  df.withColumn("row",row_number().over(windowDept)).orderBy("name","row")

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC - Get Top N rows per Group of Name

# COMMAND ----------

df3 = df2.filter(col("row") <=1 )
df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC - Create Rank within Each group of subject

# COMMAND ----------

windowDept =  Window.partitionBy("subjects").orderBy(col("marks").desc())
df4 =  df.withColumn("row",row_number().over(windowDept)).orderBy("name","row")
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC - Get Top N rows per Group of subject

# COMMAND ----------

df5 = df4.filter(col("row") <=1 )
df5.display()

# COMMAND ----------

# MAGIC %md
# MAGIC - reverse rank to get bottom N rows Per Group

# COMMAND ----------

windowDept =  Window.partitionBy("subjects").orderBy(col("marks"))
df6 =  df.withColumn("row",row_number().over(windowDept)).orderBy("name","row")
display(df6)

# COMMAND ----------

df7 = df6.filter(col("row") <=1 )
df7.display()

# COMMAND ----------

