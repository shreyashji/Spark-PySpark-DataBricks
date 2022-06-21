# Databricks notebook source
simpleData = [(1,"Sagar","CSE","UP",80,),\
              (2,"Shivam","IT","MP",86,),
              (3,"Shital","Mech","AP",70,)   
]
columns = ["ID","Student_name","Department_name","City","Marks"]
df_1 = spark.createDataFrame(data = simpleData,schema = columns)
df_1.show()

# COMMAND ----------

simpleData_2 = [(4,"Raj","CSE","UP",),\
              (5,"Tarun","IT","MP",)  
]
columns = ["ID","Student_name","Department_name","City"]
df_2 = spark.createDataFrame(data = simpleData_2,schema = columns)
df_2.show()

# COMMAND ----------

#union only if same no of coumns are there
#df = df_1.union(df_2)
#df.show()

# COMMAND ----------

from pyspark.sql.functions import col,lit
df_2 = df_2.withColumn("Marks",lit("null"))
df = df_1.union(df_2)
df.show()

# COMMAND ----------

