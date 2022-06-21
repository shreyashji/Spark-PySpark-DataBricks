# Databricks notebook source
# MAGIC %md
# MAGIC ###Create Input DataFrame

# COMMAND ----------

list_data = [ [1,"Station1","4:20 AM"],[1,"Station2","5:30 AM"],[1,"Station3","7:30 AM"]
             ,[2,"Station2","5:20 AM"] , [2,"Station4","7:30 AM"], [2,"Station5","11:30 AM"], [2,"Station6","1:30 PM"]    
]

df1 =  spark.createDataFrame(list_data,["BusID","Station","Time"])
print("Input Spark Dataframe")
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sample Cross Join

# COMMAND ----------

df1.join(df1,how="cross",on="BusID").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Apply window function to get Row_Num

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col,first,row_number,to_timestamp


windowSpec = Window.partitionBy("BusID").orderBy(to_timestamp("Time","hh:mm a").asc())

# COMMAND ----------

df_wind = df1.withColumn("row_num",row_number().over(windowSpec))

# COMMAND ----------

df_wind.display()

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")


# COMMAND ----------

# MAGIC %md
# MAGIC ###cross join with multiple condition

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col,first,row_number,to_timestamp
from pyspark.sql.functions import col

df_out = df_wind.join(df_wind.alias("df2"),(df_wind["row_num"]<col("df2.row_num") ) & 
(df_wind["BusID"]==col("df2.BusID"))) \
.select(df_wind["BusID"],
        df_wind["Station"].alias("Source_Point"),
        df_wind["Time"].alias("Source_Time"),
        col("df2.Station").alias("Dest_Point"),
        col("df2.Time").alias("Destination_Time"),
)

# COMMAND ----------

df_out.display()

# COMMAND ----------

df_out.filter("BusID=1").display()

# COMMAND ----------

print("out spark dataframe")
df_final = df_out.withColumn()