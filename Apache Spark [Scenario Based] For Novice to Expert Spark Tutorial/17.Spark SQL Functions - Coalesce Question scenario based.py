# Databricks notebook source
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/shreyashchoudhary7789@gmail.com/mob_num.csv")

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Using When OtherWise function

# COMMAND ----------

from pyspark.sql.functions import when,lit,col

# COMMAND ----------

df2 = df1.withColumn("new_mob", when(col('Personal_Mobile') != 'null',col('Personal_Mobile'))
                     .when(col('Home_Mobile') != 'null',col('Home_Mobile'))
                     .when(col('office_no') != 'null',col('office_no'))
                     .otherwise(lit("Not Available"))
                    )

# COMMAND ----------

display(df2.select('Name','new_mob'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Approach -2 Using Colace SQL Function  -SIMPLE WAY

# COMMAND ----------

from pyspark.sql.functions import lit,col,coalesce


# COMMAND ----------

df3= df1.withColumn("new_mob",coalesce('Personal_Mobile','Home_Mobile','office_no',lit('Not Available')))

# COMMAND ----------

display(df3.select('Name','new_mob'))

# COMMAND ----------

