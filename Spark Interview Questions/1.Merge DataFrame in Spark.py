# Databricks notebook source
path='/FileStore/tables/mergedf.csv'

# COMMAND ----------

df1=spark.read.option('header',True).option('delimiter','|').csv(path)

# COMMAND ----------

df1.show()

# COMMAND ----------

df2 = spark.read.option('header',True).option('delimiter','|').csv('/FileStore/tables/mergedf2.csv')

# COMMAND ----------

df2.show()

# COMMAND ----------

#matching coln sould be there
df1.union('df2')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method -1 withColumn 7 Union

# COMMAND ----------

#add coln in df1
from pyspark.sql.functions import lit
df_addedcoln = df1.withColumn("AddedCOlnGender",lit('null'))

# COMMAND ----------

df_addedcoln.show()

# COMMAND ----------

df_addedcoln.union('df2').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method-2  schema define

# COMMAND ----------

from pyspark.sql.types import *
schema=StructType([
    StructFieLd("Name",StringType(),True),
    StructFieLd("Age",StringType(),True),
    StructFieLd("Gender",StringType(),True),
]
)

# COMMAND ----------

df3 = spark.read.option('header',True).option('delimiter','|').csv(path)

# COMMAND ----------

df4=  spark.read.option('header',True).option('delimiter','|').csv('/FileStore/tables/mergedf2.csv')

# COMMAND ----------

df3.union('df4').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method-3 Using Join (Outer join)

# COMMAND ----------

first_df = spark.read.option('header',True).option('delimiter','|').csv(path)

# COMMAND ----------

sec_df = spark.read.option('header',True).option('delimiter','|').csv('/FileStore/tables/mergedf2.csv')

# COMMAND ----------

df_output = first_df.join(sec_df,on=['Name','Age'],how='Outer')

# COMMAND ----------

df_output.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Automated approach - Realtime approach

# COMMAND ----------

df_1 = spark.read.option('header',True).option('delimiter','|').csv(path)
df_2 = spark.read.option('header',True).option('delimiter','|').csv('/FileStore/tables/mergedf2.csv')

# COMMAND ----------

 listA = list(set(df_1.columns) - set(df_2.columns))
 listB = list(set(df_2.columns) - set(df_1.columns))

# COMMAND ----------

for i in listA:
    df_2 = df_1.withColumn(i,lit('null'))

for i in listB:
    df_1 = df_2.withColumn(i,lit('null'))