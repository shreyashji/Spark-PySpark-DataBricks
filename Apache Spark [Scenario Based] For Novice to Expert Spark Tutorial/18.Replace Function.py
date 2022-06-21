# Databricks notebook source
df1 = spark.read.option("header", "true").csv("/FileStore/tables/personal_transaction.csv")

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 1: Using na.replace

# COMMAND ----------

#replacing checking with cash
na_replace_df=df1.na.replace("Checking","Cash")

na_replace_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 2: Using regular expression replace

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

reg_df=df1.withColumn("card_type_rep",regexp_replace("Card_type","Checking","Cash"))

reg_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 3: Using Case When

# COMMAND ----------

from pyspark.sql.functions import when,col,lit

when_df=df1.withColumn("card_type_repl",when(col("Card_type").rlike("Checking"),lit("Cash")).otherwise(col("Card_type")))

# COMMAND ----------

