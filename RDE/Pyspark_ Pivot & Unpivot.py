# Databricks notebook source
#https://www.youtube.com/watch?v=5Oot57zVwAg&list=PLgPb8HXOGtsTpvtSK1c_aOYzA0X6ASc8A&index=12

# COMMAND ----------

# MAGIC %md
# MAGIC - pivot - it is used to transpose the list of values of columns to columns
# MAGIC - Unpivot - is quite opposite to pivot i.e transposing the columns into list  of values to a column

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Pivot syntax

# COMMAND ----------

pivot_df = df.groupBy('Company').pivot('Quarter').sum('Revenue')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unpivot syntax

# COMMAND ----------

DF = pivot_df.selectExpr("company","stack(2,'Q1','Q1','Q2','Q2') as (Quater,Revenue)").where("Revenue is not  null")

# COMMAND ----------

