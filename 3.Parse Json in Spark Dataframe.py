# Databricks notebook source
in_path = "/FileStore/tables/auto_loan.json"


# COMMAND ----------

inp = spark.read.option("header",True).option("escape","\"") \
                                        .option("multiline",True) \
                                        .csv(in_path)

# COMMAND ----------

inp.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #using SPAR SQL QUERYY NEWLY ADDED FEATURE

# COMMAND ----------

inp.createOrReplaceTempView("J")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FORM J;

# COMMAND ----------

