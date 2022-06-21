# Databricks notebook source
# MAGIC %fs cp "dbfs:/FileStore/tables/our_lacing_338417_5d93549d32ce.json" "file:/temp/big.json"

# COMMAND ----------

# MAGIC %fs head "/FileStore/tables/our_lacing_338417_5d93549d32ce.json"

# COMMAND ----------

credentialfilepath = "file:/temp/big.json"
projectName = "learntospark"
table = "demoSpark.ga_sessions_20170801"

# COMMAND ----------

df = spark.read.format("bigquery") \
            .option("credentialfile",credentialfilepath) \
            .option("parentProject",projectName) \
            .option("table",table) \
            .load()


# COMMAND ----------

display(df)