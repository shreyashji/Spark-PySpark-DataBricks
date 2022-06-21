# Databricks notebook source
# MAGIC %md
# MAGIC ###### Snappy
# MAGIC - low CPU utilization
# MAGIC - low Compression rate
# MAGIC - splitable [parallel processing for heavy calculation multiple core accesing,MPP]
# MAGIC - use Case : Hot layer [ACCESS FREQUENTLY by application,multiple times]
# MAGIC - use case : compute intensive 
# MAGIC 
# MAGIC ###### Gzip
# MAGIC - High CPU Utilization
# MAGIC - high compressionrates
# MAGIC - Non splitable
# MAGIC - Use : Cold layer [take more resource]
# MAGIC - Use case : storage intensive [audit history]

# COMMAND ----------

#default snappy for parquet
csvDF.write.format("parquet").option("compression","snappy").save("/FileStore/tables/folder_name/write_files/snappy_parquets")

# COMMAND ----------

#list down all files 
%fs
ls /FileStore/tables/folder_name/write_files/snappy_parquets/

# COMMAND ----------

csvDF.write.format("parquet").option("compression","gzip").save("/FileStore/tables/folder_name/write_files/snappy_parquets")

# COMMAND ----------

#list down all files 
%fs
ls /FileStore/tables/folder_name/write_files/snappy_parquets/

# COMMAND ----------

