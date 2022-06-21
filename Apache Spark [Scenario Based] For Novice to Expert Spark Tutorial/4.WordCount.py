# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .master("local")\
                    .appName('Firstprogram')\
                    .getOrCreate()
sc=spark.sparkContext

# COMMAND ----------

text_file = sc.textFile("/FileStore/tables/wordcount.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
                            .map(lambda word: (word, 1)) \
                           .reduceByKey(lambda x, y: x + y)

# COMMAND ----------

output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

# COMMAND ----------

sc.stop()
spark.stop()

# COMMAND ----------

