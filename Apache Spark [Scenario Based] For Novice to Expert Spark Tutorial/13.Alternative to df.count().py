# Databricks notebook source
path = '/FileStore/tables/uspop.csv'

# COMMAND ----------

in_data = spark.read\
    .option('header',True)\
    .option('inferSchema',True)\
    .option('delimiter','|')\
    .csv(path)

# COMMAND ----------

display(in_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Filter Logic

# COMMAND ----------

filter_data = in_data.filter("state_code<>'NY'")

# COMMAND ----------

filter_data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Group data logic

# COMMAND ----------

group_data = filter_data.groupBy('city').sum('2019_estimate')

# COMMAND ----------

group_data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Show final Data

# COMMAND ----------

group_data.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Accumulator 

# COMMAND ----------

filterCounter = spark.sparkContext.accumulator(0)
groupByCounter = spark.sparkContext.accumulator(0)
sourceCounter = spark.sparkContext.accumulator(0)

# COMMAND ----------

in_data.foreach(lambda x:sourceCounter.add(1))
filter_data.foreach(lambda x:filterCounter.add(1))
group_data.foreach(lambda x:groupByCounter.add(1))

# COMMAND ----------

print(sourceCounter.value)
print(filterCounter.value)
print(groupByCounter.value)

# COMMAND ----------

print(in_data.count())
print(filter_data.count())
print(group_data.count())


# COMMAND ----------

