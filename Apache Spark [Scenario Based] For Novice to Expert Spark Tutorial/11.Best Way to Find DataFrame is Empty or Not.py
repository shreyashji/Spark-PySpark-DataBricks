# Databricks notebook source
file_loc = "/FileStore/tables/uspop.csv"

# COMMAND ----------

in_data = spark.read.option("header",True).option("inferSchema",True).option("delimiter","|").csv(file_loc)

# COMMAND ----------

display(in_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Group_data - Logic

# COMMAND ----------

group_data = in_data.filter("state_code=='PK'").groupBy("city").sum("2019_estimate")

# COMMAND ----------

display(group_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Using Count

# COMMAND ----------

if group_data.count() > 0:
    print("True")
else:
    print("False")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Using First

# COMMAND ----------

if group_data.first():
    print("True")
else:
    print("False")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Using Take

# COMMAND ----------

if group_data.take(1):
    print("True")
else:
    print("False")

# COMMAND ----------

# MAGIC %md
# MAGIC ####isempty()

# COMMAND ----------

if group_data.rdd.isEmpty():
    print("False")
else:
    print("True")

# COMMAND ----------

