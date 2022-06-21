# Databricks notebook source
# MAGIC %md
# MAGIC ######  PySpark

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark) \
    .tableName("employee_demo")\
    .addColumn("emp_id","INT") \
    .addColumn("emp_Name","STRING") \
    .addColumn("gender","STRING") \
    .addColumn("salary","INT") \
    .addColumn("Dept","STRING") \
    .property("description","table created for demo purpose") \
    .location("/FileStore/tables/createTable") \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo

# COMMAND ----------

from delta.tables import *

DeltaTable.createIfNotExists(spark) \
    .tableName("employee_demo")\
    .addColumn("emp_id","INT") \
    .addColumn("emp_Name","STRING") \
    .addColumn("gender","STRING") \
    .addColumn("salary","INT") \
    .addColumn("Dept","STRING") \
    .property("description","table created for demo purpose") \
    .location("/FileStore/tables/createTable") \
    .execute()

# COMMAND ----------

from delta.tables import *

DeltaTable.createOrReplace(spark) \
    .tableName("employee_demo")\
    .addColumn("emp_id","INT") \
    .addColumn("emp_Name","STRING") \
    .addColumn("gender","STRING") \
    .addColumn("salary","INT") \
    .addColumn("Dept","STRING") \
    .property("description","table created for demo purpose") \
    .location("/FileStore/tables/createTable") \
    .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ######SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employee_demo(
# MAGIC   emp_id INT,
# MAGIC   emp_name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept String,
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists employee_demo(
# MAGIC   emp_id INT,
# MAGIC   emp_name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept String,
# MAGIC ) USING DELTA