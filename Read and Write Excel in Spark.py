# Databricks notebook source
dbutils.fs.rm('/FileStore/tables/',True)

# COMMAND ----------

path = '/FileStore/tables/studentexcel.xlsx'

# COMMAND ----------

input_excel = spark.read.format("com.crealytics.spark.excel").option("header","true").option("inferSchema","true").load(path)

# COMMAND ----------

display(input_excel)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write CSV TO EXCEL

# COMMAND ----------

in_csv_path = "/FileStore/tables/student.csv"
out_excel_path = "/FileStore/tables/student_in_excel.xlsx"

# COMMAND ----------

in_df = spark.read.csv(in_csv_path,header=True)

# COMMAND ----------

display(in_df)

# COMMAND ----------

in_df.write.format("com.crealytics.spark.excel").option("header","true").save(out_excel_path)

# COMMAND ----------

