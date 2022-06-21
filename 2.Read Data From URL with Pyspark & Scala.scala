// Databricks notebook source
// MAGIC %md
// MAGIC Read Data from URL using SCALA & PySpark
// MAGIC 
// MAGIC ## Approach 1
// MAGIC * use URL pkg (eg. urlib) and convert data to spark DF
// MAGIC 
// MAGIC ## Approach 2
// MAGIC * use Spark ADD files method to bring data to driver node memory and read the csv as spark DF

// COMMAND ----------

// MAGIC %md
// MAGIC ##import required packages

// COMMAND ----------

// MAGIC %scala
// MAGIC //jar will be already there in spark cluster no need to worry
// MAGIC import org.apache.commons.io.IOUtils
// MAGIC import java.net.URL

// COMMAND ----------

// MAGIC %md
// MAGIC ##Read Data from URL and Convert it into DS

// COMMAND ----------

val urlfile = new URL("https://raw.githubusercontent.com/azar-s91/dataset/master/BankChurners.csv")
val testcsvgit = IOUtils.toString(urlfile,"UTF-8").lines.toList.toDS()

display(testcsvgit)

// COMMAND ----------

// MAGIC %md
// MAGIC ##convert the DataSet to Spark DF 

// COMMAND ----------

val testcsv = spark.read.option("header",true)
              .option("inferSchema",true)
              .csv(testcsvgit)
display(testcsv)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Using PySpark - Approach AddFiles

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark import SparkFiles

// COMMAND ----------

// MAGIC %md
// MAGIC ##Define Url and Add to SC

// COMMAND ----------

// MAGIC %python
// MAGIC url = "https://raw.githubusercontent.com/azar-s91/dataset/master/BankChurners.csv"
// MAGIC spark.sparkContext.addFile(url)

// COMMAND ----------

// MAGIC %python
// MAGIC SparkFiles.get("BankChurners.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC #Read the Files from Local [Driver]

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.read.csv("file://"+SparkFiles.get("BankChurners.csv"),header=True,inferSchema=True)
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ###not good approad if file is big ,store file in hdfs and read this is good approach

// COMMAND ----------

