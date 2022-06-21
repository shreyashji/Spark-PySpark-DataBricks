# Databricks notebook source
# MAGIC %md
# MAGIC - it is a pyspark function that returns a new row for each element in the given array or map,uses the default column name col for elements in the array and key and value for elements in the map
# MAGIC - arr contains list of elements & map contains list of key value pairs,if you want to split elements in array or map you can go for explode function
# MAGIC - while splitting the elmnt in row, it will create a new column col by default for array elements,key & value column name for map 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Variants in explode function
# MAGIC - Explode - when an array is passed in this function,it creates a new row for each element in array,when a map is passed it creates two new colns one for key & one for value, and each element in map splitinto the rows,if array or map is null that row is eliminated
# MAGIC - Explode_outer- unlike explode ,if the array or map is null,explode_outer returns null
# MAGIC - Posexplode - when array or map is passed,creates positional columns also for each element.ignores the null elements
# MAGIC - Posexplode_outer - unlike Posexplode,if the array or map is null,Posexplode_outer returns null

# COMMAND ----------



# COMMAND ----------

