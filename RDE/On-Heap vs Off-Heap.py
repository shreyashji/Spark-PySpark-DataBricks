# Databricks notebook source
# MAGIC %md
# MAGIC - memory controlled by JVM On-Heap ,MEMORY ALLOCATED TO executor controlled by JVM
# MAGIC - memory controlled by OS Off-Heap MEMORY,not contolled by jvm
# MAGIC -- we use both when developing spark application

# COMMAND ----------

# MAGIC %md
# MAGIC - reserved memory - reserved by spark for internal purposes
# MAGIC - user memory - for storing the data structure created and managed by the user code
# MAGIC - Execution memory - jvm heap space is used by data structures during shuffle operation(join and aggreagation)
# MAGIC - storage memory - jvm heap space reserved for cached data 
# MAGIC - UMM - execution memory(50% of UMM) + STORAGE MEORY (50% of UMM)

# COMMAND ----------

# MAGIC %md
# MAGIC ####### off heap
# MAGIC - each executor within worker node has access to off heap memory
# MAGIC - off heap memory can be used by spark explicitly for storing  its data 
# MAGIC - the amount of off heap memory used by spark to store actual data framesis governed by spark.memory.offheap.size
# MAGIC - to enable offheap memory, set spark.memory.offheap.use to TRUE
# MAGIC - ACCCESSING OFFHEAP IS SLIGHTLY slower than accessing the onheap storage but still faster than reading/writing  from a disk
# MAGIC - Garbage collector (GC) scan can be avoided by using offheap memory

# COMMAND ----------

# MAGIC %md
# MAGIC ###### ON HEAP
# MAGIC - better performance than off heap coz object allocation,deallocation happen automatically
# MAGIC - managed & controlled by garbage collector JVM process so adding overhead of GC scans 
# MAGIC - data stored in the format of java bytes (desrialized) which java can process efficiently
# MAGIC - while processing smaller sets of data that can fit into heap memory, this option is suitable
# MAGIC ###### OFF HEAP
# MAGIC - SLOWER THAN on heap but still faster than  disk performance manual memory mgmt
# MAGIC - directly managed by operating system so avoiding overhead of gc
# MAGIC - data stored in the format of array bytes (serialized), so adding overhead of serilizing/deserializng when java programs needs to process the data 
# MAGIC - when needs t store bigger datasets that cannot fit into heap memory,can make advantage of off-heap memory to store data outside JVM PROCESS

# COMMAND ----------

