# Databricks notebook source
a = 20
b = 99
c = a + b
print(c)


# COMMAND ----------

dbutils.fs.ls('/mnt')
dbutils.fs.mkdirs('dbfs:/mnt/blobstorage/source_folder/deepak_test')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.test_data;

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

# MAGIC %md
# MAGIC I am learning Databricks
# MAGIC My name is Deepak Jaiswal

# COMMAND ----------

dbutils.help()

# COMMAND ----------

sc
