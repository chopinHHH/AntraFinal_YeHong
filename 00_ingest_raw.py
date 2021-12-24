# Databricks notebook source
# MAGIC %md
# MAGIC #Raw Data Generation

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# Make Notebook Idempotent
dbutils.fs.rm(commonPath, recurse=True)

# COMMAND ----------

# Display the Raw Data Directory
display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# Ingest data
rawDF = ingest_batch_raw(rawData).cache()

# COMMAND ----------

display(rawDF)

# COMMAND ----------

#Print the contents of the raw files
print(dbutils.fs.head
      (dbutils.fs.ls("dbfs:/mnt/movieShop/raw")[0].path)
     )

# COMMAND ----------

# print schema of rawDF
rawDF.printSchema()
