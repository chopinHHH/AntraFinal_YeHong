# Databricks notebook source
# MAGIC %md
# MAGIC ### Raw Data Generation

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Utility Function
%run ./includes/utilities

# COMMAND ----------

# Make Notebook Idempotent
dbutils.fs.rm(commonPath, recurse=True)

# COMMAND ----------

# Show rawData path
display([dbutils.fs.ls(path) for path in rawData])

# COMMAND ----------



# COMMAND ----------

# Ingest raw data
from pyspark.sql.functions import *

df = spark.read.json(rawData)
display(df)
