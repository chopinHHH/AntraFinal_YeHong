# Databricks notebook source
# MAGIC %md
# MAGIC #Bronze to Silver - ETL into a Silver table

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# Display the files in the Bronze Paths
display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# Make Notebook Idempotent
dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

# Step 1: Creat RawDF DataFrame
rawDF = read_batch_raw(rawPath)

# COMMAND ----------

# Step 2: Transform the Raw Data
transformedRawDF = transform_raw(rawDF)

# COMMAND ----------

transformedRawDF.printSchema()

# COMMAND ----------

# Verify the Schema with an Assertion
from pyspark.sql.types import *

assert transformedRawDF.schema == StructType(
    [
        StructField("datasource", StringType(), False),
        StructField("ingesttime", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("value", StructType(), True),
        StructField("p_ingestdate", DateType(), False),
    ]
)
print("Assertion passed.")

# COMMAND ----------

# Step 3: Write Batch to a Bronze Table
rawToBronzeWriter = batch_writer(
    dataframe=transformedRawDF, partition_column="p_ingestdate"
)

rawToBronzeWriter.save(bronzePath)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze to Silver

# COMMAND ----------

# Make Notebook Idempotent
dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

# Load New Records from the Bronze Records
bronzeDF = spark.read.table("movie_Bronze").filter("status = 'new'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract the Nested JSON from the Bronze Records

# COMMAND ----------

# Step 1: Extract the Nested JSON from the value column

from pyspark.sql.functions import from_json

json_schema = """
    time TIMESTAMP,
    name STRING,
    device_id STRING,
    steps INTEGER,
    day INTEGER,
    month INTEGER,
    hour INTEGER
"""

bronzeAugmentedDF = bronzeDF.withColumn(
    "nested_json", from_json(col("value"), json_schema)
)
