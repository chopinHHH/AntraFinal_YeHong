# Databricks notebook source
# MAGIC %md
# MAGIC ### Import Operation Functions

# COMMAND ----------

# MAGIC %run ./configuration

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
    explode
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (dataframe.select("datasource",
                             "ingesttime",
                             "value",
                             "status",
                             col("ingestdate").alias("p_ingestdate"))
            .write.format("delta")
            .mode(mode)
            .partitionBy("p_ingestdate")
)

# COMMAND ----------

def read_batch_raw(rawPath: str) -> DataFrame:
    return spark.read.format("json").option("multiline", "true").load(rawData)

# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    raw_movie_DF = rawDF.select(explode("movie").alias("value"))
    return raw_movie_DF.select("value",
                               lit("files.training.databricks.com").alias("datasource"),
                               current_timestamp().alias("ingesttime"),
                               lit("new").alias("status"),
                               current_timestamp().cast("date").alias("ingestdate")
                              )
