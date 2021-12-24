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

def ingest_batch_raw(Path: str) -> DataFrame:
    return spark.read.format("json").option("multiline", "true").option("inferSchema", "true").load(Path)

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
        .partitionBy(partition_column)
    )

# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.select("movie",
                      lit("movieShop.databricks.com").alias("datasource"),
                      current_timestamp().alias("ingesttime"),
                      lit("new").alias("status"),
                      current_timestamp().cast("date").alias("p_ingestdate")
                     )

# COMMAND ----------

def bronze_to_silver(df: DataFrame) -> DataFrame:
    return (df
                         .select("movie",
                                 "*"
                                ))
