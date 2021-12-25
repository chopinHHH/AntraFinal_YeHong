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

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.select("movie",
                      lit("movieShop.databricks.com").alias("datasource"),
                      current_timestamp().alias("ingesttime"),
                      lit("new").alias("status"),
                      current_timestamp().cast("date").alias("p_ingestdate")
                     )

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "overwrite",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .partitionBy(partition_column)
    )

# COMMAND ----------

def read_batch_bronze(spark: SparkSession) -> DataFrame:
    return spark.read.table("movie_Bronze").filter("status = 'new'")

# COMMAND ----------

def transform_bronze(bronze: DataFrame) -> DataFrame:
    
    silver_master_tracker = (bronze
                             .select("movie",
                                 col("movie.BackdropUrl"), 
                                 col("movie.Budget"), 
                                 col("movie.CreatedBy"), 
                                 col("movie.CreatedDate").cast("date").alias("CreatedDate"), 
                                 col("movie.Id"), 
                                 col("movie.ImdbUrl"), 
                                 col("movie.OriginalLanguage"), 
                                 col("movie.Overview"), 
                                 col("movie.PosterUrl"), 
                                 col("movie.Price"), 
                                 col("movie.ReleaseDate"), 
                                 col("movie.Revenue"), 
                                 col("movie.RunTime"), 
                                 col("movie.Tagline"), 
                                 col("movie.Title"), 
                                 col("movie.TmdbUrl"), 
                                 col("movie.UpdatedBy"), 
                                 col("movie.UpdatedDate").cast("date").alias("UpdatedDate"), 
                                 col("movie.genres")
                                    ))
    return silver_master_tracker

# COMMAND ----------

def generate_clean_and_quarantine_dataframes(dataframe: DataFrame) -> (DataFrame, DataFrame):
    return (
        dataframe.filter("runtime >= 0" and "budget >= 1000000"),
        dataframe.filter("budget < 1000000" or "runtime < 0")
    )

# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:
    
    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    silverAugmented = dataframe.withColumn("status", lit(status)).dropDuplicates()
    
    update_match = "bronze.movie = dataframe.movie"
    update = {"status": "dataframe.status"}
    (
        bronzeTable.alias("bronze")
        .merge(silverAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )
    return True
