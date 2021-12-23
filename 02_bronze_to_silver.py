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

# MAGIC %md
# MAGIC ### Current Delta Architecture

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
"""
from pyspark.sql.types import *

assert transformedRawDF.schema == StructType(
    [
        StructField("datasource", StringType(), False),
        StructField("ingesttime", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("movie", StringType(), True),
        StructField("p_ingestdate", DateType(), False),
    ]
)
print("Assertion passed.")
"""

# COMMAND ----------

# Step 3: Write Batch to a Bronze Table

rawToBronzeWriter = batch_writer(dataframe=transformedRawDF, partition_column="p_ingestdate")
rawToBronzeWriter.save(bronzePath)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze to Silver Step

# COMMAND ----------

# Make Notebook Idempotent
dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

# Load New Records from the Bronze Records
bronzeDF = spark.read.table("movie_Bronze").filter("status = 'new'")

# COMMAND ----------

display(bronzeDF)

# COMMAND ----------

# Transform the data to silver master table
silver_master_tracker = (bronzeDF
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

# COMMAND ----------

display(silver_master_tracker)

# COMMAND ----------

# Verify the Schema with an Assertion
'''from pyspark.sql.types import _parse_datatype_string

assert silver_master_tracker.schema == _parse_datatype_string(
    """
      movie STRING,
      BackdropUrl string,
      Budget double,
      CreatedBy string,
      CreatedDate DATE,
      Id long,
      ImdbUrl string,
      OriginalLanguage string,
      Overview string,
      PosterUrl string,
      Price double,
      ReleaseDate string,
      Revenue double,
      RunTime long,
      Tagline string,
      Title string,
      TmdbUrl string,
      UpdatedBy string,
      UpdatedDate DATE,
      genres array
      """), "Schemas do not match"
print("Assertion passed.")
'''

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Quarantine the Bad Data

# COMMAND ----------

silver_master_tracker.count()

# COMMAND ----------

silver_master_tracker.dropna(how="all").count()

# COMMAND ----------

# Split the Silver DataFrame
silver_master_tracker_clean = silver_master_tracker.filter("runtime >= 0")
silver_master_tracker_quarantine = silver_master_tracker.filter("runtime < 0")

# COMMAND ----------

# Display the Quarantined Records
display(silver_master_tracker_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC ### WRITE Clean Batch to a Silver Table

# COMMAND ----------

(silver_master_tracker_clean.select("BackdropUrl", "Budget", "CreatedBy", "CreatedDate", "Id", "ImdbUrl", "OriginalLanguage", "Overview", "PosterUrl", "Price", "ReleaseDate", "Revenue", "RunTime", "Tagline", "Title", "TmdbUrl", "UpdatedBy", "UpdatedDate", "genres")
    .write.format("delta")
    .mode("append")
    .save(silverPath))

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS master_silver
"""
)

spark.sql(
    f"""
CREATE TABLE master_silver
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

# Verify the Schema with an Assertion
'''silverTable = spark.read.table("master_silver")
expected_schema = """
      BackdropUrl string,
      Budget double,
      CreatedBy string,
      CreatedDate DATE,
      Id long,
      ImdbUrl string,
      OriginalLanguage string,
      Overview string,
      PosterUrl string,
      Price double,
      ReleaseDate string,
      Revenue double,
      RunTime long,
      Tagline string,
      Title string,
      TmdbUrl string,
      UpdatedBy string,
      UpdatedDate DATE,
      genres array
"""

assert silverTable.schema == _parse_datatype_string(expected_schema), "Schemas do not match"
print("Assertion passed.")'''

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from master_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Bronze table to Reflect the Loads

# COMMAND ----------

# Step 1: Update Clean records
from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silver_master_tracker_clean.withColumn("status", lit("loaded")).dropDuplicates()

update_match = "bronze.movie = clean.movie"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# Step 2: Update Quarantined records
silverAugmented = silver_master_tracker_quarantine.withColumn("status", lit("quarantined")).dropDuplicates()

update_match = "bronze.movie = quarantine.movie"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)
