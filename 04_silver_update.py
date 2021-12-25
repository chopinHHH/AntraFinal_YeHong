# Databricks notebook source
# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Raw to Bronze Pipeline

# COMMAND ----------

rawDF = ingest_batch_raw(rawData)
raw_movie_DF = rawDF.select(explode("movie").alias("movie"))
transformedRawDF = transform_raw(raw_movie_DF)
rawToBronzeWriter = batch_writer(dataframe=transformedRawDF, partition_column="p_ingestdate")
rawToBronzeWriter.save(bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Bronze to Silver Pipeline

# COMMAND ----------

bronzeDF = read_batch_bronze(spark)
silver_master_tracker = transform_bronze(bronzeDF)
(silverCleanDF, silverQuarantineDF) = generate_clean_and_quarantine_dataframes(silver_master_tracker)

# COMMAND ----------

bronzeToSilverWriter = batch_writer(dataframe=silverCleanDF, partition_column="UpdatedDate")
bronzeToSilverWriter.save(silverPath)

# COMMAND ----------

update_bronze_table_status(spark, bronzePath, silverCleanDF, "loaded")
update_bronze_table_status(spark, bronzePath, silverQuarantineDF, "quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform a Visual Verification of the Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM master_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handle Quarantined Records

# COMMAND ----------

# Step 1: Load Quarantined Records from the Bronze Table
bronzeQuarantinedDF = spark.read.table("movie_bronze").filter("status = 'quarantined'")
display(bronzeQuarantinedDF)

# COMMAND ----------

# Step 2: Transform the Quarantined Records
bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF).alias("quarantine")
display(bronzeQuarTransDF)

# COMMAND ----------

# Step 3: Fix runtime with absolute value of the original negative value
from pyspark.sql.functions import abs, when

silverCleanedDF_1 = bronzeQuarTransDF.select(
    "movie",
    col("movie.BackdropUrl"), 
    col("movie.Budget").cast("Integer").alias("Budget"), 
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
    abs(col("movie.RunTime").cast("Integer")).alias("RunTime"), 
    col("movie.Tagline"), 
    col("movie.Title"), 
    col("movie.TmdbUrl"), 
    col("movie.UpdatedBy"), 
    col("movie.UpdatedDate").cast("date").alias("UpdatedDate"), 
    col("movie.genres")
)
display(silverCleanedDF_1)

# COMMAND ----------

# Step 4: Fix budget with 1000000 if original value is less than 1000000
silverCleanedDF = silverCleanedDF_1.withColumn("Budget", when(col("Budget") < 1000000, 1000000).when(col("Budget") >= 1000000, "Budget").otherwise(1000000))

# COMMAND ----------

display(silverCleanedDF)

# COMMAND ----------

# Step 5: Batch Write the Repaired (formerly Quarantined) Records to the Silver Table
bronzeToSilverWriter = batch_writer(dataframe=silverCleanedDF, partition_column="UpdatedDate")
bronzeToSilverWriter.save(silverPath)
update_bronze_table_status(spark, bronzePath, silverCleanedDF, "loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Quarantined Records
# MAGIC 
# MAGIC If the update was successful, there should be no quarantined records in the Bronze table.

# COMMAND ----------

display(bronzeQuarantinedDF)
