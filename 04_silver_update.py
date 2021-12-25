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

bronzeDF = (spark.read.table("movie_bronze").filter("status = 'new'")
)
silver_master_tracker = bronze_to_silver(bronzeDF)
(silver_master_clean, silver_master_quarantine) = generate_clean_and_quarantine_dataframes(silver_master_tracker)

# COMMAND ----------

bronzeToSilverWriter = batch_writer(
    dataframe=transformedRawDF, partition_column="p_ingestdate"
)
(silver_master_clean.select("BackdropUrl", "Budget", "CreatedBy", "CreatedDate", "Id", "ImdbUrl", "OriginalLanguage", "Overview", "PosterUrl", "Price", "ReleaseDate", "Revenue", "RunTime", "Tagline", "Title", "TmdbUrl", "UpdatedBy", "UpdatedDate", "genres")
    .write.format("delta")
    .mode("append")
    .save(silverPath))

update_bronze_table_status(spark, bronzePath, silver_master_clean, "loaded")
update_bronze_table_status(spark, bronzePath, silver_master_quarantine, "quarantined")

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
bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias("quarantine")
display(bronzeQuarTransDF)

# COMMAND ----------

# Step 3: Repair Runtime Issue from the Quarantined DataFrame
from pyspark.sql.functions import abs

silverCleanedDF = bronzeQuarTransDF.select(
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
    abs(col("movie.RunTime").cast("Integer")).alias("RunTime"), 
    col("movie.Tagline"), 
    col("movie.Title"), 
    col("movie.TmdbUrl"), 
    col("movie.UpdatedBy"), 
    col("movie.UpdatedDate").cast("date").alias("UpdatedDate"), 
    col("movie.genres")
)
display(silverCleanedDF)
