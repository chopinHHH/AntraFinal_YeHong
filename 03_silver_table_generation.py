# Databricks notebook source
# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

rawDF = ingest_batch_raw(rawData)
raw_movie_DF = rawDF.select(explode("movie").alias("movie"))
transformedRawDF = transform_raw(raw_movie_DF)
rawToBronzeWriter = batch_writer(dataframe=transformedRawDF, partition_column="p_ingestdate")
bronzeDF = spark.read.table("movie_Bronze").filter("status = 'loaded'")
silver_master_tracker = bronze_to_silver(bronzeDF)
silver_master_clean = silver_master_tracker.filter(("runtime >= 0") and ("budget >= 1000000"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Movie Silver Table

# COMMAND ----------

dbutils.fs.rm("moviePath", recurse=True)

# COMMAND ----------

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

movie_SDF = sqlContext.sql("Select Id, Title, Overview, Tagline, RunTime, ReleaseDate, Price, Revenue, Budget, CreatedBy, CreatedDate,  BackdropUrl, ImdbUrl, PosterUrl, TmdbUrl, UpdatedBy, UpdatedDate, OriginalLanguage from master_silver")

# COMMAND ----------

(movie_SDF.select("Id", "Title", "Overview", "Tagline", "RunTime", "ReleaseDate", "CreatedBy", "CreatedDate", "Price", "Revenue", "Budget", "BackdropUrl", "ImdbUrl", "PosterUrl", "TmdbUrl", "UpdatedBy", "UpdatedDate", "OriginalLanguage")
    .write.format("delta")
    .mode("append")
    .save(moviePath))
# if A Schema mismatch detected, can use [.mode("overwrite").option("overwriteSchema", "true")] to overwrite the delta table 

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movie_silver
USING DELTA
LOCATION "{moviePath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Genres Silver Table

# COMMAND ----------

dbutils.fs.rm("genresPath", recurse=True)

# COMMAND ----------

# Prepare Genres Data
genres_SDF = silver_master_clean.select(explode("movie.genres").alias("genres"))

# COMMAND ----------

from pyspark.sql.functions import trim

genres_tmp = genres_SDF.select("genres.id", "genres.name")
genres_new = genres_tmp.select("id", trim(col("name")).alias("name")).where("name!=''").dropna(how="any").dropDuplicates().sort("id")
display(genres_new)

# COMMAND ----------

(genres_new.select("id","name")
    .write.format("delta")
    .mode("append")
    .save(genresPath))

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS genres_silver
"""
)

spark.sql(
    f"""
CREATE TABLE genres_silver
USING DELTA
LOCATION "{genresPath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from genres_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Original Language Silver Table

# COMMAND ----------

dbutils.fs.rm("OriginalLanguagePath", recurse=True)

# COMMAND ----------

ol_SDF = sqlContext.sql("Select OriginalLanguage from master_silver")

# COMMAND ----------

(ol_SDF.select(col("OriginalLanguage").alias("code"), lit("English").alias("name")).distinct()
    .write.format("delta")
    .mode("append")
    .save(originalLanguagePath))

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS originalLanguage_silver
"""
)

spark.sql(
    f"""
CREATE TABLE originalLanguage_silver
USING DELTA
LOCATION "{originalLanguagePath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from originalLanguage_silver
