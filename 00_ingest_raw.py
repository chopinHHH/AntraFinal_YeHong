# Databricks notebook source
# MAGIC %md
# MAGIC #Raw Data Generation

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Make Notebook Idempotent
dbutils.fs.rm(commonPath, recurse=True)

# COMMAND ----------

# Display the Raw Data Directory
display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# Ingest data
rawDF = (spark
            .read
            .format("json")
            .option("multiline", "true")
            .load(rawData)
            .cache()
           )

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

# COMMAND ----------

# Define column name
movie_schema = ["BackdropUrl", "Budget", "CreatedBy", "CreatedDate", "Id", "ImdbUrl", "OriginalLanguage", "Overview", "PosterUrl", "Price", "ReleaseDate", "Revenue", "RunTime", "Tagline", "Title", "TmdbUrl", "UpdatedBy", "UpdatedDate", "genres"]

for i in range(len(movie_schema)):
    movie_schema[i] = "movie." + movie_schema[i]

# COMMAND ----------

# Read columns
from pyspark.sql.functions import *

movieInitialDF = (rawDF
                  .select(
                      array(expr("movie.BackdropUrl")).alias("BackdropUrl"),
                      array(expr("movie.Budget")).alias("Budget"),
                      array(expr("movie.CreatedBy")).alias("CreatedBy"),
                      array(expr("movie.CreatedDate")).alias("CreatedDate"),
                      array(expr("movie.Id")).alias("Id"),
                      array(expr("movie.ImdbUrl")).alias("ImdbUrl"),
                      array(expr("movie.OriginalLanguage")).alias("OriginalLanguage"),
                      array(expr("movie.Overview")).alias("Overview"),
                      array(expr("movie.PosterUrl")).alias("PosterUrl"),
                      array(expr("movie.Price")).alias("Price"),
                      array(expr("movie.ReleaseDate")).alias("ReleaseDate"),
                      array(expr("movie.Revenue")).alias("Revenue"),
                      array(expr("movie.RunTime")).alias("RunTime"),
                      array(expr("movie.Tagline")).alias("Tagline"),
                      array(expr("movie.Title")).alias("Title"),
                      array(expr("movie.TmdbUrl")).alias("TmdbUrl"),
                      array(expr("movie.UpdatedBy")).alias("UpdatedBy"),
                      array(expr("movie.UpdatedDate")).alias("UpdatedDate"),
                      array(expr("movie.genres")).alias("Genres")
                  ))

movieInitialDF.show()

# COMMAND ----------

# Combine Columns
ms_DF = (movieInitialDF
         .withColumn("movie", explode(arrays_zip("BackdropUrl", "Budget", "CreatedBy", "CreatedDate", "Id", "ImdbUrl", "OriginalLanguage", "Overview", "PosterUrl", "Price", "ReleaseDate", "Revenue", "RunTime", "Tagline", "Title", "TmdbUrl", "UpdatedBy", "UpdatedDate", "genres")))
         .select('movie.BackdropUrl', 'movie.Budget', 'movie.CreatedBy', 'movie.CreatedDate', 'movie.Id', 'movie.ImdbUrl', 'movie.OriginalLanguage', 'movie.Overview', 'movie.PosterUrl', 'movie.Price', 'movie.ReleaseDate', 'movie.Revenue', 'movie.RunTime', 'movie.Tagline', 'movie.Title', 'movie.TmdbUrl', 'movie.UpdatedBy', 'movie.UpdatedDate', 'movie.genres'))

movieDF = (ms_DF
           .withColumn("movie", explode(arrays_zip("BackdropUrl", "Budget", "CreatedBy", "CreatedDate", "Id", "ImdbUrl", "OriginalLanguage", "Overview", "PosterUrl", "Price", "ReleaseDate", "Revenue", "RunTime", "Tagline", "Title", "TmdbUrl", "UpdatedBy", "UpdatedDate", "genres")))
           .select('movie.BackdropUrl', 'movie.Budget', 'movie.CreatedBy', 'movie.CreatedDate', 'movie.Id', 'movie.ImdbUrl', 'movie.OriginalLanguage', 'movie.Overview', 'movie.PosterUrl', 'movie.Price', 'movie.ReleaseDate', 'movie.Revenue', 'movie.RunTime', 'movie.Tagline', 'movie.Title', 'movie.TmdbUrl', 'movie.UpdatedBy', 'movie.UpdatedDate', 'movie.genres'))

# COMMAND ----------

#Display movie dataframe
display(movieDF)

# COMMAND ----------

# Testing
# movie_id.withColumn("movie", explode(arrays_zip("Id"))).select("movie.Id").show()
