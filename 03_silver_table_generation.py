# Databricks notebook source
# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Movie Silver Table

# COMMAND ----------

dbutils.fs.rm("moviePath", recurse=True)

# COMMAND ----------

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

movie_SDF = sqlContext.sql("Select Id, Title, Overview, Tagline, RunTime, ReleaseDate, Price, Revenue, Budget, CreatedBy, CreatedDate,  BackdropUrl, ImdbUrl, PosterUrl, TmdbUrl, UpdatedBy, UpdatedDate from master_silver")

# COMMAND ----------

(movie_SDF.select("Id", "Title", "Overview", "Tagline", "RunTime", "ReleaseDate", "CreatedBy", "CreatedDate", "Price", "Revenue", "Budget", "BackdropUrl", "ImdbUrl", "PosterUrl", "TmdbUrl", "UpdatedBy", "UpdatedDate")
    .write.format("delta")
    .mode("append")
    .save(moviePath))

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

# MAGIC %md
# MAGIC ### Create Original Language Silver Table

# COMMAND ----------

dbutils.fs.rm("OriginalLanguagePath", recurse=True)

# COMMAND ----------


