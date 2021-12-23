# Databricks notebook source
# MAGIC %md
# MAGIC ###Define data path

# COMMAND ----------

commonPath = f"/mnt/movieShop/"

rawPath = commonPath + "raw/"
rawData = [file.path for file in dbutils.fs.ls("dbfs:"+rawPath)]
bronzePath = commonPath + "bronze/"
silverPath = commonPath + "silver/"
silverQuarantinePath = commonPath + "silverQuarantine/"
moviePath = commonPath + "movieSilver/"
genresPath = commonPath + "genresSilver"
OriginalLanguagePath = commonPath + "olSilver"
