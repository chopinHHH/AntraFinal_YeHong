# Databricks notebook source
# MAGIC %run ./includes/configuration

# COMMAND ----------

# Display the Files in the Raw Path
display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# Make Notebook Idempotent
dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

# Ingest raw data
rawDF = (spark
            .read
            .format("json")
            .option("multiline", "true")
            .load(rawData)
            .cache()
           )

# COMMAND ----------

from pyspark.sql.types import StringType

raw_movie_DF = rawDF.select(explode("movie").alias("value"))
display(raw_movie_DF)

# COMMAND ----------

# testDF = rawDF.selectExpr("explode(movie) AS movie").selectExpr("movie.*")

# COMMAND ----------

# Ingestion Metadata
from pyspark.sql.functions import current_timestamp, lit
raw_movie_data_df = (raw_movie_DF
                     .select("value",
                             lit("files.training.databricks.com").alias("datasource"),
                             current_timestamp().alias("ingesttime"),
                             lit("new").alias("status"),
                             current_timestamp().cast("date").alias("ingestdate")
                            )
                    )

# COMMAND ----------

# WRITE Batch to a Bronze Table

from pyspark.sql.functions import col
(raw_movie_data_df.select("datasource",
                          "ingesttime",
                          "value",
                          "status",
                          col("ingestdate").alias("p_ingestdate"))
 .write.format("delta")
 .mode("append")
 .partitionBy("p_ingestdate")
 .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# Register the Bronze Table in the Metastore
spark.sql("""
drop table if exists movie_bronze
""")

spark.sql(f"""
create table movie_bronze
using delta
location "{bronzePath}"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

#Purge Raw File Path
dbutils.fs.rm(rawPath, recurse=True)
