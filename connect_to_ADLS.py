# Databricks notebook source
# MAGIC %md This example notebook closely follows the [Databricks documentation](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake.html) for how to set up Azure Data Lake Store as a data source in Databricks.

# COMMAND ----------

# MAGIC %md ### 0 - Setup
# MAGIC 
# MAGIC To get set up, do these tasks first: 
# MAGIC 
# MAGIC - Get service credentials: Client ID `<aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee>` and Client Credential `<NzQzY2QzYTAtM2I3Zi00NzFmLWI3MGMtMzc4MzRjZmk=>`. Follow the instructions in [Create service principal with portal](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal). 
# MAGIC - Get directory ID `<ffffffff-gggg-hhhh-iiii-jjjjjjjjjjjj>`: This is also referred to as *tenant ID*. Follow the instructions in [Get tenant ID](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal#get-tenant-id). 
# MAGIC - If you haven't set up the service app, follow this [tutorial](https://docs.microsoft.com/en-us/azure/azure-databricks/databricks-extract-load-sql-data-warehouse). Set access at the root directory or desired folder level to the service or everyone.

# COMMAND ----------

# MAGIC %md There are two options to read and write Azure Data Lake data from Azure Databricks:
# MAGIC 1. DBFS mount points
# MAGIC 2. Spark configs

# COMMAND ----------

# MAGIC %md ## 1 - DBFS mount points
# MAGIC [DBFS](https://docs.azuredatabricks.net/user-guide/dbfs-databricks-file-system.html) mount points let you mount Azure Data Lake Store for all users in the workspace. Once it is mounted, the data can be accessed directly via a DBFS path from all clusters, without the need for providing credentials every time. The example below shows how to set up a mount point for Azure Data Lake Store.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
# MAGIC           "dfs.adls.oauth2.client.id": "935988e4-7ee1-400d-87b4-c5f3b415f60a",
# MAGIC           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/<af37d601-d55d-4f9f-af17-901ff37f1509>/oauth2/token"}
# MAGIC 
# MAGIC # client secret value: 1ay7Q~snu42KxPot6dTkyUrusbMiYc_M.yONl
# MAGIC # client secret id: bc9820a0-1149-4b70-952b-cfbc7bd50c24

# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "adl://movieshop.azuredatalakestore.net/<movieshopRegi>",
  mount_point = "/mnt/movieshop",
  extra_configs = configs)
