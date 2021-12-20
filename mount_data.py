# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ###Mount Azure Data Lake Storage Gen2 folder

# COMMAND ----------

#https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "935988e4-7ee1-400d-87b4-c5f3b415f60a",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="movieScope",key="secretValue"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/af37d601-d55d-4f9f-af17-901ff37f1509/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@movieshop.dfs.core.windows.net/",
  mount_point = "/mnt/movieShop/raw",
  extra_configs = configs)

# COMMAND ----------

# secret value: rNT7Q~e257ytlnPn-v4kcX.EysRHMzCWoo0mz
# secret ID: 54d095c8-c631-4fc3-8536-16a52c492b69
