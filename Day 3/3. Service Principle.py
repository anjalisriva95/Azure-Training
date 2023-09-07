# Databricks notebook source
container_name       = "raw"
storage_account_name = "asadlsmichelin"
client_id            = "b5bef89a-c50a-4b64-9cff-599e7429095c"
tenant_id            = "25badf2f-1aac-4c68-9aa6-f2563ba33c37"
client_secret        = "3mc8Q~ek4aLRi5eH2z9oqsPdYHxGWHiJQAPFDbU."

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",

           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",

           "fs.azure.account.oauth2.client.id": f"{client_id}",

           "fs.azure.account.oauth2.client.secret": f"{client_secret}",

           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(

  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",

  mount_point = f"/mnt/{storage_account_name}/{container_name}",

  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/asadlsmichelin/raw/

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("KeyAcess")

# COMMAND ----------


