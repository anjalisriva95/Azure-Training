# Databricks notebook source
dbutils.fs.mount(
    source = "wasbs://raw@asadlsmichelin.blob.core.windows.net",
    mount_point = "/mnt/asadlsmichelin/raw",
    extra_configs = {"fs.azure.account.key.asadlsmichelin.blob.core.windows.net":"cCBxsHupTxobFsRlwhC9wH7a829Q4pzVbex7ciwkc/k104Vi91fP5pByIrg9GYV/u3t5L+sUMG01+ASt3Hah4Q=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/asadlsmichelin/raw/

# COMMAND ----------

dbutils.fs.unmount("/mt/nlyadls/raw")

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://raw@nlyadls.blob.core.windows.net",
    mount_point = "/mnt/nlyadls/raw",
    extra_configs = {"fs.azure.account.key.asadlsmichelin.blob.core.windows.net":"VuJ+Sv5xUp+T6iCbNtjYNLXNq2OPRlmI2+ZyzMswdXbMhZ2aj4g/fmbMO6UZAysmxpG5unZQUCbl+ASt6Q9Xrg=="})

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

dbutils.fs.unmount("/mnt/asadlsmichelin/raw")

# COMMAND ----------


