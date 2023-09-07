# Databricks notebook source
# MAGIC %md
# MAGIC SAS Token helps in mounting specific files and folders, it gives access to required files only, not to the whole container, unlike Access Key
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key="<sas-token-key>"))

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.asadlsmichelin.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.asadlsmichelin.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.asadlsmichelin.dfs.core.windows.net","?sv=2022-11-02&ss=bfqt&srt=c&sp=rlacupyx&se=2023-09-05T20:05:15Z&st=2023-09-05T12:05:15Z&spr=https&sig=aTNm9%2BG1WRzHx6erQZrwTaF87%2FYuCk%2BXsQNqe3WmEHQ%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")

# COMMAND ----------

dbutils.fs.ls("abfss://raw@asadlsmichelin.dfs.core.windows.net/Input")

# COMMAND ----------

df=spark.read.csv("abfss://raw@asadlsmichelin.dfs.core.windows.net/Input/EmployeesTable.CSV")

# COMMAND ----------


