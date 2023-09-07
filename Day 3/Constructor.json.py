# Databricks notebook source
# MAGIC %fs  ls dbfs:/mnt/asadlsmichelin/raw/

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

constructor_df=spark.read.json("dbfs:/mnt/asadlsmichelin/raw/constructors.json")

# COMMAND ----------

constructor_df_final=constructor_df.withColumn("ingestiondate", current_timestamp()).withColumn("path",input_file_name()).drop("url")

# COMMAND ----------

constructor_df_final.write.option("path","dbfs:/mnt/asadlsmichelin/raw/formuala1/constructor").saveAsTable("formula1.constructor")

# COMMAND ----------

constructor_df_final.write.mode("overwrite").option("path","dbfs:/mnt/asadlsmichelin/raw/formuala1/constructor2").saveAsTable("formula1.constructor2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.formula1.constructor2

# COMMAND ----------

constructor_df_final.write.format("parquet").option("path","dbfs:/mnt/asadlsmichelin/raw/formuala1/constructor_parquet").saveAsTable("formula1.constructor_parquet")

# COMMAND ----------


