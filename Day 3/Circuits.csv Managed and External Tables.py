# Databricks notebook source
circuits_df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/asadlsmichelin/raw/circuits.csv")

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

circuits_df_final=circuits_df.withColumn("ingestiondate",current_timestamp()).withColumn("path",input_file_name()).drop("url")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema formula1

# COMMAND ----------

circuits_df_final.write.saveAsTable("formula1.circuits_managed")

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/formula1.db/circuits_managed/

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/formula1.db/

# COMMAND ----------

circuits_df_final.write.option("path","dbfs:/mnt/asadlsmichelin/raw/formuala1/circuits").saveAsTable("formula1.circuits_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC use formula1;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table circuits_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC use formula1;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table circuits_external

# COMMAND ----------

# MAGIC %sql
# MAGIC use formula1;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/asadlsmichelin/raw/formuala1/circuits`

# COMMAND ----------

# MAGIC %md
# MAGIC CTAS- Create Table as Select

# COMMAND ----------

# MAGIC %sql
# MAGIC create table formula1.circuits as
# MAGIC select * from delta.`dbfs:/mnt/asadlsmichelin/raw/formuala1/circuits`

# COMMAND ----------


