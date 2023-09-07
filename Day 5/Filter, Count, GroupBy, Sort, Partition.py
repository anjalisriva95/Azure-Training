# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/asadlsmichelin/raw
# MAGIC

# COMMAND ----------

input_path="dbfs:/mnt/asadlsmichelin/raw/baby-names.csv"

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv(input_path)

# COMMAND ----------

df.display()

# COMMAND ----------

df.createOrReplaceTempView("baby_name")

# COMMAND ----------

df.filter("sex='boy'").show()

# COMMAND ----------

df.filter(col("Year")==1880).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from baby_name where sex='boy'

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from baby_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select year,count(year) as count from baby_name group by year sort by year desc

# COMMAND ----------

df.select("year").groupBy("Year").count().sort("year",ascending=False).display()

# COMMAND ----------

df.select("year").groupBy("Year").count().sort("year".desc).display()

# COMMAND ----------

df.select("year").groupBy("Year").count().sort(col("year").desc()).display()

# COMMAND ----------

df.write.format("parquet").option("path",f"{output_path}/yourname/babyname_parquet").saveAsTable("baby_names_parquet")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/nlyadls/raw/output_babyname

# COMMAND ----------

dbutils.fs.unmount("dbfs:/mnt/nlyadls/raw/")

# COMMAND ----------

dbutils.fs.mount(

  source = "wasbs://raw@nlyadls.blob.core.windows.net",

  mount_point = "/mnt/nlyadls/raw",

  extra_configs = {"fs.azure.account.key.nlyadls.blob.core.windows.net":"VuJ+Sv5xUp+T6iCbNtjYNLXNq2OPRlmI2+ZyzMswdXbMhZ2aj4g/fmbMO6UZAysmxpG5unZQUCbl+ASt6Q9Xrg=="})

# COMMAND ----------

input_path="dbfs:/mnt/nlyadls/raw/Baby_Names.csv"

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv(input_path)

# COMMAND ----------

df.show()

# COMMAND ----------

output_path="dbfs:/mnt/nlyadls/raw/output_babyname"

# COMMAND ----------

df.write.saveAsTable("baby_name")

# COMMAND ----------

df.write.format("parquet").saveAsTable("baby_names_parquet")

# COMMAND ----------

df.write.format("parquet").option("path",f"{output_path}/Anjali/babyname_parquet").saveAsTable("baby_name_parquet")

# COMMAND ----------

df.write.option("path",f"{output_path}/Anjali/babyname_delta").option('delta.columnMapping.mode','name').saveAsTable("baby_name_delta")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/nlyadls/raw/output_babyname/naval/babyname_parquet_year_max/Year=2007/
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from baby_names_delta where year=2007

# COMMAND ----------

df.write.format("parquet").option("path",f"{output_path}/Anjali/babyname_parquet_year").partitionBy("year").saveAsTable("baby_names_parquet_year")

# COMMAND ----------

df.write.option("path",f"{output_path}/Anjali/babyname_delta_year").option('delta.columnMapping.mode','name').partitionBy("year").saveAsTable("baby_names_delta_year")

# COMMAND ----------

df.write.format("parquet").option("path",f"{output_path}/Anjali/babyname_parquet_year_gender").partitionBy("year","Sex").saveAsTable("baby_names_parquet_year_gender")

# COMMAND ----------

df.write.format("parquet").mode("overwrite").option("path",f"{output_path}/naval/babyname_parquet_year_max").partitionBy("year").option("maxRecordsPerFile",4000).saveAsTable("baby_names_parquet_year_max")

# COMMAND ----------


