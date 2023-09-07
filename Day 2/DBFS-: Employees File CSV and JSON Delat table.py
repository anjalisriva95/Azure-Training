# Databricks notebook source
df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/FileStore/tables/raw/EmployeesTable.CSV")

# COMMAND ----------

df.display()


# COMMAND ----------

df1=spark.read.option("header",True).option("inferschema",True).json("dbfs:/FileStore/tables/raw/16_8_23.json")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

df1.display()

# COMMAND ----------

df2=df1.withColumn("current_date",current_timestamp())\
    .withColumn("filepath",input_file_name())

# COMMAND ----------


