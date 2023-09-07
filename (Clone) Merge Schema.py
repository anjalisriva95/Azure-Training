# Databricks notebook source
data=[(1,'a',30),(2,'b',30),(3,'c',31)]

# COMMAND ----------

schema=["id","name","age"]

# COMMAND ----------

df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.saveAsTable("mergeschema")

# COMMAND ----------

df.createOrReplaceTempView("mergeview")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE mergeschema as
# MAGIC select * from mergeview

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.mergeschema

# COMMAND ----------

data=[(4,'d',31,"India"),(5,'e',32,"US"),(6,'f',25,"UK"),(6,'f',25,"UK")]

# COMMAND ----------

schema2=["id","name","age","location"]

# COMMAND ----------

df1=spark.createDataFrame(data,schema2)

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.write.saveAsTable("mergeschema")

# COMMAND ----------

modes:overwrite, append, error, ignore

# COMMAND ----------

df1.write.mode("append").saveAsTable("mergeschema")

# COMMAND ----------

df1.write.mode("append").option("mergeSchema", "true").saveAsTable("mergeschema")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mergeschema

# COMMAND ----------

df1.show()

# COMMAND ----------


