# Databricks notebook source
employee= [(1,'Scott', 1000,"US"),(2,'John', 1200,"US"),(1,'Scott', 1000,"US"),(1,'Scott', 1000,"US"),(1,'Scott', 1000,"US"),(1,'Scott', 1000,None),(3,'David', None,"US"),(None,'Franklin', None,"India"),(4,'Franklin', None,"India"),(None,None, None,None),(None,None, None,None),(None,None, None,None),(None,None, None,None),(None,None, None,None),(5,'Steve', 2000,"")]
schema=["emp_id","name","salary","nationality"]
df=spark.createDataFrame(employee,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Duplicates

# COMMAND ----------

help(df.drop_duplicates)

# COMMAND ----------

df.dropDuplicates()

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.dropDuplicates(["emp_id"]).display()

# COMMAND ----------

df1=df.dropDuplicates(["emp_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC NA- Null value fucntions in Pyspark
# MAGIC 1. Drop
# MAGIC 2. Fill
# MAGIC 3. Replace

# COMMAND ----------

df.na.drop().show()

# COMMAND ----------

df.dropna(how="all").show()

# COMMAND ----------

df1.dropna(how="all",subset="emp_id").show()

# COMMAND ----------

df.display()

# COMMAND ----------

df.fillna("India").show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.fillna("India",subset=["nationality"]).show()

# COMMAND ----------

df.fillna(800,subset=["salary"]).show()

# COMMAND ----------

df.fillna({"emp_id":"100","name":"unknown","salary":800,"nationality":"India"}).show()

# COMMAND ----------

df.replace("","uk").show()

# COMMAND ----------

df.replace(1000,1500).show()

# COMMAND ----------

df.replace(None,1000).show()

# COMMAND ----------

df.replace("Scott","Steve",subset=["name"]).show()

# COMMAND ----------


