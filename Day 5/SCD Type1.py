# Databricks notebook source
Merge and Upsert(update and Insert)

# COMMAND ----------

employee= [(1,'Scott', 1000,"US"),(2,'John', 1200,"US")]
schema=["emp_id","name","salary","nationality"]

# COMMAND ----------

df=spark.createDataFrame(employee,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC create table employee as
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO employee as target
# MAGIC
# MAGIC USING source_view as source
# MAGIC
# MAGIC ON target.emp_id = source.emp_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC
# MAGIC   UPDATE SET
# MAGIC
# MAGIC     target.name=source.name,
# MAGIC
# MAGIC     target.salary=source.salary,
# MAGIC
# MAGIC     target.nationality=source.nationality
# MAGIC
# MAGIC WHEN NOT MATCHED
# MAGIC
# MAGIC   THEN INSERT (
# MAGIC
# MAGIC     emp_id,
# MAGIC
# MAGIC     name,
# MAGIC
# MAGIC     salary,
# MAGIC
# MAGIC     nationality
# MAGIC
# MAGIC   )
# MAGIC
# MAGIC   VALUES (
# MAGIC
# MAGIC    emp_id,
# MAGIC
# MAGIC     name,
# MAGIC
# MAGIC     salary,
# MAGIC
# MAGIC     nationality
# MAGIC
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

employee1= [(1,'Scott', 1000,"India"),(2,'John', 1200,"US"),(3,'Paul', 1200,"India")]
schema1=["emp_id","name","salary","nationality"]

# COMMAND ----------

df1=spark.createDataFrame(employee1,schema1)

# COMMAND ----------

df1.createOrReplaceTempView("source_view")

# COMMAND ----------

df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------


