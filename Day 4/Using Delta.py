# Databricks notebook source
# MAGIC %python
# MAGIC
# MAGIC from delta.tables import *

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC DeltaTable.createIfNotExists(spark) \
# MAGIC     .tableName("delta.people10m") \
# MAGIC         .addColumn("id", "INT") \
# MAGIC             .addColumn("firstName", "STRING") \
# MAGIC                 .addColumn("middleName", "STRING") \
# MAGIC                     .addColumn("lastName", "STRING", comment = "surname") \
# MAGIC                         .addColumn("gender", "STRING") \
# MAGIC                             .addColumn("birthDate", "TIMESTAMP") \
# MAGIC                                 .addColumn("ssn", "STRING") \
# MAGIC                                     .addColumn("salary", "INT") \
# MAGIC                                         .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.people10m (
# MAGIC
# MAGIC   id INT,
# MAGIC
# MAGIC   firstName STRING,
# MAGIC
# MAGIC   middleName STRING,
# MAGIC
# MAGIC   lastName STRING,
# MAGIC
# MAGIC   gender STRING,
# MAGIC
# MAGIC   birthDate TIMESTAMP,
# MAGIC
# MAGIC   ssn STRING,
# MAGIC
# MAGIC   salary INT
# MAGIC
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC DeltaTable.createIfNotExists(spark) \
# MAGIC     .tableName("delta.people10m") \
# MAGIC         .addColumn("id", "INT") \
# MAGIC             .addColumn("firstName", "STRING") \
# MAGIC                 .addColumn("middleName", "STRING") \
# MAGIC                     .addColumn("lastName", "STRING", comment = "surname") \
# MAGIC                         .addColumn("gender", "STRING") \
# MAGIC                             .addColumn("birthDate", "TIMESTAMP") \
# MAGIC                                 .addColumn("ssn", "STRING") \
# MAGIC                                     .addColumn("salary", "INT") \
# MAGIC                                         .execute()

# COMMAND ----------


