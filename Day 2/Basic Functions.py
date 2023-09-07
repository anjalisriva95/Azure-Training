# Databricks notebook source
data = [("James","Smith","USA","CA"),

    ("Michael","Rose","USA","NY"),

    ("Robert","Williams","USA","CA"),

    ("Maria","Jones","USA","FL")

  ]

columns = ["firstname","lastname","country","state"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONCAT
# MAGIC ## SPLIT
# MAGIC ## LITT
# MAGIC ## ALIAS
# MAGIC ## WithColumnRenamed
# MAGIC ## Upper Lower
# MAGIC ## Time Date Format
# MAGIC ## Substring

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.createDataFrame(data = data, schema = columns)

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(col("firstname"),"lastname",df.country,df["state"]).display()

# COMMAND ----------

df.select("*",concat("firstname",lit(" "),"lastname").alias("fullname")).display()

# COMMAND ----------

df.withColumnRenamed("firstname","forename").withColumnRenamed("lastname","surname").withColumnRenamed("country","Country").display()

# COMMAND ----------

new_columns=['forename', 'surname', 'country', 'state']

# COMMAND ----------

df1=df.toDF(*new_columns)

# COMMAND ----------

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

# COMMAND ----------

df.withColumn("fullname",concat("firstname",lit(" "),"middlename",lit(" "),"lastname")).display()

# COMMAND ----------

df.withColumn("firstname",concat("firstname",lit(" "),"middlename",lit(" "),"lastname")).display()

# COMMAND ----------

df = spark.createDataFrame(data = data, schema = columns)

# COMMAND ----------

df.withColumn("firstname",concat("firstname",lit(" "),"middlename",lit(" "),"lastname")).display()

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0,

                      "united states", "+1 123 456 7890", "123 45 6789"

                     ),

                     (2, "Henry", "Ford", 1250.0,

                      "India", "+91 234 567 8901", "456 78 9123"

                     ),

                     (3, "Nick", "Junior", 750.0,

                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"

                     ),

                     (4, "Bill", "Gomes", 1500.0,

                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"

                     )

                ]

# COMMAND ----------

employeesDF = spark.createDataFrame(employees,

                    schema="""employee_id INT, first_name STRING,

                    last_name STRING, salary FLOAT, nationality STRING,

                    phone_number STRING, ssn STRING"""

                   )

# COMMAND ----------

display(employeesDF)

# COMMAND ----------

employeesDF.withColumn("nationality",upper("nationality")).display()

# COMMAND ----------

employeesDF\
    .withColumn("nationality_upper",upper("nationality"))\
        .withColumn("nationality_lower",lower("nationality"))\
            .withColumn("nationality_initcap",initcap("nationality"))\
                .withColumn("length",length("nationality"))\
                    .display()

# COMMAND ----------

df.withColumn("TEST", substring('firstname', 1,4)

# COMMAND ----------

employeesDF\
    .withColumn("ssn_new",substring("ssn",-4,4))\
        .display()

# COMMAND ----------

employeesDF.withColumn("last4ssn",substring("ssn",-4,4)).display()

# COMMAND ----------

employeesDF.withColumn("countrycode",split("phone_number"," ")).display()

# COMMAND ----------

employeesDF.withColumn("countrycode",split("phone_number"," ")[0])\
    .withColumn("areacode",split("phone_number"," ")[1])\
        .display()

# COMMAND ----------

df.withColumn("current_time",current_timestamp()).display()

# COMMAND ----------

df.withColumn("newdate",date_format("dob","MM-dd-yyyy")).display()

# COMMAND ----------

df.withColumn("newdate",to_date("dob")).display()

# COMMAND ----------


