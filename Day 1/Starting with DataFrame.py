# Databricks notebook source
from datetime import datetime, date
from pyspark.sql import Row

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
df

# COMMAND ----------

display(df)

# COMMAND ----------

data=[
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
]

# COMMAND ----------

str_schema='a long, b double, c string, d date, e timestamp'

# COMMAND ----------

df = spark.createDataFrame(data, str_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

import datetime
users=[(1,

       'Claire',

       'Gute',

        'clairegute@mail.com',

        True,

        1000.2,

        datetime.date(1991,5,3),

        datetime.date(2022,1,1)

       ),

       (2,

       'Brosina',

       'kodoli',

        'brosina@mail.com',

        False,

        2000.2,

        datetime.date(2000,4,3),

        datetime.date(2022,3,5)

       ),

       (3,

       'Andrew',

       'Adam',

        'andrew@mail.com',

        True,

        1500.2,

        datetime.date(1995,10,21),

        datetime.date(2021,11,10)

       ),

       (4,

       'John',

       'Allen',

        'john@mail.com',

        False,

        None,

        datetime.date(1997,6,30),
        datetime.date(2019,3,31)

       )

]

# COMMAND ----------

df=spark.createDataFrame(users)

# COMMAND ----------

display(df)

# COMMAND ----------

users_schema='''
id INT,
first_name String,
last_name String,
email string,
is_customer Boolean,
amount_paid float,
customer_from Date,
last_update_ts date
'''

# COMMAND ----------

df1=spark.createDataFrame(users, users_schema)

# COMMAND ----------

display(df1)

# COMMAND ----------

import datetime

users2=[{"id":1,

       "first_name":'Naval',

       "last_name":'Yemul',

        "mail":'navalyemul@mail.com',

        "phone_numbers": "+1 234 567 789",

        "is_customer":True,

        "amount_paid":1000.2,

        "customer_from":datetime.date(2002,12,31),

        "last_update_ts":datetime.date(2022,1,1)

       },

       {"id":2,

       "first_name":'John',

       "last_name":'Players',

        "mail":'john@mail.com',

        "phone_numbers": "+1 234 567 789 ",

        "is_customer":True,

        "amount_paid":2000.4,

        "customer_from":datetime.date(2004,5,30),

        "last_update_ts":datetime.date(2022,11,29)

       },

       {"id":3,

       "first_name":'Killer',

       "last_name":'Spykar',

        "mail":'killer@mail.com',

        "phone_numbers":"+1 234 999 000 ",

        "is_customer":True,

        "amount_paid":3000.0,

        "customer_from":datetime.date(2016,3,18),

        "last_update_ts":datetime.date(2020,3,24)

       },

       {"id":4,

       "first_name":'Levis',

       "last_name":'Jeans',

        "mail":'jeans@mail.com',

        "phone_numbers": "+91 000777",

        "is_customer":True,

        "amount_paid":1500.5,

        "customer_from":datetime.date(1990,12,3),

        "last_update_ts":datetime.date(2022,3,2)

       },

       {"id":5,

       "first_name":'Puma',

       "last_name":'Adidas',

        "mail":'puma@mail.com',

        "phone_numbers": " ",

        "is_customer":True,

        "amount_paid":500.5,

        "customer_from":datetime.date(1995,2,19),

        "last_update_ts":datetime.date(2022,1,21)

       }
]

# COMMAND ----------

df=spark.createDataFrame(users, schema_str)

# COMMAND ----------

df.display()

# COMMAND ----------

schema_str= """

       id int,
       first_name string,

       last_name string,

        mail string,

        phone_numbers string,

        is_customer boolean,

        amount_paid double,
        customer_from date,

        last_update_ts date
        """


# COMMAND ----------

df=spark.createDataFrame(users, schema_str)


# COMMAND ----------

display(df)

# COMMAND ----------

schema_str2= """

       id int,
       first_name string,

       last_name string,

        mail string,

        phone_numbers string,

        is_customer boolean,

        amount_paid double,
        customer_from date,

        last_update_ts date
        """

# COMMAND ----------

df2=spark.createDataFrame(users2, schema_str2)

# COMMAND ----------

display(df2)

# COMMAND ----------


