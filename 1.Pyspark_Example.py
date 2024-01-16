# Databricks notebook source
# MAGIC %md
# MAGIC ##1. Find start and end date for each consecative event status window

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

data = [("2020-06-01","Won"),
("2020-06-02","Won"),
("2020-06-03","Won"),
("2020-06-04","Lost"),
("2020-06-05","Lost"),
("2020-06-06","Lost"),
("2020-06-07","Won"),
("2020-06-08","Won"),
("2020-06-09","Won"),
("2020-06-10","Lost"),
("2020-06-11","Lost")]

df = spark.createDataFrame(data,["Event_date","Event_status"])
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn('Event_date',to_date(col('Event_date')))
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Lag window function : find event changing row

# COMMAND ----------

from pyspark.sql.window import Window

df1 = df.withColumn('Event_change',when(col('Event_status') != lag('Event_status').over(Window.orderBy('Event_date')),1).otherwise(0))

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Sum Window Function: Calculate the running sum to create windows

# COMMAND ----------

df2 = df1.withColumn("Event_group",sum('Event_change').over(Window.orderBy('Event_date')))
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####First&Last window function calculating start and end date

# COMMAND ----------

output_df = df2.groupBy('Event_group','Event_status').agg(first('Event_date').alias('Start_date'), last('Event_date').alias('End_date')).drop('Event_group')
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Employees earning more than managers

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

data = [(1,'Kevin',6000,4),
        (2,'John',11000,4),
        (3,'Rajani',8000,5),
        (4,'Loura',9000,None),
        (5,'Sarah',10000,None)]

schema = StructType([StructField('Id',IntegerType(),True),
                     StructField('Name',StringType(),True),
                     StructField('Salary',IntegerType(),True),
                     StructField('Manager_id',IntegerType(),True)])

df = spark.createDataFrame(data,schema = schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Self Join

# COMMAND ----------

joinDf = df.alias('Employee').join(df.alias('Manager'),col('Employee.Manager_id')== col('Manager.id'),"inner")
display(joinDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Filter where employee salary greater than manager salary

# COMMAND ----------

filterDf = joinDf.filter(col('Employee.Salary')>col('Manager.Salary'))
display(filterDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Final Output
# MAGIC

# COMMAND ----------

ResultDf = filterDf.select('Employee.Name')
display(ResultDf)

# COMMAND ----------


