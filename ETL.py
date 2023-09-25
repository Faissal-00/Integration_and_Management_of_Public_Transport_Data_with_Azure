# Databricks notebook source
# MAGIC %md
# MAGIC ##Head Data

# COMMAND ----------

#Configure Spark to access Azure Data Lake Storage Gen2 using the account key
spark.conf.set(
    f"fs.azure.account.key.faissalmoufllastorage.dfs.core.windows.net", 
    "2uDXwle3KYb3NtDIm0YEvsslYK4OGLfbU9NMo29R83NM0KTxrjNoNuhJX9VphCZHWBcZgCPzfdzL+AStx6bt1g=="
)              

# List the contents of the 'raw' folder in Azure Data Lake Storage Gen2
dbutils.fs.ls("abfss://publictransportdata@faissalmoufllastorage.dfs.core.windows.net/public_transport_data/raw/")

#Set data lake file location
file_location = "abfss://publictransportdata@faissalmoufllastorage.dfs.core.windows.net/public_transport_data/raw/public-transport-data.csv"
#read in the data to dataframe df
df = spark.read.format("csv").option("inferSchema", "True").option("header",
"True").option("delimeter",",").load(file_location) 

# COMMAND ----------

#display the dataframe
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# unique_values = df.select('ArrivalTime').distinct().rdd.map(lambda row: row[0]).collect()
# print(unique_values)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Time calculations

# COMMAND ----------

from pyspark.sql.functions import col, expr

# Format 'DepartureTime' to display only the time
df = df.withColumn("DepartureTime", date_format("DepartureTime", "HH:mm"))

# Format 'DepartureTime' to display only the time
df = df.withColumn("DepartureTime", date_format("DepartureTime", "HH:mm"))

# Assuming you have the columns ArrivalTime and DepartureTime
df = df.withColumn("ArrivalHour", expr("CAST(split(ArrivalTime, ':')[0] AS INT)"))
df = df.withColumn("ArrivalMinute", expr("CAST(split(ArrivalTime, ':')[1] AS INT)"))
df = df.withColumn("DepartureHour", expr("CAST(split(DepartureTime, ':')[0] AS INT)"))
df = df.withColumn("DepartureMinute", expr("CAST(split(DepartureTime, ':')[1] AS INT)"))

df = df.withColumn(
    "Duration(min)",
    (col("ArrivalHour") * 60 + col("ArrivalMinute")) - (col("DepartureHour") * 60 + col("DepartureMinute"))
)

# COMMAND ----------

#display the dataframe
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Column of ArrivalTime

# COMMAND ----------

from pyspark.sql.functions import col, when, expr, to_timestamp, date_format

# Assuming your DataFrame is named df
df = df.withColumn(
    "ArrivalTime",
    when(
        expr("substring(ArrivalTime, 1, 2) > 23"),
        expr("concat('00', substring(ArrivalTime, 3, 5))")
    ).otherwise(col("ArrivalTime"))
)

# Convert the 'ArrivalTime' column to a time type
df = df.withColumn("ArrivalTime", to_timestamp("ArrivalTime", "HH:mm"))

# Format 'ArrivalTime' to display only the time
df = df.withColumn("ArrivalTime", date_format("ArrivalTime", "HH:mm"))

# COMMAND ----------

#display the dataframe
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##date transformations

# COMMAND ----------

#Extract the year, month, day, and day of the week from the date

from pyspark.sql.functions import year, month, dayofmonth, dayofweek
df = df.withColumn("Date", to_date("Date", "yyyy-MM-dd"))
df = df.withColumn("Year", year("Date"))
df = df.withColumn("Month", month("Date"))
df = df.withColumn("DayOfMonth", dayofmonth("Date"))
df = df.withColumn("DayOfWeek", dayofweek("Date"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delay Analysis

# COMMAND ----------

from pyspark.sql.functions import col, when

# Assuming you have a column named 'Delay' which represents the delay in minutes

# Create a new column 'DelayCategory' based on the delay duration
df = df.withColumn(
    "DelayCategory",
    when(col("Delay") <= 0, "Pas de Retard")
    .when((col("Delay") > 0) & (col("Delay") <= 10), "Retard Court")
    .when((col("Delay") > 10) & (col("Delay") <= 20), "Retard Moyen")
    .when(col("Delay") > 20, "Long Retard")
    .otherwise("Unknown")
)

# COMMAND ----------

display(df)
