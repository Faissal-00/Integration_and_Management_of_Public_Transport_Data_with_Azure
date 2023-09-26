# Databricks notebook source
# MAGIC %md
# MAGIC #Data Lake Infrastructure Integration

# COMMAND ----------

# Mounting data lake
storageAccountName = "faissalmoufllastorage"
storageAccountAccessKey = "5eYteI6hynYqMG8i5VkIfhlNSyMYul3isvgAMW+xsq7oNK0oGDDNgrIAwMG+51cCkcxt/c4o27V3+AStPNkLuA=="
sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-09-26T18:27:40Z&st=2023-09-26T10:27:40Z&spr=https&sig=M%2FPC62OOGOU7Jw8FJCUU9aLIcfjh5zXCYYXNuE9cVOw%3D"
blobContainerName = "publictransportdata"
mountPoint = "/mnt/publictransportdata/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

#Configure Spark to access Azure Data Lake Storage Gen2 using the account key
spark.conf.set(
    f"fs.azure.account.key.faissalmoufllastorage.dfs.core.windows.net", 
    "5eYteI6hynYqMG8i5VkIfhlNSyMYul3isvgAMW+xsq7oNK0oGDDNgrIAwMG+51cCkcxt/c4o27V3+AStPNkLuA=="
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

# MAGIC %md
# MAGIC #ETL processes with Azure Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ##Time calculations

# COMMAND ----------

from pyspark.sql.functions import col, expr, date_format

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

columns_to_drop = ['ArrivalHour', 'ArrivalMinute', 'DepartureHour', 'DepartureMinute']
df = df.drop(*columns_to_drop)

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
# MAGIC ##Date Transformations

# COMMAND ----------

#Extract the year, month, day, and day of the week from the date

from pyspark.sql.functions import to_date, year, month, dayofmonth, dayofweek
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

# COMMAND ----------

# MAGIC %md
# MAGIC ##Passenger Analysis

# COMMAND ----------

from pyspark.sql.functions import when, lit, mean

# Calculate the mean of the PassengerCount column
mean_passenger_count = df.select(mean(df["Passengers"])).collect()[0][0]

# Use the mean as the threshold to identify peak hours
df = df.withColumn("PeakHour", when(df["Passengers"] > mean_passenger_count, "Peak").otherwise("Off-Peak"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Route Analysis

# COMMAND ----------

from pyspark.sql.functions import avg, sum

# Group by the 'Route' column and calculate mean delay, mean passengers, and total trips
route_analysis = df.groupBy("Route").agg(
    avg("Delay").cast("int").alias("MeanDelay"),
    avg("Passengers").cast("int").alias("MeanPassengers"),
    sum(lit(1)).cast("int").alias("TotalTrips")
)

# Show the route analysis
route_analysis.show()

# COMMAND ----------

pandasdf=df.toPandas()

# Define the full output path including the file name
output_path = f"/dbfs{mountPoint}public_transport_data/processed/processedTransportDataOf_01_2023.csv.csv"

# Save the DataFrame to a CSV file
pandasdf.to_csv(output_path, header=True)

# Print a success message
print(f"CSV file saved to: {output_path}")
