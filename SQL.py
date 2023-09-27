# Databricks notebook source
#Configure Spark to access Azure Data Lake Storage Gen2 using the account key
spark.conf.set(
    f"fs.azure.account.key.faissalmoufllastorage.dfs.core.windows.net", 
    "SSY4QLBp7RduioHh+aJUrt7TURCcFjZDfmXfIihepoQof6u5WDvFb/+jqLiN6cUBLAvl90bxqT1H+AStZwJrDw=="
)              

# List the contents of the 'raw' folder in Azure Data Lake Storage Gen2
dbutils.fs.ls("abfss://publictransportdata@faissalmoufllastorage.dfs.core.windows.net/public_transport_data/processed/")

#Set data lake file location
file_location = "abfss://publictransportdata@faissalmoufllastorage.dfs.core.windows.net/public_transport_data/processed/processedTransportDataOf_rawTransportDataOf_01_2023.csv.csv"
#read in the data to dataframe df
df = spark.read.format("csv").option("inferSchema", "True").option("header",
"True").option("delimeter",",").load(file_location)

# COMMAND ----------

#display the dataframe
display(df)

# COMMAND ----------

df.createOrReplaceTempView("TransportPublic")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TransportPublic LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TransportPublic WHERE Date = '2023-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DelayCategory, COUNT(*) AS Count
# MAGIC FROM TransportPublic
# MAGIC GROUP BY DelayCategory

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT TransportType, ROUND(AVG(passengers)) AS AvgPassengers
# MAGIC FROM TransportPublic
# MAGIC GROUP BY TransportType
