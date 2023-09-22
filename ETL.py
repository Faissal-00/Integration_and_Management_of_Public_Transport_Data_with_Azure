# Databricks notebook source
#Configure Spark to access Azure Data Lake Storage Gen2 using the account key
spark.conf.set(
    f"fs.azure.account.key.faissalmoufllastorage.dfs.core.windows.net", 
    "MCuRaZ+x1FDZbtFgWoDFu7u9D2EpjNPFoyGSgfGbnYs03Qsvdua+2sUNzTLpNv7e2liaOXz9iEGC+AStYx6O9w=="
)                                                               

# COMMAND ----------

## List the contents of the 'raw' folder in Azure Data Lake Storage Gen2
dbutils.fs.ls("abfss://publictransportdata@faissalmoufllastorage.dfs.core.windows.net/public_transport_data/raw/")

# COMMAND ----------

#Set data lake file location
file_location = "abfss://publictransportdata@faissalmoufllastorage.dfs.core.windows.net/public_transport_data/raw/public-transport-data.csv"
#read in the data to dataframe df
df = spark.read.format("csv").option("inferSchema", "True").option("header",
"True").option("delimeter",",").load(file_location)
#display the dataframe
display(df)  
