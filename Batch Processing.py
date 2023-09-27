# Databricks notebook source
# Mounting data lake
storageAccountName = "faissalmoufllastorage"
storageAccountAccessKey = "SSY4QLBp7RduioHh+aJUrt7TURCcFjZDfmXfIihepoQof6u5WDvFb/+jqLiN6cUBLAvl90bxqT1H+AStZwJrDw=="
sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-09-27T15:59:11Z&st=2023-09-27T07:59:11Z&spr=https&sig=O53raU88xTm%2FYCTm8Eob2v0SCRn96CdZXJ08Sy5NHSA%3D"
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

# Define the paths for raw, processed, and archive folders
raw_folder = "/mnt/publictransportdata/public_transport_data/raw/"
processed_folder = "/dbfs/mnt/publictransportdata/public_transport_data/processed/"
archive_folder = "/mnt/publictransportdata/public_transport_data/archive/"

# List the contents of the 'raw' folder
files = dbutils.fs.ls(raw_folder)

# Initialize a counter to keep track of the number of files processed
file_counter = 0

# Loop through the raw data files in batches of two
for i in range(0, len(files), 2):
    batch_files = files[i:i+2]

    for file in batch_files:
        # Load the data from the raw folder
        file_location = file.path
        df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").option("delimiter", ",").load(file_location)

        # Apply your data transformations here

        from pyspark.sql.functions import col, expr, date_format

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

        from pyspark.sql.functions import col, when, expr, to_timestamp, date_format

        # Assuming your DataFrame is named df
        df = df.withColumn(
            "ArrivalTime",
            when(
                expr("substring(ArrivalTime, 1, 2) > 23"),
                expr("concat('00', substring(ArrivalTime, 3, 5))")
            ).otherwise(col("ArrivalTime")
            )
        )

        # Convert the 'ArrivalTime' column to a time type
        df = df.withColumn("ArrivalTime", to_timestamp("ArrivalTime", "HH:mm"))

        # Format 'ArrivalTime' to display only the time
        df = df.withColumn("ArrivalTime", date_format("ArrivalTime", "HH:mm"))

        # Extract the year, month, day, and day of the week from the date
        from pyspark.sql.functions import to_date, year, month, dayofmonth, dayofweek
        df = df.withColumn("Date", to_date("Date", "yyyy-MM-dd"))
        df = df.withColumn("Year", year("Date"))
        df = df.withColumn("Month", month("Date"))
        df = df.withColumn("DayOfMonth", dayofmonth("Date"))
        df = df.withColumn("DayOfWeek", dayofweek("Date"))

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

        from pyspark.sql.functions import when, lit, mean

        # Calculate the mean of the PassengerCount column
        mean_passenger_count = df.select(mean(df["Passengers"])).collect()[0][0]

        # Use the mean as the threshold to identify peak hours
        df = df.withColumn("PeakHour", when(df["Passengers"] > mean_passenger_count, "Peak").otherwise("Off-Peak"))

        # Save the DataFrame to a CSV file in the processed folder using Pandas
        file_name = "processedTransportDataOf_" + file.name
        pandasdf = df.toPandas()

        # Define the full output path including the file name (without /dbfs)
        output_path = f"{processed_folder}{file_name}.csv"

        # Save the DataFrame to a CSV file using Pandas
        pandasdf.to_csv(output_path, header=True, index=False)

        # Print a success message
        print(f"CSV file saved to: {output_path}")

        # After saving to processed, copy the file to the archive folder
        archive_path = archive_folder + file.name
        dbutils.fs.cp(file.path, archive_path)
        print(f"File copied to archive: {archive_path}")

        # Delete the file from the raw folder
        dbutils.fs.rm(file.path)

        # Increment the file counter
        file_counter += 1

        # Check if two files have been processed and display a message
        if file_counter % 2 == 0:
            print(f"Successfully processed {file_counter} files.")

# COMMAND ----------

dbutils.fs.unmount(mountPoint)
