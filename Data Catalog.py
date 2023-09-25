# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Description:
# MAGIC
# MAGIC ###Dataset Name: 
# MAGIC ######Public Transport Data
# MAGIC ###Description: 
# MAGIC ######This dataset contains records related to public transportation operations, including information on routes, schedules, passengers, and delays. The data is sourced from a generated script, and it has been processed using Azure Databricks for further analysis and insights.
# MAGIC ###Transformations:
# MAGIC
# MAGIC #####Time Format Transformation:
# MAGIC
# MAGIC ######Description: The 'DepartureTime' column was transformed to display only the time in HH:mm format.
# MAGIC ######Purpose: To standardize the format of the departure time for analysis.
# MAGIC
# MAGIC #####Data Duration Calculation:
# MAGIC
# MAGIC ######Description: The 'Duration(min)' column was calculated by subtracting the departure time from the arrival time.
# MAGIC ######Purpose: To determine the duration of each trip in minutes for analysis.
# MAGIC
# MAGIC #####Column Removal:
# MAGIC
# MAGIC ######Description: Temporary columns, namely 'ArrivalHour,' 'ArrivalMinute,' 'DepartureHour,' and 'DepartureMinute,' were removed from the dataset.
# MAGIC ######Purpose: To eliminate redundant columns used during the duration calculation.
# MAGIC
# MAGIC #####Arrival Time Transformation:
# MAGIC
# MAGIC ######Description: The 'ArrivalTime' column was transformed to handle values exceeding 23 hours, ensuring consistency.
# MAGIC ######Purpose: To correct and standardize arrival time data.
# MAGIC
# MAGIC #####Convert Arrival Time to Timestamp:
# MAGIC
# MAGIC ######Description: The 'ArrivalTime' column was converted to a timestamp data type for accurate time-based analysis.
# MAGIC ######Purpose: To work with the arrival time as a timestamp, enabling precise time-related calculations.
# MAGIC
# MAGIC #####Format Arrival Time:
# MAGIC
# MAGIC ######Description: The 'ArrivalTime' column was formatted to display only the time portion, in HH:mm format.
# MAGIC ######Purpose: To provide a visually consistent and informative representation of the arrival time.
# MAGIC
# MAGIC ###Data Lineage:
# MAGIC
# MAGIC ######The source of this data is a generated script, which provides raw data related to public transportation operations. The data has been processed and transformed using Azure Databricks.
# MAGIC
# MAGIC ###Usage Guidelines:
# MAGIC
# MAGIC ######.The data can be used for various analyses related to public transportation operations.
# MAGIC ######.Cases where this data may be valuable include route optimization, schedule planning, and understanding passenger behavior and delays.
# MAGIC ######.Precautions:Please ensure that the data is handled securely and in compliance with data protection regulations.
# MAGIC ###This documentation provides an overview of the dataset, the transformations applied, the data's lineage, and guidelines for usage.
