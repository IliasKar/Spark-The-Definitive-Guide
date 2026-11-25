# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession

# Path to the CSV file
csv_file = "/Volumes/ddd/default/lookups/countries.csv"

# Read CSV into a DataFrame
df = spark.read.csv(csv_file, header=True, inferSchema=True)

# Save DataFrame as a table in the default database
table_name = "ddd.default.countries"
df.write.saveAsTable(table_name)

# Path to the CSV file
csv_file = "/Volumes/ddd/default/lookups/places.csv"

# Read CSV into a DataFrame
df = spark.read.csv(csv_file, header=True, inferSchema=True)

# Save DataFrame as a table in the default database
table_name = "ddd.default.places"
df.write.saveAsTable(table_name)



