# Databricks notebook source
env = "qa"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Move data from 2024-01-11 to 2024-01-10 and change file_name/file_date

# COMMAND ----------

source_data = f"s3://{env}-ec1-cepal-landing-zone/external_files/newsphone/newsphone_auctions/20240111/"

input_df = spark.read.parquet(source_data)

input_df.display()

# COMMAND ----------

from pyspark.sql.functions import lit
df_casted = input_df.selectExpr(
    "cast(AuctionObJID as int) as AuctionObJID",
    "cast(GroupObjID as int) as GroupObjID",
    "AuctionID",
    "Source",
    "UniqueCode",
    "to_date(AuctionDate, 'yyyy-MM-dd') as AuctionDate",
    "to_date(PublishedDate, 'yyyy-MM-dd') as PublishedDate",
    "to_date(eAuctionPostedDate, 'yyyy-MM-dd') as eAuctionPostedDate",
    "cast(FirstOfferPrice as decimal(18,2)) as FirstOfferPrice",
    "TitleGR",
    "Area",
    "District",
    "Municipality",
    "Prefecture",
    "GeoCoordinatesX",
    "GeoCoordinatesY",
    "GeoLevel",
    "Address",
    "cast(PostCode as int) as PostCode",
    "CategoryGR",
    "SubCategoryGR",
    "LandeaUrl",
    "FloorRawValue",
    "cast(ConstructionYear as int) as ConstructionYear",
    "cast(SurfaceSqM as float) as SurfaceSqM",
    "Tags",
    "TagValues",
    "cast(Active as boolean) as Active",
    "cast(TNMergeID as int) as TNMergeID",
    "HastenerDescr",
    "HastenerVat",
    "DebtorDescr",
    "DebtorVatNumber",
    "AuctionStatusDescr",
    "cast(FinalPrice as decimal(18,2)) as FinalPrice",
    "AuctioneerName",
    "AuctioneerEmail",
    "AuctioneerPhone",
    "SeizureReportNumber",
    "cast(SeizureReportYear as int) as SeizureReportYear",
    "KAEK"
)

df_casted = df_casted.withColumn("file_date", lit("2024-01-10").cast("Date")) \
                        .withColumn("file_name", lit("auctions_20240110.csv"))
df_casted.display()

# COMMAND ----------

landing_bucket = f"{env}-ec1-cepal-landing-zone"
folder_name = "newsphone_auctions"
file_date = "20240110"
df_casted.write.mode("overwrite").parquet(
            f"s3://{landing_bucket}/external_files/newsphone/{folder_name}/{file_date}/"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC Detach and re-attach cluster

# COMMAND ----------

q = f"""
    select * from `{env}-cdh`.landing.newsphone_auctions
    where file_date = "2024-01-10"
    limit 10;
"""
spark.sql(q).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data to 2024-01-11

# COMMAND ----------

auctions_schema = """{
        "AuctionObJID": "INT",
        "GroupObjID": "INT",
        "AuctionID": "NVARCHAR(50)",
        "Source": "NVARCHAR(50)",
        "UniqueCode": "NVARCHAR(200)",
        "AuctionDate": "DATE",
        "PublishedDate": "DATE",
        "eAuctionPostedDate": "DATE",
        "FirstOfferPrice": "DECIMAL (18,2)",
        "TitleGR": "NVARCHAR(MAX)",
        "Area": "NVARCHAR(100)",
        "District": "NVARCHAR(100)",
        "Municipality": "NVARCHAR(200)",
        "Prefecture": "NVARCHAR(128)",
        "GeoCoordinatesX": "NVARCHAR(128)",
        "GeoCoordinatesY": "NVARCHAR(128)",
        "GeoLevel": "NVARCHAR(30)",
        "Address": "NVARCHAR(128)",
        "PostCode": "INT",
        "CategoryGR": "NVARCHAR(128)",
        "SubCategoryGR": "NVARCHAR(128)",
        "LandeaUrl": "NVARCHAR(MAX)",
        "FloorRawValue": "NVARCHAR(128)",
        "ConstructionYear": "INT",
        "SurfaceSqM": "FLOAT",
        "Tags": "NVARCHAR(MAX)",
        "TagValues": "NVARCHAR(MAX)",
        "Active": "BIT",
        "TNMergeID": "INT",
        "HastenerDescr": "NVARCHAR(MAX)",
        "HastenerVat": "NVARCHAR(MAX)",
        "DebtorDescr": "NVARCHAR(MAX)",
        "DebtorVatNumber": "NVARCHAR(MAX)",
        "AuctionStatusDescr": "NVARCHAR(MAX)",
        "FinalPrice": "DECIMAL (18,2)",
        "AuctioneerName": "NVARCHAR(MAX)",
        "AuctioneerEmail": "NVARCHAR(256)",
        "AuctioneerPhone": "NVARCHAR(20)",
        "SeizureReportNumber": "NVARCHAR(1000)",
        "SeizureReportYear": "INT",
        "KAEK": "NVARCHAR(200)"
    }"""

# COMMAND ----------

import json

import boto3
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pyspark.sql.functions import col, date_format, lit

def create_spark_schema_from_json(schema_string: str) -> StructType:
    schema_data = json.loads(schema_string)

    fields = []
    for column_name, data_type in schema_data.items():
        if data_type == "INT":
            field_type = IntegerType()  # type: ignore
        elif data_type == "FLOAT":
            field_type = FloatType()  # type: ignore
        elif data_type == "DOUBLE":
            field_type = DoubleType()  # type: ignore
        elif data_type.startswith("DECIMAL"):
            precision, scale = data_type.strip("DECIMAL ()").split(",")
            field_type = DecimalType(int(precision), int(scale))  # type: ignore
        elif data_type == "DATE":
            field_type = DateType()  # type: ignore
        elif data_type == "BOOLEAN":
            field_type = BooleanType()  # type: ignore
        elif data_type == "BIT":
            field_type = BooleanType()  # type: ignore
        else:
            field_type = StringType()  # type: ignore

        fields.append(StructField(column_name, field_type))

    fields.append(StructField("_corrupt_record", StringType()))

    schema = StructType(fields)
    return schema

# COMMAND ----------

source_bucket = f"{env}-ec1-cepal-landing-zone/tmp/adhoc/"
file_name = "auctions_20240111.csv"
csv_file = f"s3://{source_bucket}/{file_name}"

input_df = (
    spark.read.option("header", "true")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(csv_file)
)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

for column in input_df.columns:
    input_df = input_df.withColumn(column, regexp_replace(column, "\"", ""))

input_df.display()

# COMMAND ----------

expected_schema = create_spark_schema_from_json(auctions_schema)

# COMMAND ----------

df_casted = input_df.selectExpr(
    "cast(AuctionObJID as int) as AuctionObJID",
    "cast(GroupObjID as int) as GroupObjID",
    "AuctionID",
    "Source",
    "UniqueCode",
    "to_date(AuctionDate, 'yyyy-MM-dd') as AuctionDate",
    "to_date(PublishedDate, 'yyyy-MM-dd') as PublishedDate",
    "to_date(eAuctionPostedDate, 'yyyy-MM-dd') as eAuctionPostedDate",
    "cast(FirstOfferPrice as decimal(18,2)) as FirstOfferPrice",
    "TitleGR",
    "Area",
    "District",
    "Municipality",
    "Prefecture",
    "GeoCoordinatesX",
    "GeoCoordinatesY",
    "GeoLevel",
    "Address",
    "cast(PostCode as int) as PostCode",
    "CategoryGR",
    "SubCategoryGR",
    "LandeaUrl",
    "FloorRawValue",
    "cast(ConstructionYear as int) as ConstructionYear",
    "cast(SurfaceSqM as float) as SurfaceSqM",
    "Tags",
    "TagValues",
    "cast(Active as boolean) as Active",
    "cast(TNMergeID as int) as TNMergeID",
    "HastenerDescr",
    "HastenerVat",
    "DebtorDescr",
    "DebtorVatNumber",
    "AuctionStatusDescr",
    "cast(FinalPrice as decimal(18,2)) as FinalPrice",
    "AuctioneerName",
    "AuctioneerEmail",
    "AuctioneerPhone",
    "SeizureReportNumber",
    "cast(SeizureReportYear as int) as SeizureReportYear",
    "KAEK"
)

df_casted = df_casted.withColumn("file_date", lit("2024-01-11").cast("Date")) \
                        .withColumn("file_name", lit("auctions_20240111.csv"))
df_casted.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Manually delete 20240111 folder from path s3://p-ec1-cepal-landing-zone/external_files/newsphone/newsphone_auctions/20240111/

# COMMAND ----------

landing_bucket = f"{env}-ec1-cepal-landing-zone"
folder_name = "newsphone_auctions"
file_date = "20240111"
df_casted.write.mode("overwrite").parquet(
            f"s3://{landing_bucket}/external_files/newsphone/{folder_name}/{file_date}/"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC Detach and re-attach cluster

# COMMAND ----------

q = f"""
    select file_date, count(*) from `{env}-cdh`.landing.newsphone_auctions
    group by all
    order by file_date
"""
spark.sql(q).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define constants

# COMMAND ----------

from pyspark.sql import functions as F

# Constants
PRIMARY_KEYS = ["auctionobjid", "uniquecode", "source"]
ENV = f"{env}"
TABLE_NAME = f"{ENV}_checkpoint_newsphone_auctions"
PREV_RUN_DATE = "2024-01-10"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load initial data to a temp dataframe

# COMMAND ----------

q = f"""
    select * from `{env}-cdh`.landing.newsphone_auctions
    where file_date = "2024-01-10"
"""
df = spark.sql(q)
df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import col, date_format, lit

df_20240110 = df
df_20240110 = df_20240110.withColumn("BusinessDate", lit("2024-01-10").cast("Date")) \
                            .withColumn("LoadingDateTime", date_format("current_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

# df_20240110 = df_20240110.drop("partition_0")
df_20240110.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find dates to run 

# COMMAND ----------

q = f"""
    select distinct cast(file_date as string) from `{env}-cdh`.landing.newsphone_auctions
    order by 1;
"""
run_dates_df = spark.sql(q)
run_dates_df.display()

# COMMAND ----------

run_dates_list = [row["file_date"] for row in run_dates_df.collect() if row["file_date"] > "2024-01-10"]

print(run_dates_list)

# COMMAND ----------

RUN_DATES_LIST = run_dates_list

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data from start

# COMMAND ----------

# Initial save
df_20240110.write.format("delta").mode("overwrite").saveAsTable(TABLE_NAME)

def get_dataset_for_date(run_date):
    """Retrieve dataset for a specific run date."""
    query = (f"SELECT * FROM `{env}-cdh`.landing.newsphone_auctions "
             f"WHERE file_date = '{run_date}'")
    df = spark.sql(query)
    return df.withColumn("BusinessDate", F.lit(run_date).cast("Date")) \
             .withColumn("LoadingDateTime", F.current_timestamp())

def process_date(prev_run_date, run_date):
    """Process and update datasets for a given run date."""
    # Read the previously combined data
    df_prev = spark.read.table(TABLE_NAME).filter(F.col("BusinessDate") == prev_run_date)

    # Get current run date dataset
    df_current = get_dataset_for_date(run_date)

    # Find changes and union with new data
    df_anti_joined = df_prev.join(df_current, on=PRIMARY_KEYS, how="left_anti")
    df_anti_joined = df_anti_joined.withColumn("BusinessDate", lit(run_date).cast("Date"))
    df_final = df_anti_joined.unionByName(df_current).dropDuplicates()

    # Save updated dataset
    df_final.write.format("delta").mode("append").saveAsTable(TABLE_NAME)

# Iterate through run dates and process each
for current_run_date in RUN_DATES_LIST:
    process_date(PREV_RUN_DATE, current_run_date)
    PREV_RUN_DATE = current_run_date

# COMMAND ----------

q = f"""
    select BusinessDate, count(*) from 
    hive_metastore.default.{env}_checkpoint_newsphone_auctions
    group by all
    order by BusinessDate;
"""
spark.sql(q).display()

# COMMAND ----------

q = f"""
    select BusinessDate, auctionobjid, count(*) from 
    hive_metastore.default.{env}_checkpoint_newsphone_auctions
    group by all
    having count(*) > 1;
"""
spark.sql(q).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load to landed newsphone

# COMMAND ----------

from pyspark.sql.functions import cast, col
df = spark.read.table(f"hive_metastore.default.{env}_checkpoint_newsphone_auctions")
df = df.withColumn("LoadingDateTime", col("LoadingDateTime").cast("timestamp"))
df.count()

# COMMAND ----------

df = df.drop("partition_0")

# COMMAND ----------

df.write.format("delta").mode("overwrite").partitionBy("BusinessDate").saveAsTable(f"`{env}-cdh`.landed.newsphone_auctions")

# COMMAND ----------

q = f"""
    DESCRIBE HISTORY `{env}-cdh`.landed.newsphone_auctions
"""
spark.sql(q).display()

# COMMAND ----------

#TODO what is this? D360-2050
q = f"""
    select count(*) from hive_metastore.default.{env}_checkpoint_newsphone_auctions
"""
spark.sql(q).display()

# COMMAND ----------

df_landed = spark.read.format("delta").table(f"`{env}-cdh`.landed.newsphone_auctions")
df_landed.count()
