from typing import Any

import logging

from py4j.java_gateway import JavaObject  # type: ignore
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, translate
from pyspark.sql.types import BooleanType, FloatType

import sys, os

# Add the path to the authentication_helpers directory
relative_path_auth = "../helpers/authentication_helpers/"
path = os.path.abspath(relative_path_auth)
if path not in sys.path:
    sys.path.append(path)

# Import the authentication_utils module after adding the path
from authentication_utils import *

relative_path = "../helpers/lng_lnd/"
path = os.path.abspath(relative_path)
if path not in sys.path:
    sys.path.append(path)

from landed_helpers import *
from landing_helpers import *
from newsphone import *
from external_file_helpers import *

from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, translate, to_date, trim,
    coalesce, lit , when
)
from pyspark.sql.types import (
    BooleanType, FloatType,
    StructType, StructField, StringType
)


def load_rcg_external_file(
    spark: SparkSession,
    args: dict[str, Any]
) -> DataFrame:

    CORRUPT_RECORD = "_corrupt_record"

    spark.catalog.clearCache()
    
    table_schema = args.get("table_schema")
    file_name = args.get("file_name")
    schema_catalog = args.get("schema_catalog")
    source_bucket = args.get("source_bucket")
    folder_name = args.get("folder_name")
    cdh_location_lng = args.get("cdh_location_lng")

    object_key = f"incoming/newsphone/{file_name}"

    # ------------------ FILE CHECK ------------------
    if not file_exists_in_s3(source_bucket, object_key):
        raise FileNotFoundError(
            f"The file {object_key} does not exist in bucket {source_bucket}"
        )

    csv_file = f"s3://{source_bucket}/incoming/newsphone/{file_name}"

    # ------------------ SAFE CSV READ ------------------
    input_df = (
         spark.read
            .option("header", "true")
            .option("sep", ";")
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("quoteMode", "ALL")
            .option("encoding", "UTF-8")
            .option("inferSchema", "true")
            .csv(csv_file)
    )

    input_df = input_df.toDF(*[c.lower() for c in input_df.columns])

    # ------------------ DATE VALIDATION ------------------
    if "rcg_provider_file_date" not in input_df.columns:
        raise Exception("rcg_provider_file_date column does not exist in the CSV!")

    raw_dates = (
        input_df.select("rcg_provider_file_date")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .filter(lambda x: x is not None and x != "")
            .collect()
    )

    if len(raw_dates) == 0:
        raise Exception("rcg_provider_file_date column contains only NULL or empty values")

    if len(raw_dates) > 1:
        raise Exception(f"Multiple raw rcg_provider_file_date values found: {raw_dates}")

    # ------------------ COLUMN VALIDATION ------------------
    expected_columns = [
        c.lower() for c in extract_column_names_from_schema(table_schema)
    ]
    actual_columns = [c.lower() for c in input_df.columns]

    missing_columns = set(expected_columns) - set(actual_columns)
    extra_columns = set(actual_columns) - set(expected_columns)

    if missing_columns:
        raise Exception(f"Missing columns in CSV: {', '.join(missing_columns)}")

    if extra_columns:
        raise Exception(f"Extra columns in CSV: {', '.join(extra_columns)}")

    # ------------------ CLEAN DATA ------------------
    input_df = input_df.select(
        [regexp_replace(col(c), '"', "").alias(c) for c in input_df.columns]
    )

    # Special case
    if file_name.split(".csv")[0].split("_")[0] == "mbp_snapshot":
        input_df = input_df.withColumn(
            "surfacesqm", regexp_replace(col("surfacesqm"), "[^0-9.]", "")
        )

    expected_schema = create_spark_schema_from_json(table_schema)

    # ------------------ CAST TYPES ------------------
    boolean_fields = [
        f.name for f in expected_schema.fields if isinstance(f.dataType, BooleanType)
    ]
    for cname in boolean_fields:
        input_df = input_df.withColumn(cname, col(cname).cast("boolean"))

    float_fields = [
        f.name for f in expected_schema.fields if isinstance(f.dataType, FloatType)
    ]
    for cname in float_fields:
        input_df = input_df.withColumn(cname, translate(col(cname), ",", "."))

    # ------------------ DATE CAST (SAFE) ------------------
    input_df = input_df.withColumn(
        "rcg_provider_file_date",
        when(
            trim(col("rcg_provider_file_date")).rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),
            to_date(trim(col("rcg_provider_file_date")), "d/M/yyyy")
        ).when(
            trim(col("rcg_provider_file_date")).rlike(r"^\d{4}-\d{2}-\d{2}$"),
            to_date(trim(col("rcg_provider_file_date")), "yyyy-MM-dd")
        ).when(
            trim(col("rcg_provider_file_date")).rlike(r"^\d{8}$"),
            to_date(trim(col("rcg_provider_file_date")), "yyyyMMdd")
        ).otherwise(None)
    )
        
        # coalesce(
            
        #     to_date(trim(col("rcg_provider_file_date")), "d/M/yyyy"),
        #     to_date(trim(col("rcg_provider_file_date")), "dd/MM/yyyy"),
        #     to_date(trim(col("rcg_provider_file_date")), "yyyy-MM-dd"),
        #     to_date(trim(col("rcg_provider_file_date")), "yyyyMMdd")
        # )
    # )

    # ------------------ REORDER COLUMNS ------------------
    # Exclude _corrupt_record here
    expected_columns_in_order = [
        f.name for f in expected_schema.fields if f.name != "_corrupt_record"
    ]
    input_df = input_df.select(*expected_columns_in_order)

    #fix date format
    # for f in expected_schema.fields:
    #     if isinstance(f.dataType, DateType):
    #         input_df = input_df.withColumn(
    #             f.name,
    #             date_format(col(f.name), "yyyy-MM-dd")
    #         )

    

    # ------------------ INTERMEDIATE CSV WRITE ------------------
    new_csv_file_path = (
        f"s3://{source_bucket}/tmp/rcg/transformed_csv_files/{file_name}"
    )

    (
        input_df
            .coalesce(1)
            .write
            .option("header", True)
            .option("sep", ";")
            .option("quote", '"')
            .option("escape", '"')
            .option("quoteMode", "ALL")
            .option("encoding", "UTF-8")
            .option("multiLine", True)
            .mode("overwrite")
            .csv(new_csv_file_path)
    )


    # ------------------ SECOND READ WITH SCHEMA ------------------
    df = (
        spark.read
            .option("header", "true")
            .option("sep", ";")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
            .option("dateFormat", "yyyy-MM-dd")
            .option("quote", '"')
            .option("escape", '"')
            .option("quoteMode", "ALL")
            .option("multiLine", True)
            .option("encoding", "UTF-8")
            .schema(expected_schema)
            .csv(new_csv_file_path)
    )


    # If no rows survived the schema-enforced read and the file is empty, raise error
    if df.limit(1).count() == 0:
        raise Exception(
            f"File {file_name} produced 0 rows after schema parsing - failing job"
        )

    # ------------------ CORRUPT RECORD HANDLING ------------------
    # Rows that failed schema parsing have a non-null _corrupt_record
    bad_records = df.filter(col(CORRUPT_RECORD).isNotNull())
    bad_records.cache()
    bad_count = bad_records.count()
    logging.warning(
        f"[RCG CORRUPT RECORDS] file={file_name} bad_records={bad_count}"
    )

    if bad_count > 0:
        bad_records.coalesce(1).write.format("csv").save(
            f"s3://{source_bucket}/tmp/rcg/rejected_files/{file_name}",
            header=True,
            mode="overwrite",
            sep=";",
        )

    # ------------------ DATE VALIDATION ------------------
    # Only consider rows that parsed cleanly (corrupt rows have a null date).
    unique_dates = (
        df.filter(col("rcg_provider_file_date").isNotNull())
          .select("rcg_provider_file_date")
          .distinct()
          .collect()
    )

    if len(unique_dates) != 1:
        values = [row["rcg_provider_file_date"] for row in unique_dates]
        raise Exception(f"The file has many dates {values}")

    formatted_date = unique_dates[0]["rcg_provider_file_date"]
    formatted_date_str = formatted_date.strftime("%Y-%m-%d")

    # Derive the flag BEFORE dropping the corrupt column, then land all rows.
    df = df.withColumn(
        "CorruptRecordFlag",
        when(col(CORRUPT_RECORD).isNotNull(), lit(True)).otherwise(lit(False)),
    )
    df = df.drop(CORRUPT_RECORD)

    df = df.withColumn("Cluster_0", lit(formatted_date_str).cast("string"))

    partition_column = "Cluster_0"
    write_to_landing(
        spark,
        df,
        cdh_location_lng,
        partition_column,
        formatted_date_str
    )

    return df



def load_newsphone_external_file(
    spark: SparkSession, args: dict[str, Any] 
) -> None:
    CORRUPT_RECORD = "_corrupt_record"

    # auctions_schema = """{
    #     "AuctionObJID": "INT",
    #     "GroupObjID": "INT",
    #     "AuctionID": "NVARCHAR(50)",
    #     "Source": "NVARCHAR(50)",
    #     "UniqueCode": "NVARCHAR(200)",
    #     "AuctionDate": "DATE",
    #     "PublishedDate": "DATE",
    #     "eAuctionPostedDate": "DATE",
    #     "FirstOfferPrice": "DECIMAL (18,2)",
    #     "TitleGR": "NVARCHAR(MAX)",
    #     "Area": "NVARCHAR(100)",
    #     "District": "NVARCHAR(100)",
    #     "Municipality": "NVARCHAR(200)",
    #     "Prefecture": "NVARCHAR(128)",
    #     "GeoCoordinatesX": "NVARCHAR(128)",
    #     "GeoCoordinatesY": "NVARCHAR(128)",
    #     "GeoLevel": "NVARCHAR(30)",
    #     "Address": "NVARCHAR(128)",
    #     "PostCode": "INT",
    #     "CategoryGR": "NVARCHAR(128)",
    #     "SubCategoryGR": "NVARCHAR(128)",
    #     "LandeaUrl": "NVARCHAR(MAX)",
    #     "FloorRawValue": "NVARCHAR(128)",
    #     "ConstructionYear": "INT",
    #     "SurfaceSqM": "FLOAT",
    #     "Tags": "NVARCHAR(MAX)",
    #     "TagValues": "NVARCHAR(MAX)",
    #     "Active": "BIT",
    #     "TNMergeID": "INT",
    #     "HastenerDescr": "NVARCHAR(MAX)",
    #     "HastenerVat": "NVARCHAR(MAX)",
    #     "DebtorDescr": "NVARCHAR(MAX)",
    #     "DebtorVatNumber": "NVARCHAR(MAX)",
    #     "AuctionStatusDescr": "NVARCHAR(MAX)",
    #     "FinalPrice": "DECIMAL (18,2)",
    #     "AuctioneerName": "NVARCHAR(MAX)",
    #     "AuctioneerEmail": "NVARCHAR(256)",
    #     "AuctioneerPhone": "NVARCHAR(20)",
    #     "SeizureReportNumber": "NVARCHAR(1000)",
    #     "SeizureReportYear": "INT",
    #     "KAEK": "NVARCHAR(200)"
    # }"""

    # auction_report_content_schema = """{
    #     "UniqueCode": "NVARCHAR(200)",
    #     "FileContent": "NVARCHAR(MAX)"
    # }"""

    # basicdata_schema = """{
    #     "UniqueCode": "NVARCHAR(200)",
    #     "Status": "NVARCHAR(400)",
    #     "Court_Type": "NVARCHAR(400)",
    #     "ConductionDate": "DATE",
    #     "PublishDate": "DATE",
    #     "InitialPrice": "DECIMAL (18,2)",
    #     "Type": "NVARCHAR(400)",
    #     "DebtorVatNumber": "NVARCHAR(MAX)",
    #     "DebtorDescr": "NVARCHAR(MAX)",
    #     "MultipleDeptors": "BIT",
    #     "HastenerDescr": "NVARCHAR(MAX)",
    #     "Servicer": "NVARCHAR(400)",
    #     "HastenerVat": "NVARCHAR(MAX)",
    #     "HastenerCountry": "NVARCHAR(MAX)",
    #     "RelatedAuctionCode": "NVARCHAR(MAX)",
    #     "HasJudgment": "BIT"
    # }"""

    table_schema = args.get("table_schema")
    file_name = args.get("file_name")
    schema_catalog = args.get("schema_catalog")
    source_bucket = args.get("source_bucket")
    folder_name = args.get("folder_name")
    object_key = f"incoming/newsphone/{file_name}"
    cdh_location_lng = args.get("cdh_location_lng")

    #  Check if the file exists
    if not file_exists_in_s3(
        source_bucket,  # type: ignore
        object_key,  # type: ignore
    ):
        raise FileNotFoundError(
            f"The file {object_key} does not exist in bucket {source_bucket}"
        )

    if file_name:
        file_date = file_name.split(".csv")[0].split("_")[1]
    else:
        file_date = "00000000"

    csv_file = f"s3://{source_bucket}/incoming/newsphone/{file_name}"

    input_df = (
        spark.read.option("header", "true")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(csv_file)
    )

 


    column_names = input_df.columns

    # Define the expected column names
    expected_columns = extract_column_names_from_schema(
        table_schema  # type: ignore
    )

    # Check if all expected columns are present
    missing_columns = set(expected_columns) - set(column_names)

    # If any expected columns are missing, fail the job
    if missing_columns:
        missing_columns_str = ", ".join(missing_columns)
        raise Exception(f"Missing columns in CSV: {missing_columns_str}")

    # Check if there are any extra columns present
    extra_columns = set(column_names) - set(expected_columns)

    # If any extra columns are present, fail the job
    if extra_columns:
        extra_columns_str = ", ".join(extra_columns)
        raise Exception(f"Extra columns in CSV: {extra_columns_str}")

    input_df = input_df.select(
        [regexp_replace(col, '"', "").alias(col) for col in input_df.columns]
    )

    if file_name.split(".csv")[0].split("_")[0] == "auctions":
        input_df = input_df.withColumn(
            "SurfaceSqM", regexp_replace(col("SurfaceSqM"), "[^0-9.]", "")
        )

    expected_schema = create_spark_schema_from_json(
        table_schema  # type: ignore
    )
    boolean_fields = [
        f.name for f in expected_schema.fields if isinstance(f.dataType, BooleanType)
    ]
    for column_name in boolean_fields:
        input_df = input_df.withColumn(column_name, col(column_name).cast("boolean"))

    float_fields = [
        f.name for f in expected_schema.fields if isinstance(f.dataType, FloatType)
    ]

    for column_name in float_fields:
        input_df = input_df.withColumn(
            column_name, translate(col(column_name), ",", ".")
        )

    new_csv_file_path = (
        f"s3://{source_bucket}/tmp/newsphone/transformed_csv_files/{file_name}"
    )
    input_df.coalesce(1).write.format("csv").save(
        new_csv_file_path,
        header=True,
        mode="overwrite",
        sep=";",
    )

    # Read the CSV file with permissive mode
    df = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("mode", "PERMISSIVE")
        .schema(expected_schema)
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
        .csv(new_csv_file_path)
    )
    
    # If no rows survived the schema-enforced read and the file is empty, raise error
    if df.limit(1).count() == 0:
        raise Exception(
            f"File {file_name} produced 0 rows after schema parsing - failing job"
        )

    # Rows that failed schema parsing have a non-null _corrupt_record
    bad_records = df.filter(col(CORRUPT_RECORD).isNotNull())
    bad_records.cache()
    bad_count = bad_records.count()
    logging.warning(
        f"[NEWSPHONE CORRUPT RECORDS] file={file_name} bad_records={bad_count}"
    )

    if bad_count > 0:
        bad_records.coalesce(1).write.format("csv").save(
            f"s3://{source_bucket}/tmp/newsphone/rejected_files/{file_name}",
            header=True,
            mode="overwrite",
            sep=";",
        )

    # Derive the flag BEFORE dropping the corrupt column, then land all rows.
    df = df.withColumn(
        "CorruptRecordFlag",
        when(col(CORRUPT_RECORD).isNotNull(), lit(True)).otherwise(lit(False)),
    )
    df = df.drop(CORRUPT_RECORD)

    formatted_date_str = file_date[:4] + "-" + file_date[4:6] + "-" + file_date[6:]
    df = df.withColumn("file_date", lit(formatted_date_str).cast("date"))
    df = df.withColumn("file_name", lit(file_name))
    df = df.withColumn("Cluster_0", lit(file_date))

    print(cdh_location_lng)

    partition_column = "Cluster_0"
    write_to_landing(spark, df, cdh_location_lng, partition_column, formatted_date_str)

    return df
