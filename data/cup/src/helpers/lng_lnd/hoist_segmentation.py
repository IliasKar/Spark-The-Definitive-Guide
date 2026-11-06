from typing import Any

import logging

from py4j.java_gateway import JavaObject  # type: ignore
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, translate, when
from pyspark.sql.types import BooleanType, FloatType, DecimalType

import sys, os
from datetime import datetime
import re

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
# from newsphone import *
from external_file_helpers import *


# take date part from file name
def extract_file_date(file_name: str) -> str:
    match = re.search(r"\d{8}", file_name)

    if not match:
        return "00000000"

    date_str = match.group()

    try:
        datetime.strptime(date_str, "%Y%m%d")
        return date_str
    except ValueError:
        return "00000000"


def load_hoist_segmentation_external_file(
        spark: SparkSession, args: dict[str, Any]
) -> None:
    CORRUPT_RECORD = "_corrupt_record"

    table_schema = args.get("table_schema")
    file_name = args.get("file_name")
    schema_catalog = args.get("schema_catalog")
    source_bucket = args.get("source_bucket")
    folder_name = args.get("folder_name")
    object_key = f"incoming/hoist/{file_name}"
    cdh_location_lng = args.get("cdh_location_lng")

    #  Check if the file exists
    if not file_exists_in_s3(
            source_bucket,  # type: ignore
            object_key,  # type: ignore
    ):
        raise FileNotFoundError(
            f"The file {object_key} does not exist in bucket {source_bucket}"
        )

    file_date = extract_file_date(file_name) if file_name else "00000000"

    csv_file = f"s3://{source_bucket}/incoming/hoist/{file_name}"

    input_df = (
        spark.read.option("header", "true")
        .option("sep", ";")
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
            column_name, translate(col(column_name), ",", ".").cast("float")
        )

    decimal_fields = [
        f.name for f in expected_schema.fields if isinstance(f.dataType, DecimalType)
    ]

    for column_name in decimal_fields:
        input_df = input_df.withColumn(
            column_name, translate(col(column_name), ",", ".")
        )   

    new_csv_file_path = (
        f"s3://{source_bucket}/tmp/hoist/transformed_csv_files/{file_name}"
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
        f"[HOIST CORRUPT RECORDS] file={file_name} bad_records={bad_count}"
    )

    if bad_count > 0:
        bad_records.coalesce(1).write.format("csv").save(
            f"s3://{source_bucket}/tmp/hoist/rejected_files/{file_name}",
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
