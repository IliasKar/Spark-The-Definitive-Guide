import json

import boto3
from botocore.exceptions import ClientError
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


def extract_column_names_from_schema(schema_string: str) -> list:
    schema_data = json.loads(schema_string)
    column_names = list(schema_data.keys())
    return column_names


# def file_exists_in_s3(bucket, key):
#     s3_client = boto3.client('s3')
#     try:
#         s3_client.head_object(Bucket=bucket, Key=key)
#         return True
#     except ClientError as e:
#         # If a ClientError is thrown, check if it's because the file was not found
#         if e.response['Error']['Code'] == '404':
#             return False
#         else:
#             # If it's a different error, re-raise it
#             raise


def file_exists_in_s3(bucket: str, key: str) -> bool:
    s3_client = boto3.client("s3")
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        operation_parameters = {"Bucket": bucket, "Prefix": key}
        page_iterator = paginator.paginate(**operation_parameters)  # type: ignore

        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"] == key:
                        return True
        return False
    except ClientError:
        # Handle possible errors
        raise
