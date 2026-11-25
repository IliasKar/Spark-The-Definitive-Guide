# Databricks notebook source
# DBTITLE 1,Env Variables
from pyspark.sql.functions import collect_set, collect_list, concat_ws,concat, first, col, when, rank, datediff, months_between, lit, to_timestamp, abs as pyspark_abs
from pyspark.sql import Window, Row                                                                                                                                                                                                                                                                                                             
from pyspark.sql.types import StringType, BooleanType, NullType, IntegerType, DoubleType, FloatType
from datetime import datetime 
import boto3
from pyspark.sql.utils import AnalysisException
import os
import botocore.session
from botocore.exceptions import NoCredentialsError
import traceback

# we have an issue with the broadcast values in cmd16
spark.conf.set("spark.jvm.class.allowlist", "spark.databricks.pyspark.enablePy4JSecurity")

# Connect to AWS using boto3
ssm_client = boto3.client('ssm', region_name='eu-central-1')

s3_client = boto3.client('s3', region_name='eu-central-1')                           

# Function to connect to AWS and access SSM parameter
def connect_to_aws_and_access_ssm(ssm_key):

    # Access SSM parameter /cfn/a360/reports_output_path
    response = ssm_client.get_parameter(
        Name=ssm_key,
        WithDecryption=False
    )

    return response['Parameter']['Value']

# Get the Buckewt prefix from SSM to be sure
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')


# COMMAND ----------

# DBTITLE 1,Thesis DF
source_query = """
  select distinct A.sourcesystem as SourceSystem,  A.BusinessDate, A.TaxRegistrationNumber as SourceOwnerTaxRegistrationNumber, A.FirstNameGR as SourceOwnerFirstName, A.LastNameGR as SourceOwnerLastName,
          CASE WHEN A.sourcesystem ="arotron" THEN C.realestatekey ELSE C.RealEstateCepalKey END as SourceRealEstateKey, 
          C.RealEstateStatusCode, C.RealEstateAddress1 as SourceRealEstateAddress1,
          C.RealEstateAddress2 as SourceRealEstateAddress2, C.RealEstateArea,
          C.RealEstateBuildingNumber, C.RealEstateSubCategoryCode as SourceRealEstateSubCategoryCode, C.RealEstateCategoryCode as SourceRealEstateCategoryCode,
          C.RealEstateBuildingNetArea as SourceRealEstateBuildingNetArea,
          C.RealEstateBuildingStorageArea as SourceRealEstateBuildingStorageArea,
          C.RealEstateBuildingParkingArea as SourceRealEstateBuildingParkingArea,
          C.RealEstateCity as SourceRealEstateCity, C.RealEstateDescription as SourceRealEstateDescription, LTRIM(RTRIM(C.RealEstateLandRegistryCode)) as SourceKAEK, C.RealEstateLocationLambda,
          C.RealEstateLocationPhi, C.RealEstateNetArea as SourceRealEstateNetArea, C.RealEstateCounty, C.RealEstateMunicipality as SourceRealEstateMunicipality,
          C.RealEstateDistrict, C.RealEstateDivision, C.RealEstateUsageCodeType as SourceRealEstateUsageCodeType,
          C.RealEstateConstructionYear, C.RealEstatePostalCode as SourceRealEstatePostalCode, C.RealEstateBuildingFloorRawValue as SourceRealEstateBuildingFloorRawValue, 
          C.RealEstateBuildingNumber as SourceRealEstateBuildingNumber, B.OwnershipStatusCode, B.OwnershipPercentage as SourceOwnershipPercentage, B.OwnershipTypeCode as SourceOwnershipTypeCode
          from `{env_prefix}-cdh`.primary.party as A
          inner join (  select distinct B.realestatekey,B.OwnershipStatusCode, B.OwnershipPercentage, B.OwnershipTypeCode, B.BusinessDate, B.OwnerPartyKey,b.SourceSystem 
                        from `{env_prefix}-cdh`.primary.RealEstateInfo B) B
          on A.PartyKey =  B.OwnerPartyKey  
          and a.BusinessDate=b.BusinessDate
          and a.SourceSystem=b.SourceSystem
          inner join  (                    
              Select distinct C.BusinessDate, C.realestatekey, C.RealEstateCepalKey, C.RealEstateStatus, C.RealEstateStatusCode, C.RealEstateAddress1, C.RealEstateAddress2, C.RealEstateArea,
              C.RealEstateBuildingNumber, C.RealEstateSubCategoryCode, C.RealEstateCategoryCode,C.RealEstateBuildingNetArea, C.RealEstateBuildingStorageArea, C.RealEstateBuildingParkingArea,
              C.RealEstateCity, C.RealEstateDescription, C.RealEstateLandRegistryCode, C.RealEstateLocationLambda,
              C.RealEstateLocationPhi, C.RealEstateNetArea, C.RealEstateCounty, C.RealEstateMunicipality,
              C.RealEstateDistrict, C.RealEstateDivision, C.RealEstateActive, C.RealEstateUsageCodeType, C.RealEstateBuildingFloorRawValue, C.RealEstateBuildingNumber,c.SourceSystem,
              C.RealEstateConstructionYear, C.RealEstatePostalCode 
              from `{env_prefix}-cdh`.primary.RealEstateBuilding C 
              where coalesce(RealEstateActive,'Active') = "Active"
          ) C
          on C.realestatekey =  B.realestatekey  
          and c.BusinessDate = b.BusinessDate
          and c.SourceSystem = b.SourceSystem
          where a.BusinessDate = (  select max(BusinessDate) BusinessDate
                                    from `{env_prefix}-cdh`.primary.RealEstateInfo) 
                """.format(env_prefix=env_prefix)
                
source_df = spark.sql(source_query)

#consolidate values to SourceRealEstateBuildingNetArea
def sum_net_area(SourceRealEstateBuildingNetArea_col, 
    SourceRealEstateBuildingStorageArea_col, 
    SourceRealEstateBuildingParkingArea_col):

    if not SourceRealEstateBuildingNetArea_col:
        SourceRealEstateBuildingNetArea_col = 0.0
    
    if not SourceRealEstateBuildingStorageArea_col:
        SourceRealEstateBuildingStorageArea_col = 0.0
    
    if not SourceRealEstateBuildingParkingArea_col:
        SourceRealEstateBuildingParkingArea_col = 0.0
    
    total = SourceRealEstateBuildingNetArea_col +\
            SourceRealEstateBuildingStorageArea_col +\
            SourceRealEstateBuildingParkingArea_col
    
    return total


def unifying_address(address_1, address_2):
    final_address = ""
    
    if address_1 and len(address_1) > 0:
        final_address = address_1

    if address_2 and len(address_2) > 0:
        if address_2 not in final_address:
            final_address = final_address + " " + address_2
    
    return final_address

unifying_address_udf = udf(unifying_address, StringType())
sum_net_area_udf = udf(sum_net_area, DoubleType())

source_df = source_df.withColumn(
    "SourceRealEstateBuildingNetArea",
    sum_net_area_udf(
        source_df.SourceRealEstateBuildingNetArea, 
        source_df.SourceRealEstateBuildingStorageArea, 
        source_df.SourceRealEstateBuildingParkingArea)
)

source_df = source_df.withColumn(
    "SourceRealEstateAddress1",
    unifying_address_udf(
        source_df.SourceRealEstateAddress1, 
        source_df.SourceRealEstateAddress2)
)

source_df = source_df.drop("SourceRealEstateBuildingStorageArea")
source_df = source_df.drop("SourceRealEstateBuildingParkingArea")

source_df = source_df.filter(
    col("SourceSystem") == "thesis"
)

source_df = source_df.drop("RealEstateStatusCode")
source_df.cache()

# COMMAND ----------

# DBTITLE 1,GrantThornton DF
from pyspark.sql.functions import col, regexp_replace
gt_df = spark.sql(f"""select * from `{env_prefix}-cdh`.`reference`.`hoistrealestateresearch`""")

#Cast thsese cols to Float
for c in [c for c in gt_df.columns if "Square" in c]:
    gt_df = gt_df.withColumn(c, regexp_replace(col(c), ",", ".").cast(FloatType()))

gt_df = gt_df.withColumn("Floor", col("Floor").cast(FloatType()))

# COMMAND ----------

# DBTITLE 1,Join tables on AFM
intermediate_result_df = source_df.join(gt_df, 
                                        source_df.SourceOwnerTaxRegistrationNumber  == \
                                        gt_df.Input_VAT)

# COMMAND ----------

# DBTITLE 1,Preprocessing Pipelines
#PREPROCESSING PIPELINES
import unicodedata
import re 

# decimals pre-processing
def round_and_convert_to_string(decimal_number):

    if not decimal_number:
        return ""

    rounded_number = round(decimal_number)
    string_number = str(rounded_number)

    return string_number


def strip_greek_accents(input_string):
    # Normalize the string to decompose composite characters
    normalized = unicodedata.normalize("NFD", input_string)

    # Use a filter to remove diacritic marks and join characters back together
    stripped_string = "".join(
        [char for char in normalized if unicodedata.category(char) != "Mn"]
    )

    return stripped_string


def remove_non_letters(input_string):
    return "".join([char for char in input_string if char.isalpha()])


def remove_substrings(s: str, 
                      to_remove: list = ["nan", "none", "null", "\n"]) -> str:

    for substring in to_remove:
        s = s.replace(substring, "")

    return s


def remove_spaces(s: str) -> str:
    return re.sub(r"\s+", "", s)


def string_cast_normalize(s: int | float | str) -> str:

    s = str(s)
    s = s.lower()
    s = strip_greek_accents(s)
    s = remove_spaces(s)
    s = remove_substrings(s)

    return s

def is_empty_or_none(s):
    if not s:
        return True
    else:
        return False

def jaccard_similarity(x: str, y: str) -> float:

    intersection_cardinality: int = 0
    union_cardinality: int = 0
    similarity: float = 0.0

    intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
    union_cardinality = len(set.union(*[set(x), set(y)]))
    similarity = intersection_cardinality / float(union_cardinality)

    return similarity


def jaro_winkler_similarity(str1: str, str2: str, 
                            p: float = 0.1, max_prefix: int = 0) -> float:
    
    def get_matched_characters(_str1: str, _str2: str) -> str:
        matched = []
        limit = min(len(_str1), len(_str2)) // 2
        for i, l in enumerate(_str1):
            left = int(max(0, i - limit))
            right = int(min(i + limit + 1, len(_str2)))
            if l in _str2[left:right]:
                matched.append(l)
                _str2 = f"{_str2[0:_str2.index(l)]} {_str2[_str2.index(l) + 1:]}"
 
        return "".join(matched)
 
    # matching characters
    matching_1 = get_matched_characters(str1, str2)
    matching_2 = get_matched_characters(str2, str1)
    match_count = len(matching_1)
 
    # transposition
    transpositions = (
        len([(c1, c2) for c1, c2 in zip(matching_1, matching_2) if c1 != c2]) // 2
    )
 
    if not match_count:
        jaro = 0.0
    else:
        jaro = (
            1
            / 3
            * (
                match_count / len(str1)
                + match_count / len(str2)
                + (match_count - transpositions) / match_count
            )
        )
 
    # common prefix up to max_prefix characters
    prefix_len = 0
    for c1, c2 in zip(str1[:max_prefix], str2[:max_prefix]):
        if c1 == c2:
            prefix_len += 1
        else:
            break

    return jaro + p * prefix_len * (1 - jaro)


def string_comparison(x, y):

    conditions = [
        not is_empty_or_none(x), 
        not is_empty_or_none(y)
        ]

    if all(conditions):
        return jaccard_similarity(x, y)
    else:
        return 0.0

def compare_positions(a_string, b_string):

    found = 0

    for a, b in zip(a_string, b_string):
        if a == b:
            found +=1

    score = found / len(a_string)

    return score

def string_comparison_ordered(x, y):

    conditions = [
        not is_empty_or_none(x), 
        not is_empty_or_none(y)
        ]

    if all(conditions):
        if len(x) == len(y):
            return compare_positions(x, y)
        return jaro_winkler_similarity(x, y)
    else:
        return 0.0

def change(x, y):
    # Handle the edge case where both x and y are 0 to avoid division by zero
    if x == 0 and y == 0:
        return 0  # No change if both are zero
    
    C = (abs(max(x,y) - min(x,y)) / ((abs(x) + abs(y)) / 2))

    return C

def numeric_comparison(x, y):

    if not x:
        x = 0.0
    if not y:
        y = 0.0
    
    c = change(x, y)

    if c > 1: # y > 3x
        return 0
    else:
        return 1- c

# COMMAND ----------

# DBTITLE 1,Matching Algorithm
def matching(thesis_zip_col,
    thesis_address_1_col,
    thesis_netarea_col, 
    thesis_floor_value_col,
    newsphone_zip_col,
    newsphone_address_1_col, 
    newsphone_netarea_col,
    newsphone_floor_value_col):

    #zip col comparison
    thesis_zip_col = str(thesis_zip_col)
    newsphone_zip_col = str(newsphone_zip_col)
    zip_similarity = string_comparison_ordered(thesis_zip_col, newsphone_zip_col)

    #address col comparison
    thesis_adddress_col = str(thesis_address_1_col)
    newsphone_address_1_col = str(newsphone_address_1_col)
    thesis_adddress_col = string_cast_normalize(thesis_adddress_col)
    newsphone_address_1_col = string_cast_normalize(newsphone_address_1_col)
    address_similarity = string_comparison(thesis_adddress_col, newsphone_address_1_col)

    #net area col comparison
    if not thesis_netarea_col:
        thesis_netarea_col = 0.0
    if not newsphone_netarea_col:
        newsphone_netarea_col = 0.0
    net_area_similarity = numeric_comparison(thesis_netarea_col, newsphone_netarea_col)

    #floor value comparison
    if not thesis_floor_value_col:
        thesis_floor_value_col = 0.0
    if not newsphone_floor_value_col:
        newsphone_floor_value_col = 0.0
    floor_similarity = numeric_comparison(thesis_floor_value_col, newsphone_floor_value_col)

    return (zip_similarity + address_similarity + net_area_similarity + floor_similarity ) /4
     
def set_similarity100_sameKAEK(kaek1, kaek2, similarity):
 
    if not kaek1 and not kaek2:
        return similarity
    elif kaek1 == kaek2:
        return 1.0
    else:
        return similarity
 
matching_udf = udf(matching, FloatType())
set_similarity100_sameKAEK_udf= udf(set_similarity100_sameKAEK, FloatType())
 
 
result_df = intermediate_result_df.withColumn("MatchingPercentage", 
                                  matching_udf( 
                                                col("SourceRealEstatePostalCode"),
                                                col("SourceRealEstateAddress1"), 
                                                col("SourceRealEstateNetArea"), 
                                                col("SourceRealEstateBuildingFloorRawValue"), 
                                                col("Postal_Code"),
                                                col("Address"),
                                                col("Square_Metres_Of_Building"), 
                                                col("Floor")
                                                )
                                            )
                                                     
result_df = result_df.withColumn(
     "MatchingPercentage",
     set_similarity100_sameKAEK_udf(result_df.KAEK, result_df.SourceKAEK, result_df.MatchingPercentage)
)

# COMMAND ----------

banned_chars = ' ,;{}()\n\t='
for c in result_df.columns:
    for b in banned_chars:
        if b in c:
            print(c)
            result_df = result_df.withColumnRenamed(c, c.replace(b, ""))

# COMMAND ----------

display(result_df.select(
	"UNICUE_KEY_GT",
    "SourceKAEK", 
    "KAEK",
    "SourceRealEstatePostalCode", 
    "Postal_Code", 
    "SourceRealEstateAddress1", 
    "Address", 
    "SourceRealEstateBuildingFloorRawValue",
    "SourceRealEstateNetArea",
    "Floor",
    "Square_Metres_Of_Building",
    "MatchingPercentage"
 ))

# COMMAND ----------

display(result_df)

# COMMAND ----------

# Register the DataFrame as a temporary SQL table
result_df.createOrReplaceTempView("temp_table")

spark.sql(f"""
    DROP TABLE IF EXISTS `{env_prefix}-cdh-delivery`.`eap`.`eap` 
""")

spark.sql(f"""
    CREATE TABLE `{env_prefix}-cdh-delivery`.`eap`.`eap` AS 
    SELECT * FROM temp_table
""")

spark.sql(f"""
        REPAIR TABLE `{env_prefix}-cdh-delivery`.`eap`.`eap` SYNC METADATA
""")


# COMMAND ----------

export_path = f"/Volumes/{env_prefix}-cdh-delivery/eap/eap/"
temp_file = "gt_vs_thesis.xlsx"
result_df.toPandas().to_excel(temp_file)

import shutil
shutil.move(temp_file, export_path + temp_file)
