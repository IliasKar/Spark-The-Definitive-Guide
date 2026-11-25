# Databricks notebook source
# %run "../Libs/emailLib"

# COMMAND ----------

# DBTITLE 1,Mandatory Libs and global vars
from pyspark.sql.functions import collect_set, collect_list, concat_ws,concat, first, col, when, rank, datediff, months_between, lit, to_timestamp, abs as pyspark_abs, create_map, row_number
from pyspark.sql import Window, Row                                                                                                                                                                                                                                                                                                             
from pyspark.sql.types import StringType, BooleanType, NullType, IntegerType
from datetime import datetime 
import boto3
from pyspark.sql.utils import AnalysisException
import os
import botocore.session
from botocore.exceptions import NoCredentialsError
import traceback


# we have an issue with the broadcast values in cmd16
# spark.conf.set("spark.jvm.class.allowlist", "spark.databricks.pyspark.enablePy4JSecurity")

try:
    mode = dbutils.widgets.get("mode")
except Exception as e:
    mode = "shared"

# Function to get AWS credentials from Databricks secrets.
def get_aws_credentials(secrets_scope, key_name):
    try:
        aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_name)
        return aws_secret_key
    except Exception as e:
        print(f"Error retrieving AWS credentials: {str(e)}")
        return None

# Function to get AWS credentials from Databricks secrets.
def get_scope_key_names(secrets_scope):
    try:
        # Databricks secrets configuration.
        aws_keys = dbutils.secrets.list(scope=secrets_scope)
        key_names = [key.key for key in aws_keys] if aws_keys else []
        return key_names

    except Exception as e:
        print(f"Error retrieving AWS keys: {str(e)}")
        return None

secrets_scope = "aws-databricks"
aws_keys = get_scope_key_names(secrets_scope)

aws_key = aws_keys[0] if aws_keys else None

# Get AWS secret key from Databricks secrets.
aws_secret_key = get_aws_credentials(secrets_scope,aws_key)

if mode == "shared":
    # Connect to AWS using boto3.
    ssm_client = boto3.client('ssm', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)

    S3_client = boto3.client('s3', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)
else:
    # Connect to AWS using boto3.
    ssm_client = boto3.client('ssm', region_name='eu-central-1')

    S3_client = boto3.client('s3', region_name='eu-central-1')


# Function to connect to AWS and access SSM parameter.
def connect_to_aws_and_access_ssm(ssm_key):

    if aws_secret_key is None:
        print("AWS secret key not available.")
        return

    # Access SSM parameter /cfn/a360/reports_output_path.
    response = ssm_client.get_parameter(
        Name=ssm_key,
        WithDecryption=False
    )

    return response['Parameter']['Value']

# Get the Buckewt prefix from SSM to be sure.
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')
s3_output_path = connect_to_aws_and_access_ssm('/cfn/a360/reports_output_path')

# Specify the S3 bucket and folder
bucket_name = s3_output_path.split("/a360")[0].replace("s3://","")

# Get the current date to add to the csv filename
curr_date = datetime.now().strftime("%Y%m%d")

folder_name = f"{s3_output_path}10-TIA/output/{curr_date}"

debug = False
emulatefe = False
reducerecordsA360 = False
emulatebusinessdate = False


def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False


# COMMAND ----------

# DBTITLE 1,Select Max Business Date (CUP tables)
# get the max business date from CUP to use it in any CUP source system data query
maxBusinessDate_query = """
  select max(p.BusinessDate) maxdate from   `{env_prefix}-cdh`.primaryactive.party p
""".format(env_prefix=env_prefix)

maxBusinessDate_df = spark.sql(maxBusinessDate_query)

# Find the maximum business date
max_business_date = maxBusinessDate_df.selectExpr("to_date(maxdate, 'yyyy-MM-dd') as maxdate_date").collect()[0]["maxdate_date"].strftime('%Y-%m-%d')

print(max_business_date)

if(emulatebusinessdate):
    max_business_date = '2023-09-29'

# Store the maximum business date in a global variable
# spark.conf.set('spark.sql.business_date', max_business_date)

# COMMAND ----------

# DBTITLE 1,DAuction360Matching ALL records DATAFRAME, Lien DF

#A360-255
DAuction360Matching_common_fields_query = '''SELECT
    IncomingAuctionKey,
    incomingrealestatekey,
    incominghastener,
    IncomingInitiator,
    IncomingIsHastenerServicer,
    IncomingInitiatorCepalAB,
    IncomingPortfolioCategory
FROM (
    SELECT
        incomingrealestatekey,
        incominghastener,
        IncomingInitiator,
        IncomingAuctionKey,
        IncomingIsHastenerServicer,
        IncomingInitiatorCepalAB,
        IncomingPortfolioCategory,
        ROW_NUMBER() OVER (PARTITION BY incomingrealestatekey ORDER BY ingestiontimestamp DESC) AS rn
    FROM `{env_prefix}-cdh`.`derivativeactive`.`DAuction360Matching`
) t
WHERE rn = 1
'''.format(env_prefix=env_prefix)

if(reducerecordsA360):
  DAuction360Matching_common_fields_query = DAuction360Matching_common_fields_query + " and IncomingAuctionKey in ('rXjtqu23ul','02kGAfXUAk','1T4AmQKoAN','u0lNvM8a6w')"

DAuction360Matching_common_fields_df = spark.sql(DAuction360Matching_common_fields_query)

DAuction360Matching_common_fields_df = DAuction360Matching_common_fields_df.withColumnRenamed("IncomingRealEstateKey","IncomingRealEstateKey_2")

#A360-239 column renamed
DAuction360Matching_common_fields_df = DAuction360Matching_common_fields_df.withColumnRenamed("IncomingInitiatorCepalAB","Initiator_Cepal_AB")

# add he Initiator_Cepal_others
# "Οι τιμές στο πεδίο θα ειναι Yes / No ανάλογα με το αν είναι ο πλειστηριασμός επίσπευσης Cepal (if hastener == Cepal then yes else No)"

#A360-243
#DAuction360Matching_common_fields_df = DAuction360Matching_common_fields_df.withColumn("Initiator_Cepal_others",when(DAuction360Matching_common_fields_df["incominghastener"] #== 'Cepal', True).otherwise(False))

#A360-243
DAuction360Matching_common_fields_df = DAuction360Matching_common_fields_df.withColumnRenamed("incominghastener", "Initiator_Cepal_others")

DAuction360Matching_query = """
select
  a.*
from
  `{env_prefix}-cdh`.`derivativeactive`.`DAuction360Matching` a
  join (
    select
      AuctionMatching360ID,
      max(IngestionTimestamp) as IngestionTimestamp
    from
      `{env_prefix}-cdh`.`derivativeactive`.`DAuction360Matching`
    group by
      AuctionMatching360ID
  ) b on a.AuctionMatching360ID = b.AuctionMatching360ID
  and a.IngestionTimestamp = b.IngestionTimestamp
where
  a.MatchingMissingFlag = false
  and a.SourceRealEstateKey is not null
""".format(env_prefix=env_prefix)

if(reducerecordsA360):
  DAuction360Matching_query = DAuction360Matching_query + " and a.IncomingAuctionKey in ('rXjtqu23ul','02kGAfXUAk','1T4AmQKoAN','u0lNvM8a6w')"

DAuction360Matching_df = spark.sql(DAuction360Matching_query)


# for lien we need to calculate the max RELienCurrentOrder for the assets found in the system.

lien_query = f"""
  select lien, SourceSystem as `SourceSystem2`, realestatecepalkey as `realestatecepalkey2`  from (
  select max(rei.RELienCurrentOrder) lien, rei.SourceSystem, reb.realestatecepalkey from `{env_prefix}-cdh`.primaryactive.RealEstateInfo rei
  join `{env_prefix}-cdh`.primaryactive.RealEstateBuilding reb on rei.realestatekey = reb.realestatekey and rei.sourcesystem = reb.sourcesystem
  where rei.BusinessDate = '{max_business_date}' and reb.businessdate = '{max_business_date}' group by rei.SourceSystem, reb.realestatecepalkey
  ) where lien is not null
""".format(env_prefix=env_prefix)

lien_df = spark.sql(lien_query)


# COMMAND ----------

# DBTITLE 1,TEST. EMULATE UIApprovalFlag = true to a record
if(emulatefe):
    #TEST CODE. Emulate the front end and add a UIApprovalFlag true to a record
    valueWhenTrue = True
    DAuction360Matching_df = DAuction360Matching_df.withColumn("UIApprovalFlag",when(col("AuctionMatching360ID") == 'bMrDEvKDqm|02kGAfXUAk|02kGAfXUAk-001|027704746|thesis|CH33124',valueWhenTrue).otherwise(col("UIApprovalFlag")))

    DAuction360Matching_df = DAuction360Matching_df.withColumn("UIApprovalFlag",when(col("AuctionMatching360ID") == 'T8rOH5Ermy|0EVxFRjDdg|0EVxFRjDdg-004|050675423|qca|AB000000005879632',valueWhenTrue).otherwise(col("UIApprovalFlag")))

    DAuction360Matching_df = DAuction360Matching_df.withColumn("UIApprovalFlag",when(col("AuctionMatching360ID") == 'rXjtqu23ul|rXjtqu23ul|rXjtqu23ul|300906796|thesis|AB47065',valueWhenTrue).otherwise(col("UIApprovalFlag")))

    DAuction360Matching_df = DAuction360Matching_df.withColumn("UIApprovalFlag",when(col("AuctionMatching360ID") == 'O9E1AdKXXD|u0lNvM8a6w|u0lNvM8a6w-005|046311864|arotron|000000000223728',valueWhenTrue ).otherwise(col("UIApprovalFlag")))

    DAuction360Matching_df = DAuction360Matching_df.withColumn("UIApprovalFlag",when(col("AuctionMatching360ID") == 'ZrLXvn1se7|ZrLXvn1se7|ZrLXvn1se7-001|062621817|qca|AB000000006743912',valueWhenTrue ).otherwise(col("UIApprovalFlag")))


# COMMAND ----------

# DBTITLE 1,DAuction360Matching  PIVOT

DAuction360Matching_df_distinct = DAuction360Matching_df.select(
    "IncomingRealEstateKey",
    "IncomingOwnerTaxRegistrationNumber",
    "SourceRealEstateKey",
    "SourceSystem",
    "MatchingPercentage"
).distinct().orderBy("MatchingPercentage", ascending=False)

DAuction360Matching_df_distinct = DAuction360Matching_df_distinct.withColumn("MatchingPercentage", col("MatchingPercentage") * 100)

DAuction360Matching_df_distinct = DAuction360Matching_df_distinct.withColumn("SourceRealEstateKey", concat_ws('-',"SourceRealEstateKey", "MatchingPercentage"))

DAuction360Matching_df_distinct = DAuction360Matching_df_distinct.withColumn("SourceRealEstateKey",concat("SourceRealEstateKey",lit('%')))


matching_rows = (DAuction360Matching_df_distinct
                .select("IncomingRealEstateKey","IncomingOwnerTaxRegistrationNumber","SourceRealEstateKey","SourceSystem")
                .groupBy("IncomingRealEstateKey","IncomingOwnerTaxRegistrationNumber","SourceSystem")
                .agg(concat_ws("|", collect_list("SourceRealEstateKey")).alias("SourceRealEstateKeyList")))

# Concatenate IncomingOwnerTaxRegistrationNumber and SourceRealEstateKeyList columns into a single, delimited string
delimiter = ":"
matching_rows_with_delimiter = (matching_rows.select(concat_ws(delimiter, "IncomingOwnerTaxRegistrationNumber", "SourceRealEstateKeyList").alias("ConcatenatedColumn"),"IncomingRealEstateKey", "IncomingOwnerTaxRegistrationNumber", "SourceSystem"))

# The DF below contains the incomingrealestatekey, sourcesystem and allMatchedAssets columns with the VAT:<source_re_Key delimited with "|"
DAuction360Matching_lite_with_matched_results = (matching_rows_with_delimiter.select("IncomingRealEstateKey","SourceSystem","ConcatenatedColumn").groupBy("IncomingRealEstateKey","SourceSystem")
.agg(concat_ws("||", collect_list("ConcatenatedColumn")).alias("allMatchedAssets")))

# Pivot the resulting dataframe to add new columns for each sourcesystem
# Use explicit values to avoid ASSIGNMENT_ARITY_MISMATCH in Spark Connect
SOURCE_SYSTEMS = ["thesis", "qca", "arotron"]
pivot_df = (DAuction360Matching_lite_with_matched_results
            .groupBy("IncomingRealEstateKey")
            .pivot("SourceSystem", SOURCE_SYSTEMS)
            .agg(first("allMatchedAssets"))
           )

# Rename the columns to match the required format
renamed_cols = ["IncomingRealEstateKey"] + [f"Cepal_Key_Auto_Suggestions_{col}" for col in SOURCE_SYSTEMS]
DAuction360Matching_lite_with_matched_results_pivot = pivot_df.toDF(*renamed_cols)

#rename the ambigious column
DAuction360Matching_lite_with_matched_results_pivot = DAuction360Matching_lite_with_matched_results_pivot.withColumnRenamed("IncomingRealEstateKey","IncomingRealEstateKey_pivot")

#add the pivot columns on the original DF
DAuction360Matching_df = DAuction360Matching_df.join(DAuction360Matching_lite_with_matched_results_pivot, DAuction360Matching_df.IncomingRealEstateKey == DAuction360Matching_lite_with_matched_results_pivot.IncomingRealEstateKey_pivot, "left")

#drop the column IncomingRealEstateKey_pivot as it is duplicate of IncomingRealEstateKey
DAuction360Matching_df = DAuction360Matching_df.drop("IncomingRealEstateKey_pivot")


# now find if any of the above records has been validated



# COMMAND ----------

# DBTITLE 1,ADD THE UI MATCHED RECORD TO THE PIVOT
#this is the pivot for the final matched record per system and per VAT
cepal_Key_Final_rows_pivot = (DAuction360Matching_df
                                 .filter((col("UIApprovalFlag") == True) & (col("UIMatchingAuctionStatus") == "validated-matched"))
                                 .groupBy("IncomingRealEstateKey","SourceRealEstateKey", "SourceSystem")
                                 .pivot("SourceSystem", SOURCE_SYSTEMS)
                                 .agg(first("SourceRealEstateKey")))

# COMMAND ----------


renamed_cols = ["IncomingRealEstateKey"] + ["SourceRealEstateKey"] +  ["SourceSystem"] +  [f"Cepal_Key_Final_{col}" for col in SOURCE_SYSTEMS]


# COMMAND ----------

cepal_Key_Final_rows_pivot_df = cepal_Key_Final_rows_pivot.toDF(*renamed_cols)


# COMMAND ----------

cepal_Key_Final_rows_pivot_df = cepal_Key_Final_rows_pivot_df.drop("SourceSystem")

if not has_column(cepal_Key_Final_rows_pivot_df, "Cepal_Key_Final_thesis"):
    # Add the column with null values
    cepal_Key_Final_rows_pivot_df = cepal_Key_Final_rows_pivot_df.withColumn('Cepal_Key_Final_thesis', lit(None).cast(StringType()))

if not has_column(cepal_Key_Final_rows_pivot_df, "Cepal_Key_Final_qca"):
    # Add the column with null values
    cepal_Key_Final_rows_pivot_df  = cepal_Key_Final_rows_pivot_df.withColumn('Cepal_Key_Final_qca', lit(None).cast(StringType()))

if not has_column(cepal_Key_Final_rows_pivot_df, "Cepal_Key_Final_arotron"):
    # Add the column with null values
    cepal_Key_Final_rows_pivot_df = cepal_Key_Final_rows_pivot_df.withColumn('Cepal_Key_Final_arotron', lit(None).cast(StringType()))

# COMMAND ----------



# groupby column IncomingRealEstateKey and pick first non-null value
cepal_Key_Final_rows_pivot_df = cepal_Key_Final_rows_pivot_df.groupBy("IncomingRealEstateKey").agg(
    first(col("Cepal_Key_Final_thesis"), ignorenulls=True).alias("Cepal_Key_Final_thesis"),
    first(col("Cepal_Key_Final_qca"), ignorenulls=True).alias("Cepal_Key_Final_qca"),
    first(col("Cepal_Key_Final_arotron"), ignorenulls=True).alias("Cepal_Key_Final_arotron"),
)


# COMMAND ----------


#rename the ambigious column
cepal_Key_Final_rows_pivot_df = cepal_Key_Final_rows_pivot_df.withColumnRenamed("IncomingRealEstateKey","IncomingRealEstateKey_pivot")

#add the pivot columns on the original DF
DAuction360Matching_df = DAuction360Matching_df.join(cepal_Key_Final_rows_pivot_df, DAuction360Matching_df.IncomingRealEstateKey == cepal_Key_Final_rows_pivot_df.IncomingRealEstateKey_pivot, "left")

#drop the IncomingRealEstateKey_pivot
DAuction360Matching_df = DAuction360Matching_df.drop("IncomingRealEstateKey_pivot")

# COMMAND ----------

# DBTITLE 1,LIEN - ADD THE UI MATCHED RECORD LIEN TO A PIVOT
#LIEN


#left join with lien dataframe lien_df

#left join lien_df with DAuction360Matching_df
DAuction360Matching_df = DAuction360Matching_df.join(lien_df,  (DAuction360Matching_df.SourceSystem == lien_df.SourceSystem2) & (lien_df.realestatecepalkey2 == DAuction360Matching_df.SourceRealEstateKey), "left")

DAuction360Matching_df = DAuction360Matching_df.drop("SourceSystem2").drop("realestatecepalkey2")


#this is the pivot for the final matched record per system and per VAT
cepal_lien_Key_Final_rows_pivot = (DAuction360Matching_df
                                 .filter(col("UIApprovalFlag")==True)
                                 .groupBy("IncomingRealEstateKey","SourceRealEstateKey", "SourceSystem", "Lien")
                                 .pivot("SourceSystem", SOURCE_SYSTEMS)
                                 .agg(first("Lien")))

renamed_cols = ["IncomingRealEstateKey"] + ["SourceRealEstateKey"] +  ["SourceSystem"] + ["Lien"] + [f"Cepal_Key_lien_{col}" for col in SOURCE_SYSTEMS]

cepal_lien_Key_Final_rows_pivot = cepal_lien_Key_Final_rows_pivot.toDF(*renamed_cols)

cepal_lien_Key_Final_rows_pivot = cepal_lien_Key_Final_rows_pivot.drop("SourceSystem")

if not has_column(cepal_lien_Key_Final_rows_pivot, "Cepal_Key_lien_thesis"):
    # Add the column with null values
    cepal_lien_Key_Final_rows_pivot = cepal_lien_Key_Final_rows_pivot.withColumn('Cepal_Key_lien_thesis', lit(None).cast(IntegerType()))

if not has_column(cepal_lien_Key_Final_rows_pivot, "Cepal_Key_lien_qca"):
    # Add the column with null values
    cepal_lien_Key_Final_rows_pivot  = cepal_lien_Key_Final_rows_pivot.withColumn('Cepal_Key_lien_qca', lit(None).cast(IntegerType()))

if not has_column(cepal_lien_Key_Final_rows_pivot, "Cepal_Key_lien_arotron"):
    # Add the column with null values
    cepal_lien_Key_Final_rows_pivot = cepal_lien_Key_Final_rows_pivot.withColumn('Cepal_Key_lien_arotron', lit(None).cast(IntegerType()))


# groupby column IncomingRealEstateKey and pick first non-null value
cepal_lien_Key_Final_rows_pivot = cepal_lien_Key_Final_rows_pivot.groupBy("IncomingRealEstateKey").agg(
    first(col("Cepal_Key_lien_thesis"), ignorenulls=True).alias("Cepal_Key_lien_thesis"),
    first(col("Cepal_Key_lien_qca"), ignorenulls=True).alias("Cepal_Key_lien_qca"),
    first(col("Cepal_Key_lien_arotron"), ignorenulls=True).alias("Cepal_Key_lien_arotron"),
)

#rename the ambigious column
cepal_lien_Key_Final_rows_pivot = cepal_lien_Key_Final_rows_pivot.withColumnRenamed("IncomingRealEstateKey","IncomingRealEstateKey_lien_pivot")

#add the pivot columns on the original DF
DAuction360Matching_df = DAuction360Matching_df.join(cepal_lien_Key_Final_rows_pivot, DAuction360Matching_df.IncomingRealEstateKey == cepal_lien_Key_Final_rows_pivot.IncomingRealEstateKey_lien_pivot, "left")

#drop the IncomingRealEstateKey_pivot
DAuction360Matching_df = DAuction360Matching_df.drop("IncomingRealEstateKey_lien_pivot")


# COMMAND ----------

# DBTITLE 1,LOOKUP DATAFRAMES
# Read CSV files from DBFS

TIA_auction_result_lookup_df = spark.read.format("csv").option("delimiter", ";").option("header", True).load(f"dbfs:/Volumes/{env_prefix}-cdh/reference/TIA/TIA_auction_result_lookup.csv")
TIA_initiator_lookup_df = spark.read.format("csv").option("delimiter", ";").option("header", True).load(f"dbfs:/Volumes/{env_prefix}-cdh/reference/TIA/TIA_initiator_lookup.csv")
TIA_categories_lookup_df = spark.read.format("csv").option("delimiter", ";").option("header", True).load(f"dbfs:/Volumes/{env_prefix}-cdh/reference/TIA/TIA_categories_lookup.csv")


# COMMAND ----------

# DBTITLE 1,LOOKUP DATAFRAMES (Deprecated)
'''
# took them from help sheet in TIA.xlsx
import os
TIA_auction_result_lookup_df = spark.read.format("csv").option("delimiter", ";").option("header", True).load(f"file:{os.getcwd()}/TIA_auction_result_lookup.csv")
TIA_initiator_lookup_df = spark.read.format("csv").option("delimiter", ";").option("header", True).load(f"file:{os.getcwd()}/TIA_initiator_lookup.csv")
TIA_auction_result_lookup_df = spark.read.format("csv").option("delimiter", ";").option("header", True).load(f"file:{os.getcwd()}/TIA_auction_result_lookup.csv")
TIA_categories_lookup_df = spark.read.format("csv").option("delimiter", ";").option("header", True).load(f"file:{os.getcwd()}/TIA_categories_lookup.csv")

if(debug):
    display(TIA_auction_result_lookup_df)
    display(TIA_initiator_lookup_df)
    display(TIA_auction_result_lookup_df)
'''

# COMMAND ----------

# DBTITLE 1,CALC FIELDS FUNCTIONS
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Create DataFrames from lookup tables
broadcasted_TIA_initiator_lookup_df = TIA_initiator_lookup_df.select("Initiator", "Retained").collect()
broadcasted_TIA_categories_lookup_df = TIA_categories_lookup_df.select("Category", "CategoryEN").collect()
broadcasted_TIA_auction_result_lookup_df = TIA_auction_result_lookup_df.select(
    "Auction_Result", "Auction_Result1_active", "Auction_Result2_active", "Auction_Result3_active"
).collect()

# Convert DataFrames to dictionaries
broadcasted_TIA_initiator_lookup_dict = {row["Initiator"]: row["Retained"] for row in broadcasted_TIA_initiator_lookup_df}
broadcasted_TIA_categories_lookup_dict = {row["Category"]: row["CategoryEN"] for row in broadcasted_TIA_categories_lookup_df}
broadcasted_TIA_auction_result_lookup_dict = {row["Auction_Result"]: row for row in broadcasted_TIA_auction_result_lookup_df}

# Define UDFs for calculations
@udf(StringType())
def Analysis_Core_NPL_calc(input):
    return broadcasted_TIA_initiator_lookup_dict.get(input, None)

@udf(StringType())
def CategoryEN_calc(input):
    return broadcasted_TIA_categories_lookup_dict.get(input, None)

@udf(StringType())
def Auction_Status_X_calc(category, auction_status):
    column_name = f"Auction_Result{category}_active"
    matching_row = broadcasted_TIA_auction_result_lookup_dict.get(auction_status)

    if matching_row is None or matching_row[column_name] is None:
        return "Undefined"

    return matching_row[column_name]

# Register UDFs
spark.udf.register("AnalysisCoreNPLcalc", Analysis_Core_NPL_calc)
Analysis_Core_NPL_calc_udf = udf(Analysis_Core_NPL_calc, StringType()) # to use it in python cells
spark.udf.register("CategoryENCalc", CategoryEN_calc)
spark.udf.register("AuctionStatusXcalc", Auction_Status_X_calc)

# Additional UDF for year_month_published
@udf(StringType())
def year_month_published(PublishedDate):
    d = str(PublishedDate).split("-")
    return d[0] + "-" + d[1]

# Register the year_month_published UDF
spark.udf.register("year_month_published_func", year_month_published)


# COMMAND ----------

# DBTITLE 1,RUN THE BASIC DATAFRAME REPORT QUERY (A360-170)
#For the puroposes of A360-254
new_basic_report_query = f"""
    select distinct
    CASE WHEN are.AuctionStatusCode == 'eAuction - Ολοκληρωμένος - Κατακύρωση' THEN are.AuctionFinalPrice ELSE TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) END as `Absorption_new`,
    --CASE WHEN au_t.OriginAuctionKey == are.AuctionKey THEN 'yes' ELSE 'no' END as `NewEntry`,
    NewEntry_subquery.NewEntry,  --A360-266
    CASE
        WHEN TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) < 50000 THEN '0-50k'
        WHEN TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) >= 50000 AND TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) < 75000 THEN '50-75k'
        WHEN TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) >= 75000 AND TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) < 100000 THEN '75-100k'
        WHEN TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) >= 100000 AND TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) < 200000 THEN '100-200k'
        WHEN TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) >= 200000 AND TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) <= 1000000 THEN '200k-1mln'
        WHEN TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) > 1000000 THEN '>1mln'
    END as `Price_band`,
    DATE_FORMAT(are.AuctionDate, 'yyyyQQQ') AS `Quarter_Auction`,
    current_date() as `Today`,
    year(are.AuctionDate) as `year_Auction`,
    CONCAT(YEAR(are.AuctionPublishedDate),'-w',LPAD(EXTRACT(WEEK FROM are.AuctionPublishedDate), 2, '0')) AS `Week_published`,  --A360-240
    month(are.AuctionDate) as `month_auction`,
    DATE_FORMAT(are.AuctionDate, 'yyyy-MM') AS `year_month_auction`, --A360-240
    are.Address as `Address`,
    CASE WHEN are.auctiondate > to_date('2021-05-31 23:59:59') THEN 'Yes' ELSE 'No' END as `After_May_2021`,
    CASE WHEN are.AuctionPublishedDate > to_date('2021-01-01 00:00:00') THEN 'Yes' ELSE 'No' END as `Published_after_2021`,
    are.Area as `Area`,
    are.AuctionKey as `auction_id`,
    AuctionStatusXcalc(1,are.AuctionStatusCode) as `Κατηγορία1_active`,
    AuctionStatusXcalc(2,are.AuctionStatusCode) as `Κατηγορία2_active`,
    AuctionStatusXcalc(3,are.AuctionStatusCode) as `Κατηγορία3_active`,
    CASE WHEN are.AuctionDate > current_date() THEN 'Future' ELSE 'Past' END as `Auction_time`,
    au_t.AuctionType as `Auction_Type`,
    au_t.OriginAuctionKey  as `OriginAuctionKey`,
    au_t.Servicer as `BasicDataServicer`, --A360-256
    are.AuctionPostedDate as `eAuctionPostedDate`,
    to_date(are.AuctionDate) as `AuctionDate`,
    CONCAT(YEAR(are.AuctionDate),'-w',LPAD(EXTRACT(WEEK FROM are.AuctionDate), 2, '0')) AS `Week_Auction`, --A360-240
    are.AuctioneerEmail as `AuctioneerEmail`,
    are.AuctioneerName as `AuctioneerName`,
    are.AuctioneerPhone as `AuctioneerPhone`,
    are.AuctionObjKey as `AuctionObJID`,
    reb_avg.average as `Average_Marketability`,
    CategoryENCalc(are.Category) as `CategoryEN`,
    are.Category as `CategoryGR`,
    are.ConstructionYear as `ConstructionYear`,
    DateReceivedSubquery.Date_Received as `Date_Received`,
    are.OriginDebtorDescription as `DebtorDescr`,
    are.OriginDebtorTaxRegistrationNumber as `DebtorVatNumber`,
    TRY_CAST(are.AuctionFinalPrice AS DOUBLE) as `FinalPrice`,
    TRY_CAST(are.AuctionFirstOfferPrice AS DECIMAL(18, 2)) as `FirstOfferPrice`,
    are.FloorRawValue as `FloorRawValue`,
    are.GeoCoordinatesX as `GeoCoordinatesX`,
    are.GeoCoordinatesY as `GeoCoordinatesY`,
    are.GeoLevel as `GeoLevel`,
    are.HastenerTaxRegistrationNumber as `HastenerVat`,
    COALESCE(au_t.OriginHastener, are.Hastener) as `HastenerDescr`,
    are.LandRegistration as `KAEK`,
    are.LandeaUrl as `LandeaUrl`,
    are.Municipality as `Municipality`,
      --A360-240
      CASE are.District
              WHEN 'Αττικής' THEN 'Attica'
              WHEN 'Στερεάς Ελλάδας' THEN 'Central Greece'
              WHEN 'Κεντρικής Μακεδονίας' THEN 'Central Macedonia'
              WHEN 'Κρήτης' THEN 'Crete'
              WHEN 'Ανατολικής Μακεδονίας & Θράκης' THEN 'Eastern Macedonia & Thrace'
              WHEN 'Ηπείρου' THEN 'Epirus'
              WHEN 'Ιονίων Νήσων' THEN 'Ionian Islands'
              WHEN 'Βορείου Αιγαίου' THEN 'North Aegean Sea'
              WHEN 'Πελοποννήσου' THEN 'Peloponnese'
              WHEN 'Νοτίου Αιγαίου' THEN 'South Aegean Sea'
              WHEN 'Θεσσαλίας' THEN 'Thessaly'
              WHEN 'Δυτικής Ελλάδας' THEN 'Western Greece'
              WHEN 'Δυτικής Μακεδονίας' THEN 'Western Macedonia'
              ELSE 'Unknown'
              END AS `Periphery`,  --A360-240
    are.PostalCode as `PostCode`,
    are.Prefecture as `Prefecture`,
    DATE_FORMAT(are.AuctionPublishedDate, 'dd/MM/yyyy') as `c_Published_date`, --A360-240
    are.AuctionPublishedDate as `PublishedDate`,
    year_month_published_func(are.AuctionPublishedDate) as `year_month_published`,
    are.SeizureReportNumber as `Κατασχετήρια_Ημερομηνία`,
    are.SeizureReportYear as `SeizureReportYear`,
    are.OriginSourceSystem as `Source`,
    are.SubCategory as `SubCategoryGR`,
    are.SurfaceSqm as `SurfaceSqM`,
    are.Tags as `Tags`,
    are.TagValues as `full_ownership`,
    are.TitleGR as `TitleGR`,
    are.AuctionStatusCode as `Result_blended`,
    are.AuctionRealEstateCode as `UniqueCode`,
    are.AuctionCode as `AuctionID`,
    b.filedate as `Date_last_received`,
    are.RealEstateGroupCode,

    --rcg fields
    
    are.rcg_provider_mbp_price ,
    are.rcg_provider_uw_price ,
    are.rcg_provider_acquired_by ,
    are.rcg_provider_file_date ,
    are.rcg_provider_eligibility ,
    are.rcg_provider_initiator ,
    are.rcg_provider_Servicer ,
    are.rcg_provider_Auction_result_updated ,
    are.rcg_provider_suspension_reason_grouped ,
    are.rcg_provider_suspension_reason_full ,
    are.rcg_provider_eligibility_flag_property ,
    are.rcg_provider_eligible_for_reschedule ,
    are.rcg_provider_marketability_score ,
    are.rcg_provider_auction_round ,

    --are.BusinessDate,
    PriceChange.min_Reserve_price, --A360-254
    PriceChange.max_Reserve_price,  --A360-254
    PriceChange.Change_price_reasoning,  --A360-254
    PriceChange.Percentage_Increased,  --A360-254
    PriceChange.Increase_bucket,  --A360-254
    Portfolios.List_Portfolios,    --A360-246
    1 as Active_inactive,
    are.TNMergeCode as `TNMergeID`,
    CASE WHEN core.TaxRegistrationNumber IS NOT NULL THEN 'yes' ELSE 'no' END as `AB_Core`
    from `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate are
    join (
        SELECT distinct AuctionRealEstateId as areid, MAX(filedate) AS max_filedate, max(businessdate) businessdate
        FROM `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate
        GROUP BY AuctionRealEstateId ) are2 on are.AuctionRealEstateId = are2.areid and are.filedate = are2.max_filedate and are.businessdate = are2.businessdate
    join (
    select AuctionRealEstateId, max(filedate) filedate, max(businessdate) businessdate from `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate where activeFlag = True
    group by AuctionRealEstateId) b on are.AuctionRealEstateId = b.AuctionRealEstateId and are.filedate = b.filedate and are.businessdate = b.businessdate
    left join (select distinct p.TaxRegistrationNumber from `{env_prefix}-cdh`.primaryactive.party p where p.PortfolioName like '%Core%' and p.BusinessDate = '{max_business_date}') core ON are.DebtorTaxRegistrationNumber = core.TaxRegistrationNumber
    left join
        (
            select distinct au.auctionkey,au.OriginAuctionKey,au.AuctionType,au.SourceSystem, au.Servicer, au.OriginHastener from `{env_prefix}-cdh`.`derivativeactive`.`dauction` AS au join (select AuctionID, max(ReferenceDate) as ReferenceDate from `{env_prefix}-cdh`.`derivativeactive`.`dauction` group by AuctionID) as au2 on au.AuctionID = au2.AuctionID and au.ReferenceDate = au2.ReferenceDate
        ) AS au_t on are.AuctionKey = au_t.auctionkey and are.SourceSystem = au_t.SourceSystem
    left join (select realestatekey, avg(RealEstateMarketValue) as `average` from `{env_prefix}-cdh`.primaryactive.RealEstateBuilding group by realestatekey) reb_avg on reb_avg.realestatekey = are.AuctionRealEstateCode
  LEFT JOIN ( --A360-266
    SELECT  AuctionObjKey, AuctionRealEstateCode,
        CASE
            WHEN COUNT(DISTINCT FileDate) = 1 THEN 'yes'
            WHEN COUNT(DISTINCT FileDate) > 1 THEN 'Update'
        END AS NewEntry
    FROM `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate
    WHERE SourceSystem = 'extfile'
    GROUP BY AuctionObjKey, AuctionRealEstateCode
  ) AS NewEntry_subquery ON are.AuctionObjKey = NewEntry_subquery.AuctionObjKey AND are.AuctionRealEstateCode = NewEntry_subquery.AuctionRealEstateCode
  LEFT JOIN (  --A360-254
        SELECT
            AuctionRealEstateCode,
            MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) as min_Reserve_price,
            MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) as max_Reserve_price,
            CASE
                WHEN MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) = MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) THEN 'no value change'
                ELSE 'value increased'
            END as Change_price_reasoning,
            CASE
                WHEN MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) = MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) THEN '0%'
                ELSE CONCAT(
                    ROUND(
                        try_divide(
                            (MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) - MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))),
                            MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))
                        ) * 100
                    ), '%'
                )
            END as Percentage_Increased,
            CASE
                WHEN MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) = MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) THEN 'no change'
                WHEN try_divide(
                        (MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) - MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))),
                        MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))
                    ) * 100 <= 20 THEN '0-20%'
                WHEN try_divide(
                        (MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) - MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))),
                        MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))
                    ) * 100 > 20 AND try_divide(
                        (MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) - MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))),
                        MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))
                    ) * 100 <= 50 THEN '20-50%'
                WHEN try_divide(
                        (MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) - MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))),
                        MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))
                    ) * 100 > 50 AND try_divide(
                        (MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) - MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))),
                        MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))
                    ) * 100 <= 100 THEN '50-100%'
                WHEN try_divide(
                        (MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) - MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))),
                        MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))
                    ) * 100 > 100 AND try_divide(
                        (MAX(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2))) - MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))),
                        MIN(TRY_CAST(AuctionFirstOfferPrice AS DECIMAL(18, 2)))
                    ) * 100 <= 200 THEN '100-200%'
                ELSE 'greater than 200%'
            END as Increase_bucket

        FROM
            `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate
        WHERE
            OriginSourceSystem = 'eAuction'
        GROUP BY
            AuctionRealEstateCode
    ) AS PriceChange ON are.AuctionRealEstateCode = PriceChange.AuctionRealEstateCode  --A360-254
    LEFT JOIN (    --A360-246
        SELECT
            A.AuctionRealEstateCode,
            A.OriginDebtorTaxRegistrationNumber,
            CONCAT_WS(' || ', ARRAY_JOIN(COLLECT_SET(B.PortfolioName), ' || ')) AS List_Portfolios
        FROM
            (SELECT DISTINCT AuctionRealEstateCode, DebtorTaxRegistrationNumber, OriginDebtorTaxRegistrationNumber
            FROM `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate
            WHERE SourceSystem = 'extfile') A
        LEFT JOIN
            (SELECT DISTINCT TaxRegistrationNumber, PortfolioName
            FROM `{env_prefix}-cdh`.primaryactive.party
            WHERE NOT ISNULL(PortfolioName) AND NOT ISNULL(TaxRegistrationNumber)) B
        ON A.DebtorTaxRegistrationNumber = B.TaxRegistrationNumber
        GROUP BY A.AuctionRealEstateCode, A.OriginDebtorTaxRegistrationNumber
    ) Portfolios ON are.AuctionRealEstateCode = Portfolios.AuctionRealEstateCode AND are.OriginDebtorTaxRegistrationNumber = Portfolios.OriginDebtorTaxRegistrationNumber
    --A360-246
    LEFT JOIN ( --A360-329
        SELECT
            AuctionObjKey,
            ActiveFlag,
            CONCAT_WS(' || ', ARRAY_JOIN(COLLECT_SET(FileDate), ' || ')) AS Date_Received
        FROM
            `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate
        WHERE
            SourceSystem = 'extfile' AND ActiveFlag = true
        GROUP BY
            AuctionObjKey, ActiveFlag
    ) AS DateReceivedSubquery ON are.AuctionObjKey = DateReceivedSubquery.AuctionObjKey
    --A360-329
    where are.SourceSystem = 'extfile'
    and are.BusinessDate = (
        select max(BusinessDate) from  `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate
    )
""".format(env_prefix=env_prefix)

#TODO discuss if in the above query we nmust enter the "and are.BusinessDate = '${spark.sql.business_date}'" condition... for table `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate
#TODO the reb_avg columns above are wrong. They concern the a360 matched asset and not the AuctionRealEstateCode

if(reducerecordsA360):
  basic_report_query = new_basic_report_query + " and are.AuctionKey in ('rXjtqu23ul','02kGAfXUAk','1T4AmQKoAN','u0lNvM8a6w')"

report_df = spark.sql(new_basic_report_query)

report_df = report_df.distinct()

# AB CORE fix
result_df2 = report_df.groupBy("UniqueCode").agg(
    concat_ws("||", collect_list("AB_Core")).alias("AB_Core2")
).withColumnRenamed("UniqueCode","UniqueCode2")


result_df2 = result_df2.withColumn("AB_Core",when(result_df2["AB_Core2"].contains('yes'),'yes').otherwise('no')).drop("AB_Core2")

result_df2 = result_df2.distinct()

report_df = report_df.drop("AB_Core").join(result_df2, report_df.UniqueCode == result_df2.UniqueCode2, "left").drop("UniqueCode2").distinct()


# COMMAND ----------

# DBTITLE 1,Get KAEK based on the RealEstateGroupCode and inherit the first match
'''
A360-245

Description of feature: Σκοπός ειναι να καλυψουμε και τα κενα (30%)
Calculation : Aπό τον πίνακα `{env}-cdh`.primaryactive.AuctionRealEstate, όπου έχουμε το ίδιο RealEstateGroupCode across AuctionObjKeys, μπορούμε να γεμίζουμε τις κενές τιμές του ΚΑΕΚ, από τις υπόλοιπες τιμές για το ίδιο RealEstateGroupCode, όπου γεμίζει. Όταν υπάρχουν >1 ΚΑΕΚ στο ίδιο RealEstateGroupCode να παίρνεις το πρώτο που βρίσκεις
'''

c_KAEK_report_query = f"""
WITH RankedAuctionRealEstate AS (
    SELECT
        are.RealEstateGroupCode,
        are.LandRegistration AS `grKAEK`,
        are.AuctionRealEstateKey,
        are.AuctionDate,
        ROW_NUMBER() OVER (PARTITION BY are.RealEstateGroupCode ORDER BY are.AuctionDate) AS RowNum
    FROM
        `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate are
    JOIN (
        SELECT
            AuctionRealEstateId,
            MAX(filedate) AS filedate
        FROM
            `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate
        WHERE
            activeFlag = TRUE
        GROUP BY
            AuctionRealEstateId
    ) b ON are.AuctionRealEstateId = b.AuctionRealEstateId AND are.filedate = b.filedate
    WHERE
        are.activeFlag = TRUE
        AND are.LandRegistration IS NOT NULL
        AND are.RealEstateGroupCode IS NOT NULL
)
SELECT
    RealEstateGroupCode AS `grRealEstateGroupCode`,
    `grKAEK`,
    AuctionRealEstateKey AS `grAuctionRealEstateKey`,
    auctiondate AS `grAuctiondate`
FROM
    RankedAuctionRealEstate
WHERE
    RowNum = 1;
""".format(env_prefix=env_prefix)

c_KAEK_df = spark.sql(c_KAEK_report_query)

# left join the table
# Perform left join and conditional update
report_df = report_df.join(
    c_KAEK_df,
    report_df["RealEstateGroupCode"] == c_KAEK_df["grRealEstateGroupCode"],
    "left_outer"
).select(
    report_df["*"],
    when(col("KAEK").isNull(), col("grKAEK")).otherwise(col("KAEK")).alias("c_KAEK")
)

#drop new columns
report_df.drop("grKAEK", "KAEK", "grRealEstateGroupCode", "grAuctionRealEstateKey", "grAuctiondate")

# COMMAND ----------


report_df = report_df.join(DAuction360Matching_lite_with_matched_results_pivot, report_df.UniqueCode == DAuction360Matching_lite_with_matched_results_pivot.IncomingRealEstateKey_pivot, "left")

report_df = report_df.drop("IncomingRealEstateKey_pivot")

report_df = report_df.join(cepal_Key_Final_rows_pivot_df, report_df.UniqueCode == cepal_Key_Final_rows_pivot_df.IncomingRealEstateKey_pivot, "left")
report_df = report_df.drop("IncomingRealEstateKey_pivot")

#lien record for each sourcesystem
report_df = report_df.join(cepal_lien_Key_Final_rows_pivot, report_df.UniqueCode == cepal_lien_Key_Final_rows_pivot.IncomingRealEstateKey_lien_pivot, "left")
report_df = report_df.drop("IncomingRealEstateKey_lien_pivot")

report_df = report_df.join(DAuction360Matching_common_fields_df, report_df.UniqueCode == DAuction360Matching_common_fields_df.IncomingRealEstateKey_2, "left")

#drop unecessary columns
report_df = report_df.drop("IncomingRealEstateKey_2")

#A360-239 rename incomingHastener and incomingInititator to hastener and Initiator_final, Initiator_TIA = Initiator_final
report_df = report_df.withColumnRenamed("IncomingHastener","Hastener")
report_df = report_df.withColumnRenamed("IncomingInitiator","Initiator_final")
report_df = report_df.withColumn("Initiator_TIA", report_df["Initiator_final"])

# report_df = report_df.withColumn("Analysis_Core_NPL", Analysis_Core_NPL_calc_udf(report_df["Initiator_final"]))

TIA_initiator_lookup_df2 = TIA_initiator_lookup_df.withColumnRenamed("Initiator","InitiatorLKP").withColumnRenamed("Is_Hastener_Servicer","Is_Hastener_ServicerLKP").withColumnRenamed("Hastener","HastenerLKP").withColumnRenamed("Initiator_Cepal_AB","Initiator_Cepal_ABLKP").withColumnRenamed("Retained","Analysis_Core_NPL")

report_df = report_df.join(TIA_initiator_lookup_df2, report_df.Initiator_final == TIA_initiator_lookup_df2.InitiatorLKP, "left")
report_df = report_df.drop("InitiatorLKP","Is_Hastener_ServicerLKP","HastenerLKP","Initiator_Cepal_ABLKP") #A360-345


#add the _3_months_period
report_df = report_df.withColumn("_3_months_period",when(datediff(to_timestamp(report_df["AuctionDate"]), lit(datetime.now().strftime('%Y-%m-%d'))) < 0.0 , None).when(pyspark_abs(months_between(to_timestamp(report_df["AuctionDate"]), lit(datetime.now().strftime('%Y-%m-%d')))) <= 3.0,True).otherwise(False))



# COMMAND ----------

#count_auctions ==>

#find the auction ranking based on its originAuctionKey
# Define the window specification
window_spec = Window.partitionBy("OriginAuctionkey").orderBy("AuctionDate")
not_ranked_df = report_df.select("OriginAuctionKey","auction_id","AuctionDate").filter(col("OriginAuctionKey").isNotNull()).distinct()
ranked_df = not_ranked_df.withColumn("count_auctions", rank().over(window_spec))


# COMMAND ----------


#count_auctions ==> (old code)

from itertools import chain

# Collect the data from two columns into a list of tuples
from pyspark.sql.functions import collect_list, struct

# Collect the data from two columns into a list of tuples
# collected_data = ranked_df.select("auction_id", "count_auctions").rdd.map(lambda row: (row[0], row[1])).collect()  #OLD code


# Collect the data into a list of structs
collected_data = ranked_df.select(collect_list(struct("auction_id", "count_auctions"))).first()[0]

# Convert the list of structs into a list of tuples
collected_data = [(row.auction_id, row.count_auctions) for row in collected_data]


# Convert the list of tuples into a dictionary
mapping_dict = dict(collected_data)

# Convert the dictionary into a PySpark map type
mapping_expr = create_map([lit(x) for x in chain(*mapping_dict.items())])

# Use the map to create a new column by mapping the 'key' column
report_df = report_df.withColumn("count_auctions", mapping_expr[col("auction_id")])

# COMMAND ----------

# DBTITLE 1,Portfolios_of_CEPAL_Keys_Contracts_based
# Portfolios_of_CEPAL_Keys_Contracts_based

portfolios_query = f"""
    select b.RealEstateCepalKey as `RealEstateCepalKey2`, a.Portfolios as `Portfolios_of_CEPAL_Keys_Contracts_based`  from (
    SELECT RealEstateKey, SourceSystem, CONCAT_WS('||', COLLECT_LIST(CONCAT_WS(':', PortfolioName, AgreementKey))) AS Portfolios
    from (
    SELECT distinct reb.RealEstateKey, reb.SourceSystem, coll.PortfolioName, coll.AgreementKey
    FROM `{env_prefix}-cdh`.primaryactive.collateral AS coll
    JOIN `{env_prefix}-cdh`.primaryactive.RealEstateBuilding AS reb
    ON coll.SourceSystem = reb.SourceSystem AND CAST(coll.RealEstateKey AS STRING) = reb.RealEstateKey
    WHERE COALESCE(reb.RealEstateBuildingActive, 'Active') = 'Active'
    ) GROUP BY RealEstateKey, SourceSystem
    ) a join
    (
        Select distinct C.BusinessDate, C.realestatekey, C.RealEstateCepalKey, C.RealEstateStatus, C.RealEstateStatusCode, C.RealEstateAddress1, C.RealEstateAddress2, C.RealEstateArea,
        C.RealEstateBuildingNumber, C.RealEstateSubCategoryCode, C.RealEstateCategoryCode,C.RealEstateBuildingNetArea, C.RealEstateBuildingStorageArea, C.RealEstateBuildingParkingArea,
        C.RealEstateCity, C.RealEstateDescription, C.RealEstateLandRegistryCode, C.RealEstateLocationLambda,
        C.RealEstateLocationPhi, C.RealEstateNetArea, C.RealEstateCounty, C.RealEstateMunicipality,
        C.RealEstateDistrict, C.RealEstateDivision, C.RealEstateBuildingActive, C.RealEstateUsageCodeType, C.RealEstateBuildingFloorRawValue, C.RealEstateBuildingNumber,c.SourceSystem,
        C.RealEstateConstructionYear, C.RealEstatePostalCode
        from `{env_prefix}-cdh`.primaryactive.RealEstateBuilding C
        where coalesce(RealEstateBuildingActive,'Active') = "Active" and BusinessDate =  '{max_business_date}' and SourceSystem = 'thesis'
    ) b on a.RealEstateKey == b.realestatekey
""".format(env_prefix=env_prefix)

portfolios_df = spark.sql(portfolios_query).distinct()

portfolios_df = portfolios_df.dropDuplicates(['RealEstateCepalKey2'])

#left join report_df and portfolios_df
report_df = report_df.join(portfolios_df,
                           report_df.Cepal_Key_Final_thesis == portfolios_df.RealEstateCepalKey2, "left")
report_df = report_df.drop("RealEstateCepalKey2")


# COMMAND ----------

# DBTITLE 1,c_Related_Auction
c_Related_Auction_query = """
    SELECT IncomingOriginAuctionKey as `IncomingOriginAuctionKey2`,
    CONCAT_WS('||', COLLECT_LIST(CONCAT_WS(':', IncomingAuctionKey, IncomingAuctionDate))) AS AuctionKeyList
    FROM (
    SELECT distinct a.IncomingOriginAuctionKey, a.IncomingAuctionKey, a.IncomingAuctionDate
    FROM `{env_prefix}-cdh`.`derivativeactive`.`DAuction360Matching` AS a
    JOIN (
        SELECT AuctionMatching360ID, MAX(IngestionTimestamp) AS IngestionTimestamp
        FROM `{env_prefix}-cdh`.`derivativeactive`.`DAuction360Matching`
        GROUP BY AuctionMatching360ID
    ) AS b ON a.AuctionMatching360ID = b.AuctionMatching360ID AND a.IngestionTimestamp = b.IngestionTimestamp
    WHERE MatchingMissingFlag = FALSE AND SourceRealEstateKey IS NOT NULL
    ORDER BY IncomingAuctionDate
    ) t
    GROUP BY IncomingOriginAuctionKey
""".format(env_prefix=env_prefix)

c_Related_Auction_query_df = spark.sql(c_Related_Auction_query)

#the classic left join
report_df = report_df.join(c_Related_Auction_query_df, report_df.OriginAuctionKey == c_Related_Auction_query_df.IncomingOriginAuctionKey2, "left")
report_df = report_df.drop("IncomingOriginAuctionKey2")



# COMMAND ----------

def remove_special_chars(column_name):
    new_column_name = column_name.replace(",", "_").replace(";", "_").replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\n", "").replace("\t", "").replace(" ", "")
    return new_column_name

# Renaming the column names and removing special characters
new_column_names = [remove_special_chars(column) for column in report_df.columns]
report_df = report_df.toDF(*new_column_names)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Some more renamings
report_df = report_df.withColumnRenamed("auction_id","c_auction_id").distinct()


# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,A360-340 Cepal_Key_Final implemetation

from pyspark.sql.functions import col, when, concat_ws, lit, array, filter, size, countDistinct
# Define the new column based on the given rules
report_df = report_df.withColumn(
    "Cepal_Key_Final_single",
    when(
        (col("Cepal_Key_Final_thesis").isin(['missing', '']) | col("Cepal_Key_Final_thesis").isNull()) &
        (col("Cepal_Key_Final_qca").isin(['missing', '']) | col("Cepal_Key_Final_qca").isNull()) &
        (col("Cepal_Key_Final_arotron").isin(['missing', '']) | col("Cepal_Key_Final_arotron").isNull()),
        when(
            (col("Cepal_Key_Final_thesis") == 'missing') |
            (col("Cepal_Key_Final_qca") == 'missing') |
            (col("Cepal_Key_Final_arotron") == 'missing'),
            'missing'
        ).otherwise('')
    ).otherwise(
        # Concatenate non-missing, non-blank, non-null values with delimiter "||"
        when(
            size(
                filter(
                    array(
                        col("Cepal_Key_Final_thesis"),
                        col("Cepal_Key_Final_qca"),
                        col("Cepal_Key_Final_arotron")
                    ),
                    lambda x: (x != 'missing') & (x != '') & (x.isNotNull())
                )
            ) > 1,
            concat_ws(
                '||',
                *[
                    when(col(c).isin(['missing', '']) | col(c).isNull(), lit('')).otherwise(col(c))
                    for c in ['Cepal_Key_Final_thesis', 'Cepal_Key_Final_qca', 'Cepal_Key_Final_arotron']
                ]
            )
        ).otherwise(
            # If only one non-missing, non-blank, non-null value, select that value
            filter(
                array(
                    col("Cepal_Key_Final_thesis"),
                    col("Cepal_Key_Final_qca"),
                    col("Cepal_Key_Final_arotron")
                ),
                lambda x: (x != 'missing') & (x != '') & (x.isNotNull())
            )[0]
        )
    )
)


# COMMAND ----------

# DBTITLE 1,A360-249 LF_Confiscation
lf_df = spark.sql(
    f"""
    select IncomingAuctionKey
           ,IncomingOwnerTaxRegistrationNumber
           ,LawyerPartyName
    from (
    select Dmatching.IncomingAuctionKey
           ,Dmatching.IncomingOwnerTaxRegistrationNumber
           ,a.LegalActionDate
           ,a.LawyerPartyName
           ,row_number() over (partition by Dmatching.IncomingAuctionKey, Dmatching.IncomingOwnerTaxRegistrationNumber
                               order by a.LegalActionDate desc) rnk
    from `{env_prefix}-cdh`.`derivativeactive`.`dauction360matching` Dmatching
    inner join `{env_prefix}-cdh`.primaryactive.exposure b
    on b.PartyTaxRegistrationNumber=Dmatching.IncomingOwnerTaxRegistrationNumber
    and contains(b.PortfolioName,Dmatching.IncomingInitiator)='true'
    inner join (
                select a.*
                from `{env_prefix}-cdh`.primaryactive.legalaction a
                where a.LegalActionTypeCode in ('SYS_BA_LEG_ACT_CPL_LG_06','5527','1754')
               ) a
    on a.CDI=b.CDI
    and a.BusinessDate=b.BusinessDate
    where Dmatching.IncomingInitiatorCepalAB='Yes'
    and b.BusinessDate =(select max(BusinessDate)
    from `{env_prefix}-cdh`.primaryactive.exposure)
    ) a
    where rnk=1
    """
)

report_df_to_join = report_df

report_df = report_df_to_join.join(lf_df,
                           (report_df_to_join.c_auction_id == lf_df.IncomingAuctionKey) &
                           (report_df_to_join.DebtorVatNumber == lf_df.IncomingOwnerTaxRegistrationNumber),
                           how='left').drop("IncomingAuctionKey", "IncomingOwnerTaxRegistrationNumber").withColumnRenamed(
                               "LawyerPartyName", "LF_Confiscation"
                           )
del report_df_to_join

# COMMAND ----------

# DBTITLE 1,A360-363 Liquidation Value
lv_df = spark.sql(
    f"""
        SELECT
            RealEstateKey,
            SUM(LVNetBase) AS TotalLVNetBase,
            MAX(BusinessDate) AS BusinessDate
        FROM
            `{env_prefix}-cdh`.`derivativeactive`.`danalyticsmetrics`
        WHERE
            BusinessDate = (SELECT MAX(BusinessDate) FROM `{env_prefix}-cdh`.`derivativeactive`.`danalyticsmetrics`)
        GROUP BY
            RealEstateKey;
    """
)

validated_matches_df = spark.sql(
    f"""
    WITH max_tmp AS (
    SELECT a.*
    FROM `{env_prefix}-cdh`.derivativeactive.dauction360matching a
    JOIN (
        SELECT AuctionMatching360ID, MAX(ingestiontimestamp) AS max_timestamp
        FROM `{env_prefix}-cdh`.derivativeactive.dauction360matching
        GROUP BY AuctionMatching360ID
    ) b ON a.AuctionMatching360ID = b.AuctionMatching360ID
        AND a.ingestiontimestamp = b.max_timestamp
        AND a.UIMatchingAuctionStatus != 'Deprecated'
    )
    select distinct * from max_tmp
    where max_tmp.UIMatchingAuctionStatus IN ('validated-matched', 'toMerge', 'toSplit')
    """
    ).select("AuctionMatching360ID", "UIMatchingAuctionStatus", "IncomingRealEstateKey", "SourceRealEstateKey")

lv_df_validated = validated_matches_df.join(
    lv_df,
    (lv_df["RealEstateKey"] == validated_matches_df["SourceRealEstateKey"]),
    "inner"
    ).select("IncomingRealEstateKey", "SourceRealEstateKey", "RealEstateKey", "TotalLVNetBase").distinct()

report_df_to_join = report_df

report_df = report_df_to_join.join(
    lv_df_validated,
    (report_df_to_join["UniqueCode"] == lv_df_validated["IncomingRealEstateKey"]) &
    (report_df_to_join["Cepal_Key_Final_single"] == lv_df_validated["SourceRealEstateKey"]),
    "left"
    ).drop("IncomingRealEstateKey", "SourceRealEstateKey", "RealEstateKey" ).withColumnRenamed(
        "TotalLVNetBase", "Liquidation_Value"
    )

del report_df_to_join

# COMMAND ----------

# DBTITLE 1,A360-462 Marketability
market_df = spark.sql(
 f""" select distinct RealEstateMarketabilityCode, RealEstateCepalKey
        from `{env_prefix}-cdh`.primaryactive.realestatebuilding WHERE
            BusinessDate = (SELECT MAX(BusinessDate) FROM `{env_prefix}-cdh`.primaryactive.realestatebuilding)
            AND RealEstateCepalKey IS NOT NULL
            AND RealEstateCepalKey <> ''
            AND RealEstateMarketabilityCode IS NOT NULL
            AND RealEstateMarketabilityCode <> ''   """
            )

market_df_validated = validated_matches_df.join(
    market_df,
    (market_df["RealEstateCepalKey"] == validated_matches_df["SourceRealEstateKey"]),
    "inner"
    ).select("SourceRealEstateKey", "RealEstateCepalKey", "RealEstateMarketabilityCode").distinct()

report_df_to_join = report_df

report_df = report_df_to_join.join(
    market_df_validated,
    (report_df_to_join["Cepal_Key_Final_single"] == market_df_validated["RealEstateCepalKey"]),
    "left"
    ).drop("SourceRealEstateKey", "RealEstateCepalKey" ).withColumnRenamed(
        "RealEstateMarketabilityCode", "Marketability"
    )

del report_df_to_join

# COMMAND ----------

# DBTITLE 1,A360-405 AcquisitiondBy Implementation
from pyspark.sql import functions as F

# Step 1: Build acquiredby_df_rcg
acquiredby_df_rcg = spark.sql(f"""
    SELECT DISTINCT b.LastNameGR AS AcquisitionBy,
                    a.AuctionRealEstateCode
    FROM `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate a
    INNER JOIN `{env_prefix}-cdh`.primaryactive.party b
        ON a.BidderCode = b.PartyKey AND a.BusinessDate = b.BusinessDate
    WHERE b.SourceSystem = 'thesis'
      AND a.BidderCode IS NOT NULL
      AND a.SourceSystem = 'thesis'
      AND a.BusinessDate = (
            SELECT MAX(BusinessDate)
            FROM `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate
            WHERE SourceSystem = 'thesis'
      )
      AND a.AuctionRealEstateCode IS NOT NULL
""")

# Step 2: Build acquiredby_df_core_gemini
acquiredby_df_core_gemini = spark.sql(f"""
    SELECT DISTINCT acq.AcquisitionBy,
                    are.AuctionRealEstateCode
    FROM `{env_prefix}-cdh`.derivativeactive.DAuctionRealEstate are
    INNER JOIN `{env_prefix}-cdh`.reference.auction_acquisition_by acq
        ON are.AuctionRealEstateCode = acq.AuctionRealEstateCode
        AND are.BusinessDate = acq.ReferenceDate
    WHERE are.SourceSystem <> 'thesis'
""")

# Step 3: Union both sources
acquiredby_df = acquiredby_df_rcg.unionByName(acquiredby_df_core_gemini).distinct()

acquiredby_df = acquiredby_df.dropDuplicates(['AuctionRealEstateCode'])


# COMMAND ----------

# DBTITLE 1,A360-657
ext_ab = spark.sql(
f"""
    SELECT ab.UniqueCode, ab.AcquisitionBy as AcquisitionBy
    FROM `{env_prefix}-cdh`.`externalfiles`.`extsuccessful_ab` ab
    JOIN (
        SELECT uniquecode, MAX(businessdate) as max_businessdate
        FROM `{env_prefix}-cdh`.`externalfiles`.`extsuccessful_ab`
        GROUP BY uniquecode
    ) latest
    ON ab.uniquecode = latest.uniquecode AND ab.businessdate = latest.max_businessdate
""")
ext_ab.createOrReplaceTempView("ext_ab")


# STEP 5: Rename for clarity
acquiredby_df = acquiredby_df.withColumnRenamed("AcquisitionBy", "AcquisitionBy_internal")
ext_ab_df = ext_ab.withColumnRenamed("AcquisitionBy", "AcquisitionBy_external")

# STEP 6: Join the two sources
acquiredby_combined = acquiredby_df.join(
    ext_ab_df,
    acquiredby_df["AuctionRealEstateCode"] == ext_ab_df["UniqueCode"],
    how="outer"
)

# STEP 7: Use external value if exists, otherwise internal
acquiredby_final = acquiredby_combined.withColumn(
    "AcquisitionBy",
    F.coalesce("AcquisitionBy_external", "AcquisitionBy_internal")
).select(
    F.coalesce("AuctionRealEstateCode", "UniqueCode").alias("AuctionRealEstateCode"),
    "AcquisitionBy"
).dropDuplicates(["AuctionRealEstateCode"])

acquiredby_final.createOrReplaceTempView("acquiredby_final")


# COMMAND ----------

# DBTITLE 1,Join AcquisitionBy
report_df = (
    report_df.alias("r")
    .join(
        acquiredby_final.alias("abf"),
        F.col("r.UniqueCode") == F.col("abf.AuctionRealEstateCode"),
        "left"
    )
    .drop("AuctionRealEstateCode")
)

# COMMAND ----------

# DBTITLE 1,A360-641
from pyspark.sql.functions import collect_set, concat_ws

thesis_matches_df = spark.sql(
    f"""
    WITH max_tmp AS (
    SELECT a.*
    FROM `{env_prefix}-cdh`.derivativeactive.dauction360matching a
    JOIN (
        SELECT AuctionMatching360ID, MAX(ingestiontimestamp) AS max_timestamp
        FROM `{env_prefix}-cdh`.derivativeactive.dauction360matching
        GROUP BY AuctionMatching360ID
    ) b ON a.AuctionMatching360ID = b.AuctionMatching360ID
        AND a.ingestiontimestamp = b.max_timestamp
        AND a.UIMatchingAuctionStatus != 'Deprecated'
    )
    select distinct * from max_tmp
    where max_tmp.UIMatchingAuctionStatus IN ('validated-matched', 'toMerge', 'toSplit')
    and max_tmp.SourceSystem = 'thesis'
    """
    ).select("AuctionMatching360ID", "UIMatchingAuctionStatus", "IncomingRealEstateKey", "SourceRealEstateKey")

report_df_to_join = report_df

agg_matches = thesis_matches_df.select("IncomingRealEstateKey", "SourceRealEstateKey", "UIMatchingAuctionStatus") \
    .groupBy("IncomingRealEstateKey", "UIMatchingAuctionStatus") \
    .agg(concat_ws("||", collect_set("SourceRealEstateKey")).alias("Cepal_Key_Final"))

df_merged = report_df_to_join.join(
    agg_matches,
    report_df_to_join["UniqueCode"] == agg_matches["IncomingRealEstateKey"],
    how="left"
)


# Step 3: Drop the redundant join column
report_df = df_merged.drop("IncomingRealEstateKey")


del report_df_to_join

# COMMAND ----------

# DBTITLE 1,A360-478 DDD/eAuction dual state fix
auction_counts = report_df.groupBy("c_auction_id").agg(countDistinct("Source").alias("source_count"))

auctions_with_both_sources = auction_counts.filter(col("source_count") == 2).select("c_auction_id")

report_df_to_filter = report_df
ddd_to_exclude = report_df_to_filter.join(auctions_with_both_sources, "c_auction_id").filter(col("Source") == 'Δελτίο Δικαστικών Δημοσιεύσεων')

report_df = report_df_to_filter.join(ddd_to_exclude, ["c_auction_id", "Source"], "leftanti").distinct()

# COMMAND ----------

# DBTITLE 1,Datatype fix
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

report_df = report_df.withColumn("Absorption_new", col("Absorption_new").cast("string"))

#if "Cepal_Key_Auto_Suggestions_arotron" not in report_df.columns:
report_df = report_df.withColumn(
    "Cepal_Key_Auto_Suggestions_arotron",
    lit(None).cast(StringType())
    )


# COMMAND ----------

# DBTITLE 1,Deduplicate records based on the following requirements
# - The STATUS for properties that are matched and originate from the UI must be displayed with priority.
# - If a property coming from the UI has more than one different STATUS,
#   and one of them is “matched”, then TIA should keep only the matched STATUS(es) and ignore all others.
# - If a property coming from the UI has more than one different STATUS,
#   and none of them is related to “matched” (e.g. only Split & Merge exist),
#   then TIA should randomly select one STATUS and ignore the rest.

# Creates a df with two columns:
# - AuctionObjID
# - UIMatchingAuctionStatus (the one that will be kept based on the requirements above)
auction_obj_id_status_mapping_df = (
    report_df.select(
        col("AuctionObjID").alias("AuctionObjID_x"),
        first("UIMatchingAuctionStatus").over(
            # Rank each status according to precendence
            Window.partitionBy("AuctionObjID").orderBy(
                # validated-matched is prioritised
                when(col("UIMatchingAuctionStatus") == "validated-matched", 0)
                # toMerge and toSplit are selected at random if no validated-matched exists
                .when(col("UIMatchingAuctionStatus") == "toMerge", 1)
                .when(col("UIMatchingAuctionStatus") == "toSplit", 1)
                # nulls or other
                .otherwise(10000)
            )
        ).alias("UIMatchingAuctionStatus_x")
    )
).alias("status_mapping")

auction_obj_ids_with_single_status_df = report_df.join(
    auction_obj_id_status_mapping_df,
    on=(
        (report_df.AuctionObJID == auction_obj_id_status_mapping_df.AuctionObjID_x) &
        report_df.UIMatchingAuctionStatus.eqNullSafe(auction_obj_id_status_mapping_df.UIMatchingAuctionStatus_x)
    ),
    how="inner"
).withColumn(
    "_row_number",
    # Add a row number in case multiple rows exist for the same AuctionObjID and UIMatchingAuctionStatus
    row_number().over(Window.partitionBy("AuctionObjID").orderBy("AuctionObjID")),
).drop("AuctionObjID_x", "UIMatchingAuctionStatus_x")

report_df = auction_obj_ids_with_single_status_df.where(col("_row_number") == 1).drop("_row_number")


# COMMAND ----------

# DBTITLE 1,Datatype Check
table_df = spark.table(f"`{env_prefix}-cdh-delivery`.tia.tia")

# Get the schema from the Databricks table
table_schema = {field.name: field.dataType for field in table_df.schema}

# Get the schema from your existing DataFrame
df_schema = {field.name: field.dataType for field in report_df.schema}


# Columns with different types
mismatched_columns = {
    col: (df_schema[col], table_schema[col])
    for col in df_schema
    if col in table_schema and df_schema[col] != table_schema[col]
}

# Columns missing in either
df_only = set(df_schema.keys()) - set(table_schema.keys())
table_only = set(table_schema.keys()) - set(df_schema.keys())

print("Mismatched columns and their types:")
for col, types in mismatched_columns.items():
    print(f"{col}: DataFrame type = {types[0]}, Table type = {types[1]}")

print("\nColumns only in DataFrame:", df_only)
print("Columns only in Table:", table_only)


# COMMAND ----------

# DBTITLE 1,ASK3273 - temp fix
from pyspark.sql.functions import when, col

#report_df = report_df.withColumn(
#    "Initiator_final",
#    when(col("HastenerDescr").contains("Reditable Opportunities Fund II SCS RAIF"), "CETUS")
#    .when(col("HastenerDescr").contains("Dysart Finance II Designated Activity Company"), "Euclid")
#    .otherwise(col("Initiator_final"))
#)

# Display the result
#display(report_df.select("Initiator_final", "HastenerDescr", "*"))

# Register the DataFrame as a temporary view
report_df.createOrReplaceTempView("report_df")

# Run SQL to overwrite the 'Initiator_final' column based on conditions
report_df = spark.sql("""
    SELECT
        -- all columns except 'Initiator_final'
        c_auction_id,
        Source,
        Absorption_new,
        NewEntry,
        Price_band,
        Quarter_Auction,
        Today,
        year_Auction,
        Week_published,
        month_auction,
        year_month_auction,
        Address,
        After_May_2021,
        Published_after_2021,
        Area,
        `Κατηγορία1_active`,
        `Κατηγορία2_active`,
        `Κατηγορία3_active`,
        Auction_time,
        Auction_Type,
        OriginAuctionKey,
        BasicDataServicer,
        eAuctionPostedDate,
        AuctionDate,
        Week_Auction,
        AuctioneerEmail,
        AuctioneerName,
        AuctioneerPhone,
        AuctionObJID,
        Average_Marketability,
        CategoryEN,
        CategoryGR,
        ConstructionYear,
        Date_Received,
        DebtorDescr,
        DebtorVatNumber,
        FinalPrice,
        FirstOfferPrice,
        FloorRawValue,
        GeoCoordinatesX,
        GeoCoordinatesY,
        GeoLevel,
        HastenerVat,
        HastenerDescr,
        KAEK,
        LandeaUrl,
        Municipality,
        Periphery,
        PostCode,
        Prefecture,
        c_Published_date,
        PublishedDate,
        year_month_published,
        `Κατασχετήρια_Ημερομηνία`,
        SeizureReportYear,
        SubCategoryGR,
        SurfaceSqM,
        Tags,
        full_ownership,
        TitleGR,
        Result_blended,
        UniqueCode,
        AuctionID,
        Date_last_received,
        RealEstateGroupCode,
        min_Reserve_price,
        max_Reserve_price,
        Change_price_reasoning,
        Percentage_Increased,
        Increase_bucket,
        List_Portfolios,
        Active_inactive,
        TNMergeID,
        AB_Core,
        c_KAEK,
        Cepal_Key_Auto_Suggestions_qca,
        Cepal_Key_Auto_Suggestions_thesis,
        Cepal_Key_Final_thesis,
        Cepal_Key_Final_qca,
        Cepal_Key_Final_arotron,
        Cepal_Key_lien_thesis,
        Cepal_Key_lien_qca,
        Cepal_Key_lien_arotron,
        Initiator_Cepal_others,
        -- REPLACE this column with updated values:
        CASE
            WHEN HastenerDescr LIKE '%Creditable Opportunities Fund II SCS RAIF%' THEN 'CETUS'
            WHEN HastenerDescr LIKE '%Dysart Finance II Designated Activity Company%' THEN 'Euclid'
            ELSE Initiator_final
        END AS Initiator_final,
        IncomingIsHastenerServicer,
        Initiator_Cepal_AB,
        IncomingPortfolioCategory,
        Initiator_TIA,
        Analysis_Core_NPL,
        _3_months_period,
        count_auctions,
        Portfolios_of_CEPAL_Keys_Contracts_based,
        AuctionKeyList,
        Cepal_Key_Final_single,
        LF_Confiscation,
        Liquidation_Value,
        Marketability,
        AcquisitionBy,
        UIMatchingAuctionStatus,
        Cepal_Key_Final,
        Cepal_Key_Auto_Suggestions_arotron,

        --RCG FIELDS

        rcg_provider_mbp_price,
        rcg_provider_uw_price,
        rcg_provider_acquired_by,
        rcg_provider_file_date,
        rcg_provider_eligibility,
        rcg_provider_initiator,
        rcg_provider_Servicer,
        rcg_provider_Auction_result_updated,
        rcg_provider_suspension_reason_grouped,
        rcg_provider_suspension_reason_full,
        rcg_provider_eligibility_flag_property,
        rcg_provider_eligible_for_reschedule,
        rcg_provider_marketability_score,
        rcg_provider_auction_round

    FROM report_df
""")

# COMMAND ----------


rcg_cols = [

"rcg_provider_mbp_price",
"rcg_provider_uw_price",
"rcg_provider_marketability_score",
"rcg_provider_auction_round",
"rcg_provider_acquired_by",
"rcg_provider_file_date",
"rcg_provider_eligibility",
"rcg_provider_eligibility_flag_property",
"rcg_provider_initiator",
"rcg_provider_Servicer",
"rcg_provider_Auction_result_updated",
"rcg_provider_suspension_reason_grouped",
"rcg_provider_suspension_reason_full",
"rcg_provider_eligible_for_reschedule"

]

other_cols = [c for c in report_df.columns if c not in rcg_cols]

report_df = report_df.select(*other_cols, *rcg_cols)

# COMMAND ----------

# DBTITLE 1,EXPORT REPORT (curated)

export_type = dbutils.widgets.get("export_type")

#spark.sql(f"""DROP TABLE IF EXISTS `{env_prefix}-de-sandbox`.a360.tia""")

if export_type == 'db':
    # Write data to Unity Catalog table as a Delta table
    report_df.distinct().write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
         f"`{env_prefix}-cdh-delivery`.tia.tia")


else:
    # export the TIA report to a file
    pathtoexport = s3_output_path
    report_df.coalesce(1).write.option("header", True).csv(f"{folder_name}")

    # RENAME THE PYSPARK CSV TO FINAL FILE

    # List all part-* files in the specified folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    for file in response["Contents"]:
        if file["Key"].startswith(folder_name) and "part-00000" in file["Key"]:
            # Create the desired new file name
            new_file_name = f"{folder_name}TIA_{curr_date}.csv"
            old_file_name = file["Key"]
            s3_client.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": old_file_name}, Key=new_file_name)
            s3_client.delete_object(Bucket=bucket_name, Key=old_file_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct AcquisitionBy from report_df

# COMMAND ----------

#display(report_df.filter(col("Initiator_final") == 'Euclid'))

# COMMAND ----------

# DBTITLE 1,tia distinct count
df = spark.sql(
    f"""
select distinct AuctionObJID from `{env_prefix}-cdh-delivery`.tia.tia
""" )

display(df.count())
#display(df)

# COMMAND ----------

# DBTITLE 1,tia count
df = spark.sql(
    f"""
select * from `{env_prefix}-cdh-delivery`.tia.tia
""" )

display(df.count())
display(df)
