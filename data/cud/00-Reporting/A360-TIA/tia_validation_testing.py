# Databricks notebook source
# DBTITLE 1,Env Variables
import boto3
from botocore.exceptions import NoCredentialsError
import traceback

"""
Accessing the Hive metastore requires specific considerations:

1. Hive metastore access is not possible via the instance profile if the cluster is on shared access mode.
   The cluster must be in shared access mode to interact with the Unity catalog.

2. To access both Unity catalog and AWS services, an access token from Amazon is required.

3. To retrieve the token secret key, access the secrets scope 'aws-databricks,' where the access key is stored per environment.
    https://docs.databricks.com/en/security/secrets/index.html

4. Once AWS credentials are obtained, clients for services like S3 and SSM can be created.

5. After you authenticate successfully querying the Hive metastore is also working.
"""

try:
    mode = dbutils.widgets.get("mode")
except Exception as e:
    mode = "shared" 

# Function to get AWS credentials from Databricks secrets
def get_aws_credentials(secrets_scope, key_name):
    try:
        # Databricks secrets configuration
    
        aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_name)
        # if aws_secret_key is not None:
        #     print("AWS secret key retrieved successfully.")
        # else:
        #     print("AWS secret key is None. Please check Databricks secrets configuration.")

        return aws_secret_key
    except Exception as e:
        print(f"Error retrieving AWS credentials: {str(e)}")
        return None
    
# Function to get AWS credentials from Databricks secrets
def get_scope_key_names(secrets_scope):
    try:
        # Databricks secrets configuration

        aws_keys = dbutils.secrets.list(scope=secrets_scope)
        key_names = [key.key for key in aws_keys] if aws_keys else []
        return key_names
    
    except Exception as e:
        print(f"Error retrieving AWS keys: {str(e)}")
        return None    

secrets_scope = "aws-databricks"
aws_keys = get_scope_key_names(secrets_scope)

aws_key = aws_keys[0] if aws_keys else None

# Get AWS secret key from Databricks secrets
aws_secret_key = get_aws_credentials(secrets_scope,aws_key)

if mode == "shared":
    # Connect to AWS using boto3
    ssm_client = boto3.client('ssm', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)

    S3_client = boto3.client('s3', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)    
else:
    # Connect to AWS using boto3
    ssm_client = boto3.client('ssm', region_name='eu-central-1')

    S3_client = boto3.client('s3', region_name='eu-central-1')                           


# Function to connect to AWS and access SSM parameter
def connect_to_aws_and_access_ssm(ssm_key):

    if aws_secret_key is None:
        print("AWS secret key not available.")
        return

    # Access SSM parameter /cfn/a360/reports_output_path
    response = ssm_client.get_parameter(
        Name=ssm_key,
        WithDecryption=False
    )

    return response['Parameter']['Value']

# Get the Buckewt prefix from SSM to be sure.
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')

# COMMAND ----------

# DBTITLE 1,Active perimeter landing
df = spark.sql(
    f"""
    SELECT *
    FROM (
        SELECT *
        FROM `{env_prefix}-cdh`.landing.newsphone_auctions
        WHERE Active = TRUE 
        QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObJID ORDER BY file_date DESC) = 1
    ) AS latest_auctions
    """
)

display(df.count())
display(df)
#253921
#253921

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT file_date, COUNT(*) as row_count
    FROM `{env_prefix}-cdh`.landing.newsphone_auctions
    GROUP BY file_date
    ORDER BY file_date
    """
)

display(df)

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT *
    FROM (
        SELECT *
        FROM `{env_prefix}-cdh`.landed.newsphone_auctions
        WHERE Active = TRUE 
        QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObJID ORDER BY file_date DESC) = 1
    ) AS latest_auctions
    """
)

display(df.count())
display(df)
#254786

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT file_date, BusinessDate, COUNT(*) as row_count
    FROM `{env_prefix}-cdh`.landed.newsphone_auctions
    GROUP BY file_date, BusinessDate
    ORDER BY file_date, BusinessDate
    """
)

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, * from `p-cdh`.landing.newsphone_auctions
# MAGIC WHERE AuctionObJID = 562245

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, BusinessDate,  * from `p-cdh`.landed.newsphone_auctions
# MAGIC WHERE AuctionObJID = 562245

# COMMAND ----------

# MAGIC %sql
# MAGIC select  distinct FileDate from `p-cdh`.primaryactive.auctionrealestate
# MAGIC --where SourceSystem = 'extfile'
# MAGIC --where BusinessDate = '2025-06-09'
# MAGIC where AuctionObjKey = 562245

# COMMAND ----------

# MAGIC %sql
# MAGIC select  distinct FileDate from `p-cdh`.derivativeactive.dauctionrealestate
# MAGIC where SourceSystem = 'extfile'
# MAGIC --and BusinessDate = '2025-06-08'
# MAGIC and AuctionObjKey = 562245
# MAGIC --AND file_date = '2025-06-07'

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT file_date, BusinessDate, COUNT(*) as row_count
    FROM `{env_prefix}-cdh`.landed.newsphone_basic_data
    GROUP BY file_date, BusinessDate
    ORDER BY file_date, BusinessDate
    """
)

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from `p-cdh`.landed.newsphone_basic_data
# MAGIC where BusinessDate = (
# MAGIC   select(
# MAGIC     select max(BusinessDate) from `p-cdh`.landed.newsphone_basic_data)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct AuctionObJID) from `p-cdh`.landed.newsphone_auctions
# MAGIC where BusinessDate = (
# MAGIC   select(
# MAGIC     select max(BusinessDate) from `p-cdh`.landed.newsphone_auctions)
# MAGIC )

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT file_date, COUNT(*) as row_count
    FROM `{env_prefix}-cdh`.landing.newsphone_basic_data
    GROUP BY file_date
    ORDER BY file_date
    """
)

display(df)

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT file_date, COUNT(*) as row_count
    FROM `{env_prefix}-cdh`.landed.newsphone_basic_data
    GROUP BY file_date
    ORDER BY file_date
    """
)

display(df)

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT *
    FROM (
        SELECT *
        FROM `{env_prefix}-cdh`.primaryactive.auctionrealestate
        --WHERE ActiveFlag = TRUE 
        where SourceSystem = 'extfile'
        AND BusinessDate = (
            select max(BusinessDate) 
            FROM `{env_prefix}-cdh`.primaryactive.auctionrealestate
            WHERE SourceSystem = 'extfile'
        )
    ) AS latest_auctions
    """
)

display(df.count())
display(df.select("AuctionObjKey").distinct().count())
display(df)

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT SourceSystem, MAX(BusinessDate) as max_BusinessDate
    FROM `{env_prefix}-cdh`.primaryactive.auctionrealestate
    GROUP BY SourceSystem
    """
)

display(df)

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT FileDate, BusinessDate, COUNT(*) as row_count
    FROM `{env_prefix}-cdh`.primaryactive.auctionrealestate
    WHERE SourceSystem = 'extfile'
    GROUP BY FileDate, BusinessDate
    ORDER BY FileDate, BusinessDate
    """
)

display(df)

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT *
    FROM (
        SELECT *
        FROM `{env_prefix}-cdh`.derivativeactive.dauctionrealestate
        --WHERE ActiveFlag = TRUE 
        where SourceSystem = 'extfile'
        AND BusinessDate = (
            select max(BusinessDate) 
            FROM `{env_prefix}-cdh`.derivativeactive.dauctionrealestate
            WHERE SourceSystem = 'extfile'
        )
    ) AS latest_auctions
    """
)

display(df.count())
display(df.select("AuctionObjKey").distinct().count())
display(df)

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT *
    FROM (
        SELECT *
        FROM `{env_prefix}-cdh`.primaryactive.auction 
        where SourceSystem = 'extfile'
        AND BusinessDate = (
            select max(BusinessDate) 
            FROM `{env_prefix}-cdh`.primaryactive.auction 
            WHERE SourceSystem = 'extfile'
        )
    ) AS latest_auctions
    """
)

display(df.count())
display(df.select("AuctionKey").distinct().count())
display(df)

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT  BusinessDate, COUNT(*) as row_count
    FROM `{env_prefix}-cdh`.landed.newsphone_basic_data
    GROUP BY  BusinessDate
    ORDER BY  BusinessDate
    """
)

display(df)

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT *
    FROM (
        SELECT *
        FROM `{env_prefix}-cdh`.derivativeactive.dauction
        WHERE SourceSystem = 'extfile'
        AND ReferenceDate = (
            select max(ReferenceDate) 
            FROM `{env_prefix}-cdh`.derivativeactive.dauction
            WHERE SourceSystem = 'extfile'
        )
    ) AS latest_auctions
    """
)

display(df.count())
display(df.select("AuctionKey").distinct().count())
display(df)

# COMMAND ----------

# DBTITLE 1,max per SourceSystem
# MAGIC %sql
# MAGIC SELECT
# MAGIC   SourceSystem,
# MAGIC   MAX(BusinessDate) AS MaxBusinessDate, 
# MAGIC   count(*) as max_count
# MAGIC FROM
# MAGIC   `p-cdh`.primaryactive.auctionrealestate
# MAGIC GROUP BY
# MAGIC   SourceSystem;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SourceSystem,
# MAGIC   MAX(BusinessDate) AS MaxBusinessDate, 
# MAGIC     count(*) as max_count
# MAGIC FROM
# MAGIC   `p-cdh`.derivativeactive.dauctionrealestate
# MAGIC GROUP BY
# MAGIC   SourceSystem;
# MAGIC   --300955 extfile

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SourceSystem,
# MAGIC   MAX(BusinessDate) AS MaxBusinessDate, 
# MAGIC     count(distinct AuctionObjKey) as max_count
# MAGIC FROM
# MAGIC   `p-cdh`.derivativeactive.dauctionrealestate
# MAGIC GROUP BY
# MAGIC   SourceSystem;
# MAGIC   --300955 extfile

# COMMAND ----------

# MAGIC %sql
# MAGIC     SELECT  BusinessDate, COUNT(*) as row_count
# MAGIC     FROM `p-cdh`.primaryactive.auction
# MAGIC     GROUP BY  BusinessDate
# MAGIC     ORDER BY  BusinessDate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  ReferenceDate, COUNT(*) as row_count
# MAGIC     FROM `p-cdh`.derivativeactive.dauction
# MAGIC     GROUP BY  ReferenceDate
# MAGIC     ORDER BY  ReferenceDate
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC     FROM `p-cdh`.derivativeactive.dauction
# MAGIC     ORDER BY IngestionTimeStamp desc;

# COMMAND ----------

# DBTITLE 1,Landing vs Primary
df = spark.sql(f"""
        WITH newsphone_latest AS (
            SELECT *
            FROM `{env_prefix}-cdh`.landing.newsphone_auctions
            WHERE Active = TRUE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObJID ORDER BY file_date DESC) = 1
        ),
        realestate_latest AS (
            SELECT *
            FROM `{env_prefix}-cdh`.primaryactive.auctionrealestate
            WHERE ActiveFlag = TRUE
            AND SourceSystem = 'extfile'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObjKey ORDER BY FileDate DESC) = 1
        )
        SELECT 
            n.AuctionObJID,
            n.file_date AS newsphone_file_date,
            r.FileDate AS realestate_file_date,
            n.*, 
            r.*
        FROM newsphone_latest n
        FULL OUTER JOIN realestate_latest r
        ON n.AuctionObJID = r.AuctionObjKey
        WHERE n.file_date != r.FileDate
        ORDER BY COALESCE(n.AuctionObJID, r.AuctionObjKey)

""")
display(df)

# COMMAND ----------

# DBTITLE 1,Landed vs Primary
df = spark.sql(f"""
        WITH newsphone_latest AS (
            SELECT *
            FROM `{env_prefix}-cdh`.landed.newsphone_auctions
            WHERE Active = TRUE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObJID ORDER BY file_date DESC) = 1
        ),
        realestate_latest AS (
            SELECT *
            FROM `{env_prefix}-cdh`.primaryactive.auctionrealestate
            WHERE ActiveFlag = TRUE
            AND SourceSystem = 'extfile'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObjKey ORDER BY FileDate DESC) = 1
        )
        SELECT 
            n.AuctionObJID,
            n.file_date AS newsphone_file_date,
            r.FileDate AS realestate_file_date,
            n.*, 
            r.*
        FROM newsphone_latest n
        FULL OUTER JOIN realestate_latest r
        ON n.AuctionObJID = r.AuctionObjKey
        WHERE n.file_date != r.FileDate
        ORDER BY COALESCE(n.AuctionObJID, r.AuctionObjKey)

""")
display(df)

# COMMAND ----------

# DBTITLE 1,Landing vs Derivative
df = spark.sql(f"""
        WITH newsphone_latest AS (
            SELECT *
            FROM `{env_prefix}-cdh`.landing.newsphone_auctions
            WHERE Active = TRUE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObJID ORDER BY file_date DESC) = 1
        ),
        realestate_latest AS (
            SELECT *
            FROM `{env_prefix}-cdh`.derivativeactive.dauctionrealestate
            WHERE ActiveFlag = TRUE
            AND SourceSystem = 'extfile'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObjKey ORDER BY FileDate DESC) = 1
        )
        SELECT 
            n.AuctionObJID,
            n.file_date AS newsphone_file_date,
            r.FileDate AS realestate_file_date,
            n.*, 
            r.*
        FROM newsphone_latest n
        FULL OUTER JOIN realestate_latest r
        ON n.AuctionObJID = r.AuctionObjKey
        WHERE n.file_date != r.FileDate
        ORDER BY COALESCE(n.AuctionObJID, r.AuctionObjKey)

""")
display(df)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Landed vs Derivative
df = spark.sql(f"""
        WITH newsphone_latest AS (
            SELECT *
            FROM `{env_prefix}-cdh`.landed.newsphone_auctions
            WHERE Active = TRUE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObJID ORDER BY file_date DESC) = 1
        ),
        realestate_latest AS (
            SELECT *
            FROM `{env_prefix}-cdh`.derivativeactive.dauctionrealestate
            WHERE ActiveFlag = TRUE
            AND SourceSystem = 'extfile'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObjKey ORDER BY FileDate DESC) = 1
        )
        SELECT 
            n.AuctionObJID,
            n.file_date AS newsphone_file_date,
            r.FileDate AS realestate_file_date,
            n.*, 
            r.*
        FROM newsphone_latest n
        FULL OUTER JOIN realestate_latest r
        ON n.AuctionObJID = r.AuctionObjKey
        WHERE n.file_date != r.FileDate
        ORDER BY COALESCE(n.AuctionObJID, r.AuctionObjKey)

""")
display(df)

# COMMAND ----------

# DBTITLE 1,primary vs derivative
df = spark.sql(f"""
        WITH Pnewsphone_latest AS (
            SELECT *
            FROM `{env_prefix}-cdh`.primaryactive.auctionrealestate
            WHERE ActiveFlag = TRUE
            AND SourceSystem = 'extfile'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObjKey ORDER BY FileDate DESC) = 1
        ),
        Drealestate_latest AS (
            SELECT *
            FROM `{env_prefix}-cdh`.derivativeactive.dauctionrealestate
            WHERE ActiveFlag = TRUE
            AND SourceSystem = 'extfile'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObjKey ORDER BY FileDate DESC) = 1
        )
        SELECT 
            n.AuctionObjKey,
            n.FileDate AS Pnewsphone_latest,
            r.FileDate AS Drealestate_latest,
            n.*, 
            r.*
        FROM Pnewsphone_latest n
        FULL OUTER JOIN Drealestate_latest r
        ON n.AuctionObjKey = r.AuctionObjKey
        WHERE n.FileDate != r.FileDate
        ORDER BY COALESCE(n.AuctionObjKey, r.AuctionObjKey)

""")
display(df)

# COMMAND ----------

# DBTITLE 1,Primary active perimeter
# MAGIC %sql
# MAGIC SELECT FileDate, AuctionObjKey, BusinessDate
# MAGIC FROM `p-cdh`.primaryactive.auctionrealestate
# MAGIC WHERE SourceSystem = 'extfile'
# MAGIC AND AuctionObjKey IN (587945)--, 562247)--, 572925, 572930, 581604, 581608, 582846, 582847, 583283, 583286, 584477)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT FileDate, AuctionObjKey, BusinessDate
# MAGIC FROM `p-cdh`.derivativeactive.dauctionrealestate
# MAGIC WHERE SourceSystem = 'extfile'
# MAGIC AND AuctionObjKey IN (587945)--, 562247)--, 572925, 572930, 581604, 581608, 582846, 582847, 583283, 583286, 584477)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM `p-cdh`.primaryactive.auctionrealestate
# MAGIC WHERE ActiveFlag = TRUE
# MAGIC AND SourceSystem = 'extfile'
# MAGIC AND BusinessDate = '2025-06-06'
# MAGIC AND AuctionObjKey = 562245

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM `p-cdh`.primaryactive.auctionrealestate
# MAGIC WHERE ActiveFlag = TRUE
# MAGIC AND SourceSystem = 'extfile'
# MAGIC AND BusinessDate = '2025-06-09'
# MAGIC AND AuctionObjKey = 562245

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(FileDate)
# MAGIC FROM `p-cdh`.derivativeactive.dauctionrealestate
# MAGIC WHERE ActiveFlag = TRUE
# MAGIC AND SourceSystem = 'extfile'
# MAGIC AND AuctionObjKey = 562245

# COMMAND ----------

# DBTITLE 1,Primary active perimeter max BusinessDate
df = spark.sql(
    f"""
    SELECT *
    FROM (
        SELECT *
        FROM `{env_prefix}-cdh`.primaryactive.auctionrealestate
        WHERE ActiveFlag = TRUE 
        AND SourceSystem = 'extfile'
        AND BusinessDate = (select max(BusinessDate) from `{env_prefix}-cdh`.primaryactive.auctionrealestate)
    ) AS latest_auctions
    """
)

display(df.count())
display(df.select("AuctionObjKey").distinct().count())
display(df)

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT *
    FROM (
        SELECT *
        FROM `{env_prefix}-cdh`.primaryactive.auction
        WHERE SourceSystem = 'extfile'
        AND BusinessDate = (select max(BusinessDate) from `{env_prefix}-cdh`.primaryactive.auction)
    ) AS latest_auctions
    """
)

display(df.count())
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC         FROM `d-cdh`.primaryactive.auction

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC         FROM `d-cdh`.derivativeactive.dauction

# COMMAND ----------

df = spark.sql(
    f"""
    SELECT *
    FROM (
        SELECT *
        FROM `{env_prefix}-cdh`.derivativeactive.dauction
        WHERE SourceSystem = 'extfile'
        AND ReferenceDate = (select max(ReferenceDate) from `{env_prefix}-cdh`.derivativeactive.dauction)
    ) AS latest_auctions
    """
)

display(df.count())
display(df)

# COMMAND ----------

# DBTITLE 1,Derivative active perimeter
df = spark.sql(
    f"""
    SELECT *
    FROM (
        SELECT *
        FROM `{env_prefix}-cdh`.derivativeactive.dauctionrealestate
        WHERE ActiveFlag = TRUE 
        AND SourceSystem = 'extfile'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObjKey ORDER BY FileDate DESC) = 1
    ) AS latest_auctions
    """
)

display(df.count())
display(df.select("AuctionObjKey").distinct().count())
display(df)

# COMMAND ----------

# DBTITLE 1,Derivative active perimeter max BusinessDate
df = spark.sql(
    f"""
    SELECT *
    FROM (
        SELECT *
        FROM `{env_prefix}-cdh`.derivativeactive.dauctionrealestate
        WHERE ActiveFlag = TRUE 
        AND SourceSystem = 'extfile'
        AND BusinessDate = (select max(BusinessDate) from `{env_prefix}-cdh`.derivativeactive.dauctionrealestate)
    ) AS latest_auctions
    """
)

display(df.count())
display(df.select("AuctionObjKey").distinct().count())
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `p-cdh`.primaryactive.auctionrealestate
# MAGIC where AuctionRealEstateCode like '0gnMqRxi0T%'
# MAGIC and FileDate = (
# MAGIC   select max(Filedate) from from `p-cdh`.primaryactive.auctionrealestate
# MAGIC where AuctionRealEstateCode like '0gnMqRxi0T%'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `p-cdh`.primaryactive.auctionrealestate
# MAGIC where AuctionRealEstateCode like '0gnMqRxi0T%'
# MAGIC and FileDate = (
# MAGIC   select max(Filedate) from from `p-cdh`.primaryactive.auctionrealestate
# MAGIC where AuctionRealEstateCode like '0gnMqRxi0T%'
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Tia count
df = spark.sql(
    f""" 
select * from `{env_prefix}-cdh-delivery`.tia.tia
""" )

display(df.count())
display(df)

# COMMAND ----------

# DBTITLE 1,Tia distinct AuctionObJID
df = spark.sql(
    f""" 
select distinct AuctionObJID from `{env_prefix}-cdh-delivery`.tia.tia
""" )

display(df.count())
display(df)

# COMMAND ----------

df = spark.sql(
    f""" 
select * from `{env_prefix}-cdh-delivery`.tia.tia
where AuctionObJID in (
    select AuctionObJID
    from `{env_prefix}-cdh-delivery`.tia.tia
    group by AuctionObJID
    having count(*) > 1
)
""" )

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM `p-cdh`.derivativeactive.dauctionrealestate
# MAGIC WHERE ActiveFlag = TRUE 
# MAGIC AND SourceSystem = 'extfile'
# MAGIC and AuctionObjKey = 194701
# MAGIC --QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObjKey ORDER BY FileDate DESC) = 1
# MAGIC       

# COMMAND ----------

# MAGIC %sql
# MAGIC         SELECT *
# MAGIC         FROM `p-cdh`.landed.newsphone_auctions
# MAGIC         WHERE Active = TRUE 
# MAGIC         and AuctionObJID = 194701
# MAGIC         QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObJID ORDER BY file_date DESC) = 1

# COMMAND ----------

df_tia = spark.sql(f"""
               select * from `{env_prefix}-cdh-delivery`.tia.tia    
                   """)
display(df_tia.count())
display(df_tia.distinct().count())

# COMMAND ----------

df_tia = spark.sql(f"""
                   
               select * from `p-cdh-delivery`.tia.tia    
                   """)
display(df_tia.count())
display(df_tia.distinct().count())

# COMMAND ----------


