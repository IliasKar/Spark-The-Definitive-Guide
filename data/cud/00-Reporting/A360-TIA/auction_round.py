# Databricks notebook source
# DBTITLE 1,Env Variables
# Databricks notebook source
from pyspark.sql.functions import collect_set, collect_list, concat_ws,concat, first, col, when, rank, datediff, months_between, lit, to_timestamp, abs as pyspark_abs, create_map
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

# DBTITLE 1,Tables setup
tia_table =  f"`{env_prefix}-cdh-delivery`.tia.tia"
step1_table = f"`{env_prefix}-de-sandbox`.a360.auction_round_data_step1"
step2_table = f"`{env_prefix}-de-sandbox`.a360.auction_round_data_step2"
step3_table = f"`{env_prefix}-de-sandbox`.a360.auction_round_data_step3"
step4_table = f"`{env_prefix}-de-sandbox`.a360.auction_round_data_step4"
tables_to_drop = [step1_table, step2_table, step3_table, step4_table]

# COMMAND ----------

# DBTITLE 1,Step_1 -  AuctionRound_Query_step1
AuctionRound_Query_step1 = f"""WITH BaseData AS (
                    SELECT
                            ROW_NUMBER() over (order by 1)  AS    Row_Num_ObJID ,
                            `RealEstateGroupCode`,

                            `c_KAEK` as    KAEK_OrnlText ,
                            `Κατασχετήρια_Ημερομηνία` as Katasx_OrnlText,

                            `FirstOfferPrice`,
                            `FinalPrice`,
                            `OriginAuctionKey`,
                            `Result_blended`,
                            `AuctionDate`,
                            `PublishedDate`,
                            `AuctionObJID`,
                            `c_auction_id`,
                            `Source`,
                            `HastenerDescr`,
                            `Initiator_Cepal_others`,
                            `Initiator_final`

                    FROM
                        {tia_table}
                    WHERE Result_blended NOT IN ('ΔΔΔ - Δεν ανέβηκε στο eAuction')

) ,


CreateServicer_pre as (
                        select distinct  `c_auction_id`  ,
                              CASE
                                    WHEN `Initiator_Cepal_others` = 'doValue'   and `HastenerDescr` ILIKE '%QQUANT%' THEN '08.QQuant'
                                    WHEN `Initiator_Cepal_others` = 'doValue'   and `HastenerDescr` not ILIKE '%QQUANT%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` is null       and `HastenerDescr` ILIKE '%Value%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'Other'     AND `HastenerDescr` ILIKE '%doValue Greece%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'Bank'      AND `HastenerDescr` ILIKE '%FPS%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'Bank'      AND `HastenerDescr` ILIKE '%Value%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'ETE'       AND `HastenerDescr` ILIKE '%FRONTIER%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'QQuant'    AND `HastenerDescr` ILIKE '%Value%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'QQuant'    AND `HastenerDescr` ILIKE '%PILLAR%' THEN '01.doValue'

                                    WHEN `Initiator_Cepal_others` = 'Cepal' THEN '02.Cepal'
                                    WHEN `Initiator_Cepal_others` is null        AND `HastenerDescr` ILIKE '%CEPAL%' THEN '02.Cepal'
                                    WHEN `HastenerDescr` ILIKE '%aΛΦa%' THEN '02.Cepal'
                                    WHEN `HastenerDescr` ILIKE '%αΛΦα%' THEN '02.Cepal'
                                    WHEN `Initiator_Cepal_others` <> 'Cepal'      AND `HastenerDescr` ILIKE '%ΑΛΦΑ%' THEN '02.Cepal'
                                    WHEN `Initiator_Cepal_others` <> 'Cepal'      AND `HastenerDescr` ILIKE '%AΛΦΑ%' THEN '02.Cepal'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%LPH%' THEN '02.Cepal'
                                    WHEN `Initiator_Cepal_others` <> 'Cepal'      AND `HastenerDescr` ILIKE '%LPH%' THEN '02.Cepal'
                                    -- WHEN `Initiator_Cepal_others` <> 'ETE'        AND `HastenerDescr` ILIKE '%CEPAL%' THEN '02.Cepal'
                                    WHEN `HastenerDescr` ILIKE '%POSEIDON%' THEN '02.Cepal'

                                    WHEN `Initiator_Cepal_others` = 'Intrum' THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%Πειραιώς%'  THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%ΠΕΙΡΑΙΩ%'   THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%ΠΕΙΡΑΩΣ%'   THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Other'       AND `HastenerDescr` ILIKE '%NTRUM%'     THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%NTRUM%'     THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%PIRAEUS%'   THEN '03.Intrum'

                                    WHEN `Initiator_Cepal_others` = 'Bank'        and `Initiator_final` ILIKE 'Eurobank'  THEN '05.Eurobank'
                                    WHEN `Initiator_Cepal_others` = 'doValue'     AND `HastenerDescr`   ILIKE 'Τράπεζα Eurobank Ανώνυμη Εταιρεία' THEN '05.Eurobank'
                                    WHEN `Initiator_Cepal_others` = 'Other'       AND `HastenerDescr`   ILIKE '%Εurobank%' THEN '05.Eurobank'
                                    WHEN `Initiator_Cepal_others` = 'Other'       AND `HastenerDescr`   ILIKE '%Polis'       THEN '05.Eurobank'

                                    WHEN `Initiator_Cepal_others` = 'ETE'                                               THEN '06.ETE'
                                    WHEN `Initiator_Cepal_others` = 'Other'       AND `HastenerDescr` ILIKE '%ΕΘΝΙΚΗ%' THEN '06.ETE'
                                    WHEN `Initiator_Cepal_others` = 'Other'       AND `HastenerDescr` ILIKE '%ΕΤΕ ΑΕ%' THEN '06.ETE'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%ΕΘΝΙΚΗ%' THEN '06.ETE'
                                    WHEN `Initiator_Cepal_others` = 'Undefined'   AND `HastenerDescr` ILIKE '%ΕΘΝΙΚΗ%' THEN '06.ETE'

                                    WHEN `Initiator_Cepal_others` = 'Special Financial Solutions' THEN '07.Special Financial Solutions'

                                    WHEN `Initiator_Cepal_others` = 'QQuant'        AND `HastenerDescr` not ILIKE '%Value%' THEN '08.QQuant'
                                    WHEN `Initiator_Cepal_others` = 'QQuant'        AND `HastenerDescr`     ILIKE '%Quant%' THEN '08.QQuant'

                                    WHEN `Initiator_Cepal_others` = 'PQH'                                                     THEN '09.PQH'
                                    WHEN `Initiator_Cepal_others` = 'Other'        AND `HastenerDescr` ILIKE '%ΑΝΕΛΛΗΝΙ%'     THEN '09.PQH'
                                    WHEN `Initiator_Cepal_others` = 'Other'        AND `HastenerDescr` ILIKE '%ΤΑΧΥΔΡΟΜΙΚΟ%'  THEN '09.PQH'
                                    WHEN `Initiator_Cepal_others` = 'Other'        AND `HastenerDescr` ILIKE '%ΑΧΑΙΚΗ%'       THEN '09.PQH'
                                    WHEN `Initiator_Cepal_others` = 'Other'        AND `HastenerDescr` ILIKE '%ΑΧΑΪΚΗ%'       THEN '09.PQH'
                                    WHEN `Initiator_Cepal_others` = 'Other'        AND `HastenerDescr` ILIKE '%FBB%'           THEN '09.PQH'

                                    WHEN `HastenerDescr` ILIKE '%Δ.Ο.Υ.%'
                                      OR `HastenerDescr` ILIKE '%Δ.ΟΥ.%'
                                      OR `HastenerDescr` ILIKE 'ΔΟΥ %'
                                      OR `HastenerDescr` ILIKE '%ΦΟΡΟΛΟΓΟΥΜΕΝΩΝ%'
                                      OR `HastenerDescr` ILIKE '%ΤΕΛΩΝΕΙ%'
                                      OR `HastenerDescr` ILIKE '%ΑΑΔΕ%'
                                      OR `HastenerDescr` ILIKE '%Α.Α.Δ.Ε.%'
                                      OR `HastenerDescr` ILIKE '%Κ.Ε.ΦΟ.ΜΕ.Π.%'
                                      OR `HastenerDescr` ILIKE '%ΗΜΟΣΙΩΝ ΕΣ%'
                                      OR `HastenerDescr` ILIKE '%ΜΟΝΑΔΑ ΕΙΣΠΡΑΞΗΣ%'
                                      OR `HastenerDescr` ILIKE '%ρχή Δημοσ%'
                                      OR `HastenerDescr` ILIKE '%ΕΞΑΡΤΗΤΗ ΑΡ%'
                                      OR `HastenerDescr` ILIKE '%ΕΛΛΗΝΙΚΟ ΔΗΜ%' THEN '10.ΑΑΔΕ'
                                    WHEN c_auction_id = 'hctISxBADm' then '01.doValue'
                                    WHEN c_auction_id = 'tRyXkATIL2' then '02.Cepal'
                                    WHEN c_auction_id = 'v7skT49OpJ' then '01.doValue'
                                    WHEN c_auction_id = 'R6N5Mr0nJ9' then '03.Intrum'
                                    WHEN c_auction_id = 'aiZlmg70St' then '02.Cepal'


                                   ELSE '04.Other'

                                  END AS `Servicer`


                        from BaseData
),
fails_tbl as (
      select c_auction_id
      from CreateServicer_pre
      group by c_auction_id
      having count(distinct Servicer) > 1
),
CreateServicer as (
      select *
      from CreateServicer_pre
      where c_auction_id not in (select c_auction_id from fails_tbl)
)
------- improve_Servicer          ////  not in use
,

MidleTable_a as  (  select  AuctionObJID  ,`RealEstateGroupCode`,  KAEK_OrnlText,  Katasx_OrnlText
                    from    BaseData
)

,



CreateClearVal_str as  (
                      select AuctionObJID,
                          COALESCE(CAST(`RealEstateGroupCode` AS VARCHAR(20)), 'Unknown')    AS REGroupCode_str,

                          COALESCE(CAST( KAEK_OrnlText AS VARCHAR(255)),                'Unknown')   AS KAEK_Str,
                          LENGTH(CAST(KAEK_OrnlText AS VARCHAR(10)))                                AS KAEK_Len,


                          COALESCE(CAST(`Katasx_OrnlText` AS VARCHAR(255)), 'Unknown')              AS  Katasx_Str ,
                          LENGTH(CAST(Katasx_OrnlText AS VARCHAR(10)))                              AS  KATASX_Len ,
                          left(REGEXP_REPLACE(CAST(`Katasx_OrnlText` AS STRING), '[^0-9]', ''), 3)  AS  KATASX_Left3
                      from MidleTable_a
)   ,

CreateClearVal_str2  (
                      select AuctionObJID,
                        case when   REGroupCode_str = ''  then 'Unknown'                                                          else REGroupCode_str end  AS `REGroupCode_str_C2` ,
                        case when   KAEK_Len   < 30       then REGEXP_REPLACE(REGEXP_REPLACE(KAEK_Str, '[^0-9]', ''), '^0+', '')  else 'Unknown'       end  AS `KAEK_C2`,

                        case when   KATASX_Len > 2        then REGEXP_REPLACE(Katasx_Str, '[^0-9]', '')                           else 'Unknown'       end  AS `KATASX_C2`,
                        case when   KATASX_Len > 2        then `KATASX_Left3`                                                     else 'Unknown'       end  AS `KATASX_Left3_C2`
                      from CreateClearVal_str
)  ,

CreateClearVal_str3  (
                      select AuctionObJID,
                        case when   REGroupCode_str_C2 = ''   then 'Unknown' else REGroupCode_str_C2  end AS `REGroupCode_C3` ,
                        case when   KAEK_C2 = ''              then 'Unknown' else KAEK_C2             end AS `KAEK_C3` ,

                        case when   KATASX_C2 = ''            then 'Unknown' else KATASX_C2           end AS `KATASX_C3`,
                        case when   KATASX_Left3_C2  = ''     then 'Unknown' else KATASX_Left3_C2     end AS `KATASX_Left3_C3`
                      from CreateClearVal_str2
)  ,


MidleTable_b as (  select

                          REGroupCode_C3,
                          KAEK_C3, KATASX_C3, KATASX_Left3_C3,
                          BaseData.*

                    from  BaseData
                    left  join
                          CreateClearVal_str3      on CreateClearVal_str3.AuctionObJID = BaseData.AuctionObJID
)

,


base_data_Counters as (

                        WITH data_Counters AS (
                                                  SELECT DISTINCT
                                                               c_auction_id , AuctionObJID,
                                                               REGroupCode_C3, KAEK_C3

                                                  FROM MidleTable_b
                                              ),

                    KAEK_Over_REGroupCode AS (
                                                  SELECT REGroupCode_C3,
                                                        COUNT(DISTINCT CASE WHEN KAEK_C3 != 'Unknown' THEN KAEK_C3 END) AS Count_KAEK_Over_DISTINCT_REGroupCode
                                                  FROM data_Counters
                                                  WHERE REGroupCode_C3 != 'Unknown'
                                                  GROUP BY REGroupCode_C3
                                              ),

                    REGroupCode_Over_KAEK AS (
                                                  SELECT KAEK_C3,
                                                        COUNT(DISTINCT CASE WHEN REGroupCode_C3 != 'Unknown' THEN REGroupCode_C3 END) AS Count_REGroupCode_Over_DISTINCT_KAEK
                                                  FROM data_Counters
                                                  WHERE KAEK_C3 != 'Unknown'
                                                  GROUP BY KAEK_C3
                                              ),

                          ObJID_Over_KAEK AS (
                                                  SELECT KAEK_C3 ,
                                                        COUNT(DISTINCT AuctionObJID) AS Count_ObJID_Over_KAEK
                                                  FROM data_Counters
                                                  WHERE KAEK_C3 != 'Unknown'
                                                  GROUP BY KAEK_C3
                                              ),

                     ObJID_Over_cAuctionID AS (
                                                  SELECT c_auction_id,
                                                        COUNT(DISTINCT AuctionObJID) AS Count_ObJID_Over_cAuctionID
                                                  FROM data_Counters
                                                  GROUP BY c_auction_id
                                              )
                      SELECT
                            data_Counters.AuctionObJID,
                            KAEK_Over_REGroupCode.Count_KAEK_Over_DISTINCT_REGroupCode,
                            REGroupCode_Over_KAEK.Count_REGroupCode_Over_DISTINCT_KAEK,
                            ObJID_Over_KAEK.Count_ObJID_Over_KAEK,
                            ObJID_Over_cAuctionID.Count_ObJID_Over_cAuctionID
                      FROM
                            data_Counters
                      LEFT JOIN KAEK_Over_REGroupCode  ON data_Counters.REGroupCode_C3 = KAEK_Over_REGroupCode.REGroupCode_C3
                      LEFT JOIN REGroupCode_Over_KAEK  ON data_Counters.KAEK_C3 = REGroupCode_Over_KAEK.KAEK_C3
                      LEFT JOIN ObJID_Over_KAEK        ON data_Counters.KAEK_C3 = ObJID_Over_KAEK.KAEK_C3
                      LEFT JOIN ObJID_Over_cAuctionID  ON data_Counters.c_auction_id = ObJID_Over_cAuctionID.c_auction_id
)    ,




JoinData_a as  (  select   t1.Count_KAEK_Over_DISTINCT_REGroupCode , t1.Count_REGroupCode_Over_DISTINCT_KAEK ,t1.Count_ObJID_Over_KAEK ,t1.Count_ObJID_Over_cAuctionID ,
                           t3.Servicer,
                           t2.*
                          from
                            (select * from base_data_Counters) as t1
                          left join
                            (select * from MidleTable_b) as t2 on t1.AuctionObJID = t2.AuctionObJID
                          left join
                            (select * from CreateServicer) as t3 on t2.c_auction_id = t3.c_auction_id
)  ,   --19 sec
---   AuctionObJID      =  225,737
---   REGroupCode_C3 <> Unknown  =  158,439
---   REGroupCode_C3  Unknown    =  67,297



--add KAEK_C3  for 'Unknown'  REGroupCode_n_C3
Create_REGroupCode_C4 as (

                           SELECT case when REGroupCode_C3 =  'Unknown'
                                        and Count_REGroupCode_Over_DISTINCT_KAEK in ( 0 , 1)
                                        and Count_ObJID_Over_cAuctionID = 1  then KAEK_C3
                                          else REGroupCode_C3
                                    end as REGroupCode_C4
                              ,     *
                           FROM JoinData_a
)  ,  --19 sec
---   REGroupCode_C4 <>  Unknown  =  175,409
---   REGroupCode_C4   Unknown    =  50,328



--add KAEK_n_C3  for 'Unknown'  REGroupCode_C4  where  Count_REGroupCode_Over_DISTINCT_KAEK = 0
Create_REGroupCode_C5 as (
                            WITH Temp_Data AS (   SELECT   *
                                                  FROM Create_REGroupCode_C4
                                              ),

                                Sub_Temp_a AS (
                                                  SELECT  AuctionObJID, KAEK_C3 as REGroupCode_C5 , REGroupCode_C4
                                                  FROM Temp_Data
                                                  where Count_REGroupCode_Over_DISTINCT_KAEK = 0  and REGroupCode_C4 = 'Unknown'
                                              )  ,


                                Sub_Temp_b AS (
                                                  SELECT    AuctionObJID,
                                                            case when Sub_Temp_a.REGroupCode_C4 =  'Unknown' then Sub_Temp_a.REGroupCode_C5 else Sub_Temp_a.REGroupCode_C4 end as REGroupCode_C5_Temp
                                                  FROM Sub_Temp_a
                                              )    ,


                                Sub_Temp_c AS (
                                                      select
                                                            case when Sub_Temp_b.REGroupCode_C5_Temp is null then Temp_Data.REGroupCode_C4 else Sub_Temp_b.REGroupCode_C5_Temp end as REGroupCode_C5  ,
                                                            Temp_Data.*
                                                      from
                                                                   Temp_Data
                                                      left join   Sub_Temp_b
                                                       on          Temp_Data.AuctionObJID  = Sub_Temp_b.AuctionObJID
                                              )

                                select
                                          *
                                from
                                          Sub_Temp_c
)

---   REGroupCode_C4 <>  Unknown  =  175,409
---   REGroupCode_C4   Unknown    =  50,328
,


Create_REGroupCode_C6 as (
                                WITH Temp_Data AS (
                                                      SELECT *
                                                      FROM Create_REGroupCode_C5
                                                  ),

                                    Sub_Temp_a AS (
                                                      SELECT
                                                            AuctionObJID,  KATASX_C3, OriginAuctionKey,
                                                            COUNT( AuctionObJID )   OVER (PARTITION BY KATASX_C3 ) AS KATASX_C3_Counter,
                                                            COUNT( AuctionObJID )   oVER (PARTITION BY OriginAuctionKey )   AS OriginAuctionKey_Counter
                                                      FROM Temp_Data
                                                      WHERE Count_ObJID_Over_cAuctionID = 1 and REGroupCode_C5 = 'Unknown'
                                                      GROUP BY AuctionObJID , KATASX_C3, OriginAuctionKey
                                              ) ,

                                    Sub_Temp_b AS (
                                                      SELECT
                                                            AuctionObJID,
                                                            case
                                                                  when  KATASX_C3_Counter = 2 then  KATASX_C3
                                                                  when  OriginAuctionKey_Counter = 2 then OriginAuctionKey
                                                            end as REGroupCode_C6_Temp
                                                      FROM Sub_Temp_a
                                                      WHERE KATASX_C3_Counter = 2  or OriginAuctionKey_Counter = 2

                                              ) ,

                              Sub_Temp_c  AS (
                                                SELECT
                                                      case when Temp_Data.REGroupCode_C5 = 'Unknown' then Sub_Temp_b.REGroupCode_C6_Temp else Temp_Data.REGroupCode_C5  end as REGroupCode_C6_Temp_Temp,
                                                      Temp_Data.AuctionObJID
                                                FROM        Temp_Data
                                                left join   Sub_Temp_b
                                                   on       Sub_Temp_b.AuctionObJID  = Temp_Data.AuctionObJID

                                              )

                              select
                                    case when REGroupCode_C6_Temp_Temp is null  then 'Unknown' else REGroupCode_C6_Temp_Temp end as REGroupCode_C6,
                                    datediff(AuctionDate, PublishedDate) as DateDIff_AucPubl,
                                    Temp_Data.*
                              FROM        Temp_Data
                              left join   Sub_Temp_c
                                    on    Sub_Temp_c.AuctionObJID  = Temp_Data.AuctionObJID

)

,

Create_REGroupCode_C7 as (
                                WITH Temp_Data AS (
                                                      SELECT *
                                                      FROM Create_REGroupCode_C6
                                                  )  ,

                                 Sub_Temp_a  AS  ( select
                                                             AuctionObJID as AuctionObJID_Temp_a , case when REGroupCode_C6 = 'Unknown' then  AuctionObJID end as REGroupCode_C7
                                                            ,  *
                                                      from   Temp_Data
                                                      where  REGroupCode_C6 = 'Unknown'  and Count_ObJID_Over_cAuctionID = 1
                                                      and    DateDIff_AucPubl >= 165
                                                )  ,

                                 Sub_Temp_b  AS  ( select
                                                              REGroupCode_C7
                                                            , Temp_Data.*
                                                      from      Temp_Data
                                                      left join Sub_Temp_a
                                                      on        Sub_Temp_a.AuctionObJID_Temp_a = Temp_Data.AuctionObJID
                                                )  ,

                                  Sub_Temp_c  as (  SELECT
                                                          CASE
                                                                WHEN  Temp_Data.REGroupCode_C6 !=  'Unknown' then  Sub_Temp_b.REGroupCode_C6
                                                                wHEN  Temp_Data.REGroupCode_C6 =  'Unknown' then  Sub_Temp_b.REGroupCode_C7

                                                             --     else Temp_Data.REGroupCode_C6
                                                            END AS REGroupCode_C7,
                                                            Temp_Data.*

                                                    FROM       Temp_Data
                                                    LEFT JOIN   Sub_Temp_b
                                                        ON Temp_Data.AuctionObJID = Sub_Temp_b.AuctionObJID
                                                  )

                              select
                                    case when Sub_Temp_c.REGroupCode_C7 is null then 'Unknown' else Sub_Temp_c.REGroupCode_C7 end as REGroupCode_C7
                                    , Temp_Data.*
                              from          Sub_Temp_c
                              LEFT JOIN     Temp_Data
                                on          Temp_Data.AuctionObJID   =   Sub_Temp_c.AuctionObJID

)  ,

Create_REGroupCode_C8  as (
                                WITH Temp_Data AS (
                                                      SELECT *
                                                      FROM Create_REGroupCode_C7
                                                  )  ,

                                 Sub_Temp_a  AS  ( select
                                                             AuctionObJID as AuctionObJID_Temp_a , case when REGroupCode_C7 = 'Unknown' then concat('Unknown', '_' , AuctionObJID ) end as REGroupCode_C8
                                                      from   Temp_Data
                                                      where  REGroupCode_C7 = 'Unknown'
                                                )

                              select
                                       case when Temp_Data.REGroupCode_C7 != 'Unknown' then  Temp_Data.REGroupCode_C7
                                          when Temp_Data.REGroupCode_C7 = 'Unknown' then Sub_Temp_a.REGroupCode_C8 end as REGroupCode_C8
                                    ,  Temp_Data.*
                              from          Temp_Data
                              LEFT JOIN     Sub_Temp_a
                                on          Temp_Data.AuctionObJID   =   Sub_Temp_a.AuctionObJID_Temp_a

)
--  50 sec
,

----c_auction_id_Flag
Create_REGroupCode_C9  as (
                                WITH Temp_Data AS (
                                                      SELECT *
                                                      FROM Create_REGroupCode_C8
                                                  )  ,

                                 Sub_Temp_a  AS  (  SELECT
                                                          AuctionObJID,
                                                          c_auction_id,
                                                          REGroupCode_C8,
                                                          -- Δημιουργία flag
                                                          CASE WHEN COUNT(c_auction_id) OVER (PARTITION BY REGroupCode_C8, c_auction_id) > 1 THEN 'Duplicate'     ELSE 'Unique' END AS c_auction_id_Flag
                                                      FROM Create_REGroupCode_C8
                                                )   ,

                              Sub_Temp_b  AS  (  SELECT distinct
                                                          Sub_Temp_a.AuctionObJID,
                                                          Sub_Temp_a.c_auction_id_Flag,
                                                          case
                                                              when Sub_Temp_a.c_auction_id_Flag  = 'Duplicate' then  Sub_Temp_a.AuctionObJID
                                                              when Sub_Temp_a.c_auction_id_Flag != 'Duplicate' then  Sub_Temp_a.REGroupCode_C8
                                                          end as REGroupCode_C9

                                                      FROM   Sub_Temp_a
                                                )   ,

                              Sub_Temp_c  AS  (  SELECT distinct
                                                               Sub_Temp_b.REGroupCode_C9,
                                                             Sub_Temp_b.c_auction_id_Flag,

                                                           Temp_Data.*

                                                      FROM       Temp_Data
                                                      left join  Sub_Temp_b
                                                        on  Temp_Data.AuctionObJID = Sub_Temp_b.AuctionObJID

                                                )

                              select * from Sub_Temp_c
)
,
Level_Calc_REGroupCode  as (       with Temp_Data as(  select  *
                                                        from Create_REGroupCode_C9
                                                    )  ,

                                      Sub_Temp_a  AS  (   select  distinct
                                                                AuctionObJID  ,
                                                                REGroupCode_C9, REGroupCode_C8,  REGroupCode_C7 , REGroupCode_C6  , REGroupCode_C5  , REGroupCode_C4  , REGroupCode_C3
                                                          from Temp_Data
                                                     ) ,

                                      Sub_Temp_b  AS  (   select  distinct
                                                                  CASE
                                                                      WHEN REGroupCode_C3 != 'Unknown' THEN 'REGroupCode_C3'
                                                                      WHEN REGroupCode_C3 = 'Unknown' and REGroupCode_C4 != 'Unknown' THEN 'REGroupCode_C4'
                                                                      WHEN REGroupCode_C3 = 'Unknown' and REGroupCode_C4 = 'Unknown' and REGroupCode_C5 != 'Unknown' THEN 'REGroupCode_C5'
                                                                      WHEN REGroupCode_C3 = 'Unknown' and REGroupCode_C4 = 'Unknown' and REGroupCode_C5 = 'Unknown' and REGroupCode_C6 != 'Unknown' THEN 'REGroupCode_C6'
                                                                      WHEN REGroupCode_C3 = 'Unknown' and REGroupCode_C4 = 'Unknown' and REGroupCode_C5 = 'Unknown' and REGroupCode_C6 = 'Unknown' and REGroupCode_C7 != 'Unknown' THEN 'REGroupCode_C7'
                                                                      WHEN REGroupCode_C3 = 'Unknown' and REGroupCode_C4 = 'Unknown' and REGroupCode_C5 = 'Unknown' and REGroupCode_C6 = 'Unknown' and REGroupCode_C7 = 'Unknown' and REGroupCode_C8 != 'Unknown' THEN 'REGroupCode_C8'
                                                                      WHEN c_auction_id_Flag = 'Duplicate'   THEN 'REGroupCode_C9'
                                                                  END AS Level_of_Calc_REGroupCode,
                                                                  AuctionObJID as AuctionObJID_Temp_b
                                                          FROM Temp_Data
                                                     )
                                    select   Sub_Temp_b.Level_of_Calc_REGroupCode
                                            , Temp_Data.*
                                    from Temp_Data
                                    left join Sub_Temp_b
                                    on Temp_Data.AuctionObJID = Sub_Temp_b.AuctionObJID_Temp_b
)


SELECT distinct * FROM   Level_Calc_REGroupCode"""


# COMMAND ----------

AuctionRound_Query_step1_tester = f"""WITH BaseData AS (
                    SELECT
                            ROW_NUMBER() over (order by 1)  AS    Row_Num_ObJID ,
                            `RealEstateGroupCode`,

                            `c_KAEK` as    KAEK_OrnlText ,
                            `Κατασχετήρια_Ημερομηνία` as Katasx_OrnlText,

                            `FirstOfferPrice`,
                            `FinalPrice`,
                            `OriginAuctionKey`,
                            `Result_blended`,
                            `AuctionDate`,
                            `PublishedDate`,
                            `AuctionObJID`,
                            `c_auction_id`,
                            `Source`,
                            `HastenerDescr`,
                            `Initiator_Cepal_others`,
                            `Initiator_final`

                    FROM
                        {tia_table}
                    WHERE Result_blended NOT IN ('ΔΔΔ - Δεν ανέβηκε στο eAuction')

) ,


CreateServicer_pre as (
                        select distinct  `c_auction_id`  ,
                              CASE
                                    WHEN `Initiator_Cepal_others` = 'doValue'   and `HastenerDescr` ILIKE '%QQUANT%' THEN '08.QQuant'
                                    WHEN `Initiator_Cepal_others` = 'doValue'   and `HastenerDescr` not ILIKE '%QQUANT%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` is null       and `HastenerDescr` ILIKE '%Value%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'Other'     AND `HastenerDescr` ILIKE '%doValue Greece%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'Bank'      AND `HastenerDescr` ILIKE '%FPS%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'Bank'      AND `HastenerDescr` ILIKE '%Value%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'ETE'       AND `HastenerDescr` ILIKE '%FRONTIER%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'QQuant'    AND `HastenerDescr` ILIKE '%Value%' THEN '01.doValue'
                                    WHEN `Initiator_Cepal_others` = 'QQuant'    AND `HastenerDescr` ILIKE '%PILLAR%' THEN '01.doValue'

                                    WHEN `Initiator_Cepal_others` = 'Cepal' THEN '02.Cepal'
                                    WHEN `Initiator_Cepal_others` is null        AND `HastenerDescr` ILIKE '%CEPAL%' THEN '02.Cepal'
                                    WHEN `HastenerDescr` ILIKE '%aΛΦa%' THEN '02.Cepal'
                                    WHEN `HastenerDescr` ILIKE '%αΛΦα%' THEN '02.Cepal'
                                    WHEN `Initiator_Cepal_others` <> 'Cepal'      AND `HastenerDescr` ILIKE '%ΑΛΦΑ%' THEN '02.Cepal'
                                    WHEN `Initiator_Cepal_others` <> 'Cepal'      AND `HastenerDescr` ILIKE '%AΛΦΑ%' THEN '02.Cepal'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%LPH%' THEN '02.Cepal'
                                    WHEN `Initiator_Cepal_others` <> 'Cepal'      AND `HastenerDescr` ILIKE '%LPH%' THEN '02.Cepal'
                                    -- WHEN `Initiator_Cepal_others` <> 'ETE'        AND `HastenerDescr` ILIKE '%CEPAL%' THEN '02.Cepal'
                                    -- WHEN `Initiator_Cepal_others` <> 'ETE'        AND `HastenerDescr` ILIKE '%CEPAL%' THEN '02.Cepal'
                                    WHEN `HastenerDescr` ILIKE '%POSEIDON%' THEN '02.Cepal'

                                    WHEN `Initiator_Cepal_others` = 'Intrum' THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%Πειραιώς%'  THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%ΠΕΙΡΑΙΩ%'   THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%ΠΕΙΡΑΩΣ%'   THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Other'       AND `HastenerDescr` ILIKE '%NTRUM%'     THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%NTRUM%'     THEN '03.Intrum'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%PIRAEUS%'   THEN '03.Intrum'

                                    WHEN `Initiator_Cepal_others` = 'Bank'        and `Initiator_final` ILIKE 'Eurobank'  THEN '05.Eurobank'
                                    WHEN `Initiator_Cepal_others` = 'doValue'     AND `HastenerDescr`   ILIKE 'Τράπεζα Eurobank Ανώνυμη Εταιρεία' THEN '05.Eurobank'
                                    WHEN `Initiator_Cepal_others` = 'Other'       AND `HastenerDescr`   ILIKE '%Εurobank%' THEN '05.Eurobank'
                                    WHEN `Initiator_Cepal_others` = 'Other'       AND `HastenerDescr`   ILIKE '%Polis'       THEN '05.Eurobank'

                                    WHEN `Initiator_Cepal_others` = 'ETE'                                               THEN '06.ETE'
                                    WHEN `Initiator_Cepal_others` = 'Other'       AND `HastenerDescr` ILIKE '%ΕΘΝΙΚΗ%' THEN '06.ETE'
                                    WHEN `Initiator_Cepal_others` = 'Other'       AND `HastenerDescr` ILIKE '%ΕΤΕ ΑΕ%' THEN '06.ETE'
                                    WHEN `Initiator_Cepal_others` = 'Bank'        AND `HastenerDescr` ILIKE '%ΕΘΝΙΚΗ%' THEN '06.ETE'
                                    WHEN `Initiator_Cepal_others` = 'Undefined'   AND `HastenerDescr` ILIKE '%ΕΘΝΙΚΗ%' THEN '06.ETE'

                                    WHEN `Initiator_Cepal_others` = 'Special Financial Solutions' THEN '07.Special Financial Solutions'

                                    WHEN `Initiator_Cepal_others` = 'QQuant'        AND `HastenerDescr` not ILIKE '%Value%' THEN '08.QQuant'
                                    WHEN `Initiator_Cepal_others` = 'QQuant'        AND `HastenerDescr`     ILIKE '%Quant%' THEN '08.QQuant'

                                    WHEN `Initiator_Cepal_others` = 'PQH'                                                     THEN '09.PQH'
                                    WHEN `Initiator_Cepal_others` = 'Other'        AND `HastenerDescr` ILIKE '%ΑΝΕΛΛΗΝΙ%'     THEN '09.PQH'
                                    WHEN `Initiator_Cepal_others` = 'Other'        AND `HastenerDescr` ILIKE '%ΤΑΧΥΔΡΟΜΙΚΟ%'  THEN '09.PQH'
                                    WHEN `Initiator_Cepal_others` = 'Other'        AND `HastenerDescr` ILIKE '%ΑΧΑΙΚΗ%'       THEN '09.PQH'
                                    WHEN `Initiator_Cepal_others` = 'Other'        AND `HastenerDescr` ILIKE '%ΑΧΑΪΚΗ%'       THEN '09.PQH'
                                    WHEN `Initiator_Cepal_others` = 'Other'        AND `HastenerDescr` ILIKE '%FBB%'           THEN '09.PQH'

                                    WHEN `HastenerDescr` ILIKE '%Δ.Ο.Υ.%'
                                      OR `HastenerDescr` ILIKE '%Δ.ΟΥ.%'
                                      OR `HastenerDescr` ILIKE 'ΔΟΥ %'
                                      OR `HastenerDescr` ILIKE '%ΦΟΡΟΛΟΓΟΥΜΕΝΩΝ%'
                                      OR `HastenerDescr` ILIKE '%ΤΕΛΩΝΕΙ%'
                                      OR `HastenerDescr` ILIKE '%ΑΑΔΕ%'
                                      OR `HastenerDescr` ILIKE '%Α.Α.Δ.Ε.%'
                                      OR `HastenerDescr` ILIKE '%Κ.Ε.ΦΟ.ΜΕ.Π.%'
                                      OR `HastenerDescr` ILIKE '%ΗΜΟΣΙΩΝ ΕΣ%'
                                      OR `HastenerDescr` ILIKE '%ΜΟΝΑΔΑ ΕΙΣΠΡΑΞΗΣ%'
                                      OR `HastenerDescr` ILIKE '%ρχή Δημοσ%'
                                      OR `HastenerDescr` ILIKE '%ΕΞΑΡΤΗΤΗ ΑΡ%'
                                      OR `HastenerDescr` ILIKE '%ΕΛΛΗΝΙΚΟ ΔΗΜ%' THEN '10.ΑΑΔΕ'
                                    WHEN c_auction_id = 'hctISxBADm' then '01.doValue'
                                    WHEN c_auction_id = 'tRyXkATIL2' then '02.Cepal'
                                    WHEN c_auction_id = 'v7skT49OpJ' then '01.doValue'
                                    WHEN c_auction_id = 'R6N5Mr0nJ9' then '03.Intrum'
                                    WHEN c_auction_id = 'aiZlmg70St' then '02.Cepal'
                                   ELSE '04.Other'

                                  END AS `Servicer`


                        from BaseData
),
fails_tbl as (
select c_auction_id, count(distinct Servicer) as cnt_servicer
from CreateServicer_pre
group by c_auction_id
having count(distinct Servicer) > 1
)
select * from fails_tbl
"""
df_result = spark.sql(AuctionRound_Query_step1_tester)
display(df_result)

# COMMAND ----------

# DBTITLE 1,Step_1 -  AuctionRound_Query_step1 to PySpark DataFrame  /
df_step1 = spark.sql(AuctionRound_Query_step1)
df_step1.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{step1_table}")

# COMMAND ----------

# DBTITLE 1,Step_2 -  AuctionRound_Query_step2
AuctionRound_Query_step2 = f"""with Level_Calc_REGroupCode as  (  select * from {step1_table}  )
,

 Create_REGroupCode_C10    as  (
                             
                              WITH DistinctServicer_Data AS (
                                                                  SELECT
                                                                        AuctionObJID, REGroupCode_C9, Servicer, Initiator_final, KATASX_C3, KATASX_Left3_C3, OriginAuctionKey,c_auction_id
                                                                  FROM
                                                                        Level_Calc_REGroupCode
                                                            )  ,

                                  DistinctServicer      AS (
                                                                  SELECT  distinct
                                                                        REGroupCode_C9, Servicer  
                                                                  FROM  
                                                                        DistinctServicer_Data  
                                                            )  ,


                                  DistinctInitiator_final AS (
                                                                  SELECT  distinct
                                                                        REGroupCode_C9, Initiator_final  
                                                                  FROM  
                                                                       DistinctServicer_Data  
                                                            )  ,


                                   DistinctServicer_a    AS (
                                                                  SELECT  
                                                                        REGroupCode_C9,  Count(distinct Servicer) as Count_Servicer_Over_REGroupCode_C9    
                                                                  FROM  
                                                                        DistinctServicer  
                                                                  group by REGroupCode_C9  
                                                            )  ,

                                    DistinctServicer_b   AS (
                                                                  SELECT  
                                                                        REGroupCode_C9, Servicer ,
                                                                        DENSE_RANK() OVER (ORDER BY Servicer  )  AS Row_Num_Servicer
                                                                  FROM  DistinctServicer
                                                            )  ,

                                    DistinctServicer_c as   (     SELECT      DistinctServicer_a.Count_Servicer_Over_REGroupCode_C9, c_auction_id
                                                                           ,  DistinctServicer_b.Row_Num_Servicer
                                                                           ,  DistinctServicer_Data.*

                                                                  FROM        DistinctServicer_Data
                                                                  left join   DistinctServicer_a
                                                                        on    DistinctServicer_Data.REGroupCode_C9 = DistinctServicer_a.REGroupCode_C9
                                                                  left join   DistinctServicer_b
                                                                        on    DistinctServicer_Data.REGroupCode_C9    =   DistinctServicer_b.REGroupCode_C9
                                                                        and   DistinctServicer_Data.Servicer        =   DistinctServicer_b.Servicer  
                                                            )  

                                    SELECT       distinct
                                                 AuctionObJID, c_auction_id
                                             ,   KATASX_C3 ,KATASX_Left3_C3  , OriginAuctionKey
                                             ,   Initiator_final
                                             ,   Row_Num_Servicer
                                             ,   Concat(REGroupCode_C9, '_', Row_Num_Servicer ) as  REGroupCode_C10
                                             ,   DistinctServicer_c.Count_Servicer_Over_REGroupCode_C9  , Servicer
                                    FROM    DistinctServicer_c
                             


)

,


--add : Set Diff id for Diff Initiator over  REGroupCode_C10
---  REGroupCode_C11
--CountServicer_Over_ REGroupCode_C10
Create_REGroupCode_C11   as  (

                        with Distinct_Initiator_Data as  (  SELECT   *
                                                            FROM
                                                                  Create_REGroupCode_C10
                                                          )  ,


                                 
                                DistinctInitiator      AS (
                                                            SELECT  
                                                                  distinct REGroupCode_C10, Initiator_final  
                                                            FROM
                                                                  Distinct_Initiator_Data  
                                                          )  ,


                                DistinctInitiator_a    AS (
                                                            SELECT  
                                                                  REGroupCode_C10,  Count(distinct Initiator_final ) as Count_Initiator_Over_REGroupCode_C10    
                                                            fROM
                                                                  DistinctInitiator  
                                                            group by
                                                                  REGroupCode_C10  
                                                          )  ,

                                DistinctInitiator_b   AS (
                                                            SELECT  
                                                                  REGroupCode_C10, Initiator_final ,
                                                                  DENSE_RANK() OVER (ORDER BY Initiator_final  )  AS Row_Num_Initiator_final
                                                              FROM  DistinctInitiator
                                                            )    ,


                               DistinctInitiator_c   as   (     SELECT  
                                                                          DistinctInitiator_a.Count_Initiator_Over_REGroupCode_C10
                                                                        , DistinctInitiator_b.Row_Num_Initiator_final
                                                                        , Distinct_Initiator_Data.*
                                                                  FROM       Distinct_Initiator_Data
                                                                  left join DistinctInitiator_a
                                                                        on Distinct_Initiator_Data.REGroupCode_C10 = DistinctInitiator_a.REGroupCode_C10
                                                                  left join DistinctInitiator_b
                                                                        on Distinct_Initiator_Data.REGroupCode_C10   =  DistinctInitiator_b.REGroupCode_C10
                                                                        and Distinct_Initiator_Data.Initiator_final  =  DistinctInitiator_b.Initiator_final  

                                                                  left join  DistinctInitiator
                                                                        on   DistinctInitiator.REGroupCode_C10   =  DistinctInitiator_b.REGroupCode_C10
                                                            )   ,



                                CountObJID_GroupCode_C10 as (
                                                              select  REGroupCode_C10 , Count(AuctionObJID) as Count_AuctionObJID_Over_REGroupCode_C10
                                                              from    Distinct_Initiator_Data
                                                              group by REGroupCode_C10
                                                            )   ,



                                 CountOriginAuctionKey as  (    select REGroupCode_C10 , Count(distinct OriginAuctionKey) as Count_OriginAuctionKey_Over_REGroupCode_C10
                                                                  from Distinct_Initiator_Data
                                                                  group by REGroupCode_C10
                                                            )



                                         

                                    SELECT     Distinct
                                                 Count_OriginAuctionKey_Over_REGroupCode_C10
                                             ,   Count_AuctionObJID_Over_REGroupCode_C10
                                             ,   AuctionObJID
                                             ,   Row_Num_Initiator_final
                                             ,   Concat(DistinctInitiator_c.REGroupCode_C10, '_', Row_Num_Initiator_final) as  REGroupCode_C11
                                             ,   DistinctInitiator_c.Count_Initiator_Over_REGroupCode_C10
                                 
                                    FROM         DistinctInitiator_c


                                    left join   CountObJID_GroupCode_C10
                                      on        DistinctInitiator_c.REGroupCode_C10 = CountObJID_GroupCode_C10.REGroupCode_C10


                                    left join   CountOriginAuctionKey
                                      on        DistinctInitiator_c.REGroupCode_C10 = CountOriginAuctionKey.REGroupCode_C10
)
--select * from Create_REGroupCode_C11
,


missing_KATASX as  (  with Level_Calc as  (select  distinct * from Level_Calc_REGroupCode
                                          )
                                          ,

                        GroupCode_C10 as  (   select * from Create_REGroupCode_C10
                                          )
                                          ,

                        GroupCode_C11 as  (   select * from Create_REGroupCode_C11
                                          )
                                          ,

                       CountKATASX_C3 as  (   select
                                                      GroupCode_C10.REGroupCode_C10 , Count(distinct GroupCode_C10.KATASX_Left3_C3) as Count_KATASX_C3_Over_REGroupCode_C10
                                              from
                                                      GroupCode_C10
                                              where
                                                      GroupCode_C10.KATASX_Left3_C3  != 'Unknown'
                                              group by
                                                      GroupCode_C10.REGroupCode_C10
                                          )
                                          ,

                  missing_KATASX_flag as  (  SELECT
                                                      REGroupCode_C10,
                                                      KATASX_Left3_C3,
                                                      CASE  WHEN EXISTS ( SELECT 1   FROM GroupCode_C10 t2  WHERE t2.REGroupCode_C10 = t1.REGroupCode_C10    AND t2.KATASX_Left3_C3 = 'Unknown' )  THEN 'Yes'  ELSE 'No'  END AS missing_KATASX
                                                FROM
                                                      GroupCode_C10 as t1
                                            )

                  select    distinct
                              Count_AuctionObJID_Over_REGroupCode_C10

                         ,    Count_OriginAuctionKey_Over_REGroupCode_C10
                         ,    REGroupCode_C11
                         ,    missing_KATASX_flag.missing_KATASX

                         ,    CountKATASX_C3.Count_KATASX_C3_Over_REGroupCode_C10
                         ,    GroupCode_C11.Count_Initiator_Over_REGroupCode_C10

                         ,    GroupCode_C11.Row_Num_Initiator_final

                         ,    GroupCode_C10.REGroupCode_C10
                         ,    GroupCode_C10.Row_Num_Servicer

                         ,    Level_Calc.*
                         --,    Level_Calc.REGroupCode_C9,  Level_Calc.Level_of_Calc_REGroupCode,	Level_Calc.DateDIff_AucPubl,	Level_Calc.Servicer,	Level_Calc.KATASX_C3,	Level_Calc.KATASX_Left3_C3
                         --,	Level_Calc.Row_Num_ObJID,  Level_Calc.FirstOfferPrice, Level_Calc.FinalPrice,	  Level_Calc.OriginAuctionKey, Level_Calc.Result_blended
                         --,	Level_Calc.AuctionDate,	   Level_Calc.AuctionObJID,	 Level_Calc.c_auction_id, Level_Calc.Source,	       Level_Calc.HastenerDescr,	Level_Calc.Initiator_final
                         --,    Level_Calc.Count_ObJID_Over_cAuctionID


                  from              Level_Calc

                        left join   GroupCode_C10
                              on    Level_Calc.AuctionObJID = GroupCode_C10.AuctionObJID

                        left join   GroupCode_C11
                              on    Level_Calc.AuctionObJID = GroupCode_C11.AuctionObJID

                        left join   CountKATASX_C3
                              on    GroupCode_C10.REGroupCode_C10 = CountKATASX_C3.REGroupCode_C10

                        left join   missing_KATASX_flag
                              on    missing_KATASX_flag.REGroupCode_C10 = CountKATASX_C3.REGroupCode_C10

                  order by REGroupCode_C10, AuctionDate desc
)


--select * from missing_KATASX

,

Update_missing_KATASX_Yes as (
                                    with TempData as (      select
                                                                  *
                                                            from
                                                                  missing_KATASX
                                                      )
                                                      ,

                                          Temp_1 as (       select
                                                                  REGroupCode_C10,	KATASX_Left3_C3, AuctionDate,	AuctionObJID ,  missing_KATASX, OriginAuctionKey
                                                            from
                                                                  TempData
                                                            where
                                                                  missing_KATASX = 'Yes'
                                                            order by
                                                                  REGroupCode_C10, AuctionDate asc

                                                      )
                                                      ,

                                          Temp_2 as  (
                                                      WITH FilledData AS (
                                                                        SELECT *,
                                                                              -- Εδώ χρησιμοποιούμε την συνάρτηση LAG για να πάρουμε την προηγούμενη έγκυρη τιμή
                                                                              LAG(KATASX_Left3_C3) OVER (PARTITION BY REGroupCode_C10 ORDER BY AuctionDate) AS Prev_KATASX
                                                                        FROM Temp_1
                                                                        )
                                                                        ,

                                                                        FilledWithFirstValid AS (
                                                                        SELECT *,
                                                                              -- Εδώ ψάχνουμε για την πρώτη έγκυρη τιμή που εμφανίζεται μετά το Unknown
                                                                              FIRST_VALUE(CASE
                                                                                                WHEN KATASX_Left3_C3 <> 'Unknown' THEN KATASX_Left3_C3
                                                                                                ELSE NULL
                                                                                          END)
                                                                              IGNORE NULLS
                                                                              OVER (PARTITION BY REGroupCode_C10 ORDER BY AuctionDate ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS First_Valid_KATASX
                                                                        FROM FilledData
                                                                        )
                                                                        SELECT *,
                                                                              -- Αν η τιμή είναι Unknown, γεμίζουμε με την πρώτη έγκυρη τιμή
                                                                              COALESCE(
                                                                              CASE
                                                                                    WHEN KATASX_Left3_C3 = 'Unknown' THEN First_Valid_KATASX
                                                                                    ELSE KATASX_Left3_C3
                                                                              END,

                                                                              -- Προεπιλεγμένη τιμή για την πρώτη εγγραφή ή αν δεν υπάρχει έγκυρη τιμή
                                                                              LAG(KATASX_Left3_C3) OVER (PARTITION BY REGroupCode_C10 ORDER BY AuctionDate)
                                                                             --'Default_Value'

                                                                              ) AS Filled_KATASX_Left3_C3
                                                                        FROM FilledWithFirstValid
                                                      )


                              select  distinct
                                          Temp_2.Filled_KATASX_Left3_C3
                                    ,     TempData.*
                              from

                                    TempData
                              left join
                                    Temp_2
                              on Temp_2.AuctionObJID  =  TempData.AuctionObJID



)

,

Create_REGroupCode_C12   as  (
                                    with TempData as (select  distinct * from Update_missing_KATASX_Yes
                                                      )  ,

                                          Temp_1 as   (   select

                                                                  CASE
                                                                        WHEN missing_KATASX = 'No'   AND Count_KATASX_C3_Over_REGroupCode_C10    >= 1 THEN CONCAT(REGroupCode_C10, '_', KATASX_Left3_C3)

                                                                        WHEN missing_KATASX =  'Yes'   THEN  CONCAT(REGroupCode_C10, '_', Filled_KATASX_Left3_C3)



                                                                        WHEN missing_KATASX is null  AND Count_AuctionObJID_Over_REGroupCode_C10  = 1 THEN  REGroupCode_C10

                                                                        WHEN missing_KATASX is null  AND  Count_KATASX_C3_Over_REGroupCode_C10  is not null
                                                                                                     and  Count_Initiator_Over_REGroupCode_C10   = 1
                                                                                                      THEN Concat(REGroupCode_C10, '_' , AuctionObJID)

                                                                        WHEN missing_KATASX is null  AND  Count_AuctionObJID_Over_REGroupCode_C10 =   Count_OriginAuctionKey_Over_REGroupCode_C10
                                                                                                      THEN  REGroupCode_C10

                                                                        WHEN missing_KATASX is null  AND  Count_AuctionObJID_Over_REGroupCode_C10 >   Count_OriginAuctionKey_Over_REGroupCode_C10
                                                                                                      THEN  REGroupCode_C10
                                                                              ELSE NULL
                                                            END AS REGroupCode_C12
                                                            , *

                                                            from TempData
                                                      )
                              select  
                                    distinct *
                              from
                                    Temp_1
                              order by
                                    REGroupCode_C12, AuctionDate desc

)


,

New_auction_Milestone as (  with TempData  as   (     select  distinct * from Create_REGroupCode_C12
                                                )
                                                ,

                                       Temp_1 as (
                                                WITH group_data AS (
                                                                        SELECT
                                                                              REGroupCode_C12,
                                                                              COUNT(CASE WHEN DateDiff_AucPubl <= 199 THEN 1 END) AS non_milestone_count,
                                                                            --MIN(CASE WHEN DateDiff_AucPubl >= 199 THEN AuctionDate END) AS first_milestone_date
                                                                              MIN(CASE WHEN DateDiff_AucPubl >= 179 THEN AuctionDate END) AS first_milestone_date
                                                                        FROM
                                                                              TempData
                                                                        GROUP BY
                                                                              REGroupCode_C12
                                                                  )
                                                      SELECT
                                                            a.AuctionObJID,
                                                            a.REGroupCode_C12,
                                                            a.AuctionDate,
                                                            a.DateDiff_AucPubl,
                                                            CASE
                                                                  WHEN g.non_milestone_count = 0 THEN  ROW_NUMBER() OVER (PARTITION BY a.REGroupCode_C12 ORDER BY a.AuctionDate)
                                                                  ELSE
                                                                        CASE
                                                                        WHEN g.first_milestone_date IS NULL THEN 1
                                                                        WHEN a.AuctionDate < g.first_milestone_date THEN 1
                                                                        ELSE 2
                                                                        END
                                                            END AS New_auction
                                                      FROM
                                                            TempData a
                                                      JOIN
                                                            group_data g ON a.REGroupCode_C12 = g.REGroupCode_C12
                                                      ORDER BY
                                                            a.REGroupCode_C12,   a.AuctionDate ASC
                                                      ) ,

                                            Temp_2 AS (
                                                            select
                                                                        REGroupCode_C12,  Count(distinct KATASX_Left3_C3) as Count_KATASX_Left3_C3_Over_REGroupCode_C12
                                                            from
                                                                        TempData
                                                            Group by    REGroupCode_C12
                                                      )

                              select      distinct
                                          Temp_2.Count_KATASX_Left3_C3_Over_REGroupCode_C12
                                    ,     Concat(Temp_1.REGroupCode_C12, '_', Temp_1.New_auction) as FinalGroupCode
                                    ,     Temp_1.New_auction
                                    ,     TempData.*

                              from
                                          TempData

                              left join
                                          Temp_1
                              on
                                          TempData.AuctionObJID = Temp_1.AuctionObJID

                              left join
                                          Temp_2
                              on
                                          TempData.REGroupCode_C12 = Temp_2.REGroupCode_C12


                              ORDER BY TempData.REGroupCode_C12,  TempData.AuctionDate ASC
            )
--select distinct * from New_auction_Milestone
--where REGroupCode_C12 = '474646_2_128'
,


Create_MinMax    as  (              WITH Temp_Data  AS (
                                                            select  distinct
                                                                        *
                                                            from
                                                                        New_auction_Milestone

                                                         ) ,

                                           Sub_Data  AS (
                                                            select  distinct
                                                                        FinalGroupCode ,
                                                                        AuctionObJID  ,
                                                                        DateDIff_AucPubl,
                                                                        Result_blended  ,
                                                                        Count_ObJID_Over_cAuctionID,
                                                                        `FirstOfferPrice` ,
                                                                        `AuctionDate`
                                                            from
                                                                        Temp_Data

                                                         ) ,

                                          Sub_Data_a AS (
                                                            select
                                                                        ROW_NUMBER() OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate asc ) AS FinalGroupCode_RowNum  ,
                                                                        *
                                                            from
                                                                        New_auction_Milestone


                                                         )  ,

                                          Sub_Data_b AS (
                                                            select      distinct

                                                                        AuctionObJID ,
                                                                        FIRST_VALUE(TRY_CAST(`FirstOfferPrice` AS DECIMAL(18,4)))  OVER (PARTITION BY FinalGroupCode ORDER BY FinalGroupCode_RowNum ASC ) AS FinalGroupCode_MaxFirstOfferPrice,
                                                                        FIRST_VALUE(TRY_CAST(`FirstOfferPrice` AS DECIMAL(18,4)))  OVER (PARTITION BY FinalGroupCode ORDER BY FinalGroupCode_RowNum DESC) AS FinalGroupCode_MinFirstOfferPrice,

                                                                        --MAX( TRY_CAST(`FirstOfferPrice` AS DECIMAL(18,4)) ) OVER (PARTITION BY FinalGroupCode) AS FinalGroupCode_MaxFirstOfferPrice  ,
                                                                        --MIN( TRY_CAST(`FirstOfferPrice` AS DECIMAL(18,4)) ) OVER (PARTITION BY FinalGroupCode) AS FinalGroupCode_MinFirstOfferPrice ,

                                                                        MAX(`AuctionDate`) OVER (  PARTITION BY FinalGroupCode  ) AS FinalGroupCode_MaxAuctionDate   ,
                                                                        MIN(`AuctionDate`) OVER (  PARTITION BY FinalGroupCode  ) AS FinalGroupCode_MinAuctionDate
                                                            from
                                                                        Sub_Data_a
                                                         )   ,

                                          Sub_Data_c AS (
                                                            select
                                                                        REGroupCode_C12  ,
                                                                        Count(distinct FinalGroupCode) AS Count_FinalGroupCode_Over_REGroupCode_C12
                                                            from
                                                                        New_auction_Milestone
                                                            group by
                                                                        REGroupCode_C12


                                                         )


                              select      distinct
                                                Sub_Data_a.FinalGroupCode_RowNum


                                          ,     Sub_Data_b.FinalGroupCode_MaxFirstOfferPrice
                                          ,     Sub_Data_b.FinalGroupCode_MinFirstOfferPrice
                                          ,     Sub_Data_b.FinalGroupCode_MaxAuctionDate
                                          ,     Sub_Data_b.FinalGroupCode_MinAuctionDate

                                          ,     Temp_Data.*

                              from
                                          Sub_Data_a

                              left join
                                          Sub_Data_b
                              on
                                          Sub_Data_b.AuctionObJID = Sub_Data_a.AuctionObJID

                              left join
                                          Sub_Data_c
                              on
                                          Sub_Data_c.REGroupCode_C12 = Sub_Data_a.REGroupCode_C12


                              left join
                                          Temp_Data
                              on
                                          Temp_Data.AuctionObJID = Sub_Data_a.AuctionObJID
)


select  distinct
              FinalGroupCode_RowNum ,
              FinalGroupCode_MaxFirstOfferPrice ,
              FinalGroupCode_MinFirstOfferPrice ,
              FinalGroupCode_MaxAuctionDate ,
              FinalGroupCode_MinAuctionDate ,
              Count_KATASX_Left3_C3_Over_REGroupCode_C12 ,
              FinalGroupCode ,
              New_auction ,
              REGroupCode_C12 ,
              Filled_KATASX_Left3_C3 ,
              Count_AuctionObJID_Over_REGroupCode_C10 ,
              Count_OriginAuctionKey_Over_REGroupCode_C10 ,
              REGroupCode_C11 ,
              missing_KATASX ,
              Count_KATASX_C3_Over_REGroupCode_C10 ,
              Count_Initiator_Over_REGroupCode_C10 ,
              Row_Num_Initiator_final ,
              REGroupCode_C10 ,
              Row_Num_Servicer ,
              Level_of_Calc_REGroupCode ,
              REGroupCode_C9 ,
              c_auction_id_Flag ,
              REGroupCode_C8 ,
              REGroupCode_C7 ,
              REGroupCode_C6 ,
              DateDIff_AucPubl ,
              REGroupCode_C5 ,
              REGroupCode_C4 ,
              Count_KAEK_Over_DISTINCT_REGroupCode ,
              Count_REGroupCode_Over_DISTINCT_KAEK ,
              Count_ObJID_Over_KAEK ,
              Count_ObJID_Over_cAuctionID ,
              Servicer ,
              REGroupCode_C3 ,
              KAEK_C3 ,
              KATASX_C3 ,
              KATASX_Left3_C3 ,
              Row_Num_ObJID ,
              RealEstateGroupCode ,
              KAEK_OrnlText ,
              Katasx_OrnlText ,
              TRY_CAST(`FirstOfferPrice` AS DECIMAL(18,4)) as FirstOfferPrice ,
              FinalPrice ,
              OriginAuctionKey ,
              Result_blended ,
              AuctionDate ,
              PublishedDate ,
              AuctionObJID ,
              c_auction_id ,
              Source ,
              HastenerDescr ,
              Initiator_Cepal_others ,
              Initiator_final
from
      Create_MinMax"""

# COMMAND ----------

# DBTITLE 1,Step_2 -  AuctionRound_Query_step2 to PySpark DataFrame  /
df_step2 = spark.sql(AuctionRound_Query_step2)
df_step2.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{step2_table}")

# COMMAND ----------

# DBTITLE 1,Step_3 - AuctionRound_Query (split into sub-steps to avoid DEADLINE_EXCEEDED)
# The full CTE chain (RankedData → RankedData_8) is too complex for a single query plan.
# Split into 3 sub-queries with intermediate temp tables to keep each plan manageable.

_step3_temp_a = f"`{env_prefix}-de-sandbox`.a360._auction_round_step3_temp_a"
_step3_temp_b = f"`{env_prefix}-de-sandbox`.a360._auction_round_step3_temp_b"

# --- Sub-step 3a: Base data + RankedData_1 through RankedData_4 ---
AuctionRound_Query_step3a = f"""
WITH Create_MinMax AS (SELECT * FROM {step2_table}),

RankedData_Temp AS (SELECT * FROM Create_MinMax),

price_overlap AS (
    WITH TempData AS (
        SELECT
            FinalGroupCode_RowNum,
            FinalGroupCode AS price_overlapFinalGroupCode,
            AuctionObJID, AuctionDate, `FirstOfferPrice`,
            CASE
                WHEN LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) IS NULL THEN 0
                WHEN `FirstOfferPrice` <= LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) THEN 0
                ELSE 1
            END AS Price_Anomaly_Flag
        FROM Create_MinMax
    )
    SELECT DISTINCT price_overlapFinalGroupCode, Price_Anomaly_Flag
    FROM TempData WHERE Price_Anomaly_Flag = 1
),

ObJID_Over_FinalGroupCode AS (
    SELECT FinalGroupCode, COUNT(AuctionObJID) AS Count_ObJID_Over_FinalGroupCode
    FROM Create_MinMax GROUP BY FinalGroupCode
),

RankedData AS (
    SELECT
        CASE WHEN c.Price_Anomaly_Flag = 1 THEN 1 ELSE 0 END AS Price_Anomaly_Flag,
        a.*, b.Count_ObJID_Over_FinalGroupCode
    FROM RankedData_Temp a
    LEFT JOIN ObJID_Over_FinalGroupCode b ON a.FinalGroupCode = b.FinalGroupCode
    LEFT JOIN price_overlap c ON a.FinalGroupCode = c.price_overlapFinalGroupCode
),

RankedData_1 AS (
    WITH cte AS (
        SELECT
            Price_Anomaly_Flag, FinalGroupCode, FinalGroupCode_RowNum, Count_ObJID_Over_FinalGroupCode,
            AuctionObJID, DateDIff_AucPubl, Result_blended, `FirstOfferPrice`, KATASX_Left3_C3,
            AuctionDate, FinalGroupCode_MaxAuctionDate, FinalGroupCode_MinAuctionDate,
            FinalGroupCode_MinFirstOfferPrice, FinalGroupCode_MaxFirstOfferPrice,
            CASE
                WHEN Count_ObJID_Over_FinalGroupCode = 1 THEN 1
                WHEN LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) NOT IN ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                    AND FirstOfferPrice BETWEEN 0.78 * FinalGroupCode_MaxFirstOfferPrice AND 0.82 * FinalGroupCode_MaxFirstOfferPrice
                    AND Price_Anomaly_Flag = 0 THEN 3
                WHEN LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) NOT IN ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                    AND `FirstOfferPrice` BETWEEN 0.63 * FinalGroupCode_MaxFirstOfferPrice AND 0.67 * FinalGroupCode_MaxFirstOfferPrice
                    AND Price_Anomaly_Flag = 0 THEN 4
                WHEN `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) * 0.8125 THEN 5
                WHEN `FirstOfferPrice` < FinalGroupCode_MaxFirstOfferPrice * 0.6
                    AND LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) NOT IN ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος') THEN 5
                ELSE NULL
            END AS raw_ar
        FROM RankedData
    )
    SELECT
        Price_Anomaly_Flag, FinalGroupCode, FinalGroupCode_RowNum, Count_ObJID_Over_FinalGroupCode,
        AuctionObJID, DateDIff_AucPubl, Result_blended, `FirstOfferPrice`, KATASX_Left3_C3,
        AuctionDate, FinalGroupCode_MaxAuctionDate, FinalGroupCode_MinAuctionDate,
        FinalGroupCode_MinFirstOfferPrice, FinalGroupCode_MaxFirstOfferPrice,
        CASE
            WHEN SUM(CASE WHEN raw_ar IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY FinalGroupCode) = Count_ObJID_Over_FinalGroupCode
                AND FinalGroupCode_RowNum = 1 THEN 1
            WHEN raw_ar = 3 THEN 3
            WHEN raw_ar = 4 THEN
                CASE WHEN SUM(CASE WHEN raw_ar = 4 THEN 1 ELSE 0 END) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) = 1 THEN 4 ELSE NULL END
            WHEN raw_ar = 5 AND raw_ar NOT IN (3,4) THEN 5
            ELSE raw_ar
        END AS `ar_index__1`
    FROM cte
)

SELECT * FROM RankedData_1
"""

spark.sql(AuctionRound_Query_step3a).write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(_step3_temp_a)
print("Step 3a complete: RankedData_1 materialized")

# COMMAND ----------

# DBTITLE 1,Step_3b - RankedData_2 through RankedData_4
# Read from the materialized temp_a and continue the CTE chain
query_3b = f"""
WITH RankedData_1 AS (SELECT * FROM {_step3_temp_a}),

RankedData_2 AS (
    SELECT Price_Anomaly_Flag,
      FinalGroupCode, FinalGroupCode_RowNum, Count_ObJID_Over_FinalGroupCode, AuctionObJID, KATASX_Left3_C3,
      FinalGroupCode_MinAuctionDate, FinalGroupCode_MaxAuctionDate,
      FinalGroupCode_MinFirstOfferPrice, FinalGroupCode_MaxFirstOfferPrice,
      DateDIff_AucPubl, Result_blended, `FirstOfferPrice`, AuctionDate,
      `ar_index__1`,

      CASE
          WHEN `ar_index__1` IS NULL
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
          THEN 1

          WHEN `ar_index__1` IS NULL
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
          THEN 1

          WHEN `ar_index__1` IS NULL
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and `FirstOfferPrice` > LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `AuctionDate` = FinalGroupCode_MaxAuctionDate
            and Count_ObJID_Over_FinalGroupCode = 2
          THEN 1

          WHEN `ar_index__1` IS NULL
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` > LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
          then 1

          WHEN `ar_index__1` IS NULL
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) != 3
          THEN 2

          WHEN `ar_index__1` IS NULL
            and `AuctionDate` = FinalGroupCode_MinAuctionDate
            and Lead(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
          then 2

          WHEN `ar_index__1` IS NULL
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
          then 2

          WHEN `ar_index__1` IS NULL
            and `FirstOfferPrice` > Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) != 3
          then 2

          WHEN `ar_index__1` IS NULL
            and `FirstOfferPrice` > Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) is null
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 2

          WHEN `ar_index__1` IS NULL
            and `FirstOfferPrice` > Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` < LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) is null
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY AuctionDate) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 2

          WHEN `ar_index__1` IS NULL
            and `FirstOfferPrice` > Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) is null
          then 2

          WHEN `ar_index__1` IS NULL
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
          then 3

          WHEN `ar_index__1` IS NULL
            and Lead(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 4
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` > Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
          then 3

          WHEN `ar_index__1` is null
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 4
          then 4

          WHEN `ar_index__1` IS NULL
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
            and Lead(`ar_index__1`,2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) != 4
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` < LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
          then 4

          WHEN `ar_index__1` is null
            and `AuctionDate` < Lead(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` > Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 5)
          then 4

          WHEN `ar_index__1` is null
            and `FirstOfferPrice` > Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `AuctionDate` < Lead(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 5 )
          then 4

          WHEN `ar_index__1` is null
            and `FirstOfferPrice` < LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `AuctionDate` > LAG(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 3 )
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 4

          WHEN `ar_index__1` is null
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 5 )
          then 5

          WHEN `ar_index__1` is null
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 4 )
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 5

          WHEN `ar_index__1` is null
            and `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `AuctionDate` > LAG(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and LAG(`ar_index__1`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 4 )
            and LAG(`ar_index__1`,2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 3 )
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 5

          else `ar_index__1`
      END AS `ar_index__2`
    FROM RankedData_1
),

RankedData_3 AS (
    SELECT Price_Anomaly_Flag,
      FinalGroupCode, FinalGroupCode_RowNum, Count_ObJID_Over_FinalGroupCode, AuctionObJID, KATASX_Left3_C3,
      FinalGroupCode_MinAuctionDate, FinalGroupCode_MaxAuctionDate,
      FinalGroupCode_MinFirstOfferPrice, FinalGroupCode_MaxFirstOfferPrice,
      DateDIff_AucPubl, Result_blended, `FirstOfferPrice`, AuctionDate,
      `ar_index__2`, `ar_index__1`,

      CASE
          WHEN `ar_index__2` IS NULL
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` = FinalGroupCode_MinAuctionDate
            and Lead(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
          THEN 1

          WHEN `ar_index__2` IS NULL
            and LAG( Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` > LAG(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and LAG(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
          THEN 1

          WHEN `ar_index__2` IS NULL
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and LAG( `ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) IS not NULL
          then 1

          WHEN `ar_index__2` IS NULL
            and LAG(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` > LAG(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and LAG(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
          then 1

          WHEN `ar_index__2` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` = FinalGroupCode_MinAuctionDate
            and Lead(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
          THEN 2

          WHEN `ar_index__2` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and LAG(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
          then 2

          WHEN `ar_index__2` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
          THEN 2

          WHEN `ar_index__2` IS NULL
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
          THEN 2

          WHEN `ar_index__2` IS NULL
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          THEN 2

          WHEN `ar_index__2` IS NULL
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lag(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
          THEN 2

          WHEN `ar_index__2` IS NULL
            and `FirstOfferPrice` > Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` < Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lag(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and Lag(`ar_index__2`, 2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) != 3
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 2

          WHEN `ar_index__2` IS NULL
            and `FirstOfferPrice` > Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lag(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and Lag(`ar_index__2`,2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 2

          WHEN `ar_index__2` IS NULL
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `Result_blended` in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `Price_Anomaly_Flag` = 0
            and Lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and Lead(`ar_index__2`,2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
          then 2

          WHEN `ar_index__2` is null
            and `AuctionDate` < Lead(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` > Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `Result_blended` not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ( 4 )
            and Lead(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 4)
          then 3

          WHEN `ar_index__2` is null
            and `AuctionDate` < Lead(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `Result_blended` not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 4)
          then 3

          WHEN `ar_index__2` is null
            and `AuctionDate` > Lag(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lag(`Result_blended`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 3 )
          then 3

          WHEN `ar_index__2` is null
            and `AuctionDate` > Lag(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice`> Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 3 )
          then 3

          WHEN `ar_index__2` is null
            and `AuctionDate` < Lead(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice`> Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 4 )
          then 3

          WHEN `ar_index__2` is null
            and `AuctionDate` > Lag(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` < Lag( `FirstOfferPrice` ) OVER (PARTITION BY FinalGroupCode ORDER BY 1  )
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 3)
            and Lead(`ar_index__2`,2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) != 4
          then 4

          WHEN `ar_index__2` is null
            and `AuctionDate` > Lag(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lag( `FirstOfferPrice` ) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`   )
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(`ar_index__2`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 4)
          then 4

          WHEN SUM( CASE WHEN `ar_index__2` = 5 THEN 1 ELSE 0 END )
              OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) > 0
          then 5

          else `ar_index__2`
      END AS `ar_index__3`
    FROM RankedData_2
),

RankedData_4 AS (
    SELECT Price_Anomaly_Flag,
      FinalGroupCode, FinalGroupCode_RowNum, Count_ObJID_Over_FinalGroupCode, AuctionObJID, KATASX_Left3_C3,
      FinalGroupCode_MinAuctionDate, FinalGroupCode_MaxAuctionDate,
      FinalGroupCode_MinFirstOfferPrice, FinalGroupCode_MaxFirstOfferPrice,
      DateDIff_AucPubl, Result_blended, `FirstOfferPrice`, AuctionDate,
      `ar_index__3`, `ar_index__2`, `ar_index__1`,

      CASE
          WHEN `ar_index__3` IS NULL
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
          THEN 1

          WHEN `ar_index__3` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` = FinalGroupCode_MinAuctionDate
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
          THEN 1

          WHEN `ar_index__3` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` = FinalGroupCode_MinAuctionDate
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
          THEN 1

          WHEN `ar_index__3` IS NULL
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` = FinalGroupCode_MinAuctionDate
            and `FirstOfferPrice` <> Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and Lead(`ar_index__3`,2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
          THEN 1

          WHEN `ar_index__3` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lag(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and Lag(`ar_index__3`,2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          THEN 1

          WHEN `ar_index__3` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and lead(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and lead(`ar_index__3`,2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          THEN 1

          WHEN `ar_index__3` IS NULL
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and LAG(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and `AuctionDate` = FinalGroupCode_MaxAuctionDate
          then 2

          WHEN `ar_index__3` IS NULL
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` > FinalGroupCode_MinAuctionDate
          THEN 2

          WHEN `ar_index__3` IS NULL
            and `FirstOfferPrice` > Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` > Lag(`FirstOfferPrice`,2 ) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lag(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and Lag(`ar_index__3`, 2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) != 3
            and Lag(`ar_index__3`, 3) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) != 3
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` = FinalGroupCode_MaxAuctionDate
          then 2

          WHEN `ar_index__3` IS NULL
            and Lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` > Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
            and Lag(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) != 3
          THEN 2

          WHEN `ar_index__3` IS NULL
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lag(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and Lag(`ar_index__3`,2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) != 3
          THEN 2

          WHEN `ar_index__3` is null
            and `AuctionDate` < Lead(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` > Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 3 )
            and `AuctionDate` = FinalGroupCode_MinAuctionDate
          then 2

          WHEN `ar_index__3` IS NULL
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 2

          WHEN `ar_index__3` is null
            and `AuctionDate` < Lead(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 3 )
          then 3

          WHEN `ar_index__3` is null
            and `AuctionDate` < Lead(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 3 )
          then 3

          WHEN `ar_index__3` is null
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(`ar_index__3`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 3 )
          then 3

          else `ar_index__3`
      END AS `ar_index__4`
    FROM RankedData_3
)

SELECT * FROM RankedData_4
"""

spark.sql(query_3b).write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(_step3_temp_b)
print("Step 3b complete: RankedData_4 materialized")

# COMMAND ----------

# DBTITLE 1,Step_3c - RankedData_5 through RankedData_8 (final)
# Read from temp_b and finish
query_3c = f"""
WITH RankedData_4 AS (SELECT * FROM {_step3_temp_b}),

RankedData_5 AS (
    SELECT Price_Anomaly_Flag,
      FinalGroupCode, FinalGroupCode_RowNum, Count_ObJID_Over_FinalGroupCode, AuctionObJID, KATASX_Left3_C3,
      FinalGroupCode_MinAuctionDate, FinalGroupCode_MaxAuctionDate,
      FinalGroupCode_MinFirstOfferPrice, FinalGroupCode_MaxFirstOfferPrice,
      DateDIff_AucPubl, Result_blended, `FirstOfferPrice`, AuctionDate,
      `ar_index__4`,`ar_index__3`,`ar_index__2`,`ar_index__1`,

      CASE
          WHEN `ar_index__4` IS NULL
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__4`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and `AuctionDate` = FinalGroupCode_MinAuctionDate
          THEN 1

          WHEN `ar_index__4` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` = FinalGroupCode_MinAuctionDate
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lead(`ar_index__4`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
          THEN 1

          WHEN `ar_index__4` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and lead(`ar_index__4`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          THEN 1

          WHEN `ar_index__4` IS NULL
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Lag(`ar_index__4`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and Lag(`ar_index__4`,2) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) != 3
          THEN 2

          WHEN `ar_index__4` IS NULL
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` > FinalGroupCode_MinAuctionDate
            and `AuctionDate` < FinalGroupCode_MaxAuctionDate
            and Lead(`ar_index__4`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
            and Lag(`ar_index__4`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
          THEN 3

          WHEN `ar_index__4` is null
            and `AuctionDate` < Lead(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(`ar_index__4`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 3 )
            and Lead(`ar_index__4`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 4 )
          then 3

          WHEN `ar_index__4` is null
            and `AuctionDate` > Lag(`AuctionDate`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(`ar_index__4`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ( 4 )
          then 5

          else `ar_index__4`
      END AS `ar_index__5`
    FROM RankedData_4
),

RankedData_6 AS (
    SELECT Price_Anomaly_Flag,
      FinalGroupCode, FinalGroupCode_RowNum, Count_ObJID_Over_FinalGroupCode, AuctionObJID, KATASX_Left3_C3,
      FinalGroupCode_MinAuctionDate, FinalGroupCode_MaxAuctionDate,
      FinalGroupCode_MinFirstOfferPrice, FinalGroupCode_MaxFirstOfferPrice,
      DateDIff_AucPubl, Result_blended, `FirstOfferPrice`, AuctionDate,
      `ar_index__5`, `ar_index__4`,`ar_index__3`,`ar_index__2`,`ar_index__1`,

      CASE
          WHEN `ar_index__5` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and LAG(`ar_index__4`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and LEAD(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          THEN 1

          WHEN `ar_index__5` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and lead(`ar_index__5`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          THEN 1

          WHEN `ar_index__5` is null
            AND Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND LAG(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND LAG(`ar_index__5`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 1
          THEN 2

          WHEN `ar_index__5` is null
            AND Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND FirstOfferPrice BETWEEN 0.72 * FinalGroupCode_MaxFirstOfferPrice AND 0.88 * FinalGroupCode_MaxFirstOfferPrice
            AND Lead(`ar_index__5`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) < 4
          THEN 3

          WHEN `ar_index__5` is null
            AND LAG(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND LAG(`ar_index__5`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 2
            AND FirstOfferPrice BETWEEN 0.72 * FinalGroupCode_MaxFirstOfferPrice AND 0.88 * FinalGroupCode_MaxFirstOfferPrice
          THEN 3

          WHEN `ar_index__5` is null
            AND LAG(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND LAG(Result_blended, 2) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND FirstOfferPrice BETWEEN 0.64 * FinalGroupCode_MaxFirstOfferPrice AND 0.88 * FinalGroupCode_MaxFirstOfferPrice
            AND LAG(`ar_index__5`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 2
            AND LAG(`ar_index__5`, 2) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 1
          THEN 3

          WHEN `ar_index__5` is null
            and `FirstOfferPrice` >= Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            AND LAG(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND Lead(`ar_index__5`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 4
          THEN 3

          WHEN `ar_index__5` is null
            AND Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND Lead(`ar_index__5`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 3
            AND Lead(`ar_index__5`,2) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 4
            AND Lead(`ar_index__5`,3) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 5
          THEN 3

          WHEN `ar_index__5` is null
            AND Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND LAG(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND LAG(Result_blended,2) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND FirstOfferPrice BETWEEN 0.60 * FinalGroupCode_MaxFirstOfferPrice AND 0.88 * FinalGroupCode_MaxFirstOfferPrice
            AND LAG(`ar_index__5`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 2
            AND LAG(`ar_index__5`,2) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 1
          THEN 3

          WHEN `ar_index__5` is null
            AND LAG(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND FirstOfferPrice BETWEEN 0.62 * FinalGroupCode_MaxFirstOfferPrice AND 0.69 * FinalGroupCode_MaxFirstOfferPrice
            AND LAG(`ar_index__5`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) is null
            AND LAG(`ar_index__5`,2) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) != 5
          THEN 4

          WHEN `ar_index__5` is null
            AND LAG(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND FirstOfferPrice BETWEEN 0.62 * FinalGroupCode_MaxFirstOfferPrice AND 0.69 * FinalGroupCode_MaxFirstOfferPrice
            AND LAG(`ar_index__5`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 3
          THEN 4

          WHEN `ar_index__5` is null
            AND LAG(`ar_index__5`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 5
            AND Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Price_Anomaly_Flag = 0
          THEN 5

          else `ar_index__5`
      END AS `ar_index__6`
    FROM RankedData_5
),

RankedData_7 AS (
    SELECT Price_Anomaly_Flag,
      FinalGroupCode, FinalGroupCode_RowNum, Count_ObJID_Over_FinalGroupCode, AuctionObJID, KATASX_Left3_C3,
      FinalGroupCode_MinAuctionDate, FinalGroupCode_MaxAuctionDate,
      FinalGroupCode_MinFirstOfferPrice, FinalGroupCode_MaxFirstOfferPrice,
      DateDIff_AucPubl, Result_blended, `FirstOfferPrice`, AuctionDate,
      `ar_index__6`, `ar_index__5`, `ar_index__4`,`ar_index__3`,`ar_index__2`,`ar_index__1`,

      CASE
          WHEN `ar_index__6` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and LAG(`ar_index__5`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and LEAD(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          THEN 1

          WHEN `ar_index__6` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and lead(`ar_index__6`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          THEN 1

          WHEN `ar_index__6` is null
            AND Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND LAG(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND LAG(`ar_index__6`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) is null
            AND Lead(`ar_index__6`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 3
          THEN 2

          WHEN `ar_index__6` IS NULL
            and Lead(`ar_index__6`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 3
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 2

          WHEN `ar_index__6` IS NULL
            and Lag(`ar_index__6`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and Lead(`ar_index__6`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 4
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Price_Anomaly_Flag = 1
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 3

          WHEN `ar_index__6` IS NULL
            and Lead(`ar_index__6`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 4
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 4

          WHEN `ar_index__6` IS NULL
            and Lead(`ar_index__6`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 4
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 4

          WHEN `ar_index__6` IS NULL
            and Lead(`ar_index__6`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 4
            and Lag(`ar_index__6`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 4
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Price_Anomaly_Flag = 1
          then 4

          else `ar_index__6`
      END AS `ar_index__7`
    FROM RankedData_6
),

RankedData_8 AS (
    SELECT Price_Anomaly_Flag,
      FinalGroupCode, FinalGroupCode_RowNum, Count_ObJID_Over_FinalGroupCode, AuctionObJID, KATASX_Left3_C3,
      FinalGroupCode_MinAuctionDate, FinalGroupCode_MaxAuctionDate,
      FinalGroupCode_MinFirstOfferPrice, FinalGroupCode_MaxFirstOfferPrice,
      DateDIff_AucPubl, Result_blended, `FirstOfferPrice`, AuctionDate,
      `ar_index__7`, `ar_index__6`, `ar_index__5`, `ar_index__4`,`ar_index__3`,`ar_index__2`,`ar_index__1`,

      CASE
          WHEN `ar_index__7` is null
            and AuctionDate = FinalGroupCode_MinAuctionDate
            AND Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND Lead(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND Lead(`ar_index__7`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 2
          THEN 1

          WHEN `ar_index__7` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and LAG(`ar_index__6`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and LEAD(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          THEN 1

          WHEN `ar_index__7` IS NULL
            and Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` = FinalGroupCode_MinAuctionDate
            and lead(`ar_index__7`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 1
            and lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          THEN 1

          WHEN `ar_index__7` IS NULL
            and Price_Anomaly_Flag = 0
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and `AuctionDate` != FinalGroupCode_MinAuctionDate
            and lead(`ar_index__7`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            and lead(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          THEN 1

          WHEN `ar_index__7` is null
            AND Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND Lead(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            AND Lead(`ar_index__7`) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) = 2
          THEN 2

          WHEN `ar_index__7` IS NULL
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            AND Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and lead(`ar_index__7`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 2
            AND Lead(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 2

          WHEN `ar_index__7` IS NULL
            and `FirstOfferPrice` > Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            AND Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and lead(`ar_index__7`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 4
            AND Lead(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 3

          WHEN `ar_index__7` IS NULL
            AND Result_blended in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and lead(`ar_index__7`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 4
            AND Lead(Result_blended) OVER ( PARTITION BY FinalGroupCode ORDER BY AuctionDate ) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
          then 4

          WHEN `ar_index__7` = 4
            and Lead(`ar_index__7`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 4
            and Lag(`ar_index__7`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) = 4
            and Result_blended not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Lag(Result_blended) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
            and Price_Anomaly_Flag = 1
            and `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
            and `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY FinalGroupCode ORDER BY `AuctionDate`)
          then 5

          else `ar_index__7`
      END AS `ar_index__8`
    FROM RankedData_7
)

SELECT
    `ar_index__8`, `ar_index__7`, `ar_index__6`, `ar_index__5`,
    `ar_index__4`, `ar_index__3`, `ar_index__2`, `ar_index__1`,
    Price_Anomaly_Flag, FinalGroupCode, FinalGroupCode_RowNum,
    Count_ObJID_Over_FinalGroupCode, AuctionObJID, KATASX_Left3_C3,
    FinalGroupCode_MinAuctionDate, FinalGroupCode_MaxAuctionDate,
    FinalGroupCode_MinFirstOfferPrice, FinalGroupCode_MaxFirstOfferPrice,
    DateDIff_AucPubl, Result_blended, `FirstOfferPrice`, AuctionDate
FROM RankedData_8
"""

spark.sql(query_3c).write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(step3_table)
print("Step 3c complete: final step3 table written")

# Cleanup temp tables
spark.sql(f"DROP TABLE IF EXISTS {_step3_temp_a}")
spark.sql(f"DROP TABLE IF EXISTS {_step3_temp_b}")
print("Step 3 temp tables cleaned up")

# COMMAND ----------

# DBTITLE 1,Step_4 -  AuctionRound_Query_step4
AuctionRound_Query_step4 = f"""
with Create_step4_temp as  (
                              select
                                    *
                              from
                                   {step3_table}
)
,
--///////////////////////////////////
RankedData_9 as (
                  with Temp_cte as
                                 -- 1) Κρατάμε μόνο τις στήλες που θέλουμε (και τυχόν DISTINCT αν απαιτείται):
                                     (
                                          SELECT
                                                distinct
                                                ar_index__8,
                                                ar_index__7,
                                                ar_index__6,
                                                ar_index__5,
                                                ar_index__4,
                                                ar_index__3,
                                                ar_index__2,
                                                ar_index__1,
                                                Price_Anomaly_Flag,
                                                FinalGroupCode_RowNum,
                                                FinalGroupCode_MaxFirstOfferPrice,
                                                FinalGroupCode,
                                                Result_blended,
                                                `AuctionDate` ,
                                                `FirstOfferPrice` ,
                                                AuctionObJID as AuctionObJID_Temp
                                          from
                                                Create_step4_temp
                                          order by
                                                FinalGroupCode,
                                                FinalGroupCode_RowNum
                                    )
                                    ,
                           -- 2) Ελέγχουμε για κάθε FinalGroupCode αν ΟΛΕΣ οι null-ar_index__8 γραμμές είναι 'Ανεσταλμένες'
                  Null_ar_check AS (
                                          SELECT
                                                FinalGroupCode,
                                                CASE  WHEN COUNT(*) = SUM( CASE WHEN Result_blended = 'eAuction - Ανεσταλμένος' THEN 1 ELSE 0 END ) THEN 1 ELSE 0 END AS CheckResult
                                          FROM
                                                Temp_cte
                                          WHERE
                                                ar_index__8 IS NULL
                                          GROUP BY
                                                FinalGroupCode
                                    ),

                                    -- 3) Ενώνουμε τα αποτελέσματα για κάθε γραμμή
            Temp_RankedData_9 AS (
                                          SELECT
                                                t.*,
                                                COALESCE(n.CheckResult, 0) AS NullSuspendedCheck  -- αν δεν υπάρχει καθόλου NULL ar_index__8 για το FG, θα επιστρέψει NULL, άρα το κάνουμε 0
                                          FROM
                                                Temp_cte t
                                          LEFT JOIN
                                                Null_ar_check n
                                          ON
                                                t.FinalGroupCode = n.FinalGroupCode
                                    )


                                    -- 4) Παράγουμε την τελική λογική
                                          SELECT
                                                CASE
                                                      WHEN `ar_index__8` IS NULL  AND NullSuspendedCheck = 1  THEN 1
                                                      WHEN `ar_index__8` IS NULL  AND NullSuspendedCheck = 2  THEN 2
                                                      WHEN `ar_index__8` IS NULL  AND NullSuspendedCheck = 3  THEN 3
                                                      WHEN `ar_index__8` IS NULL  AND NullSuspendedCheck = 4  THEN 4
                                                      WHEN `ar_index__8` IS NULL  AND NullSuspendedCheck = 5  THEN 5

                                                      --Round  3
                                                      WHEN
                                                                    Price_Anomaly_Flag = 0
                                                               AND   `ar_index__8` IS NULL
                                                               and  FirstOfferPrice BETWEEN 0.70 * FinalGroupCode_MaxFirstOfferPrice  AND 0.93 * FinalGroupCode_MaxFirstOfferPrice
                                                      THEN    3

                                                      --Round  4
                                                      WHEN
                                                                    Price_Anomaly_Flag = 0
                                                               AND   `ar_index__8` IS NULL
                                                               and  FirstOfferPrice BETWEEN 0.60 * FinalGroupCode_MaxFirstOfferPrice  AND 0.69999 * FinalGroupCode_MaxFirstOfferPrice
                                                      THEN    4

                                                      --Round  5
                                                      WHEN
                                                                     Price_Anomaly_Flag = 0
                                                               AND   `ar_index__8` IS NULL
                                                               and   FirstOfferPrice <  0.59 * FinalGroupCode_MaxFirstOfferPrice
                                                      THEN    5

                                                      --Round  5
                                                      WHEN
                                                                  `ar_index__8` IS NULL
                                                            AND   Price_Anomaly_Flag = 0
                                                            AND   LAG(`ar_index__8`) OVER (  PARTITION BY FinalGroupCode   ORDER BY AuctionDate ) = 5
                                                      THEN
                                                          -- Χρησιμοποιούμε LAST_VALUE() με IGNORE NULLS για να τραβήξουμε  την πιο πρόσφατη μη-null τιμή του ar_index__11 ώς τώρα
                                                            last_VALUE(`ar_index__8`, TRUE) OVER ( PARTITION BY FinalGroupCode  ORDER BY FinalGroupCode_RowNum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )


                                                      else `ar_index__8`

                                                  END AS `ar_index__9`,
                                                *
                                          FROM
                                                Temp_RankedData_9
                                          ORDER BY
                                                FinalGroupCode,
                                                FinalGroupCode_RowNum
)
,

--///////////////////////////////////
RankedData_10  as
                              (
                                    SELECT
----------------------------------------------------------------------------
--Round  1
                                    CASE
                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Result_blended    in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 0
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 1
                                          THEN 1


                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   `FirstOfferPrice` <  LAG(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 1
                                          THEN 1

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 1
                                                and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   `FirstOfferPrice` =  Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                          THEN 1

----------------------------------------------------------------------------
--Round  2

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   `FirstOfferPrice` > LAG(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 1
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 2
                                          THEN 2

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   `FirstOfferPrice` > LAG(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 1
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 2
                                          THEN 2

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 0
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 2
                                          THEN 2

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 2
                                          THEN 2

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Result_blended not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 0
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 2
                                          THEN 2


                                          WHEN
                                                      `ar_index__9` = 1
                                                and   `FirstOfferPrice` >  Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  3
                                          THEN 2


                                          WHEN
                                                     `ar_index__9` IS NULL
                                                and   `FirstOfferPrice` =  Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   `FirstOfferPrice` =  Lag(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lag(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  1
                                                and   Lag(`Result_blended`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   `Result_blended` not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                          THEN 2

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   `FirstOfferPrice` =  Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   `FirstOfferPrice` =  Lag(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 0
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 1
                                          THEN 2


                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   `FirstOfferPrice` >  Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   `FirstOfferPrice` =  Lag(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Result_blended     in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                --and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 3
                                          THEN 2


                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   Result_blended    not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                          THEN 2

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   IS NULL
                                                and   Lead(`ar_index__9`,2) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                          THEN 2

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   Result_blended not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) >  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                                and   Lead(`ar_index__9`,3) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                          THEN 2

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   Result_blended    in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Lag(`Result_blended`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   (`FirstOfferPrice` ) =  Lag(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lag(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 1
                                          THEN 2

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   Result_blended    in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Lag(`Result_blended`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   (`FirstOfferPrice` ) =  Lag(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lag(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                          THEN 2


----------------------------------------------------------------------------
--Round  3
                                          WHEN
                                                      `ar_index__9` =  2
                                                and   LAG(`ar_index__9`) OVER ( PARTITION BY FinalGroupCode  ORDER BY AuctionDate ) = 2
                                                and   Lead(`ar_index__9`) OVER ( PARTITION BY FinalGroupCode  ORDER BY AuctionDate ) = 3
                                                and   FirstOfferPrice BETWEEN 0.70 * FinalGroupCode_MaxFirstOfferPrice  AND 0.90 * FinalGroupCode_MaxFirstOfferPrice
                                                and   Price_Anomaly_Flag = 0
                                          THEN 3


                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) > (`FirstOfferPrice` )
                                                and   Price_Anomaly_Flag = 1
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 2
                                          THEN 3

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) < Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 4
                                          THEN 3


                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) < Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                                and   Lead(`ar_index__9`,2) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                          THEN 3

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   `FirstOfferPrice` =  Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  3
                                          THEN 3


                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   `FirstOfferPrice` BETWEEN 0.70 * LAG(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                        AND 0.93 * LAG(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                --and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  3
                                          THEN 3

                                          WHEN  -- einai ekseresi kai to ebala 26/03/2025
                                                      `ar_index__9` IS NULL
                                                and   (`FirstOfferPrice` ) =   Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   (`FirstOfferPrice` ) =   LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 4
                                          THEN 3

----------------------------------------------------------------------------
--Round  4
                                          WHEN
                                                      `ar_index__9` = 3
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 4
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 4
                                          THEN 4

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) <  LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                          THEN 4



                                          WHEN
                                                      `ar_index__9` is  NULL
                                                and   (`FirstOfferPrice` ) >   Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   (`FirstOfferPrice` ) =   LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 5
                                          THEN 4





                                          WHEN
                                                      `ar_index__9` = 3
                                                and   Price_Anomaly_Flag = 0
                                                and   (`FirstOfferPrice` ) >  lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   (`FirstOfferPrice` ) = LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Lead(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 5
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                                and   Result_blended not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   LAG(Result_blended)  OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')

                                          THEN 4 ----4444


----------------------------------------------------------------------------
--Round  5
                                          WHEN
                                                      `ar_index__9` is  NULL
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 4
                                          THEN 5

                                          WHEN
                                                      `ar_index__9` = 4
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 5
                                          THEN 5

                                          WHEN
                                                      `ar_index__9` is  NULL
                                                and   Lag(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 1
                                                and   LAG(`ar_index__9`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 4
                                          THEN 5

                                          WHEN
                                                      `ar_index__9` IS NULL
                                                AND   Price_Anomaly_Flag = 0
                                                AND   LAG(`ar_index__9`, 2) OVER (  PARTITION BY FinalGroupCode   ORDER BY AuctionDate ) = 5
                                          THEN
                                              -- Χρησιμοποιούμε LAST_VALUE() με IGNORE NULLS για να τραβήξουμε  την πιο πρόσφατη μη-null τιμή του ar_index__11 ώς τώρα
                                              last_VALUE(`ar_index__9`, TRUE) OVER ( PARTITION BY FinalGroupCode  ORDER BY FinalGroupCode_RowNum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
----------------------------------------------------------------------------
                                            else `ar_index__9`
                                        END AS `ar_index__10` ,
                                        *
                                    FROM   RankedData_9
)
,

--///////////////////////////////////
RankedData_11  as
                              (
                                    SELECT

----------------------------------------------------------------------------
--Round  1
                                    CASE

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   IS NULL
                                                and   Lead(`ar_index__10`,2) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                          THEN 1

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 1
                                                and   Lead(`ar_index__10`,2) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 1
                                          THEN 1
----------------------------------------------------------------------------
--Round  2

                                          WHEN
                                                      `ar_index__10` = 1
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) > Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                                and   Lead(`ar_index__10`,2) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                          THEN 2

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Lag(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) = Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   (`FirstOfferPrice` ) = Lag(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                          THEN 2


                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Result_blended not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) > Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                          THEN 2

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Price_Anomaly_Flag = 0
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   `FirstOfferPrice` = LAG(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   LAG(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) = 1
                                          THEN 2

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   `FirstOfferPrice` = Lag(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  3
                                          THEN 2

                                          WHEN
                                                     `ar_index__10` IS NULL
                                                and   `FirstOfferPrice` =  Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   `FirstOfferPrice` =  Lag(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lag(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  2
                                                and   Lag(`Result_blended`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                --and   `Result_blended` not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                          THEN 2


                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                          THEN 2

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Result_blended    not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                          THEN 2

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Result_blended    not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) >  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                          THEN 2


                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) >  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                          THEN 2

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Result_blended not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   LAG(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                          THEN 2
----------------------------------------------------------------------------
--Round  3

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) < LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   LAG(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                          THEN 3


                                          WHEN
                                                      `ar_index__10` = 2
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) < LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                          THEN 3


                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   LAG(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                          THEN 3

                                          WHEN
                                                      `ar_index__10` = 2
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) =  LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   LAG(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                          THEN 3

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) <  LAG(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                          THEN 3



                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   `FirstOfferPrice` < Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  3
                                                and   Lead(`ar_index__10`,2) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  3
                                          THEN 3

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   `Result_blended` not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  3
                                          THEN 3

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   `Result_blended` not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   `FirstOfferPrice` < Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  3
                                          THEN 3

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   `Result_blended`    in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  3
                                          THEN 3


                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   `Result_blended`  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  3
                                          THEN 3

----------------------------------------------------------------------------
--Round  4

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Result_blended not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   (`FirstOfferPrice` ) >  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 4
                                          THEN 4

                                          WHEN
                                                      `ar_index__10` is  NULL
                                                and   (`FirstOfferPrice` ) =   Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 4
                                          THEN 4

                                          WHEN
                                                      `ar_index__10` is  NULL
                                                and   (`FirstOfferPrice` ) >   Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Result_blended    in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 0
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 5
                                          THEN 4

                                          WHEN
                                                      `ar_index__10` is  NULL
                                                and   (`FirstOfferPrice` ) >   Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                and   Price_Anomaly_Flag = 1
                                                and   Lead(`ar_index__10`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 5
                                          THEN 4
----------------------------------------------------------------------------
--Round  5

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                AND   Price_Anomaly_Flag = 1
                                                AND   LAG(`ar_index__10`) OVER (  PARTITION BY FinalGroupCode   ORDER BY AuctionDate ) = 5
                                          THEN
                                              -- Χρησιμοποιούμε LAST_VALUE() με IGNORE NULLS για να τραβήξουμε  την πιο πρόσφατη μη-null τιμή του ar_index__11 ώς τώρα
                                              last_VALUE(`ar_index__10`, TRUE) OVER ( PARTITION BY FinalGroupCode  ORDER BY FinalGroupCode_RowNum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )


                                          WHEN
                                                      `ar_index__10` IS NULL
                                                AND   Price_Anomaly_Flag = 0
                                                AND   LAG(`ar_index__10`, 2) OVER (  PARTITION BY FinalGroupCode   ORDER BY AuctionDate ) = 5
                                          THEN
                                              -- Χρησιμοποιούμε LAST_VALUE() με IGNORE NULLS για να τραβήξουμε  την πιο πρόσφατη μη-null τιμή του ar_index__11 ώς τώρα
                                              last_VALUE(`ar_index__10`, TRUE) OVER ( PARTITION BY FinalGroupCode  ORDER BY FinalGroupCode_RowNum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )

                                          WHEN
                                                      `ar_index__10` IS NULL
                                                AND   Price_Anomaly_Flag = 1
                                                AND   LAG(`ar_index__10`) OVER (  PARTITION BY FinalGroupCode   ORDER BY AuctionDate ) = 5
                                          THEN
                                              -- Χρησιμοποιούμε LAST_VALUE() με IGNORE NULLS για να τραβήξουμε  την πιο πρόσφατη μη-null τιμή του ar_index__11 ώς τώρα
                                              last_VALUE(`ar_index__10`, TRUE) OVER ( PARTITION BY FinalGroupCode  ORDER BY FinalGroupCode_RowNum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )

----------------------------------------------------------------------------
                                            else `ar_index__10`
                                        END AS `ar_index__11` ,
                                        *
                                    FROM   RankedData_10
)
,

--///////////////////////////////////
RankedData_12  as
                              (
                                    SELECT

----------------------------------------------------------------------------
--Round  1
                                          CASE

                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended    in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) <   Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 1
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                                THEN 1

                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 0
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   IS NULL
                                                      and   Lead(`ar_index__11`,2) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                                THEN 1


                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 1
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                                THEN 1

                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended    in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 0
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 1
                                                THEN 1


                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended  not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 0
                                                      and   Lead(`ar_index__11`,3) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) IS NULL
                                                THEN 1

----------------------------------------------------------------------------
--Round  2

                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Lead(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) >  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 0
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                                THEN 2

                                                WHEN
                                                            `ar_index__11` = 1
                                                      and   Result_blended  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 0
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  2
                                                      and   Lead(`ar_index__11`,2) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  2
                                                THEN 2


                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   `FirstOfferPrice` = Lead(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   `FirstOfferPrice` = lag(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 1
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  2
                                                      and   lag(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  2
                                                THEN 2


                                                WHEN
                                                      `ar_index__11` IS NULL
                                                      and   `FirstOfferPrice` =  Lag(`FirstOfferPrice`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 1
                                                      and   Lag(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) =  2
                                                      and   Lag(`Result_blended`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   `Result_blended` not  in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                THEN 2

                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      AND   Price_Anomaly_Flag = 0
                                                      AND   LAG(`ar_index__11`) OVER (  PARTITION BY FinalGroupCode   ORDER BY AuctionDate ) = 2
                                                      and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                THEN
                                                    -- Χρησιμοποιούμε LAST_VALUE() με IGNORE NULLS για να τραβήξουμε  την πιο πρόσφατη μη-null τιμή του ar_index__11 ώς τώρα
                                                    last_VALUE(`ar_index__11`, TRUE) OVER ( PARTITION BY FinalGroupCode  ORDER BY FinalGroupCode_RowNum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )


                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 0
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                                THEN 2




                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended    not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 0
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                                THEN 2


                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) >  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 1
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                                THEN 2

                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) =  Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 1
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                                THEN 2

                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) > Lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag      = 0
                                                      and   Lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                                THEN 2


                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended  not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) = Lag(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 0
                                                      and   Lag(`ar_index__11`,2) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 2
                                                THEN 2


                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      AND   Price_Anomaly_Flag = 0
                                                      and   LAG(Result_blended) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` ) not in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                THEN
                                                    -- Χρησιμοποιούμε LAST_VALUE() με IGNORE NULLS για να τραβήξουμε  την πιο πρόσφατη μη-null τιμή του ar_index__11 ώς τώρα
                                                    last_VALUE(`ar_index__11`, TRUE) OVER ( PARTITION BY FinalGroupCode  ORDER BY FinalGroupCode_RowNum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )

----------------------------------------------------------------------------
--Round  3
                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) = Lag(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 1
                                                      and   Lag(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                                THEN 3

                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) = Lag(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 1
                                                      and   Lag(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 3
                                                THEN 3


----------------------------------------------------------------------------
--Round  4

                                                WHEN
                                                            `ar_index__11` IS NULL
                                                      and   Result_blended   in ('eAuction - Ανεσταλμένος', 'eAuction - Ματαιωμένος')
                                                      and   (`FirstOfferPrice` ) = lead(`FirstOfferPrice` ) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )
                                                      and   Price_Anomaly_Flag = 1
                                                      and   lead(`ar_index__11`) OVER (PARTITION BY  FinalGroupCode ORDER BY `AuctionDate` )  = 4
                                                THEN 4
----------------------------------------------------------------------------
--Round  5
                                              WHEN
                                                          `ar_index__11` IS NULL
                                                    AND   Price_Anomaly_Flag = 0
                                                    AND   LAG(`ar_index__11`, 2) OVER (  PARTITION BY FinalGroupCode   ORDER BY AuctionDate ) = 5
                                              THEN
                                                  -- Χρησιμοποιούμε LAST_VALUE() με IGNORE NULLS για να τραβήξουμε  την πιο πρόσφατη μη-null τιμή του ar_index__11 ώς τώρα
                                                  last_VALUE(`ar_index__11`, TRUE) OVER ( PARTITION BY FinalGroupCode  ORDER BY FinalGroupCode_RowNum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )

                                              WHEN
                                                          `ar_index__11` IS NULL
                                                    AND   Price_Anomaly_Flag = 1
                                                   -- AND   LAG(`ar_index__11`, 2) OVER (  PARTITION BY FinalGroupCode   ORDER BY AuctionDate ) = 5
                                              THEN
                                                  -- Χρησιμοποιούμε LAST_VALUE() με IGNORE NULLS για να τραβήξουμε  την πιο πρόσφατη μη-null τιμή του ar_index__11 ώς τώρα
                                                  last_VALUE(`ar_index__11`, TRUE) OVER ( PARTITION BY FinalGroupCode  ORDER BY FinalGroupCode_RowNum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
----------------------------------------------------------------------------
                                            else `ar_index__11`
                                        END AS `ar_index__12` ,
                                        *
                                    FROM   RankedData_11
)

,
----------------------------------------------------------------------------
----------------------------------------------------------------------------
collect_data as
                (
                        with Temp_data as (
                                                select
                                                      MD5(Create_step4_temp.FinalGroupCode) AS unique_auction_asset_id,
                                                      RankedData_12.`ar_index__12`,
                                                      RankedData_11.`ar_index__11`,
                                                      RankedData_10.`ar_index__10`,
                                                      RankedData_10.`ar_index__9`,
                                                      Create_step4_temp.*
                                                from
                                                      Create_step4_temp

                                                left join
                                                      RankedData_12
                                                on
                                                      RankedData_12.AuctionObJID_Temp  = Create_step4_temp.AuctionObJID

                                                left join
                                                      RankedData_11
                                                on
                                                      RankedData_11.AuctionObJID_Temp  = Create_step4_temp.AuctionObJID

                                                left join
                                                      RankedData_10
                                                on
                                                      RankedData_10.AuctionObJID_Temp  = Create_step4_temp.AuctionObJID

                                                order by
                                                      Create_step4_temp.FinalGroupCode,
                                                      Create_step4_temp.FinalGroupCode_RowNum
                                          )
                              select
                                    Temp_data.*
                              from
                                    Temp_data
)

select
      distinct *
from
      collect_data

order by FinalGroupCode_RowNum

"""

# COMMAND ----------

# DBTITLE 1,Step_4 -  AuctionRound_Query_step4 to PySpark DataFrame  /
df_step4 = spark.sql(AuctionRound_Query_step4)
df_step4.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{step4_table}")

# COMMAND ----------

# DBTITLE 1,Step_5 - Tia table query with AuctionRound
df_tia = spark.sql(f"""
WITH joined_data AS (

SELECT
    tia.Absorption_new,
    tia.NewEntry,
    tia.Price_band,
    tia.Quarter_Auction,
    tia.Today,
    tia.year_Auction,
    tia.Week_published,
    tia.month_auction,
    tia.year_month_auction,
    tia.Address,
    tia.After_May_2021,
    tia.Published_after_2021,
    tia.Area,
    tia.c_auction_id,
    tia.Result_blended,
    tia.`Κατηγορία1_active`,
    tia.`Κατηγορία2_active`,
    tia.`Κατηγορία3_active`,
    tia.Auction_time,
    tia.Auction_Type,
    tia.OriginAuctionKey,
    tia.BasicDataServicer,
    tia.eAuctionPostedDate,
    tia.AuctionDate,
    tia.Week_Auction,
    tia.AuctioneerEmail,
    tia.AuctioneerName,
    tia.AuctioneerPhone,
    tia.AuctionObJID,
    tia.Average_Marketability,
    tia.CategoryEN,
    tia.CategoryGR,
    tia.ConstructionYear,
    tia.Date_Received,
    tia.DebtorDescr,
    tia.DebtorVatNumber,
    tia.FinalPrice,
    tia.FirstOfferPrice,
    tia.FloorRawValue,
    tia.GeoCoordinatesX,
    tia.GeoCoordinatesY,
    tia.GeoLevel,
    tia.HastenerVat,
    tia.HastenerDescr,
    tia.KAEK,
    tia.LandeaUrl,
    tia.Municipality,
    tia.Periphery,
    tia.PostCode,
    tia.Prefecture,
    tia.c_Published_date,
    tia.PublishedDate,
    tia.year_month_published,
    tia.`Κατασχετήρια_Ημερομηνία`,
    tia.SeizureReportYear,
    tia.Source,
    tia.SubCategoryGR,
    tia.SurfaceSqM,
    tia.Tags,
    tia.full_ownership,
    tia.TitleGR,
    tia.UniqueCode,
    tia.AuctionID,
    tia.Date_last_received,
    tia.RealEstateGroupCode,
    tia.min_Reserve_price,
    tia.max_Reserve_price,
    tia.Change_price_reasoning,
    tia.Percentage_Increased,
    tia.Increase_bucket,
    tia.List_Portfolios,
    tia.Active_inactive,
    tia.TNMergeID,
    tia.AB_Core,
    tia.c_KAEK,
    tia.Cepal_Key_Auto_Suggestions_arotron,
    tia.Cepal_Key_Auto_Suggestions_qca,
    tia.Cepal_Key_Auto_Suggestions_thesis,
    tia.Cepal_Key_Final_thesis,
    tia.Cepal_Key_Final_qca,
    tia.Cepal_Key_Final_arotron,
    tia.Cepal_Key_lien_thesis,
    tia.Cepal_Key_lien_qca,
    tia.Cepal_Key_lien_arotron,
    --CAST(NULL AS STRING) AS IncomingAuctionKey,
    tia.Initiator_Cepal_others,
    tia.Initiator_final,
    tia.IncomingIsHastenerServicer,
    tia.Initiator_Cepal_AB,
    tia.IncomingPortfolioCategory,
    tia.Initiator_TIA,
    --CAST(NULL AS STRING) AS InitiatorLKP,
    --CAST(NULL AS STRING) AS Is_Hastener_ServicerLKP,
    --CAST(NULL AS STRING) AS HastenerLKP,
    --CAST(NULL AS STRING) AS Initiator_Cepal_ABLKP,
    tia.Analysis_Core_NPL,
    tia._3_months_period,
    tia.count_auctions,
    tia.Portfolios_of_CEPAL_Keys_Contracts_based,
    tia.AuctionKeyList,
    --CAST(NULL AS STRING) AS Hastener,
    tia.UIMatchingAuctionStatus,
    tia.Cepal_Key_Final,
    tia.LF_Confiscation,
    tia.Liquidation_Value,
    tia.Marketability,
    tia.AcquisitionBy,
    tia.Cepal_Key_Final_single,
    --rcg fields
    tia.rcg_provider_mbp_price,
    tia.rcg_provider_uw_price,
    tia.rcg_provider_marketability_score,
    tia.rcg_provider_auction_round,
    tia.rcg_provider_acquired_by,
    tia.rcg_provider_file_date,
    tia.rcg_provider_eligibility,
    tia.rcg_provider_eligibility_flag_property,
    tia.rcg_provider_initiator,
    tia.rcg_provider_Servicer,
    tia.rcg_provider_Auction_result_updated,
    tia.rcg_provider_suspension_reason_grouped,
    tia.rcg_provider_suspension_reason_full,
    tia.rcg_provider_eligible_for_reschedule,
    step4.unique_auction_asset_id AS AuctionRound_property_id,
    step4.ar_index__12 AS AuctionRound_property
    FROM {tia_table} as tia
    LEFT JOIN {step4_table} as step4
    ON tia.AuctionObJID = step4.AuctionObJID
),

max_auction_round AS (
    SELECT
        c_auction_id,
        MAX(AuctionRound_property) AS AuctionRound_auction
    FROM joined_data
    GROUP BY c_auction_id
)

-- Final selection with joined max values
SELECT
    j.*,
    m.AuctionRound_auction
FROM joined_data j
LEFT JOIN max_auction_round m
ON j.c_auction_id = m.c_auction_id
""")

# COMMAND ----------

# DBTITLE 1,Create tia production table with auction round
#spark.sql(f"TRUNCATE TABLE IF EXISTS `{env_prefix}-cdh-delivery`.tia.tia")

df_tia.distinct().write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
     f"`{env_prefix}-cdh-delivery`.tia.tia")


# COMMAND ----------

# DBTITLE 1,Drop intermediate step tables
# Drop all auction_round_data_step tables if they exist
for table_step in tables_to_drop:
    spark.sql(f"""DROP TABLE IF EXISTS {table_step}""")
