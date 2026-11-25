# Databricks notebook source
# MAGIC %md
# MAGIC # Hoist Portfolios - Collections Assignment
# MAGIC Assigns matched collection payments to agents/channels for Hoist portfolios by applying specific rules. Writes to `derivativeinternal.collectionsassignment`.
# MAGIC
# MAGIC Runs AFTER:
# MAGIC - CollectionPaymentType (requires PaymentTypeGroup)

# COMMAND ----------

# DBTITLE 1,aws_utils
import boto3
from botocore.exceptions import NoCredentialsError

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql.functions import from_utc_timestamp, current_timestamp

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


def get_aws_credentials(secrets_scope, key_name):
    try:
        aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_name)
        return aws_secret_key
    except Exception as e:
        print(f"Error retrieving AWS credentials: {str(e)}")
        return None


def get_scope_key_names(secrets_scope):
    try:
        aws_keys = dbutils.secrets.list(scope=secrets_scope)
        key_names = [key.key for key in aws_keys] if aws_keys else []
        return key_names
    except Exception as e:
        print(f"Error retrieving AWS keys: {str(e)}")
        return None


def connect_to_aws_and_access_ssm(ssm_key, ssm_client, aws_secret_key):
    if aws_secret_key is None:
        print("AWS secret key not available.")
        return
    response = ssm_client.get_parameter(Name=ssm_key, WithDecryption=False)
    return response['Parameter']['Value']


def get_env_prefix():
    secrets_scope = "aws-databricks"
    aws_keys = get_scope_key_names(secrets_scope)
    aws_key = aws_keys[0] if aws_keys else None
    aws_secret_key = get_aws_credentials(secrets_scope, aws_key)
    ssm_client = boto3.client('ssm', region_name='eu-central-1',
                              aws_access_key_id=aws_key,
                              aws_secret_access_key=aws_secret_key)
    env = connect_to_aws_and_access_ssm('/cfn/bucket_prefix', ssm_client, aws_secret_key)
    return env

# COMMAND ----------

# DBTITLE 1,create widgets
dbutils.widgets.text("run_date", "", "Run Date")
dbutils.widgets.text("portfolio", "", "Portfolio")

# COMMAND ----------

# DBTITLE 1,env + portf
run_date = dbutils.widgets.get("run_date")
portfolio = dbutils.widgets.get("portfolio")
env = get_env_prefix()

# Cut-off date for QC migration (qc migrated to qca): process records with ActualDate > CUT_OFF_DATE
CUT_OFF_DATE = "2026-06-05"

# COMMAND ----------

# DBTITLE 1,set portf parameters
portfolio_df = spark.sql(f"""
    SELECT *
    FROM `{env}-cdh`.primary.portfolio p
    WHERE businessdate='{run_date}'
    AND (
        lower(PortfolioName) = lower('{portfolio}')
        OR ('{portfolio}' = '' AND Short_Name_Asset_Owner = 'Hoist')
    )
""")

portfolio_df.createOrReplaceTempView("portfolio")

portfolio_names = [r["PortfolioName"] for r in portfolio_df.collect()]
portfolio_filter = ", ".join(f"'{p}'" for p in portfolio_names)
 
print(f"Hoist portfolios ({len(portfolio_names)}): {portfolio_names}")

# COMMAND ----------

# DBTITLE 1,temps
# ================== primary.collection ==================
collections_perim = spark.sql(f"""
    SELECT
        c.CollectionId,
        c.SourceSystem, 
        c.PortfolioKey AS Unit_Code, 
        c.PortfolioName,
        c.CollectionKey AS Puid, 
        c.CollectionSourceKey, 
        CASE 
            WHEN LEFT(de.ExposureKey, 3) = p.Prefix 
                then de.ExposureKey 
            else 
                concat(p.Prefix,de.ExposureKey)
        end as ExposureKeyWithPrefix,
        CASE 
            WHEN LEFT(de.ExposureKey, 3) = p.Prefix 
                then substring(de.ExposureKey,4,100)
            else 
                de.exposurekey
        END AS ExposureKeyWithoutPrefix,
        de.CDI as PrimaryCDI, 
        de.SubProductDescription, 
        de.DaysPastDueWithMateriality, 
        c.Amount, 
        c.RecordDate,
        c.ActualDate, 
        c.LoanBankCode, 
        c.MatchedDate, 
        c.FinalReason, 
        cpt.PaymentTypeGroup, 
        c.BusinessDate,
        ca.assignmentstatusflag,
        calendar.previousBusinessday
    FROM `{env}-cdh`.primary.collection c
    join `{env}-cdh`.primary.portfolio p on p.PortfolioName=c.portfolioname and p.BusinessDate ='{run_date}'
    join `{env}-cdh`.reference.ref_calendar calendar on calendar.CalendarDate = c.actualdate
    join `{env}-cdh`.derivative.dexposure de on  calendar.previousBusinessday=de.businessdate
        and (c.exposurekey=de.exposurekey 
            or 
            (c.exposurekey=substring(de.exposurekey,4,100) and c.portfolioName = de.portfolioname)
        )
    left join `{env}-cdh`.derivativeinternal.collectionassignment  ca on c.collectionkey=ca.puid 
    left join `{env}-cdh`.derivativeinternal.collectionpaymenttype  cpt on c.collectionkey=cpt.collectionkey and cpt.BusinessDate=c.BusinessDate
    WHERE c.BusinessDate='{run_date}'
    and c.portfolioName IN ({portfolio_filter})
    and (ca.AssignmentStatusFlag='pending' or ca.puid is null)
    and c.matcheddate is not null

""")
collections_perim.createOrReplaceTempView("collections_perim")

# COMMAND ----------

# DBTITLE 1,temps
assignment = spark.sql(f"""
-- Step 1: Assign priority to each assignmentprocess per exposurekey
WITH assignment_ranked AS (
    SELECT *,
        CASE
            WHEN lower(assignmentprocess) = 'collection' THEN 1
            WHEN lower(assignmentprocess) LIKE '%offer' THEN 2
            WHEN lower(assignmentprocess) = 'ca/fa/sme officer' THEN 3
            ELSE 4
        END AS process_priority,
        ROW_NUMBER() OVER (
            PARTITION BY exposurekey,businessdate
            ORDER BY
                CASE
                    WHEN lower(assignmentprocess) = 'collection' THEN 1
                    WHEN lower(assignmentprocess) LIKE '%offer' THEN 2
                    WHEN lower(assignmentprocess) = 'ca/fa/sme officer' THEN 3
                    ELSE 4
                END ASC
        ) AS rn
    FROM `{env}-cdh`.primary.assignment
    WHERE 1=1
        AND sourcesystem IN ('qc','qca')
),

-- Step 2: Keep only the top-priority record per exposurekey
assignment_deduped AS (
    SELECT * FROM assignment_ranked WHERE rn = 1
),

-- Step 3: Join to collections perimeter as before
final AS (
    SELECT DISTINCT
        a.ProcessType, a.assignmentprocess, a.channel, a.team,
        a.ExternalCompanyFullName, a.AgentFullName, a.casecode,
        a.teamcode, a.channelcode, a.businessdate, a.sourcesystem,
        c.exposurekeyWithoutPrefix,
        c.ExposureKeyWithPrefix,
        c.Unit_Code,
        c.Puid,
        c.CollectionSourceKey,
        c.LoanBankCode,
        c.Amount,
        c.ActualDate,
        c.MatchedDate,
        c.FinalReason,
        c.AssignmentStatusFlag,
        c.SubProductDescription,
        c.DaysPastDueWithMateriality,
        c.PrimaryCDI,
        c.PaymentTypeGroup
    FROM assignment_deduped a
    JOIN collections_perim c
        ON a.businessdate = c.previousBusinessday
        AND a.PortfolioName = c.PortfolioName
        AND (c.ExposureKeyWithPrefix = a.exposurekey OR c.ExposureKeywithoutprefix = a.exposurekey)
)

SELECT * FROM final
""")
assignment.createOrReplaceTempView("assignment")

# COMMAND ----------

# DBTITLE 1,temps
# ================== primary.firmstructure ==================
firmstructure = spark.sql(f"""
    SELECT DISTINCT EmployeePartyKey, 
            EmployeeFullName,
            sourcesystem,
            BusinessDate
    FROM `{env}-cdh`.primaryactive.firmstructure
    WHERE sourcesystem in ('qc', 'qca')
      AND BusinessDate='{run_date}'
      AND employeeCategory = 'Employee'
      AND TeamCode != '-100' 
""")
firmstructure.createOrReplaceTempView("firmstructure")

# ================== primary.interaction ==================
interactions = spark.sql(f"""
    SELECT  a.InteractionID, 
            c.exposurekeywithoutprefix,
            c.ExposureKeyWithPrefix,
            a.interactiontypekey, 
            a.interactiontype,
            a.InteractionDate, 
            a.externalCompanyKey,
            a.IsPromiseFlag,
            a.IsLegalFlag,
            a.IsCallFlag,
            a.Promisekey, 
            a.PromiseDate, 
            a.PromiseDueDate, 
            a.PromiseStatus, 
            fs.EmployeeFullName, 
            a.CDI as InteractionCDI, 
            a.businessdate
    FROM `{env}-cdh`.primary.get_interactions_by_business_date('{run_date}')  a 
    join collections_perim c on c.portfolioname=a.portfolioname
        and (
            a.exposurekey = c.ExposurekeyWithPrefix 
            or 
            a.exposurekey=c.exposurekeywithoutprefix
            )
    join firmstructure fs on fs.EmployeePartyKey= a.CreatedBy 
        and fs.SourceSystem = a.SourceSystem
""")
interactions.createOrReplaceTempView("interactions")

# ================== primary.settlementexposure ==================
settlement = spark.sql(f"""
    SELECT c.exposurekeywithoutprefix,
        c.ExposurekeyWithPrefix, 
        a.SettlementKey, 
        a.SubmissionDate, 
        a.isActiveFlag, 
        a.isMigratedFlag,
        a.ExpiredDate, 
        a.implementationDate, 
        a.isPrimarySettlementFlag, 
        a.SettlementType,
        a.SettlementStatusCode, 
        a.SettlementStatus, 
        a.LastUpdatedDate, 
        a.Bucket, 
        a.OverdueDate, 
        a.BusinessDate,
        se.Eligible
    FROM `{env}-cdh`.primary.settlementexposure a
    join collections_perim c on c.portfolioname=a.portfolioname
        and (
            a.exposurekey = c.ExposurekeyWithPrefix 
            or 
            a.exposurekey=c.exposurekeywithoutprefix
            )
    inner join `{env}-cdh`.reference.collectionassignmentsettlementeligibility se on se.SettlementType = a.SettlementType
    WHERE a.BusinessDate='{run_date}'
    AND a.isActiveFlag = 1
    AND se.Eligible = 1 --keep only eligible settlements
""")
settlement.createOrReplaceTempView("settlement")

# ================== primary.application (FLDT) ==================
application_fldt = spark.sql(f"""
    SELECT 
        a.ApplicationId, 
        c.exposurekeywithoutprefix,
        c.ExposurekeyWithPrefix, 
        a.CDI as ApplicationCDI, 
        a.ApplicationDate, 
        a.ApplicationStatus,
        a.ApplicationStatusCode, 
        a.SourceSystem, 
        a.BusinessDate,
        a.Createddate
    FROM `{env}-cdh`.primary.application a
    join collections_perim c on c.portfolioname=a.portfolioname
        and a.CDI = c.primaryCDI
    WHERE a.SourceSystem = 'fldt'
    AND a.BusinessDate = '{run_date}'
""")
application_fldt.createOrReplaceTempView("application_fldt")

# COMMAND ----------

# DBTITLE 1,merge insert new matched records
collectionassignment = f"`{env}-cdh`.derivativeinternal.collectionassignment"

# Only records matched TODAY AND ActualDate > CUT_OFF_DATE
new_unassigned = assignment.filter(
    (F.col("AssignmentStatusFlag")).isNull()
)

df_final = new_unassigned.select(
    F.col("Unit_Code").cast("int").alias("UnitCode"),
    F.col("Puid").cast("string").alias("Puid"),
    F.col("CollectionSourceKey").cast("string").alias("id"),
    F.col("LoanBankCode").cast("string").alias("LoanBankcode"),
    F.col("Amount").cast("decimal(19,4)").alias("Amount"),
    F.col("ActualDate").cast("date").alias("ActualDate"),
    F.col("MatchedDate").cast("timestamp").alias("MatchDate"),
    F.col("FinalReason").cast("string").alias("FinalReason"),
    F.lit("pending").cast("string").alias("AssignmentStatusFlag"),
    F.lit(None).cast("string").alias("AssignReason"),
    F.col("ProcessType").cast("string").alias("ProcessType")
)

# COMMAND ----------

# DBTITLE 1,append new unassigned records
df_final.write.mode("append").saveAsTable(collectionassignment)

# COMMAND ----------

# DBTITLE 1,common_rule_join
# Base join for rules: ca -> dexposure -> assignment -> firmstructure
common_rule_join = (
    spark.table(collectionassignment).alias("ca")
    .filter(
        (F.lower(F.col("AssignmentStatusFlag")) == F.lit("pending")) &
        (F.lower(F.col("FinalReason")) == "matched")
    )
    .join(
        spark.table("assignment").alias("a"),
        ((F.col("a.Puid") == F.col("ca.Puid")) & (F.col("a.Unit_Code") == F.col("ca.UnitCode"))),
        "inner",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sequential Rules
# MAGIC
# MAGIC 1. External Channel (Offer/Collection ProcessType)
# MAGIC 2. 1 Personal Assignment - settlement eligibility (DPD <11 or >90, eligible settlement)
# MAGIC 2. 2 Personal Assignment (DPD <11 or >90, SubProductDescription NOT 3869/OCW/4738/4605)
# MAGIC 3. 8702 Activity (0-30 days after)
# MAGIC 4. 8701 Activity + FLDT Application (0-60 days, app date ±5 days)
# MAGIC 5. Promise (until expiration + 10 days)
# MAGIC 6. Excluded (κατάσχεση εις χείρας τρίτου / εκπλειστηρίασμα)
# MAGIC 7. Unassigned (everything else except Rule 6)

# COMMAND ----------

# DBTITLE 1,Rule 1 - External Channel (Offer/Collection)
rule1_source = (
    common_rule_join
    .filter(
        (F.col("a.PaymentTypeGroup").like("Cash%")) &
        (F.col("a.ExternalCompanyFullName").isNotNull()) &
        (
            F.lower(F.col("a.assignmentprocess")).like("%offer%") |
            F.lower(F.col("a.assignmentprocess")).like("%collection%")
        )
    )
    .select(
        F.col("a.Puid").alias("puid"),
        F.col("a.ExternalCompanyFullName").alias("externalchannelname"),
        F.col("a.Unit_Code").alias("UnitCode"),
        F.col("a.AssignmentProcess").alias("AssignmentProcess")
    )
    .distinct()
)


Rule_Update = (
    DeltaTable.forName(spark, collectionassignment).alias("t")
    .merge(
        rule1_source.alias("s"),
        f"s.puid = t.puid AND t.UnitCode = s.UnitCode" #AND t.UnitCode = {int(UnitCodeProfits)}
    )
    .whenMatchedUpdate(set={
        "AssignmentStatusFlag": F.lit("LF/Collection"),
        "AssignReason": F.lit("Rule 1 - External Channel (Offer/Collection)"),
        "AgentName": F.lit(None),
        "TeamName": F.lit(None),
        "ChannelName": F.lit(None),
        "ExternalChannelName": F.col("s.externalchannelname"),
        "AssignmentProcess": F.col("s.AssignmentProcess"),
        "IngestionTimestamp": from_utc_timestamp(current_timestamp(), 'Europe/Athens')
    })
    .execute()
)


# COMMAND ----------

# DBTITLE 1,Rule 2.1 - Personal Assignment (settlement eligibility, no SubProduct filter)
# Rule 2.1: assignmentprocess = 'CA/FA/Sme Officer', DPD <11 or >90.
# If settlement exists on exposure must be eligible (from reference table).
# AgentFullName from assignment

rule2_1_source = (
    common_rule_join
    .join(
        spark.table("settlement").alias("se"),
        (F.col("se.ExposurekeyWithPrefix") == F.col("a.ExposurekeyWithPrefix")),
        "inner"
    )
    .filter(
        (F.col("a.AgentFullName").isNotNull()) &
        (F.col("a.assignmentprocess") == F.lit("CA/FA/Sme Officer")) &
        (
            (F.coalesce(F.col("a.DaysPastDueWithMateriality"), F.lit(0)) < 11) | (F.coalesce(F.col("a.DaysPastDueWithMateriality"), F.lit(0)) > 90)
        )
    )
    .select(
        F.col("a.Puid").alias("puid"),
        F.col("a.AgentFullName").alias("agentname"),
        F.when(F.col("a.Team").isNotNull(), F.col("a.Team")).otherwise(F.lit(None)).alias("teamname"),
        F.when(F.col("a.Channel").isNotNull(), F.col("a.Channel")).otherwise(F.lit(None)).alias("channelname"),
        F.col("a.Unit_Code").alias("UnitCode"),
        F.col("a.AssignmentProcess").alias("AssignmentProcess")
    )
    .distinct()
)

Rule_Update = (
    DeltaTable.forName(spark, collectionassignment).alias("t")
    .merge(
        rule2_1_source.alias("s"),
        "s.puid = t.puid AND t.UnitCode = s.UnitCode"
    )
    .whenMatchedUpdate(set={
        "AssignmentStatusFlag": F.lit("personal assignment"),
        "AssignReason": F.lit("Rule 2.1 - Personal Assignment - eligible settlement"),
        "AgentName": F.col("s.agentname"),
        "TeamName": F.col("s.teamname"),
        "ChannelName": F.col("s.channelname"),
        "ExternalChannelName": F.lit(None),
        "AssignmentProcess": F.col("s.AssignmentProcess"),
        "IngestionTimestamp": from_utc_timestamp(current_timestamp(), 'Europe/Athens')
    })
    .execute()
)

# COMMAND ----------

# DBTITLE 1,Rule 2.2 - Personal Assignment (SubProduct filter, no settlement check)
# Rule 2.2: assignmentprocess = 'CA/FA/Sme Officer', DPD <11 or >90.
# SubProductDescription NOT like 3869/OCW/4738/4605,
# AgentFullName from assignment

rule2_2_source = (
    common_rule_join
    .filter(
        (F.col("a.PaymentTypeGroup").like("Cash%")) &
        (F.col("a.AgentFullName").isNotNull()) &
        (F.col("a.assignmentprocess") == F.lit("CA/FA/Sme Officer")) &
        (~F.coalesce(F.col("a.SubProductDescription"), F.lit("")).rlike("(?i)(3869|OCW|4738|4605)")) &
        (
            (F.coalesce(F.col("a.DaysPastDueWithMateriality"), F.lit(0)) < 11) | (F.coalesce(F.col("a.DaysPastDueWithMateriality"), F.lit(0)) > 90)
        )
    )
    .select(
        F.col("a.Puid").alias("puid"),
        F.col("a.AgentFullName").alias("agentname"),
        F.when(F.col("a.Team").isNotNull(), F.col("a.Team")).otherwise(F.lit(None)).alias("teamname"),
        F.when(F.col("a.Channel").isNotNull(), F.col("a.Channel")).otherwise(F.lit(None)).alias("channelname"),
        F.col("a.Unit_Code").alias("UnitCode"),
        F.col("a.AssignmentProcess").alias("AssignmentProcess")
    )
    .distinct()
)

Rule_Update = (
    DeltaTable.forName(spark, collectionassignment).alias("t")
    .merge(
        rule2_2_source.alias("s"),
        "s.puid = t.puid AND t.UnitCode = s.UnitCode"
    )
    .whenMatchedUpdate(set={
        "AssignmentStatusFlag": F.lit("personal assignment"),
        "AssignReason": F.lit("Rule 2.2 - Personal Assignment - Law SubProduct"),
        "AgentName": F.col("s.agentname"),
        "TeamName": F.col("s.teamname"),
        "ChannelName": F.col("s.channelname"),
        "ExternalChannelName": F.lit(None),
        "AssignmentProcess": F.col("s.AssignmentProcess"),
        "IngestionTimestamp": from_utc_timestamp(current_timestamp(), 'Europe/Athens')
    })
    .execute()
)

# COMMAND ----------

# DBTITLE 1,Rule 3 - 8702 Activity (0-30 days)
# Payment attributed to user who RECORDED 8702 activity (EmployeeFullName)
# if payment occurred 0-30 days AFTER the activity date.

rule3_source = (
    common_rule_join
    .filter(F.col("a.PaymentTypeGroup").like("Cash%"))
    .join(
        spark.table("interactions").alias("act"),
        ((F.col("a.ExposurekeyWithPrefix") == F.col("act.ExposurekeyWithPrefix"))) &
        (
            ((F.col("act.interactiontypekey") == F.lit("8702")) & (F.col("ca.ActualDate") >= CUT_OFF_DATE)) | 
            ((F.col("act.interactiontypekey") == F.lit("3006")) & (F.col("ca.ActualDate") < CUT_OFF_DATE))
        ) &
        (F.col("ca.ActualDate") >= F.col("act.InteractionDate")) &
        (F.col("ca.ActualDate") <= F.date_add(F.col("act.InteractionDate"), 30)),
        "inner",
    )
    .select(
        F.col("ca.Puid").alias("puid"),
        F.col("act.EmployeeFullName").alias("agentname"),
        F.col("ca.UnitCode"),
        F.col("a.channel"),
        F.col("a.team"),
        F.row_number().over(
            Window.partitionBy(F.col("ca.Puid"))
            .orderBy(F.col("act.InteractionDate").desc())
        ).alias("rnk"),
        F.col("a.AssignmentProcess").alias("AssignmentProcess")
    )
    .filter(F.col("rnk") == 1)
    .select("puid", "agentname", "UnitCode","channel","team","AssignmentProcess")
    .distinct()
)

Rule_Update = (
    DeltaTable.forName(spark, collectionassignment).alias("t")
    .merge(
        rule3_source.alias("s"),
        f"s.puid = t.puid AND t.UnitCode = s.UnitCode"
    )
    .whenMatchedUpdate(set={
        "AssignmentStatusFlag": F.lit("activity 8702"),
        "AssignReason": F.lit("Rule 3 - 8702 Activity (0-30 days)"),
        "AgentName": F.col("s.agentname"),
        "TeamName": F.col("s.team"),
        "ChannelName": F.col("s.channel"),
        "ExternalChannelName": F.lit(None),
        "AssignmentProcess": F.col("s.AssignmentProcess"),
        "IngestionTimestamp": from_utc_timestamp(current_timestamp(), 'Europe/Athens')
    })
    .execute()
)


# COMMAND ----------

# DBTITLE 1,Rule 4 - 8701 Activity + FLDT Application (0-60 days)
# Payment attributed to user who RECORDED 8701 activity (EmployeeFullName)
# if payment 0-60 days after activity AND active FLDT application with date ±5 days from activity.
rule4_source = (
    common_rule_join
    .filter(F.col("a.PaymentTypeGroup").like("Cash%"))
    .join(
        spark.table("interactions").alias("act"),
        (F.col("a.ExposurekeyWithPrefix") ==  F.col("act.ExposurekeyWithPrefix")) &
        (
            ((F.col("act.interactiontypekey") == F.lit("8701")) & (F.col("ca.ActualDate") >= CUT_OFF_DATE)) | 
            ((F.col("act.interactiontypekey") == F.lit("3005")) & (F.col("ca.ActualDate") < CUT_OFF_DATE)) 
        ) &
        (F.col("ca.ActualDate") >= F.col("act.InteractionDate")) &
        (F.col("ca.ActualDate") <= F.date_add(F.col("act.InteractionDate"), 60)),
        "left",
    )
    .join(
        spark.table("application_fldt").alias("app"),
        (F.col("app.ApplicationCDI") == F.col("a.PrimaryCDI")) &
        (F.to_date(F.col("app.CreatedDate")) >= F.date_sub(F.col("act.InteractionDate"), 5)) &
        (F.to_date(F.col("app.CreatedDate")) <= F.date_add(F.col("act.InteractionDate"), 5)) &
        (~F.lower(F.col("app.ApplicationStatus")).like("%cancelled%")),
        "inner",
    )
    .select(
        F.col("a.ExposurekeyWithPrefix").alias("exposurekey"),
        F.col("ca.Puid").alias("puid"),
        F.col("act.EmployeeFullName").alias("agentname"),
        F.col("ca.UnitCode"),
        F.col("a.channel"),
        F.col("a.team"),
        F.row_number().over(
            Window.partitionBy(F.col("ca.Puid"))
            .orderBy(F.col("act.InteractionDate").desc())
        ).alias("rnk"),
        F.col("a.AssignmentProcess").alias("AssignmentProcess")
    )
    .filter(F.col("rnk") == 1)
    .select("puid", "agentname", "UnitCode", "exposurekey","channel","team","AssignmentProcess")
    .distinct()
)

Rule_Update = (
    DeltaTable.forName(spark, collectionassignment).alias("t")
    .merge(
        rule4_source.alias("s"),
        f"s.puid = t.puid AND t.UnitCode = s.UnitCode"
    )
    .whenMatchedUpdate(set={
        "AssignmentStatusFlag": F.lit("activity 8701 + FLDT"),
        "AssignReason": F.lit("Rule 4 - 8701 Activity (0-60d) + FLDT App (±5d)"),
        "AgentName": F.col("s.agentname"),
        "TeamName": F.col("s.team"),
        "ChannelName": F.col("s.channel"),
        "ExternalChannelName": F.lit(None),
        "AssignmentProcess": F.col("s.AssignmentProcess"),
        "IngestionTimestamp": from_utc_timestamp(current_timestamp(), 'Europe/Athens')
    })
    .execute()
)


# COMMAND ----------

# DBTITLE 1,Rule 5 - Promise (until expiration + 10 days)
# Payment attributed to user who RECORDED the promise (EmployeeFullName)
# if payment between PromiseDate and PromiseDueDate + 10 days.

rule5_source = (
    common_rule_join
    .filter(F.col("a.PaymentTypeGroup").like("Cash%"))
    .join(
        spark.table("interactions").alias("prom"),
        (F.col("a.ExposurekeyWithPrefix") == F.col("prom.ExposurekeyWithPrefix")) &
        (F.col("prom.IsPromiseFlag") == F.lit(True)) &
        (F.col("ca.ActualDate") >= F.to_date(F.col("prom.PromiseDate"))) &
        (F.col("ca.ActualDate") <= F.date_add(F.to_date(F.col("prom.PromiseDueDate"),"yyyyMMdd"), 10)),
        "inner",
    )
    .select(
        F.col("ca.Puid").alias("puid"),
        F.col("prom.EmployeeFullName").alias("agentname"),
        F.col("ca.UnitCode"),
        F.col("a.channel"),
        F.col("a.team"),
        F.row_number().over(
            Window.partitionBy(F.col("ca.Puid"))
            .orderBy(
                F.when(F.lower(F.col("prom.PromiseStatus")).isin(["pending", "215"]), F.lit(1))
                 .when(F.lower(F.col("prom.PromiseStatus")).isin(["kept", "213"]), F.lit(2))
                 .when(F.lower(F.col("prom.PromiseStatus")).isin(["broken", "214"]), F.lit(3))
                 .otherwise(F.lit(4)).asc(),
                F.col("prom.InteractionDate").desc()
            )
        ).alias("rnk"),
        F.col("a.AssignmentProcess").alias("AssignmentProcess")
    )
    .filter(F.col("rnk") == 1)
    .select("puid", "agentname", "UnitCode","channel","team","AssignmentProcess")
    .distinct()
)

Rule_Update = (
    DeltaTable.forName(spark, collectionassignment).alias("t")
    .merge(
        rule5_source.alias("s"),
        f"s.puid = t.puid AND t.UnitCode = s.UnitCode "
    )
    .whenMatchedUpdate(set={
        "AssignmentStatusFlag": F.lit("promise"),
        "AssignReason": F.lit("Rule 5 - Promise (until expiration + 10d)"),
        "AgentName": F.col("s.agentname"),
        "TeamName": F.col("s.team"),
        "ChannelName": F.col("s.channel"),
        "ExternalChannelName": F.lit(None),
        "AssignmentProcess": F.col("s.AssignmentProcess"),
        "IngestionTimestamp": from_utc_timestamp(current_timestamp(), 'Europe/Athens')
    })
    .execute()
)

# COMMAND ----------

# DBTITLE 1,Rule 6 - Excluded Payments (κατάσχεση/εκπλειστηρίασμα)
# Payments from "κατάσχεση εις χείρας τρίτου" or "εκπλειστηρίασμα"
# are NOT assigned to any channel — marked as excluded.


rule6_source = (
    common_rule_join
    .filter(
        F.col("a.PaymentTypeGroup").like("%RE Collateral Liquidation%")
    )
    .select(
        F.col("ca.Puid").alias("puid"),
        F.col("ca.UnitCode"),
        F.col("a.channel"),
        F.col("a.team"),
        F.col("a.AssignmentProcess").alias("AssignmentProcess")
    )
    .distinct()
)

Rule_Update = (
    DeltaTable.forName(spark, collectionassignment).alias("t")
    .merge(
        rule6_source.alias("s"),
        f"s.puid = t.puid AND t.UnitCode = s.UnitCode"
    )
    .whenMatchedUpdate(set={
        "AssignmentStatusFlag": F.lit("excluded"),
        "AssignReason": F.lit("Rule 6 - Excluded (κατάσχεση/εκπλειστηρίασμα)"),
        "AgentName":  F.lit(None),
        "TeamName":  F.lit(None),
        "ChannelName":  F.lit(None),
        "ExternalChannelName": F.lit(None),
        "AssignmentProcess": F.col("s.AssignmentProcess"),
        "IngestionTimestamp": from_utc_timestamp(current_timestamp(), 'Europe/Athens')
    })
    .execute()
)

# COMMAND ----------

# DBTITLE 1,Rule 7 - Unassigned
# All remaining pending payments → Unassigned.

rule7_source = (
    common_rule_join
    .select(
        F.col("ca.Puid").alias("puid"),
        F.col("ca.UnitCode"),
        F.col("a.AssignmentProcess").alias("AssignmentProcess")
    )
    .distinct()
)

Rule_Update = (
    DeltaTable.forName(spark, collectionassignment).alias("t")
    .merge(
        rule7_source.alias("s"),
        f"s.puid = t.puid AND t.UnitCode = s.UnitCode"
    )
    .whenMatchedUpdate(set={
        "AssignmentStatusFlag": F.lit("unassigned"),
        "AssignReason": F.lit("Rule 7 - Unassigned (no rule matched)"),
        "AgentName":  F.lit('Unassigned'),
        "TeamName":  F.lit('Unassigned'),
        "ChannelName":  F.lit('Unassigned'),
        "ExternalChannelName": F.lit('Unassigned'),
        "AssignmentProcess": F.col("s.AssignmentProcess"),
        "IngestionTimestamp": from_utc_timestamp(current_timestamp(), 'Europe/Athens')
    })
    .execute()
)
