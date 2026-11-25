# Databricks notebook source
# DBTITLE 1,Imports
import logging
from delta import DeltaTable  # type: ignore
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    from_utc_timestamp,
    lit,
    sha2,
    substring,
    trim,
    when,
    months_between,
    dateadd,
    datediff,
)
from datetime import datetime, timedelta

import os
# Import the module
import sys
sys.path.append("../aws_utils")
 
import aws_utils
# Get Environment Prefix
 
env = aws_utils.get_env_prefix()
import delta_table_creator, transform

# COMMAND ----------

# DBTITLE 1,Schema, Widgets & Delete existing records for run_date
schema = ("DCollectionId	string,"
    "SourceSystem	string,"
    "PortfolioKey	int,"
    "PortfolioName	string,"
    "IngestionTimestamp	timestamp,"
    "DCollectionKey	string,"
    "CollectionSourceKey	string,"
    "ExposureKey	string,"
    "SourceExposureKey string,"
    "CepalKey	string,"
    "Amount	decimal(19,4),"
    "RecordDate	date,"
    "ActualDate	date,"
    "MatchedDate date,"
    "ReverseCode	string,"
    "Currency	string,"
    "AllocationAgent string,"
    "AllocationTeam	string,"
    "AllocationChannel	string,"
    "AllocationExternalCompany string,"
    "AssignmentProcess string,"
    "CorectionMechanism	boolean,"
    "ProcessType string,"
    "Comments	string,"
    "SplitFlag	boolean,"
    "DiasNumber	string,"
    "ExternalPaymentFlag	boolean,"
    "ITReason	string,"
    "FinalReason	string,"
    "DoNotUpdateFlag	boolean,"
    "PaymentTypeCode	int,"
    "PaymentType	string,"
    "PaymentTypeEng	string,"
    "PaymentTypeGroup	string,"
    "AssignmentProcessedFlag	boolean,"
    "PaymentFileChannel	string,"
    "OpapTanCode	string,"
    "OpapBranchCode	string,"
    "OpapCancellationDate	timestamp,"
    "OpapTaxRegistrationNumber	string,"
    "PayerCDI	string,"
    "PrimaryOwnerCDI	string,"
    "DiasChannel	int,"
    "IsPaymentFlag	boolean,"
    "PaymentMethod	string,"
    "BusinessDate	date"
)
partition_columns = ["BusinessDate"]
run_date = dbutils.widgets.get("run_date")
unity = dbutils.widgets.get("unity")
table_name = "DCollection"
derivative_table_path = ""

#================== Delete Delta Table if exists for given run_date ==================
if unity == "True":
    derivative_table_path = f"`{env}-cdh`.derivativeactive.{table_name}"
    # Check if table exists and delete for run_date if it does
    if spark.catalog.tableExists(derivative_table_path):
        transform.delete_from_unity_table(
            spark,
            derivative_table_path,
            run_date,  # type: ignore
        )

# COMMAND ----------

# DBTITLE 1,Run CollectionPaymentType notebook (prerequisite)
dbutils.notebook.run("CollectionPaymentType", 3600, {"run_date": run_date, "unity": unity})

# COMMAND ----------

# DBTITLE 1,Run CollectionPaymentMethod notebook (prerequisite)
dbutils.notebook.run("CollectionPaymentMethod", 3600, {"run_date": run_date, "unity": unity})

# COMMAND ----------

# DBTITLE 1,Run CollectionAssignment notebook (prerequisite)
dbutils.notebook.run("CollectionAssignmentHoist", 3600, {"run_date": run_date, "unity": unity})

# COMMAND ----------

# DBTITLE 1,Load Temps
portfolio = spark.sql(f"""
                    select distinct Prefix,portfolioName,unit_code_profits
                    from `{env}-cdh`.primary.portfolio
                    where BusinessDate = '{run_date}'
                    """)
portfolio.createOrReplaceTempView("portfolio")

dexposure = spark.sql(f""" 
                      with cte as (
                        SELECT distinct
                          CDI, 
                          ExposureKey, 
                          casecode, 
                          row_number() OVER (PARTITION BY ExposureKey ORDER BY BusinessDate desc) AS rn
                        FROM `{env}-cdh`.derivative.DExposure
                      ) 
                      SELECT CDI, ExposureKey, casecode
                      FROM cte
                      WHERE rn = 1
                    """)
dexposure.createOrReplaceTempView("dexposure")

# --- Collections Assignment (source for allocation fields) ---
collections_assignment = spark.sql(f"""
    SELECT 
        Puid,
        UnitCode,
        AgentName AS CA_AgentName,
        TeamName AS CA_TeamName,
        ChannelName AS CA_ChannelName,
        ExternalChannelName AS CA_ExternalChannelName,
        AssignmentProcess AS CA_AssignmentProcess,
        ProcessType AS CA_ProcessType
    FROM `{env}-cdh`.derivativeinternal.collectionassignment
""")
collections_assignment.createOrReplaceTempView("collections_assignment")

# --- CollectionPaymentType: pre-computed payment type fields ---
collection_payment_type = spark.sql(f"""
    SELECT CollectionKey,
           PaymentTypeCode,
           PaymentType,
           PaymentTypeEng,
           PaymentTypeGroup,
           IsPaymentFlag
    FROM `{env}-cdh`.derivativeinternal.collectionpaymenttype
    WHERE BusinessDate = '{run_date}'
""")
collection_payment_type.createOrReplaceTempView("collection_payment_type")

# --- CollectionPaymentMethod: pre-computed payment method field (auxiliary, no BusinessDate) ---
collection_payment_method = spark.sql(f"""
    SELECT CollectionKey,
           PaymentMethod
    FROM `{env}-cdh`.derivativeinternal.collectionpaymentmethod
""")
collection_payment_method.createOrReplaceTempView("collection_payment_method")

# --- Previous DCollection: carry-forward for allocation fields only ---
prev_dcollection = spark.sql(f"""
    SELECT DCollectionKey,
           AllocationAgent AS prev_AllocationAgent,
           AllocationTeam AS prev_AllocationTeam,
           AllocationChannel AS prev_AllocationChannel,
           AllocationExternalCompany AS prev_AllocationExternalCompany,
           AssignmentProcess AS prev_AssignmentProcess,
           ProcessType AS prev_ProcessType,
           AssignmentProcessedFlag AS prev_AssignmentProcessedFlag
    FROM `{env}-cdh`.derivativeactive.DCollection
    WHERE BusinessDate = (
        SELECT MAX(BusinessDate)
        FROM `{env}-cdh`.derivativeactive.DCollection
        WHERE BusinessDate < '{run_date}'
    )
""")
prev_dcollection.createOrReplaceTempView("prev_dcollection")

# COMMAND ----------

# DBTITLE 1,Collection
Collection = spark.sql(f""" SELECT  ExposureKey,
                                    CollectionId,
                                    SourceSystem,
                                    PortfolioKey,
                                    PortfolioName,
                                    CollectionKey,
                                    CollectionSourceKey,
                                    CepalKey,
                                    Amount,
                                    RecordDate,
                                    ActualDate,
                                    MatchedDate,
                                    ReverseCode,
                                    Currency,
                                    Comments,
                                    SplitFlag,
                                    DiasNumber,
                                    ExternalPaymentFlag,
                                    ITReason,
                                    FinalReason,
                                    DoNotUpdateFlag,
                                    PaymentFileChannel,
                                    TanCode,
                                    BranchCode,
                                    CancellationDate,
                                    TaxNumber,
                                    CDI,
                                    BusinessDate,
                                    DiasChannel,
                                    loanbankcode
                                    FROM `{env}-cdh`.primary.collection
                                        WHERE BusinessDate = '{run_date}'
                                      """)

Collection.createOrReplaceTempView("Collection")

# COMMAND ----------

# DBTITLE 1,Build target_table_dcollection_pre
target_table_dcollection_pre = spark.sql(f"""
    select 
        CollectionId,
        SourceSystem,
        PortfolioKey,
        c.PortfolioName,
        c.CollectionKey,
        CollectionSourceKey,
        CepalKey,
        Amount,
        RecordDate,
        ActualDate,
        cast(MatchedDate as date) as MatchedDate,
        ReverseCode,
        Currency,
        ca.CA_AgentName as AllocationAgent,
        ca.CA_TeamName as AllocationTeam,
        ca.CA_ChannelName as AllocationChannel,
        ca.CA_ExternalChannelName as AllocationExternalCompany,
        CASE WHEN ca.Puid IS NOT NULL THEN True ELSE False END as AssignmentProcessedFlag, -- AssignmentProcessedFlag: derived from CA existence
        ca.CA_AssignmentProcess,
        ca.CA_ProcessType,
        Comments,
        SplitFlag,
        DiasNumber,
        ExternalPaymentFlag,
        ITReason,
        FinalReason,
        DoNotUpdateFlag,
        PaymentFileChannel,
        TanCode,
        BranchCode,
        CancellationDate,
        TaxNumber,
        CDI,
        c.BusinessDate,
        DiasChannel,
        case when substr(loanbankcode,1,3) = Prefix then loanbankcode else concat(Prefix, loanbankcode) end exposurekeyFull,
        loanbankcode,
        cpt.PaymentTypeCode,
        cpt.PaymentType,
        cpt.PaymentTypeEng,
        cpt.PaymentTypeGroup,
        cpt.IsPaymentFlag,
        cpm.PaymentMethod
    from Collection c
    join portfolio on portfolio.PortfolioName = c.PortfolioName
    -- Collections Assignment: allocation fields
    left join collections_assignment ca on ca.Puid = c.CollectionKey --and ca.UnitCode = c.PortfolioKey
    -- Payment type fields from pre-computed table
    left join collection_payment_type cpt on cpt.CollectionKey = c.CollectionKey
    -- Payment method field from pre-computed table
    left join collection_payment_method cpm on cpm.CollectionKey = c.CollectionKey
    WHERE c.BusinessDate = {run_date!r}
""")
target_table_dcollection_pre.createOrReplaceTempView("target_table_dcollection_pre")

# COMMAND ----------

#primary.assignment perimeter
perimeter1 = [
    {"portfolioName": "euclid", "SourceSystem": "whl"},
]

perimeter2 = [
    {"portfolioName": "domus", "SourceSystem": "qca", "AssignmentTypeCode": "QCAB004"},
    {"portfolioName": "monza", "SourceSystem": "qca", "AssignmentTypeCode": "QCAB004"},
    {"portfolioName": "rhodium", "SourceSystem": "qca", "AssignmentTypeCode": "QCAB004"},
    {"portfolioName": "gaia", "SourceSystem": "qca", "AssignmentTypeCode": "QCAB004"},
    {"portfolioName": "cosmos", "SourceSystem": "qca", "AssignmentTypeCode": "QCAB004"},
    {"portfolioName": "orion", "SourceSystem": "qca", "AssignmentTypeCode": "QCAB004"},
    {"portfolioName": "galaxy", "SourceSystem": "qca", "AssignmentTypeCode": "QCAB004"},
]

conditions_1 = []
conditions_2 = []

for item in perimeter1:
    cond_1 = (
        f"(assignment.portfolioName ilike '%{item['portfolioName']}%' "
        f"AND assignment.SourceSystem = '{item['SourceSystem']}') "
        
    )
    cond_2 =(
       f"(collection.portfolioName ilike '%{item['portfolioName']}%')" 
    )
    
    conditions_1.append(cond_1)
    conditions_2.append(cond_2)

for item in perimeter2:
    cond_1 = (
        f"(assignment.portfolioName ilike '%{item['portfolioName']}%' "
        f"AND assignment.SourceSystem = '{item['SourceSystem']}' "
        f"AND assignment.AssignmentTypeCode = '{item['AssignmentTypeCode']}')"
        
    )
    cond_2 =(
       f"(collection.portfolioName ilike '%{item['portfolioName']}%')" 
    )
    
    conditions_1.append(cond_1)
    conditions_2.append(cond_2)

cond_assignment = " OR ".join(conditions_1)
cond_collection = " OR ".join(conditions_2)
print(cond_assignment)
print(cond_collection)

# COMMAND ----------

calendar = spark.sql(f"""
  select distinct PreviousBusinessDay,CalendarDate 
  from `{env}-cdh`.reference.ref_calendar calendar
  join target_table_dcollection_pre collection on calendar.CalendarDate = collection.actualdate
  where {cond_collection}
""")
calendar.createOrReplaceTempView("calendar")

assignment = spark.sql(f"""
                      select distinct 
                        assignment.casecode,assignment.CDI,assignment.AssignmentProcess,assignment.ProcessType,
                        nullif(assignment.ExternalCompanyFullName,'') as AllocationExternalCompany,
                        nullif(assignment.Channel,'') as AllocationChannel,
                        nullif(assignment.agentfullname,'') as AllocationAgent,
                        nullif(assignment.Team,'') as AllocationTeam,
                        case when substr(assignment.exposurekey,1,3) = Prefix then assignment.exposurekey else concat(Prefix, assignment.exposurekey) end exposurekeyFull,
                        assignment.PortfolioName,
                        assignment.SourceSystem,
                        calendar.CalendarDate
                      from `{env}-cdh`.primary.assignment
                      join portfolio on portfolio.portfolioName = assignment.PortfolioName
                      join calendar on calendar.PreviousBusinessDay = assignment.BusinessDate
                      where {cond_assignment}
                       """)
assignment.createOrReplaceTempView("assignment")

# COMMAND ----------

# Build join conditions outside for clarity and reuse
join_dexposure = [col("c.exposurekeyFull") == col("de.ExposureKey")]
join_assignment_qca = [
    col("a.SourceSystem") == lit("qca"),
    col("c.actualdate") == col("a.calendarDate"),
    col("de.ExposureKey") == col("a.exposurekeyFull"),
    when(col("de.CaseCode").isNotNull(), col("de.CaseCode") == col("a.CaseCode")).otherwise(
        col("a.CDI") == coalesce(col("c.CDI"), col("de.CDI"))
    )
]


join_assignment_whl = [
    col("awhl.SourceSystem") == lit("whl"),
    col("c.actualdate") == col("awhl.calendarDate"),
    col("awhl.CDI") == coalesce(col("c.CDI"), col("de.CDI")),
]

target_table_dcollectionid_primary_key_fields = [
    target_table_dcollection_pre.CollectionId,
]

target_table_dcollectionKey_primary_key_fields = [
    target_table_dcollection_pre.CollectionKey,
]

target_table_dcollection = (
    target_table_dcollection_pre.withColumn(
        "DCollectionId",
        sha2(
            concat_ws(";", *target_table_dcollectionid_primary_key_fields),
            256,
        ),
    ).withColumn(
        "DCollectionKey",
        concat_ws(";", *target_table_dcollectionKey_primary_key_fields)
    ).withColumn(
        "IngestionTimestamp",
        lit(from_utc_timestamp(current_timestamp(), "Europe/Athens"))
    )

)

# Join with previous DCollection for carry-forward fallback
join_prev = [col("c.DCollectionKey") == col("prev.DCollectionKey")]

# Euclid RE Collateral Liquidation exclusion: nullify allocation fields
is_euclid_liquidation = (
    (col("c.PortfolioName") == "EUCLID") &
    col("c.PaymentTypeGroup").ilike("%RE Collateral Liquidation%")
)

result_df = (
    target_table_dcollection.alias("c")
    .join(dexposure.alias("de"), join_dexposure, "left")
    .join(assignment.alias("a"), join_assignment_qca, "left")
    .join(assignment.alias("awhl"), join_assignment_whl, "left")
    .join(prev_dcollection.alias("prev"), join_prev, "left")
    .select(
        col("c.DCollectionId"),
        col("c.SourceSystem"),
        col("c.PortfolioKey"),
        col("c.PortfolioName"),
        col("c.IngestionTimestamp"),
        col("c.DCollectionKey"),
        col("c.CollectionSourceKey"),
        col("de.ExposureKey"),
        col("c.loanbankcode").alias("SourceExposureKey"),
        col("c.CepalKey"),
        col("c.Amount"),
        col("c.RecordDate"),
        col("c.ActualDate"),
        col("c.MatchedDate"),
        col("c.ReverseCode"),
        col("c.Currency"),
        # Agent: CA/today > assignment > previous DCollection
        when(is_euclid_liquidation, None).otherwise(
            when(col("c.AssignmentProcessedFlag").cast("int") == 1, col("c.AllocationAgent"))
            .otherwise(coalesce(col("a.AllocationAgent"),col("awhl.AllocationAgent"), col("prev.prev_AllocationAgent")))
        ).alias("AllocationAgent"),
        # Team: CA/today > assignment > previous DCollection
        when(is_euclid_liquidation, None).otherwise(
            when(col("c.AssignmentProcessedFlag").cast("int") == 1, col("c.AllocationTeam"))
            .otherwise(coalesce(col("a.AllocationTeam"),col("awhl.AllocationTeam"), col("prev.prev_AllocationTeam")))
        ).alias("AllocationTeam"),
        # Channel: CA/today > assignment > previous DCollection
        when(is_euclid_liquidation, None).otherwise(
            when(col("c.AssignmentProcessedFlag").cast("int") == 1, col("c.AllocationChannel"))
            .otherwise(coalesce(col("a.AllocationChannel"),col("awhl.AllocationChannel"), col("prev.prev_AllocationChannel")))
        ).alias("AllocationChannel"),
        # External Company: CA/today > assignment > previous DCollection
        when(is_euclid_liquidation, None).otherwise(
            when(col("c.AssignmentProcessedFlag").cast("int") == 1, col("c.AllocationExternalCompany"))
            .otherwise(coalesce(col("a.AllocationExternalCompany"),col("awhl.AllocationExternalCompany"), col("prev.prev_AllocationExternalCompany")))
        ).alias("AllocationExternalCompany"),
        # AssignmentProcess: assignment > previous DCollection
        when(col("c.AssignmentProcessedFlag").cast("int") == 1, col("c.CA_AssignmentProcess"))
            .otherwise(coalesce(col("a.AssignmentProcess"), col("prev.prev_AssignmentProcess")))
            .alias("AssignmentProcess"),
        when(col("c.AssignmentProcessedFlag").cast("int") == 1, col("c.CA_ProcessType"))
            .otherwise(coalesce(col("a.ProcessType"), col("prev.prev_ProcessType")))
            .alias("ProcessType"),
        col("c.Comments"),
        col("c.Comments"),
        col("c.SplitFlag"),
        col("c.DiasNumber"),
        col("c.ExternalPaymentFlag"),
        col("c.ITReason"),
        col("c.FinalReason"),
        col("c.DoNotUpdateFlag"),
        # Payment type fields from collectionpaymenttype
        col("c.PaymentTypeCode"),
        col("c.PaymentType"),
        col("c.PaymentTypeEng"),
        col("c.PaymentTypeGroup"),
        # AssignmentProcessedFlag: true if CA matched OR prev had it true
        when(col("c.AssignmentProcessedFlag").cast("int") == 1, lit(True))
            .otherwise(
                when(col("a.AllocationAgent").isNotNull(), lit(True))
                .otherwise(coalesce(col("prev.prev_AssignmentProcessedFlag"), lit(False)))
            ).alias("AssignmentProcessedFlag"),
        col("c.PaymentFileChannel"),
        col("c.TanCode").alias("OpapTanCode"),
        col("c.BranchCode").alias("OpapBranchCode"),
        col("c.CancellationDate").alias("OpapCancellationDate"),
        col("c.TaxNumber").alias("OpapTaxRegistrationNumber"),
        col("c.CDI").alias("PayerCDI"),
        col("de.CDI").alias("PrimaryOwnerCDI"),
        col("c.DiasChannel"),
        col("c.IsPaymentFlag"),
        col("c.PaymentMethod"),
        col("c.BusinessDate"),
    )
)

# COMMAND ----------

# DBTITLE 1,Override Allocation Fields from External File
ext_allocation = spark.sql(f"""
    WITH cte AS (
        SELECT DCollectionKey,
               Agent,
               Team,
               Channel,
               ExternalCompanie,
               AssignmentProcess,
               ROW_NUMBER() OVER (
                   PARTITION BY DCollectionKey
                   ORDER BY BusinessDate DESC
               ) AS rn
        FROM `{env}-cdh`.externalfiles.extassignment_collections
        WHERE BusinessDate <= '{run_date}'
    )
    SELECT DCollectionKey,
           Agent AS ext_AllocationAgent,
           Team AS ext_AllocationTeam,
           Channel AS ext_AllocationChannel,
           ExternalCompanie AS ext_AllocationExternalCompany,
           AssignmentProcess AS ext_AssignmentProcess
    FROM cte
    WHERE rn = 1
""")

final_df = (
    result_df.alias("r")
    .join(ext_allocation.alias("ext"),
          col("r.DCollectionKey") == col("ext.DCollectionKey"), "left")
    .select(
        col("r.DCollectionId"),
        col("r.SourceSystem"),
        col("r.PortfolioKey"),
        col("r.PortfolioName"),
        col("r.IngestionTimestamp"),
        col("r.DCollectionKey"),
        col("r.CollectionSourceKey"),
        col("r.ExposureKey"),
        col("r.SourceExposureKey"),
        col("r.CepalKey"),
        col("r.Amount"),
        col("r.RecordDate"),
        col("r.ActualDate"),
        col("r.MatchedDate"),
        col("r.ReverseCode"),
        col("r.Currency"),
        # Allocation fields: when the DCollectionKey exists in
        # externalfiles.extassignment_collections take the external file value,
        # otherwise fall back to the value computed in result_df.
        when(col("ext.DCollectionKey").isNotNull(), col("ext.ext_AllocationAgent"))
            .otherwise(col("r.AllocationAgent")).alias("AllocationAgent"),
        when(col("ext.DCollectionKey").isNotNull(), col("ext.ext_AllocationTeam"))
            .otherwise(col("r.AllocationTeam")).alias("AllocationTeam"),
        when(col("ext.DCollectionKey").isNotNull(), col("ext.ext_AllocationChannel"))
            .otherwise(col("r.AllocationChannel")).alias("AllocationChannel"),
        when(col("ext.DCollectionKey").isNotNull(), col("ext.ext_AllocationExternalCompany"))
            .otherwise(col("r.AllocationExternalCompany")).alias("AllocationExternalCompany"),
        when(col("ext.DCollectionKey").isNotNull(), col("ext.ext_AssignmentProcess"))
            .otherwise(col("r.AssignmentProcess")).alias("AssignmentProcess"),
        # CorectionMechanism: True when the record exists in
        # externalfiles.extassignment_collections (i.e. the external correction
        # mechanism was applied), otherwise False.
        col("ext.DCollectionKey").isNotNull().alias("CorectionMechanism"),
        col("r.ProcessType"),
        col("r.Comments"),
        col("r.SplitFlag"),
        col("r.DiasNumber"),
        col("r.ExternalPaymentFlag"),
        col("r.ITReason"),
        col("r.FinalReason"),
        col("r.DoNotUpdateFlag"),
        col("r.PaymentTypeCode"),
        col("r.PaymentType"),
        col("r.PaymentTypeEng"),
        col("r.PaymentTypeGroup"),
        col("r.AssignmentProcessedFlag"),
        col("r.PaymentFileChannel"),
        col("r.OpapTanCode"),
        col("r.OpapBranchCode"),
        col("r.OpapCancellationDate"),
        col("r.OpapTaxRegistrationNumber"),
        col("r.PayerCDI"),
        col("r.PrimaryOwnerCDI"),
        col("r.DiasChannel"),
        col("r.IsPaymentFlag"),
        col("r.PaymentMethod"),
        col("r.BusinessDate"),
    )
)

# COMMAND ----------

if unity == "True":
    final_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path) # in order to write in delta_lake
else: 
    final_df.write \
        .format("delta") \
        .mode("append") \
        .save(derivative_table_path) # in order to write in delta_lake 
