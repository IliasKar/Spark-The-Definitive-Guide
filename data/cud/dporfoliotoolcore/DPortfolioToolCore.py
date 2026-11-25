# Databricks notebook source
import logging

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Import Libaries and Functions

# COMMAND ----------

import boto3
import logging
from pyspark.sql.functions import coalesce, concat_ws, substring, when, col, regexp_replace, trim
from pyspark.sql import DataFrame

import delta_table_creator, extract, transform
from delta import DeltaTable  # type: ignore

# Import the module
import sys
sys.path.append("../aws_utils")

import aws_utils
# Get Environment Prefix
env = aws_utils.get_env_prefix()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create widgets with default values. Change them if needed from UI
# MAGIC * run_date : '2024-01-31'
# MAGIC * unity : True | False
# MAGIC
# MAGIC In case of Unity Catalog, the appropriate cluster should be attached

# COMMAND ----------

# dbutils.widgets.text("run_date", '2024-01-31')
# dbutils.widgets.text("table_name", 'DPortfolioToolCore')
# dbutils.widgets.text("unity", 'True')
# dbutils.widgets.text("utp_date", '2024-01-17')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get the values of widgets and initialize the environment variable
# MAGIC Ensure that widgets are assigned the wanted values before proceeding with the next step.

# COMMAND ----------

# Takes the values of widgets
run_date = dbutils.widgets.get("run_date") 
# table_name = dbutils.widgets.get("table_name") 
table_name = 'DPortfolioToolCore'
unity = dbutils.widgets.get("unity")

PreviousMonthLastDate = spark.sql(f"""
    SELECT PreviousMonthLastDate
    FROM `{env}-cdh`.reference.ref_calendar
    WHERE CalendarDate = '{run_date}' 
""").collect()[0][0]

# """
# Takes the s3 parameter value from ssm

# ssm_client = boto3.client("ssm", region_name="eu-central-1") # Initialize the SSM client

# response = ssm_client.get_parameter(
#     Name='/cfn/a360/matching/conf_s3_path',
#     WithDecryption=False
# )

# s3_path = response['Parameter']['Value']
# """

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Initialize the schema retrieved from Redis

# COMMAND ----------

# DBTITLE 0,SCHEMA INITIALIZATION
schema = (
"BusinessDate date,"
"BridgePerimeter string,"
"BalanceL2 decimal(19,2),"
"SubsidyCategory string,"
"BridgeEndOfMonthSubsidyDate string,"
"BridgeProgrammBridgeIBridgeII string,"
"BridgeStatus string,"
"ECAAssetClass string,"
"IFRSGrossCarryingAmt decimal(19,4),"
"EBAStatusPreviousMonth string,"
"EBAStatus string,"
"HFS string,"
"ProbationDateExit date,"
"LTV decimal(19,10),"
"MaterialBucket int,"
"NonMaterialBucket int,"
"StateGuaranteeFlag string,"
"CurrentMonthRPC int,"
"PreviousMonthsRPC int,"
"SSE string,"
"DeceasedFlag boolean,"
"StepUpModificationDate date,"
"SplitModificationDate date,"
"HaircutModificationDate date,"
"DPOModificationDate date,"
"AnchorModificationDate date,"
"PaymentJumpExtensionModificationDate date,"
"OverdraftProduct boolean,"
"AccountUnder11Dpds boolean,"
"AccountsWithAllBorrowersDeceased boolean,"
"AFM string,"
"CIF string,"
"Contract string,"
"BU string,"
"DenouncedStatus string,"
"DebtAmount decimal(19,4),"
"DueDebtAmount decimal(26,4),"
"QueueDescription string,"
"Product string,"
"SPVFlag string,"
"DaysPastDue int,"
"OperationalBucket int,"
"SoMOperationalBucket int,"
"InterestRate decimal(19,6),"
"AffluentGoldBusiness string,"
"InstallmentAmount decimal(19,2),"
"SubProductCode int,"
"SubProduct string,"
"CurrentMonthPromiseToPay int,"
"CurrentMonthPromiseToPayAmount int,"
"PreviousMonthsPromiseToPay int,"
"PrevMonthsPromiseToPayAmount int,"
"CurrentMonthPositiveToModify int,"
"PreviousMonthsPositiveToModify int,"
"SpecialModifications string,"
"SplitModified string,"
"HaircutModified string,"
"DPOModified string,"
"AnchorModified string,"
"PaymentJumpExtensionModification string,"
"Calypso string,"
"SpecialModificationsDate date,"
"Staff string,"
"Wholesale boolean,"
"OCWCustomers boolean,"
"WatchList string,"
"GovernmentGuaranteed boolean,"
"SecuredAccount boolean,"
"SecuredCustomer boolean,"
"CustomerWholesale boolean,"
"FinalBalance decimal(19,4),"
"MLTAmount decimal(19,4),"
"TotalBalance decimal(19,4),"
"L3869Status string,"
"AutoLoan boolean,"
"ConnectedBorrowerFlag boolean,"
"Eggyhmena boolean,"
"BankruptcyFlag boolean,"
"PrebankruptcyFlag boolean,"
"ContractCurrency string,"
"PrivateBanking boolean,"
"SpecialQueues string,"
"CollateralValue int,"
"CustomerType string,"
"COAFlag string,"
"AssignmentSegmentDetailed string,"
"AssignmentChannel string,"
"AccWithCollateralRE boolean,"
"EBAStrategyCalc string,"
"EBAStrategyCustomerCalc string,"
"InflowTargetDateCalc date,"
"EligibleForSettlement string,"
"UTPReason string,"
"SoMUnionFileExistenceFlag boolean,"
"UTPTypeCode int,"
"DODDPDStartDate date,"
"DODDPDEndDate date,"
"PreviousMonthsContacts int,"
"LastModification string,"
"CountOfModifications int,"
"LegalProcessStatusLevel1 string,"
"LegalProcessStatusLevel2 string,"
"OperationalLegalProcessStatus string,"
"LegalProcessStatusChangeDate date,"
"Account string,"
"StepUpModified string,"
"OnBalance decimal(19,2),"
"OffBalance decimal(19,2),"
"AccountStatusDate date,"
"Customer string,"
"LastModificationDate date,"
"MonthsSinceLastModification int,"
"EligibleForHaircut string,"
"NotInITPool string,"
"FailUAT string,"
"ContractOnBalance decimal(19,2),"
"ContractGBVBalance double,"
"ContractOnOffBalance decimal(19,2),"
"CustomerGBV double,"
"CustomerOnOffBalance decimal(19,2),"
"CustomerOn decimal(19,2),"
"MaxCustomerOperationalBucket int,"
"MinCustomerOperationalBucket int,"
"AccountLowBalance decimal(19,2),"
"CustomerLowBalance decimal(19,2),"
"AccountLowDebt boolean,"
"RiskBucketCalc int," 
"MaximumCustomersCalculatedRiskBucket int,"
"MinimumCustomersCalculatedRiskBucket int,"
"MaximumCustomerRiskBucket int,"
"MinimumCustomerRiskBucket int,"
"EBAStatusCalc string,"
"FPLUnderProbationFlag string,"
"EBAStrategy string,"
"CustomerEBAStrategy string,"
"InflowTargetDate date,"
"NotAllowedToCure string,"
"PaymentJumpCategory string,"
"PaymentJumpMonth string,"
"PaymentJumpMonthDate date,"
"PaymentJumpExtension string,"
"PaymentJumpCriteriaNotMet string,"
"ActiveApplicationDate date,"
"ActiveApplicationCreditReviewStepDate date,"
"ActiveApplicationChannel string,"
"ActiveLegalSettlementsFlag boolean,"
"CurrentMonthPayments double,"
"Previous12MonthsPayments double,"
"CurrentMonthContacts int," 
"CurrentMonthContactedAccount string,"
"CurrentMonthContactedCustomer string,"
"PreviousMonthsContactedAccount string,"
"PreviousMonthsContactedCustomer string,"
"CurrentMonthRPCAccount string,"
"CurrentMonthRPCCustomer string,"
"PreviousMonthsRPCAccount string,"
"PreviousMonthsRPCCustomer string,"
"CurrentMonthPromiseToPayAccount string,"
"CurrentMonthPromiseToPayCustomer string,"
"PreviousMonthsPromiseToPayAccount string,"
"PreviousMonthsPromiseToPayCustomer string,"
"CurrentMonthPositiveToModifyAccount string,"
"CurrentMonthPositiveToModifyCustomer string,"
"PreviousMonthsPositiveToModifyAccount string,"
"PreviousMonthsPositiveToModifyCustomer string,"
"ActiveApplicationNumber string,"
"SecuredContract boolean," 
"SecuredUnsecuredAccountWithCollateralRE int,"
"SecuredUnsecuredContractWithCollateralRE int," 
"SecuredUnsecuredCustomerWithCollateralRE int,"
"eap int,"
"eapafm string,"
"CustomerWatchlist string,"
"Settlements string,"
"TreatmentBranch string,"
"AgentRM string,"
"AgentRMFullName string,"
"Agency string,"
"SavedBalance string," 
"SavedFlag string," 
"SavedDate date,"
"SavedReason string," 
"InflowType string,"
"FinalOperationalSaved string,"
"FinalOperationalSavedDate date,"
"FinalRiskSaved string,"
"FinalRiskSavedDate string,"
"OperationalMLSaved string,"
"OperationalLMSSaved string,"
"OperationalCMSSaved string,"
"OperationalSBSaved string,"
"RiskSaved string,"
"OutflowSaved string,"
"PotentialPoolFNPLCurings boolean,"
"RelationType string,"
"ObjectiveStatusCode string,"
"ExposureSegmentation string,"
"NonCoreFlag string,"
"ActiveApplicationStatus string,"
"CustomerStatusN3869 string,"
"CourtDate date,"
"LastModificationDetailed string,"
"FutureAuctionDate date,"
"PastAuctionDate date,"
"PastAuctionResult string,"
"AttributionCOA string,"
"LowLTVModified boolean,"
"SnapshotDate date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create - Delete Delta Table

# COMMAND ----------

partition_columns = ["BusinessDate"]
derivative_table_path = f"`{env}-cdh`.derivativeactive.{table_name}"
 
# ================== Create Delta Table ==================
logging.info("Creating Delta Table...")
if spark.catalog.tableExists(derivative_table_path) is False:
    create_delta_table(
        spark,
        derivative_table_path,
        partition_columns,
        schema,  # type: ignore
    )
else:
    logging.info("Table already exists.")
 
 
# ================== Delete From Delta Table ==================
 
transform.delete_from_unity_table(
    spark,
    derivative_table_path,
    run_date,  # type: ignore
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create wanted Delta Tables with selected fields & Temporary Views for each delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extunionfile

# COMMAND ----------

extunionfile = spark.sql(
f'''
SELECT 
    IFRSGrossCarryingAmount,
    EBAStatus,
    FPLUnderProbationFlag,
    LTV,
    HFSFlag,
    ProbationStartDate,
    ECAAssetClass,
    StateGuaranteeFlag,
    ExposureSegmentation,
    NonCoreFlag,
    UTPIndicator as UTPTypeCode,
    UTPIndicator as UTPType,
    ExposureKey,
    AgreementKey,
    ProductType as ProductCode,
    ExposureSourceSystemKey,
    AcMaintenServiceBranch as AcMaintenServiceBranchCode,
    BucketCategoryNMateriality,
    BucketCategoryWithMateriality,
    CollateralCapTotalTangible,
    BorrowerKey,
    ExposureNumber,
    A.SnapshotDate
FROM  `{env}-cdh`.externalfiles.extunionfile A
INNER JOIN (SELECT SnapshotDate, MAX(IngestionTimestamp) IngestionTimestamp FROM `{env}-cdh`.externalfiles.extunionfile
            GROUP BY SnapshotDate
           )B ON A.SnapshotDate=B.SnapshotDate AND A.IngestionTimestamp=B.IngestionTimestamp
'''
)

extunionfile.createOrReplaceTempView("extunionfile")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dod_dpd_fcy

# COMMAND ----------

dod_dpd_fcy = spark.sql(
f'''
SELECT 
    AC_CODE as ExposureKey,
    TO_DATE(DOD_DPD_CNTER_STRT_DT, 'd/M/yyyy') as DODDPDStartDate,
    TO_DATE(DOD_DPD_CNTER_END_DT, 'd/M/yyyy') as DODDPDEndDate,
    AC_SRC_STM_CODE as ExposureSourceSystem,
    BusinessDate
FROM  `{env}-cdh`.externalfiles.extdod_dpd_fcy
'''
)

dod_dpd_fcy.createOrReplaceTempView("dod_dpd_fcy")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extlpfxb364

# COMMAND ----------

extlpfxb364 = spark.sql(
f'''
SELECT 
    RestructuringType,
    OldExposureKey,
    RestructuringProductKey,
    NewExposureKey,
    CASE 
        -- If the value matches the format 'YYYY-MM-DD HH:mm:ss.0'
        WHEN SUBSTRING(RestructuringDate, 5, 1) = '-' THEN try_cast(RestructuringDate AS date)
        -- If the value matches the format 'DD/MM/YYYY'
        WHEN SUBSTRING(RestructuringDate, 3, 1) = '/' THEN TO_DATE(RestructuringDate, 'dd/MM/yyyy')
        -- For any other format, assuming it's already in the desired format
    END AS RestructuringDate,
    RestructuringTypeCode,
    AgreementKey,
    BusinessDate
FROM  `{env}-cdh`.externalfiles.extlpfxb364
'''
)

extlpfxb364.createOrReplaceTempView("extlpfxb364")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### underlaw

# COMMAND ----------

underlaw = spark.sql(
f'''
SELECT 
    UnderLawType,
    CalcLawStatus,
    CDI,
    CourtOutcome,
    CourtDate,
    SourceSystem,
    BusinessDate
FROM   `{env}-cdh`.derivative.dunderlaw
WHERE BusinessDate = {run_date!r}
'''
)

underlaw.createOrReplaceTempView("underlaw")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### assignment

# COMMAND ----------

assignment = spark.sql(
f'''
SELECT 
    ExposureKey,
    ChannelCode,
    AgentPartyKey,
    PartyKey,
    AssignmentType,
    AgentLoginName,
    ExternalCompanyPartyKey,
    channel as AttributionChannel,
    SourceSystem,
    BusinessDate
FROM  `{env}-cdh`.primary.assignment
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'qca'
'''
)

assignment.createOrReplaceTempView("assignment")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### firmstructure

# COMMAND ----------

firmstructure = spark.sql(
f'''
SELECT 
    EmployeeUsername,
    EmployeePartyKey,
    EmployeeCategory,
    EmployeeUserType,
    EmployeeFirstName,
    EmployeeLastName,
    EmployeeFullName,
    SourceSystem,
    BusinessDate
FROM  `{env}-cdh`.primary.firmstructure
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'qca'
'''
)

firmstructure.createOrReplaceTempView("firmstructure")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extreportunified

# COMMAND ----------

reportunified = spark.sql(
f'''
SELECT 
    ExposureKey,
    ApplicationNumber,
    FirstDateReceiptValuation,
    CAST(SUBSTRING(ReceiptDateCurrentStep, 1, 10) AS DATE) AS ReceiptDateCurrentStep ,
    null as CommonApplicationStep,
    Channel,
    UnifiedApplicationStatus,
    BusinessDate
FROM  `{env}-cdh`.externalfiles.extreport_unified_cl
WHERE BusinessDate >=DATE_TRUNC('MONTH', DATE({run_date!r})) and BusinessDate <ADD_MONTHS(DATE_TRUNC('MONTH', DATE({run_date!r})),1) --AN 20250703
UNION ALL 
SELECT 
    ExposureKey,
    ApplicationNumber,
    FirstDateReceiptValuation,
    CAST(SUBSTRING(CurrentStepDate , 1, 10) AS DATE)  as ReceiptDateCurrentStep,
    CommonApplicationStep,
    Channel,
    UnifiedApplicationStatus,
    BusinessDate
FROM  `{env}-cdh`.externalfiles.extreport_unified_ml
WHERE BusinessDate >=DATE_TRUNC('MONTH', DATE({run_date!r})) and BusinessDate <ADD_MONTHS(DATE_TRUNC('MONTH', DATE({run_date!r})),1) --AN 20250703
UNION ALL 
SELECT 
    ExposureKey,
    ApplicationNumber,
    FirstDateReceiptValuation,
    DATE_FORMAT(TO_DATE(CurrentStepReceiptDate,'dd/MM/yyyy'),'yyyy-MM-dd') AS ReceiptDateCurrentStep,
    Description as CommonApplicationStep,
    Channel,
    UnifiedApplicationStatus,
    BusinessDate
FROM  `{env}-cdh`.externalfiles.extreport_unified_sb
WHERE BusinessDate >=DATE_TRUNC('MONTH', DATE({run_date!r})) and BusinessDate <ADD_MONTHS(DATE_TRUNC('MONTH', DATE({run_date!r})),1) --AN 20250703
'''
)

reportunified.createOrReplaceTempView("reportunified")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### lsbr23t

# COMMAND ----------

lsbr23t = spark.sql(
f'''
SELECT 
    ExposureKey,
    CollateralCategory,
    SourceSystem as ExposureSourceSystem,
    BusinessDate
FROM  `{env}-cdh`.externalfiles.extlsbr23t
WHERE BusinessDate = {run_date!r}
'''
)

lsbr23t.createOrReplaceTempView("lsbr23t")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extfasttrack

# COMMAND ----------

extfasttrack = spark.sql(
f'''
SELECT 
    FastTrackFlag,
    Exposurekey,
    BusinessDate
FROM  `{env}-cdh`.externalfiles.extfasttrack
WHERE BusinessDate = {run_date!r}
'''
)

extfasttrack.createOrReplaceTempView("extfasttrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ref_calendar

# COMMAND ----------

ref_calendar = spark.sql(
f'''
SELECT 
    CalendarDate,
    CurrentMonthLastWorkingDate,
    PreviousMonthLastWorkingDate,
    BusinessDayOfMonth
FROM  `{env}-cdh`.reference.ref_calendar
'''
)

ref_calendar.createOrReplaceTempView("ref_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### core_queusmapping

# COMMAND ----------

core_queusmapping = spark.sql(
f'''
SELECT DISTINCT
    SavedQueueDescription,
    ReferenceDate,
    CourseOfAction,
    AssignmentSegmentDetailed
FROM  `{env}-cdh`.reference.corequeuesmapping
'''
)

core_queusmapping.createOrReplaceTempView("core_queusmapping")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### payment

# COMMAND ----------

payment = spark.sql(
f'''
SELECT 
    ExposureKey,
    PaymentActualDate,
    PaymentAmount,
    PortfolioName
FROM  `{env}-cdh`.primary.payment
WHERE PortfolioName = 'Core Retail'
AND BusinessDate = {run_date!r}
'''
)

payment.createOrReplaceTempView("payment")

# COMMAND ----------



lpsagreementdebtor = spark.sql(
f'''
SELECT 
    AgreementKey,
    CDI,
    OperationalLegalStatusLevel3,
    BusinessDate
FROM  `{env}-cdh`.derivative.dlegalprocessstatusagreementdebtor
WHERE   BusinessDate = {run_date!r}
'''
)

lpsagreementdebtor.createOrReplaceTempView("lpsagreementdebtor")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### exposure

# COMMAND ----------

exposure_current = spark.sql(
f'''
SELECT 
    PartyKey,
    SourceSystem,
    ExposureSourceSystemKey,
    AgreementKey,
    ExposureKey,
    ExposureStatus,
    OnBalance,
    OffBalance,
    Bucket,
    PartyTaxRegistrationNumber,
    CDI,
    CaseCode,
    ProductKey,
    IsGuaranteeFlag,
    PartyRelationshipType,
    AssetSegment,
    OutstandingTotalBalance,
    OutstandingTotalBalanceMLT,
    DueAmount,
    PortfolioName,
    LastBillingdate,
    AllBorrowersDeceasedFlag,
    DaysPastDueWithMateriality AS DaysPastDue,
    ObjectiveStatus,
    ObjectiveStatusCode,
    ExposureStatusDate,
    TreatmentBranch,
    NextInstallmentAmount,
    SPV,
    BranchCode,
    Currency,
    InterestRate,
    ObjectiveStatusdate,
    BusinessDate,
    COALESCE(NULLIF(CASE WHEN PartyTaxRegistrationNumber RLIKE '^[0]+$' THEN '' ELSE PartyTaxRegistrationNumber END , ''),CDI) AS Customer

FROM  `{env}-cdh`.primary.exposure
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'qca' 
        AND PortfolioName = 'Core Retail'
'''
)

exposure_current.createOrReplaceTempView("exposure_current")

# COMMAND ----------

exposure_inactive = spark.sql(
f'''
SELECT 
    PartyKey,
    SourceSystem,
    ExposureSourceSystemKey,
    AgreementKey,
    ExposureKey,
    ExposureStatus,
    null as OnBalance,
    null as OffBalance,
    null as Bucket,
    PartyTaxRegistrationNumber,
    CDI,
    CaseCode,
    ProductKey,
    IsGuaranteeFlag,
    PartyRelationshipType,
    AssetSegment,
    null as OutstandingTotalBalance,
    null as OutstandingTotalBalanceMLT,
    null as DueAmount,
    PortfolioName,
    null as LastBillingdate,
    AllBorrowersDeceasedFlag,
    null as DaysPastDue,
    ObjectiveStatus,
    ObjectiveStatusCode,
    ExposureStatusDate,
    TreatmentBranch,
    NextInstallmentAmount,
    SPV,
    BranchCode,
    Currency,
    InterestRate,
    null as ObjectiveStatusdate,
    {run_date!r} as BusinessDate,
    COALESCE(NULLIF(CASE WHEN PartyTaxRegistrationNumber RLIKE '^[0]+$' THEN '' ELSE PartyTaxRegistrationNumber END , ''),CDI) AS Customer

FROM  (SELECT *, RANK() OVER (partition by exposurekey,cdi ORDER BY businessdate desc) as rank_businessdate 
        FROM  `{env}-cdh`.primary.exposure
        WHERE   BusinessDate >= (SELECT DISTINCT PreviousMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r}) --last_day(add_months({run_date!r},-1)) --ΙΒ 11022026
                and businessdate<{run_date!r}
                AND SourceSystem = 'qca' 
                AND PortfolioName = 'Core Retail'
                AND IsActiveFlag= true ) ranked_businessdate
WHERE rank_businessdate =1;
'''
)

exposure_inactive.createOrReplaceTempView("exposure_inactive")

# COMMAND ----------

exposure = spark.sql(
f'''
SELECT * FROM exposure_current
UNION
SELECT exin.* FROM exposure_inactive exin
LEFT JOIN exposure_current ex on ex.exposurekey=exin.exposurekey
WHERE ex.exposurekey is null
'''
)

exposure.createOrReplaceTempView("exposure")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### party

# COMMAND ----------

party = spark.sql(
f'''
SELECT *, RANK() OVER (PARTITION BY PartyKey, CDI ORDER BY Order ASC) Rank
FROM (
    SELECT 
        PartyKey,
        SourceSystem,
        PartyType,
        TaxRegistrationNumber,
        DeceasedFlag,
        CDI,
        Watchlist,
        IsWholeshaleFlag,
        StaffFlag,
        OCWStatus,
        ConnectedBorrowerFlag,
        BusinessDate, 
        1 as Order
    FROM  `{env}-cdh`.primary.party
    WHERE  BusinessDate={run_date!r}
            AND SourceSystem = 'qca' 
    UNION
    SELECT 
        PartyKey,
        SourceSystem,
        PartyType,
        TaxRegistrationNumber,
        DeceasedFlag,
        CDI,
        Watchlist,
        IsWholeshaleFlag,
        StaffFlag,
        OCWStatus,
        ConnectedBorrowerFlag,
        {run_date!r} as BusinessDate,
        2 as Order
    FROM  `{env}-cdh`.primary.party
    WHERE BusinessDate=(SELECT DISTINCT PreviousMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r}) --last_day(add_months({run_date!r},-1)) --IB 20260202
    AND SourceSystem = 'qca' 
) 
'''
)

party.createOrReplaceTempView("party")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dexposure

# COMMAND ----------

dexposure = spark.sql(
f'''
SELECT 
    GBV,
    ExposureKey,
    Bucket,
    SourceSystem,
    BusinessDate
FROM  `{env}-cdh`.derivative.dexposure
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'qca' 
'''
)

dexposure.createOrReplaceTempView("dexposure")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### casecode

# COMMAND ----------

casecode = spark.sql(
f'''
SELECT 
    Queue,
    CustCode,
    DebtCode,
    CaseCodeKey,
    WatchlistFlag,
    QueueCode,
    SourceSystem,
    BusinessDate
FROM  `{env}-cdh`.primary.casecode
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'qca' 
'''
)

casecode.createOrReplaceTempView("casecode")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### settlementexposure

# COMMAND ----------

settlementexposure = spark.sql(
f'''
SELECT 
    SettlementType,
    SettlementStatus,
    SettlementStatusCode,
    BucketCode,
    ExposureKey,
    SourceSystem,
    BusinessDate
FROM  `{env}-cdh`.primary.settlementexposure
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'qca' 
'''
)

settlementexposure.createOrReplaceTempView("settlementexposure")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### realestateresearch

# COMMAND ----------

realestateresearch = spark.sql(
f'''
SELECT 
    OutcomeCode,
    PartyKey,
    RealEstateResearchDate,
    CancelledDate,
    CancelledFlag,
    RealEstateResearchStatusCode,
    SourceSystem,
    BusinessDate
FROM  `{env}-cdh`.primary.realestateresearch
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'qca' 
'''
)

realestateresearch.createOrReplaceTempView("realestateresearch")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### product

# COMMAND ----------

product = spark.sql(
f'''
SELECT 
    ProductCode,
    ProductDescription,
    ProductKey,
    SubProductCode,
    SubProduct,
    BankingUnitCode,
    SourceSystem,
    BusinessDate
FROM  `{env}-cdh`.primary.product
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'qca' 
'''
)

product.createOrReplaceTempView("product")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### interaction

# COMMAND ----------

interaction = spark.sql(
f'''
SELECT 
    ExposureKey,
    InteractionTypeKey,
    PortfolioName,
    PartyKey,
    SourceSystem,
    PromiseToPayAmount,
    InteractionDate,
    isrpcflag,
    ispromiseflag,
    ContactTypeCode,
    BusinessDate
FROM  `{env}-cdh`.primary.get_interactions_by_business_date({run_date!r})
WHERE   SourceSystem = 'qca' 
AND     PortfolioName = 'Core Retail'
'''
)

interaction.createOrReplaceTempView("interaction")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### paymentjumptotalperimeter

# COMMAND ----------

paymentjumptotalperimeter = spark.sql(
f'''
SELECT 
    ExposureKey,
    PaymentJumpMonth,
    BusinessDate
FROM  `{env}-cdh`.externalfiles.extpaymentjumptotalperimeter
WHERE BusinessDate = {run_date!r}
'''
)

paymentjumptotalperimeter.createOrReplaceTempView("paymentjumptotalperimeter")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extkama

# COMMAND ----------

extkama = spark.sql(
f'''
SELECT 
    AccountNo as ExposureKey,
    Bucket1,
    Bucket2,
    Bucket3,
    Bucket4,
    Bucket5,
    Bucket6,
    `Bucket7+`,
    BusinessDate
FROM  `{env}-cdh`.externalfiles.extkama
'''
)

extkama.createOrReplaceTempView("extkama")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extlmsbucketanalysis

# COMMAND ----------

extlmsbucketanalysis = spark.sql(
f'''
SELECT 
    ExposureKey,
    BUCKET1,
    BUCKET2,
    BUCKET3,
    BUCKET4,
    BUCKET5,
    BUCKET6,
    BUCKET7,
    BUCKET8,
    BUCKET9,
    BUCKET10,
    BUCKET11,
    BUCKET12,
    BUCKET13,
    BUCKET14,
    BusinessDate
FROM  `{env}-cdh`.externalfiles.extlms_bucket_analysis
'''
)

extlmsbucketanalysis.createOrReplaceTempView("extlmsbucketanalysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extfile_implementations_gefyra_i_31_01_2023

# COMMAND ----------

extfile_implementations_gefyra_i_31_01_2023 = spark.sql(
f'''
SELECT 
    Account_Number,
    SubsidyCategory,
    Balance_L2_3112,
    Discretion_Start_Date,
    Discretion_End_Month_updated,
    non_core_flag
FROM  `{env}-cdh`.reference.extfile_implementations_gefyra_i_31_01_2023
WHERE   non_core_flag = 'Core'
'''
)

extfile_implementations_gefyra_i_31_01_2023.createOrReplaceTempView("extfile_implementations_gefyra_i_31_01_2023")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extfile_implementations_gefyra_ii_31_01_2023

# COMMAND ----------

extfile_implementations_gefyra_ii_31_01_2023 = spark.sql(
f'''
SELECT 
    Account_Number,
    SubsidyCategory,
    Balance_L2_3112,
    Discretion_Start_Date,
    Discretion_End_Month,
    Non_Core_Flag
FROM  `{env}-cdh`.reference.extfile_implementations_gefyra_ii_31_01_2023
WHERE Non_Core_Flag = 'Core'
'''
)

extfile_implementations_gefyra_ii_31_01_2023.createOrReplaceTempView("extfile_implementations_gefyra_ii_31_01_2023")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extfile_utp

# COMMAND ----------

extfile_utp = spark.sql(
f'''
SELECT 
    AC_CODE,
    UTP_DRTN_CLCN,
    UTP_IND,
    UTP_ID,
    BusinessDate
FROM  `{env}-cdh`.externalfiles.extutp
WHERE   BusinessDate = {run_date!r} --AN 20250703 one file for 2025 03
'''
)

extfile_utp.createOrReplaceTempView("extfile_utp")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### auctionrealestate

# COMMAND ----------

auctionrealestate = spark.sql(
f'''
SELECT 
    AuctionDate, 
    OriginDebtorTaxRegistrationNumber, 
    AuctionStatusCode
FROM `{env}-cdh`.primary.AuctionRealEstate are        
WHERE BusinessDate = {run_date!r}     
''')

auctionrealestate.createOrReplaceTempView("auctionrealestate")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define some subqueries from the final query as temporary views for better readability and maintenance

# COMMAND ----------

# MAGIC %md
# MAGIC ##### securedwithRE_join

# COMMAND ----------

securedwithRE_join = spark.sql(f"""
    select distinct ExposureKey,  SecuredAccount, AccWithCollateralRE, BusinessDate,
           case when SecuredAccount = 1 or AccWithCollateralRE = 1 then 1 else 0 end SecuredUnsecuredAccountWithCollateralRE,
           case when sum(case when SecuredAccount = 1 or AccWithCollateralRE = 1 then 1 else 0 end) over (partition by AgreementKey) >= 1 then 1 else 0 end SecuredUnsecuredContractWithCollateralRE,
           case when sum(case when SecuredAccount = 1 or AccWithCollateralRE = 1 then 1 else 0 end) over (partition by CDI) >= 1 then 1 else 0 end SecuredUnsecuredCustomerWithCollateralRE
           --case when sum(case when SecuredAccount = 1  then 1 else 0 end) over (partition by CDI) >= 1 then 1 else 0 end SecuredCustomer,
           --case when sum(case when SecuredAccount = 1  then 1 else 0 end) over (partition by AgreementKey) >= 1 then 1 else 0 end SecuredContract
    from (
    select e.ExposureKey, e.BusinessDate, e.AgreementKey, e.CDI,
           case when (try_cast(uf.LTV as double) > 0 and try_cast(uf.LTV as double) < 5) then 1 else 0 end as SecuredAccount,
           case when lsbr23t.ExposureKey is not null OR lsbr23t_cms.ExposureKey is not null OR lsbr23t_2.ExposureKey is not null THEN 1 ELSE 0 END AS AccWithCollateralRE
    from exposure e
    left join (
    select ExposureKey, ExposureSourceSystem
    from  lsbr23t
    where CollateralCategory in ('40001','40002','40003','40004','40005','40006','40013','40014','40015','40016','40017','40018'
                                        ,'40019','40020','40021','40022','40023','40026','40027','40028','40029','40030','40031','40032' )
          and BusinessDate = {run_date!r} -- (select max (BusinessDate) from lsbr23t where BusinessDate <= {run_date!r})
              ) lsbr23t on e.ExposureKey = lsbr23t.ExposureKey
    left join (
    select ExposureKey, ExposureSourceSystem
    from  lsbr23t
    where CollateralCategory in ('40001','40002','40003','40004','40005','40006','40013','40014','40015','40016','40017','40018'
                                        ,'40019','40020','40021','40022','40023','40026','40027','40028','40029','40030','40031','40032' )
          and BusinessDate = {run_date!r} -- (select max (BusinessDate) from lsbr23t where BusinessDate <= {run_date!r})
              ) lsbr23t_cms on e.AgreementKey = lsbr23t_cms.ExposureKey and lsbr23t_cms.ExposureSourceSystem = 'CMS'
    left join (
    select ExposureKey, ExposureSourceSystem
    from  lsbr23t
    where CollateralCategory in ('40001','40002','40003','40004','40005','40006','40013','40014','40015','40016','40017','40018'
                                        ,'40019','40020','40021','40022','40023','40026','40027','40028','40029','40030','40031','40032' )
          and BusinessDate = {run_date!r} -- (select max (BusinessDate) from lsbr23t where BusinessDate <= {run_date!r})
               ) lsbr23t_2 on right(concat('0000000000000000',trim(e.ExposureKey)),16) = lsbr23t_2.ExposureKey
    left join extunionfile uf on uf.ExposureNumber = e.ExposureKey
    where e.BusinessDate = {run_date!r} and PartyRelationshipType='Primary Owner'
    --   and uf.BusinessDate = (select max(BusinessDate) from extunionfile where -- to be removed
	-- 	      SnapshotDate =	(SELECT cast(DATEADD(day, -1, DATEADD(quarter, DATEDIFF(quarter, '1900-01-01',{run_date!r}) +1 -1,'1900-01-01')) as date)))
      and uf.SnapshotDate =	(SELECT cast(DATEADD(day, -1, DATEADD(quarter, DATEDIFF(quarter, '1900-01-01',{run_date!r}) +1 -1,'1900-01-01')) as date))
    ) a
""")
    
securedwithRE_join.createOrReplaceTempView("securedwithRE_join")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ebacalc_join

# COMMAND ----------

ebacalc_join = spark.sql(f"""
      select distinct c.ExposureKey,
           c.BorrowerKey,
           c.EBAStatusCalc,
           case when c.EBAStatusCalc in ('PL','FPL','CFPL') then 'PE' when c.EBAStatusCalc in ('NPL','FNPL') then 'NPE' end EBAStrategyCalc,
           case when sum(case when c.EBAStatusCalc in ('PL','FPL','CFPL') then 1 else 0 end) over (partition by c.BorrowerKey) >= 1 then 'PE' else'NPE' end EBAStrategyCustomerCalc,
           case 
            when c.DODDPDEndDate <> '9999-12-31' then NULL
            when c.EBAStatusCalc = 'CFPL' and dateadd(day, 30, c.DODDPDStartDate) >= c.BusinessDate then dateadd(day, 30, c.DODDPDStartDate)
            when c.EBAStatusCalc <> 'CFPL' and dateadd(day, 90, c.DODDPDStartDate) >= c.BusinessDate then dateadd(day, 90, c.DODDPDStartDate)
            else NULL
           end as InflowTargetDateCalc,
           RiskBucketCalc
     from (
     select uf.ExposureNumber as ExposureKey, uf.BorrowerKey, case when b.ExposureKey is null then 0 else b.RiskBucketCalc end as RiskBucketCalc, b.DODDPDStartDate, b.DODDPDEndDate, b.BusinessDate,  rank() over (partition by ExposureKey order by BusinessDate ASC) as Rank,
            case when uf.EBAStatus = 'PL'   and b.RiskBucketCalc in (0,1,2,3) then 'PL'
                 when uf.EBAStatus = 'PL'   and b.RiskBucketCalc >= 4         then 'NPL'
                 when uf.EBAStatus = 'FPL'  and b.RiskBucketCalc in (0,1,2,3) then 'FPL'
                 when uf.EBAStatus = 'FPL'  and b.RiskBucketCalc >= 4         then 'FNPL'
                 when uf.EBAStatus = 'CFPL' and b.RiskBucketCalc in (0,1)     then 'CFPL'
                 when uf.EBAStatus = 'CFPL' and b.RiskBucketCalc >= 2         then 'FNPL'
                 when uf.EBAStatus = 'FNPL' and b.RiskBucketCalc >= 0         then 'FNPL'
                 when uf.EBAStatus = 'NPL'  and b.RiskBucketCalc >= 0         then 'NPL'
                 when b.ExposureKey is null then uf.EBAStatus
            end EBAStatusCalc
     from (
     select ExposureKey, DPDs_mtd, DODDPDStartDate, DODDPDEndDate, BusinessDate,
           case when DPDs_mtd = 0                    then 0
                when DPDs_mtd>=1   and DPDs_mtd<=30  then 1
                when DPDs_mtd>=31  and DPDs_mtd<=60  then 2
                when DPDs_mtd>=61  and DPDs_mtd<=90  then 3
                when DPDs_mtd>=91  and DPDs_mtd<=120 then 4
                when DPDs_mtd>=121 and DPDs_mtd<=150 then 5
                when DPDs_mtd>=151 and DPDs_mtd<=180 then 6
                when DPDs_mtd>=181 and DPDs_mtd<=210 then 7
                when DPDs_mtd>=211 and DPDs_mtd<=240 then 8
                when DPDs_mtd>=241 and DPDs_mtd<=270 then 9
                when DPDs_mtd>=271 and DPDs_mtd<=300 then 10
                when DPDs_mtd>=301 and DPDs_mtd<=330 then 11
                when DPDs_mtd>=331 and DPDs_mtd<=360 then 12
                when DPDs_mtd>=361                   then 13
      end RiskBucketCalc
      from (
      select distinct ExposureKey, datediff(last_day(add_months({run_date!r},-1)),DODDPDStartDate) + 1 DPDs_mtd, DODDPDStartDate, DODDPDEndDate, BusinessDate
      from (

            SELECT  DISTINCT  ExposureSourceSystem
                          , ExposureKey
                          , CAST(DODDPDStartDate AS DATE) AS DODDPDStartDate
                          , CAST(DODDPDEndDate AS DATE)  AS DODDPDEndDate
                          , BusinessDate
                      FROM dod_dpd_fcy
                      WHERE BusinessDate IN (select DISTINCT CalendarDate
                                                  from ref_calendar
                                                  where year(CalendarDate) = year({run_date!r}) and month(CalendarDate) = month({run_date!r}) and BusinessDayOfMonth in (1,2,3))

    /*      SELECT    ExposureSourceSystem
              , ExposureKey
              , CAST(DODDPDStartDate AS DATE) AS DODDPDStartDate
              , CAST(DODDPDEndDate AS DATE)  AS DODDPDEndDate
              , BusinessDate
          FROM dod_dpd_fcy
          WHERE BusinessDate = (select CalendarDate
                                      from ref_calendar
                                      where year(CalendarDate) = year({run_date!r}) and month(CalendarDate) = month({run_date!r}) and BusinessDayOfMonth = 1)
          UNION
          SELECT    ExposureSourceSystem
              , ExposureKey
              , CAST(DODDPDStartDate AS DATE) AS DODDPDStartDate
              , CAST(DODDPDEndDate AS DATE)  AS DODDPDEndDate
              , BusinessDate
          FROM dod_dpd_fcy
          WHERE BusinessDate = (select CalendarDate
                                      from ref_calendar
                                      where year(CalendarDate) = year({run_date!r}) and month(CalendarDate) = month({run_date!r}) and BusinessDayOfMonth = 2)
          AND DODDPDStartDate = BusinessDate
          UNION
          SELECT    ExposureSourceSystem
              , ExposureKey
              , CAST(DODDPDStartDate AS DATE) AS DODDPDStartDate
              , CAST(DODDPDEndDate AS DATE)  AS DODDPDEndDate
              , BusinessDate
          FROM dod_dpd_fcy
          WHERE BusinessDate = (select CalendarDate
                                      from ref_calendar
                                      where year(CalendarDate) = year({run_date!r}) and month(CalendarDate) = month({run_date!r}) and BusinessDayOfMonth = 3)
          AND DODDPDStartDate = BusinessDate */ 
        ) A
	WHERE 1=1 AND A.ExposureSourceSystem IN ('CMS', 'HSL', 'HST', 'LMS', 'SGL', 'SSE') AND A.DODDPDEndDate = '9999-12-31'
      ) 
      a
      ) 
      b
      right join (
        select * from (
      select case when ECAAssetClass = 'Revolving Consumer' then AgreementKey else ExposureNumber end as ExposureNumberCalc, ExposureNumber, BorrowerKey, case when EBAStatus = 'FPL' and FPLUnderProbationFlag = 1 then 'CFPL' else EBAStatus end EBAStatus    
      from extunionfile e
        inner join ref_calendar rfc
      on e.SnapshotDate = try_cast(rfc.CalendarDate as date)
        -- inner join (select max(BusinessDate) BusinessDate, SnapshotDate  from extunionfile group by SnapshotDate) e2
        -- on e.BusinessDate = e2.BusinessDate and e2.SnapshotDate = e.SnapshotDate
        where e.SnapshotDate = (case when rfc.CurrentMonthLastWorkingDate = {run_date!r} then last_day(add_months({run_date!r}, -1)) else last_day(add_months({run_date!r}, -2)) end)) a
      ) uf 
      on b.ExposureKey = uf.ExposureNumberCalc
      ) c where rank = 1
""")
    
ebacalc_join.createOrReplaceTempView("ebacalc_join")

# COMMAND ----------

previous_union = spark.sql(f"""   
    select distinct
        FPLUnderProbationFlag,
        ExposureKey,
        EBAStatus,
        e.SnapshotDate,
        ExposureNumber,
        case when FPLUnderProbationFlag  = 1 then 'CFPL' else EBAStatus end as EBAStatusPreviousMonth
    from extunionfile e
    inner join (select max(SnapshotDate) SnapshotDate from extunionfile
                 where SnapshotDate <= IF({run_date!r} = (select CurrentMonthLastWorkingDate from ref_calendar where CalendarDate = {run_date!r}),last_day(dateadd(month, -1, {run_date!r})) ,{run_date!r} )) e1
    on e.SnapshotDate = e1.SnapshotDate
""")
    
previous_union.createOrReplaceTempView("previous_union")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### savedinfo_join

# COMMAND ----------

savedinfo_exposure = spark.sql(f"""   
select  
            c.CalendarDate BusinessDate, 
            ifnull(de.ExposureKey, som.ExposureKey) as ExposureKey, 
            de.DueAmount,
            de.OnBalance,
            de.AssetSegment,
            de.exposurestatus,
            de.OutstandingTotalBalance,
            ifnull(p.Bucket,0) Bucket,
            de1.LastBillingdate,
            de.DaysPastDueWithMateriality AS DaysPastDue,
            de.ObjectiveStatus,
            de.ObjectiveStatusdate,
            ifnull(de.IsActiveFlag, false) as IsActiveFlag,
            ifnull(de.PartyRelationshipType, 'Primary Owner') as PartyRelationshipType,
            ifnull(de.PortfolioName, 'Core Retail') as PortfolioName, 
            de.CaseCode
            from `{env}-cdh`.reference.ref_calendar c  
            -- cross join (select ExposureKey from `{env}-cdh`.derivative.dexposure de where de.BusinessDate = last_day(add_months({run_date!r},-1)) ) som
            cross join (select ExposureKey from `{env}-cdh`.derivative.dexposure de where de.BusinessDate = (select max(BusinessDate) from `{env}-cdh`.derivative.dexposure de where last_day(de.BusinessDate) = last_day(add_months({run_date!r},-1)))
             ) som      --IB 03/12/25      
            left join `{env}-cdh`.derivative.dexposure de on c.CalendarDate = de.BusinessDate and de.ExposureKey = som.Exposurekey
            left join (SELECT DISTINCT c.CalendarDate BusinessDate,ExposureKey, MAX(LastBillingdate) LastBillingdate
                      FROM `{env}-cdh`.reference.ref_calendar C 
                      LEFT JOIN (SELECT DISTINCT ExposureKey,LastBillingdate FROM `{env}-cdh`.derivative.dexposure w
                                 WHERE (last_day(w.BusinessDate) = {run_date!r}) and (last_day(w.LastBillingdate) = {run_date!r}) and LastBillingdate is not null) w ON c.CalendarDate>w.LastBillingdate     
                      WHERE (last_day(c.CalendarDate) = {run_date!r} and c.IsBusinessDay = 1 or c.CalendarDate=(SELECT DISTINCT PreviousMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r}))   --last_day(add_months({run_date!r},-1))) --IB 11022026
                      GROUP BY c.CalendarDate,ExposureKey) de1 on de1.ExposureKey = som.ExposureKey AND c.CalendarDate=de1.BusinessDate 
            left join `{env}-cdh`.primary.exposure p on de.BusinessDate = p.BusinessDate and p.ExposureKey = som.Exposurekey and p.cdi=de.cdi 
            --where last_day(c.CalendarDate) = {run_date!r} and c.IsBusinessDay = 1 or c.CalendarDate=last_day(add_months({run_date!r},-1))
            where c.CalendarDate>=(SELECT DISTINCT PreviousMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r}) AND c.IsBusinessDay = 1 AND c.CalendarDate<={run_date!r} --IB 27/11/25
            order by  c.CalendarDate   
""")
   
savedinfo_exposure.createOrReplaceTempView("savedinfo_exposure")

# COMMAND ----------

cFPLInflow = spark.sql(f"""
SELECT ExposureKey , CASE WHEN SUM(cFPLInflow)>=2 THEN 1 ELSE 0 END AS cFPLInflow
FROM (
SELECT exMtD. ExposureKey, ex.BusinessDate
,case when cs.Queue ilike '%CFPL_Inflow_Next_Month%' then 1 else 0 end cFPLInflow
FROM savedinfo_exposure exMtD
left join savedinfo_exposure ex on exMtD.ExposureKey = ex.ExposureKey --and exMtD.CDI = ex.CDI 
left join `{env}-cdh`.primary.casecode cs on ex.CaseCode = cs.CaseCodeKey and ex.BusinessDate = cs.BusinessDate and cs.SourceSystem = 'qca'
where 
  exMtD.BusinessDate = {run_date!r}
  and ex.BusinessDate >  last_day(add_months({run_date!r},-1)) --AN 20250407
  and ex.BusinessDate <= {run_date!r}
  and exMtD.PortfolioName = 'Core Retail'
  and exMtD.PartyRelationshipType='Primary Owner' --check 20250707
  and ex.PartyRelationshipType='Primary Owner' 
) GROUP BY ExposureKey
""")
   
cFPLInflow.createOrReplaceTempView("cFPLInflow")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Collection_offer

# COMMAND ----------

collection_offer = spark.sql(
f'''
select * from (
    select * from (
        select  co.Exposurekey, co.BusinessDate as MaxBusinessDate, `_update_timestamp` as MaxUpdateTimeStamp, co.Step_2_segment_mapped_values, RANK() OVER (PARTITION BY co.ExposureKey ORDER BY `_update_timestamp` DESC) AS Rank
        from `{env}-ptools`.assignments.offer_collection co
        left join (
            select e.Exposurekey, e.ExposureNumber, case when e.EBAStatus = 'FPL' and e.FPLUnderProbationFlag = 1 then 'CFPL' else e.EBAStatus end EBAStatus, e.SnapshotDate
          from extunionfile e 
          WHERE SnapshotDate = '{PreviousMonthLastDate}'
        ) uf 
        on uf.ExposureKey = co.ExposureKey
        where 1=1 
        --and Step_2_segment_mapped_values LIKE '%CFPLProActiveInflow%'
        and MONTH(co.BusinessDate) =  MONTH({run_date!r}) 
        and YEAR(co.BusinessDate) = YEAR({run_date!r}) 
        and MONTH(`_update_timestamp`) =  MONTH({run_date!r}) 
        and YEAR(`_update_timestamp`) = YEAR({run_date!r}) 
        and coalesce(uf.EBAStatus, co.EBAStatus) = 'CFPL'
    )
    WHERE rank = 1
)
WHERE Step_2_segment_mapped_values LIKE '%CFPLProActiveInflow%'
'''
)
collection_offer.createOrReplaceTempView("collection_offer")

# COMMAND ----------

savedinfo_join_2 = spark.sql(f"""
       select distinct MtDBusinessDate BusinessDate,
       MtDExposureKey ExposureKey,   
       COALESCE(BucketCategoryWithMateriality,0) as BucketCategoryWithMateriality , --AN 20250707
       InflowTargetDate,
       cFPLInflow,
       SoMOutstandingTotalBalance,
       CASE when OutflowSavedIntramonthDate IS NOT NULL  then 'Saved'
            when MtdBusinessDate = (select CurrentMonthLastWorkingDate from `{env}-cdh`.reference.ref_calendar where CalendarDate = {run_date!r}) and MtDDueAmount <= 100 then 'Saved'
            when MtdBusinessDate = (select CurrentMonthLastWorkingDate from `{env}-cdh`.reference.ref_calendar where CalendarDate = {run_date!r}) and MtDOnBalance = 0 then 'Saved'
            when MtdBusinessDate = (select CurrentMonthLastWorkingDate from `{env}-cdh`.reference.ref_calendar where CalendarDate = {run_date!r}) and try_divide(MtDDueAmount, MtDOnBalance) <= 0.01 and AssetSegment in ('CL','SB')	 then 'Saved' else 'Not Saved' end as OutflowSaved,
       case when MLSavedDate is null then 'Not Saved' else 'Saved' end OperationalMLSaved,
       case when LMSSavedDate is null then 'Not Saved' else 'Saved' end OperationalLMSSaved,
       case when CMSSavedDate is null then 'Not Saved' else 'Saved' end  OperationalCMSSaved,
       case when SBSavedDate is null then 'Not Saved' else 'Saved' end OperationalSBSaved,
       case 
            when MLSavedDate is not null then 'Saved'  
            when LMSSavedDate is not null then 'Saved'
            when CMSSavedDate is not null then 'Saved'
            when SBSavedDate is not null then 'Saved'
            else 'Not Saved'
       end as OperationalSaved,
       case
            when MLSavedDate is not null then MLSavedDate  
            when LMSSavedDate is not null then LMSSavedDate
            when CMSSavedDate is not null then CMSSavedDate
            when SBSavedDate is not null then SBSavedDate
       end as OperationalSavedDate,
       case 
            when EBAStatus in ('PL','CFPL','FPL') and QualcoDisappearanceDate <= InflowTargetDate then 'Saved'   --QualcoDisappearanceDate < InflowTargetDate --IB 20260202
            when EBAStatus not in ('PL','CFPL','FPL') and QualcoDisappearanceDate is not null then 'Saved'
            when cFPLInflow = 0 and RiskSavedNotCFPLDate is not null then 'Saved'
            when cFPLInflow = 1 and RiskSavedCFPLDate is not null then 'Saved'
            else 'Not Saved'
       end RiskSaved,
       case     
            when cFPLInflow = 0 and RiskSavedNotCFPLDate is not null then RiskSavedNotCFPLDate
            when cFPLInflow = 1 and RiskSavedCFPLDate is not null then RiskSavedCFPLDate
            when EBAStatus in ('PL','CFPL','FPL') and QualcoDisappearanceDate <= InflowTargetDate then QualcoDisappearanceDate  --QualcoDisappearanceDate < InflowTargetDate --IB 20260202
            when EBAStatus not in ('PL','CFPL','FPL') and QualcoDisappearanceDate is not null then QualcoDisappearanceDate
            else null
       end RiskSavedDate,
       case when ModificationsCampaigns is not null then ModificationsCampaigns
			when MtDPaymentAmount > 0					then 'Payments'
			else 'Other Reason'
       end as SavedReason,
       case when ModificationsCampaigns is not null and EBAStatus = 'CFPL' then 1 else 0 end  ModifiedCFPL, 
       case when Category in ('A','B','C','D','Fast Track','Extension') and lpfx_new.NewExposureKey is not null then 1 else 0 end DoubleModified,
       ECAAssetClass,
       ExposureSegmentation,
       NonCoreFlag
from (
select ex_1.MtDBusinessDate,
	   ex_1.MtDExposureKey,
	   ex_1.AssetSegment,
	   ex_1.MtDOutstandingTotalBalance,
        ex_1.SoMOutstandingTotalBalance,
	   ex_1.MtDOnBalance,
	   ex_1.ECAAssetClass,
        ex_1.ExposureSegmentation,
        ex_1.NonCoreFlag,
	   ex_1.EBAStatus,
	   ex_1.BucketCategoryWithMateriality,
	   ex_1.InflowTargetDate,
        MtDDueAmount,
	   ex_1.MtDPaymentAmount,
	   ex_1.Category,
	   ex_1.ModificationsCampaigns,
	   ex_1.cFPLInflow as cFPLInflow, --AN 20250728 change max to min
       min(case when MtDBucket > 3 or SoMBucket < MtDBucket then null else MLSaved end) MLSavedDate,
       min(case when MtDBucket > 3 or SoMBucket < MtDBucket then null else LMSSaved end) LMSSavedDate,
       min(case when MtDBucket > 3 or SoMBucket < MtDBucket then null else CMSSaved end) CMSSavedDate,
       min(case when MtDBucket > 3 or SoMBucket < MtDBucket then null else SBSaved end) SBSavedDate,
       min(QualcoDisappearance) QualcoDisappearanceDate,
       min(RiskSavedNotCFPL) RiskSavedNotCFPLDate,
       min(case when MtDDODDPDEndDate = '9999-12-31' then null else RiskSavedCFPL end) RiskSavedCFPLDate,
	  min(OutflowSavedIntramonth) OutflowSavedIntramonthDate
from (
select 
ex.MtDBusinessDate,
ex.MtDExposureKey,
ex.BusinessDate,
ex.AssetSegment,
ex.MtDOutstandingTotalBalance,
ex.SoMOutstandingTotalBalance,
ex.MtDOnBalance,
ex.Bucket,
ex.SoMBucket,
ex.MtDBucket,
ex.Queue,
ex.LastBillingdate,
ex.DaysPastDue,
ex.ECAAssetClass,
ex.ExposureSegmentation,
ex.NonCoreFlag,
ex.EBAStatus,
ex.BucketCategoryWithMateriality,
ex.DODDPDEndDate,
ex.MtDDODDPDEndDate,
ex.ObjectiveStatus,
ex.ObjectiveStatusdate,
ex.EoMBucket,
ex.InflowTargetDate,
ex.MLRemainingDebt,
ex.LMSRemainingDebt,
ex.PaymentAmount,
ex.MtDPaymentAmount,
ex.Category,
ex.ModificationsCampaigns,
--ex.cFPLInflowOld,
ex.cFPLInflow,
ex.QualcoDisappearance,
ex.exposurestatus,
ex.MtDDueAmount,
case when ex.ECAAssetClass is null or  ex.ECAAssetClass <> 'Residential'	                  then null
	   when ex.Bucket < ex.SoMBucket and ex.Bucket <= 2						                                                                   then ex.BusinessDate
       when ex.BusinessDate=(SELECT DISTINCT CurrentMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r}) and ex.Bucket = ex.SoMBucket and ex.Bucket <= 3 then ex.BusinessDate  --IB 20260204
	   when ex.Bucket = ex.SoMBucket and ex.Bucket <= 3 and coalesce(ex.MLRemainingDebt,0) - coalesce(ex.PaymentAmount,0) <= 100 then ex.BusinessDate
       when ex.BusinessDate=(SELECT DISTINCT CurrentMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r}) and ex.Bucket < ex.SoMBucket and ex.Bucket = 3 then ex.BusinessDate --IB 20260204
	   when ex.Bucket < ex.SoMBucket and ex.Bucket = 3  and coalesce(ex.MLRemainingDebt,0) - coalesce(ex.PaymentAmount,0) <= 100 then ex.BusinessDate
	   else null
end as MLSaved,
case when ex.ECAAssetClass is null or ex.ECAAssetClass <> 'Non-revolving Consumer'	                   then null
	   when ex.Bucket < ex.SoMBucket and ex.Bucket <= 2						                                                                   then ex.BusinessDate
       when ex.BusinessDate=(SELECT DISTINCT CurrentMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r}) and ex.Bucket = ex.SoMBucket and ex.Bucket <= 3 then ex.BusinessDate  --IB 20260204
	   when ex.Bucket = ex.SoMBucket and ex.Bucket <= 3 and coalesce(ex.LMSRemainingDebt,0) - coalesce(ex.PaymentAmount,0) <= 50 then ex.BusinessDate
       when ex.BusinessDate=(SELECT DISTINCT CurrentMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r}) and ex.Bucket < ex.SoMBucket and ex.Bucket = 3 then ex.BusinessDate --IB 20260204
	   when ex.Bucket < ex.SoMBucket and ex.Bucket  = 3 and coalesce(ex.LMSRemainingDebt,0) - coalesce(ex.PaymentAmount,0) <= 50 then ex.BusinessDate
	   else null
end as LMSSaved,
case when ex.ECAAssetClass is null or ex.ECAAssetClass <> 'Revolving Consumer'	                                           then null
	   when ex.Bucket < ex.SoMBucket and ex.Bucket <= 3						                                                                                                  then ex.BusinessDate  
	   when ex.Bucket = ex.SoMBucket and ex.Bucket <= 3 and ex.LastBillingdate <= ex.BusinessDate and ex.LastBillingdate > last_day(add_months(ex.BusinessDate,-1)) then ex.BusinessDate
	   else null
end as CMSSaved,
case when ex.ECAAssetClass is null or ex.ECAAssetClass <> 'SBPs'  then null
	   when ex.EoMBucket <= ex.SoMBucket and ex.EoMBucket <= 3			 then ex.BusinessDate
	   when ex.DaysPastDue = 0 OR ex.DaysPastDue is null              then ex.BusinessDate     
	   else null
end as SBSaved,
case when ex.EBAStatus in ('PL','CFPL','FPL') and (ex.InflowTargetDate IS null OR ex.InflowTargetDate < ex.BusinessDate) OR ex.cFPLInflow = 1	then null   
	   when ex.OnBalance <= 0 then ex.BusinessDate
	   when cast(ex.DueAmount as decimal(19,2)) <= 100															                                              then ex.BusinessDate
	   when cast(ex.DueAmount as decimal(19,2)) / ex.OnBalance <= 0.01 then ex.BusinessDate
	   when ex.DODDPDEndDate <= ex.BusinessDate							                                                                                    then ex.BusinessDate
	   else null
end as RiskSavedNotCFPL,
case WHEN ex.MtDLastbillingdate>last_day(add_months({run_date!r},-1)) AND ex.MtDLastbillingdate <= DATEADD(DAY,2,ex.DODDPDEndDate) AND ex.DODDPDEndDate <> '9999-12-31' THEN ex.BusinessDate
	 WHEN ex.MtDLastbillingdate>last_day(add_months({run_date!r},-1)) AND ex.MtDLastbillingdate <= ex.BusinessDate  AND ex.DODDPDEndDate IS NULL THEN ex.BusinessDate
     when ex.cFPLInflow = 0	or ex.BusinessDate <= ex.MtDLastbillingdate or ex.DODDPDEndDate = '9999-12-31' then null
	   else ex.BusinessDate				
end as RiskSavedCFPL,
case when ex.Bucket in ('0','00')   then ex.BusinessDate 
	   else null
end as OutflowSavedIntramonth
from (
select distinct 
exMtD.BusinessDate MtDBusinessDate,
exMtD.ExposureKey MtDExposureKey,
exMtD.Lastbillingdate MtDLastbillingdate,
ex.BusinessDate,
ex.exposurestatus,
ex.DueAmount,
ex.OnBalance,
exMtD.AssetSegment,
exMtD.DueAmount MtDDueAmount,
exMtD.OutstandingTotalBalance MtDOutstandingTotalBalance,
exMtD.OnBalance MtDOnBalance,
exSoM.OutstandingTotalBalance SoMOutstandingTotalBalance,
 coalesce(cast(case when ex.exposurestatus='Writen Off' then '0' else  ex.Bucket end  as int),0) as Bucket,
 coalesce(cast(case when ex.exposurestatus='Writen Off' then '0' else  exSoM.Bucket end  as int),0)  as SoMBucket,
 coalesce(cast(case when ex.exposurestatus='Writen Off' then '0' else  exMtD.Bucket end  as int),0)  as MtDBucket,
-- coalesce(cast(case when ex.Bucket = '99' then '00' else ex.Bucket end  as int),0) as Bucket,
-- coalesce(cast(case when exSoM.Bucket = '99' then '00' else exSoM.Bucket end  as int),0)  as SoMBucket,
-- coalesce(cast(case when exMtD.Bucket = '99' then '00' else exMtD.Bucket end  as int),0)  as MtDBucket,
cs.Queue,
ex.LastBillingdate,
ex.DaysPastDue,
uf.ECAAssetClass,
uf.ExposureSegmentation,
uf.NonCoreFlag,
case when uf.EBAStatus = 'FPL' and uf.FPLUnderProbationFlag = 1 then 'CFPL' else uf.EBAStatus end EBAStatus,
uf.BucketCategoryWithMateriality,
cast(dod_td.DODDPDEndDate as date) DODDPDEndDate,
cast(dodMtD.DODDPDEndDate as date) MtDDODDPDEndDate,
ex.ObjectiveStatus,
ex.ObjectiveStatusdate,
case when (ex.DaysPastDue + datediff(day,ex.BusinessDate, last_day({run_date!r}) )) / 30 >= 13 then 13   
	 when ex.DaysPastDue = 0 then 0  
	  else floor((ex.DaysPastDue + datediff(day,ex.BusinessDate, last_day({run_date!r})) ) / 30) + 1
end as EoMBucket,
case when uf.EBAStatus = 'FPL' AND uf.FPLUnderProbationFlag = 1 then dateadd(day,30,cast(dod_td1.DODDPDStartDate as date)) 
     else dateadd(day,90,cast(dod_td1.DODDPDStartDate as date)) 
end as InflowTargetDate,
case when exSoM.Bucket = 0 or exSoM.exposurestatus='Writen Off' then 0				
	 when exSoM.Bucket = 1	then kama.`Bucket1+`
	 when exSoM.Bucket = 2	then kama.`Bucket2+`
	 when exSoM.Bucket >= 3	then kama.`Bucket3+`
end as MLRemainingDebt,
case when exSoM.Bucket = 0 or exSoM.exposurestatus='Writen Off' then 0				
	 when exSoM.Bucket = 1	then lms.`Bucket1+`
	 when exSoM.Bucket = 2	then lms.`Bucket2+`
	 when exSoM.Bucket >= 3	then lms.`Bucket3+`
end as LMSRemainingDebt,
pay.PaymentAmount,
payMtD.PaymentAmount MtDPaymentAmount,
lpfx.Category,
case when lpfx.Category in ('A','B','Συμβατικά')       then 'Συμβατική Ρύθμιση'
     when lpfx.Category in ('D','C')		           then 'Ρύθμιση Haircut'
     when lpfx.Category = 'Δικαστικές Ρυθμίσεις N3869' then 'Ρυθμίσεις N3869'
     when lpfx.Category = 'Extension'                  then 'Extension'
end as ModificationsCampaigns,
-- case when cs.Queue like concat('CC_CFPL_Inflow_Next_Month_%',date_format(ex.BusinessDate,'MMM'),date_format(ex.BusinessDate,'yy'),'%')  or cs.Queue like concat('CC_CFPL_Inflow_Next_Month_%','intramonth%','') then 1 else 0 end cFPLInflow,
--CASE WHEN (cFPL.cFPLInflow=1 AND  ebacalc_join.RiskBucketCalc = 0 AND previousunion.EBAStatusPreviousMonth = 'CFPL') THEN 1 ELSE 0 END AS cFPLInflowOld,  --ebacalc_join.EBAStatusCalc = 'CFPL' and

CASE WHEN (
     (uf.EBAStatus = 'CFPL' or (uf.EBAStatus = 'FPL' and uf.FPLUnderProbationFlag = 1)) -- previousunion.EBAStatusPreviousMonth = 'CFPL'
    and ebacalc_join.RiskBucketCalc = 0 
    -- and MONTH(dod_td1.DODDPDStartDate) =  MONTH({run_date!r}) 
    -- and YEAR(dod_td1.DODDPDStartDate) = YEAR({run_date!r}) 
    and MONTH(dodMtD.DODDPDStartDate) =  MONTH({run_date!r}) 
    and YEAR(dodMtD.DODDPDStartDate) = YEAR({run_date!r})
) OR (co.MaxBusinessDate IS NOT NULL and co.MaxUpdateTimestamp IS NOT NULL)

THEN 1 ELSE 0 END AS cFPLInflow,  --ebacalc_join.EBAStatusCalc = 'CFPL' and
case when qualcoex.ExposureKey is null then ex.BusinessDate end QualcoDisappearance

from savedinfo_exposure exMtD
left join  savedinfo_exposure exSoM on exSoM.PartyRelationshipType='Primary Owner' and exSoM.ExposureKey = exMtD.ExposureKey and exSoM.BusinessDate = (select PreviousMonthLastWorkingDate from ref_calendar where CalendarDate = {run_date!r}) --and exSoM.PartyRelationshipType='Primary Borrower' AN 20250707
left join (select dod.*, un.exposurenumber as exposurenumber from dod_dpd_fcy dod 
           inner join extunionfile un on un.exposurekey=dod.exposurekey and un.snapshotdate=(SELECT MAX(snapshotdate) FROM extunionfile WHERE last_day(snapshotdate)>=last_day(add_months({run_date!r},-2)) AND last_day(snapshotdate)<{run_date!r}) --IB 20251222
            -- inner join extunionfile un on un.exposurekey=dod.exposurekey and un.snapshotdate=last_day(add_months({run_date!r},-1))
            ) dodMtD on dodMtD.exposurenumber = exMtD.ExposureKey and dodMtD.BusinessDate = exMtD.BusinessDate
left join (select e.SnapshotDate, e.ExposureNumber, e.BucketCategoryWithMateriality, e.ECAAssetClass, e.ExposureSegmentation, e.NonCoreFlag, e.EBAStatus, e.FPLUnderProbationFlag
          from extunionfile e 
          WHERE SnapshotDate = (SELECT MAX(snapshotdate) FROM extunionfile WHERE last_day(snapshotdate)>=last_day(add_months({run_date!r},-2)) AND last_day(snapshotdate)<{run_date!r})  --IB 20251222
        --   inner join (select max(SnapshotDate) SnapshotDate from extunionfile where SnapshotDate =(select PreviousMonthLastWorkingDate from ref_calendar where CalendarDate = {run_date!r})) e1
        --   on e.SnapshotDate = e1.SnapshotDate
          -- inner join (select max(BusinessDate) BusinessDate, SnapshotDate  from extunionfile group by SnapshotDate) e2
          -- on e.BusinessDate = e2.BusinessDate and e2.SnapshotDate = e1.SnapshotDate
          ) uf 
          on uf.ExposureNumber = exMtD.ExposureKey   --AN 2025_07_08 
-- left join (select e.SnapshotDate, e.ExposureNumber, e.BucketCategoryWithMateriality, e.ECAAssetClass, e.ExposureSegmentation, e.NonCoreFlag, e.EBAStatus, e.FPLUnderProbationFlag
--           from extunionfile e 
--           inner join (select max(SnapshotDate) SnapshotDate from extunionfile where SnapshotDate <= {run_date!r}) e1
--           on e.SnapshotDate = e1.SnapshotDate
--           -- inner join (select max(BusinessDate) BusinessDate, SnapshotDate  from extunionfile group by SnapshotDate) e2
--           -- on e.BusinessDate = e2.BusinessDate and e2.SnapshotDate = e1.SnapshotDate
--           ) uf 
--           on uf.ExposureNumber = exMtD.ExposureKey           ---comment out AN 2025_07_08
left join savedinfo_exposure ex on exMtD.ExposureKey = ex.ExposureKey --and exMtD.CDI = ex.CDI 
left join  `{env}-cdh`.primary.exposure qualcoex on qualcoex.ExposureKey = ex.ExposureKey and qualcoex.BusinessDate = ex.BusinessDate and qualcoex.SourceSystem = 'qca' 
left join (select dod.*, un.exposurenumber as exposurenumber from dod_dpd_fcy dod 
            inner join extunionfile un on un.exposurekey=dod.exposurekey and un.snapshotdate=(SELECT MAX(snapshotdate) FROM extunionfile WHERE last_day(snapshotdate)>=last_day(add_months({run_date!r},-2)) AND last_day(snapshotdate)<{run_date!r})) dod_td 
on dod_td.exposurenumber = ex.ExposureKey and dod_td.BusinessDate = ex.BusinessDate
left join `{env}-cdh`.primary.casecode cs on ex.CaseCode = cs.CaseCodeKey and ex.BusinessDate = cs.BusinessDate and cs.SourceSystem = 'qca'
left join cFPLInflow cFPL ON cFPL.ExposureKey = exMtD.ExposureKey 
left join collection_offer co ON co.Exposurekey = exMtD.Exposurekey 
left join previous_union previousunion on previousunion.ExposureNumber = exMtD.ExposureKey
left join  ebacalc_join ebacalc_join on ebacalc_join.ExposureKey = exMtD.ExposureKey
left join (SELECT ExposureSourceSystem,
                    ExposureKey,  
                    UnionExposureKey,  
                     DODDPDStartDate,
                     DODDPDEndDate,
                     BusinessDate
                FROM 
                (
            SELECT ExposureSourceSystem,
                ExposureKey,
                UnionExposureKey,
                CAST(DODDPDStartDate AS DATE) AS DODDPDStartDate,
                CAST(DODDPDEndDate AS DATE)  AS DODDPDEndDate,
                BusinessDate
			FROM (
                SELECT 
                dod.*,
                un.ExposureNumber as UnionExposureKey
                FROM  dod_dpd_fcy dod
                inner join extunionfile un on un.exposurekey=dod.ExposureKey and un.snapshotdate=(SELECT MAX(snapshotdate) FROM extunionfile WHERE last_day(snapshotdate)>=last_day(add_months({run_date!r},-2)) AND last_day(snapshotdate)<{run_date!r})
                ) d
                WHERE BusinessDate = (SELECT DISTINCT PreviousMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r}) --last_day(add_months({run_date!r},-1)) IB 11022026
			
			    UNION
   
			SELECT  ExposureSourceSystem,
					ExposureKey,
                    UnionExposureKey,
					CAST(DODDPDStartDate AS DATE) AS DODDPDStartDate,
					CAST(DODDPDEndDate AS DATE)  AS DODDPDEndDate,
            BusinessDate
			FROM (SELECT 
                dod.*,
                un.ExposureNumber as UnionExposureKey
            FROM  dod_dpd_fcy dod
            inner join  extunionfile un on un.exposurekey=dod.ExposureKey and un.snapshotdate=(SELECT MAX(snapshotdate) FROM extunionfile WHERE last_day(snapshotdate)>=last_day(add_months({run_date!r},-2)) AND last_day(snapshotdate)<{run_date!r}) ) d
			WHERE BusinessDate = (select CalendarDate
                                  from `{env}-cdh`.reference.ref_calendar
                                  where year(CalendarDate) = year({run_date!r}) and month(CalendarDate) = month({run_date!r}) and BusinessDayOfMonth = 1)
                                  and DODDPDStartDate=(select CalendarDate
                                  from `{env}-cdh`.reference.ref_calendar
                                  where year(CalendarDate) = year({run_date!r}) and month(CalendarDate) = month({run_date!r}) and BusinessDayOfMonth = 1)
        ) A
	WHERE
     1=1
    AND A.ExposureSourceSystem IN ('CMS', 'HSL', 'HST', 'LMS', 'SGL', 'SSE') AND A.DODDPDEndDate = '9999-12-31'
) dod_td1 on dod_td1.UnionExposureKey= ex.ExposureKey 
left join (
   select ExposureKey, 
          BusinessDate,
          Bucket1 + Bucket2 + Bucket3 + Bucket4 + Bucket5 + Bucket6 + `Bucket7+` AS `Bucket1+`,
          Bucket2 + Bucket3 + Bucket4 + Bucket5 + Bucket6 + `Bucket7+` AS `Bucket2+`,
          Bucket3 + Bucket4 + Bucket5 + Bucket6 + `Bucket7+` AS `Bucket3+`
    from (
    select ExposureKey, 
          BusinessDate,
          cast(replace(Bucket1,',','.') as decimal(19,2)) Bucket1,
          cast(replace(Bucket2,',','.') as decimal(19,2)) Bucket2,
		  cast(replace(Bucket3,',','.') as decimal(19,2)) Bucket3,
		  cast(replace(Bucket4,',','.') as decimal(19,2)) Bucket4,
          cast(replace(Bucket5,',','.') as decimal(19,2)) Bucket5,
          cast(replace(Bucket6,',','.') as decimal(19,2)) Bucket6,
          cast(replace(`Bucket7+`,',','.') as decimal(19,2)) `Bucket7+`
		from extkama) a
) kama on kama.ExposureKey = exMtD.ExposureKey and kama.BusinessDate = (select PreviousMonthLastWorkingDate from ref_calendar where trim(CalendarDate) = {run_date!r})
 -- kama business date SoMBusinessDate (last working day of previous month from common calendar)
left join (
select ExposureKey,
BusinessDate,
BUCKET1 + BUCKET2 + BUCKET3 + BUCKET4 + BUCKET5 + BUCKET6 + BUCKET7 + BUCKET8 + BUCKET9 + BUCKET10 + BUCKET11 + BUCKET12 + BUCKET13 + BUCKET14 AS `Bucket1+`,
BUCKET2 + BUCKET3 + BUCKET4 + BUCKET5 + BUCKET6 + BUCKET7 + BUCKET8 + BUCKET9 + BUCKET10 + BUCKET11 + BUCKET12 + BUCKET13 + BUCKET14				   AS `Bucket2+`,
BUCKET3 + BUCKET4 + BUCKET5 + BUCKET6 + BUCKET7 + BUCKET8 + BUCKET9 + BUCKET10 + BUCKET11 + BUCKET12 + BUCKET13 + BUCKET14							       AS `Bucket3+`
from (
    select ExposureKey,
    BusinessDate,
            cast(replace(BUCKET1,',','.') as decimal(19,2))   BUCKET1,
            cast(replace(BUCKET2,',','.') as decimal(19,2))   BUCKET2,
            cast(replace(BUCKET3,',','.') as decimal(19,2))   BUCKET3,
            cast(replace(BUCKET4,',','.') as decimal(19,2))   BUCKET4,
            cast(replace(BUCKET5,',','.') as decimal(19,2))   BUCKET5,
            cast(replace(BUCKET6,',','.') as decimal(19,2))   BUCKET6,
            cast(replace(BUCKET7,',','.') as decimal(19,2))   BUCKET7,
            cast(replace(BUCKET8,',','.') as decimal(19,2))   BUCKET8,
            cast(replace(BUCKET9,',','.') as decimal(19,2))   BUCKET9,
            cast(replace(BUCKET10,',','.') as decimal(19,2))  BUCKET10,
            cast(replace(BUCKET11,',','.') as decimal(19,2))  BUCKET11,
            cast(replace(BUCKET12,',','.') as decimal(19,2))  BUCKET12,
            cast(replace(BUCKET13,',','.') as decimal(19,2))  BUCKET13,
            cast(replace(BUCKET14,',','.') as decimal(19,2))  BUCKET14
    from extlmsbucketanalysis) a
) lms on lms.ExposureKey = exMtD.ExposureKey and lms.BusinessDate = (select PreviousMonthLastWorkingDate from ref_calendar where CalendarDate = {run_date!r})
 -- lms business date SoMBusinessDate (last working day of previous month from common calendar)
left join (
    select e.BusinessDate, e.ExposureKey,
           sum(case when PaymentActualDate <= e.BusinessDate then PaymentAmount else 0 end) PaymentAmount
    from savedinfo_exposure e --AN 20250711  
    left join (
        select ExposureKey, PaymentActualDate, sum(PaymentAmount) PaymentAmount
	    from payment
	    where PaymentActualDate >  last_day(add_months({run_date!r},-1)) and PaymentActualDate <= {run_date!r} and PortfolioName = 'Core Retail'
        group by ExposureKey, PaymentActualDate) p on p.ExposureKey = e.ExposureKey 
        where e.BusinessDate >  last_day(add_months({run_date!r},-1)) and e.BusinessDate <= {run_date!r} and e.PortfolioName = 'Core Retail'
        group by e.BusinessDate, e.ExposureKey
) pay on ex.ExposureKey = pay.ExposureKey and ex.BusinessDate = pay.BusinessDate 
left join (
    select ExposureKey, sum(PaymentAmount) PaymentAmount
	from payment
	where PaymentActualDate >  last_day(add_months({run_date!r},-1))
    and PaymentActualDate <= {run_date!r}
    and PortfolioName = 'Core Retail'
    group by ExposureKey
) payMtD on payMtD.ExposureKey = exMtD.ExposureKey 
left join (
    select distinct OldExposureKey,RestructuringDate,RestructuringType,
          case when RestructuringType in ('Ρύθμιση FT','Παράταση δόσης') or RestructuringTypeCode = 'BL' then 'Extension'
	           when RestructuringType like 'A%'                                         then 'A'
	           when RestructuringType like 'B%'                                         then 'B'
	           when RestructuringType like 'C%'                                         then 'C'
	           when RestructuringType like 'D%'                                         then 'D'
	           when RestructuringType  = 'ST'                                           then 'D1 Small Ticket'
	           when RestructuringProductKey in ('4015','4016','6078','6079','7363','7362') then 'Δικαστικές Ρυθμίσεις N3869'
	           else 'Συμβατικά'                                  
          end as Category,
          rank() over (partition by OldExposureKey order by RestructuringDate desc) as Rank 
    from extlpfxb364
    where 1=1 and try_cast(RestructuringDate AS date) > last_day(add_months({run_date!r}, -1)) AND try_cast(RestructuringDate AS date) <= {run_date!r}
) lpfx on lpfx.OldExposureKey = exMtD.ExposureKey and lpfx.Rank = 1
where exMtD.BusinessDate = {run_date!r}
  and ex.BusinessDate >  last_day(add_months({run_date!r},-1)) --AN 20250407
  and ex.BusinessDate <= {run_date!r}
  and exMtD.PortfolioName = 'Core Retail'
  and exMtD.PartyRelationshipType='Primary Owner' --check 20250707
  and ex.PartyRelationshipType='Primary Owner' 
  --and lpfx.RestructuringType NOT IN ('ΠΕΕ 47 Wholesale', 'Calypso Proactive Management')
  --and exMtD.ExposureKey = '989002771058700'

) ex 
) ex_1
group by ex_1.MtDBusinessDate, ex_1.MtDExposureKey, ex_1.AssetSegment,ex_1.MtDOutstandingTotalBalance,ex_1.MtDOnBalance, ex_1.ECAAssetClass,ex_1.ExposureSegmentation, ex_1.NonCoreFlag, ex_1.EBAStatus,ex_1.BucketCategoryWithMateriality ,ex_1.cfplinflow,--, ex_1.cFPLInflowOld,
ex_1.InflowTargetDate,ex_1.MtDPaymentAmount,ex_1.Category,ex_1.ModificationsCampaigns,ex_1.SoMOutstandingTotalBalance--,ex_1.outflowsaved
,MtDDueAmount
--ex_1.exposurestatus -- add AN 20250709
) ex_2
left join (
    select distinct NewExposureKey 
    from (
        select NewExposureKey, RestructuringDate, RestructuringType, rank() over (partition by NewExposureKey order by RestructuringDate desc) as Rank 
        from extlpfxb364
    ) a 
    where 1=1 and a.Rank = 1 and try_cast(RestructuringDate AS date) > last_day(add_months({run_date!r}, -6)) AND try_cast(RestructuringDate AS date) <= last_day(add_months({run_date!r},-1)) 
    --cast(RestructuringDate as date) >  last_day(add_months({run_date!r},-6)) and cast(RestructuringDate as date) <= last_day(add_months({run_date!r},-1))
       and (RestructuringType in ('Ρύθμιση FT','Παράταση δόσης') 
       or RestructuringType LIKE 'A%' OR RestructuringType LIKE 'B%' OR RestructuringType LIKE 'C%' OR RestructuringType LIKE 'D%') 
         

) lpfx_new on lpfx_new.NewExposureKey = ex_2.MtDExposureKey
""")
    
savedinfo_join_2.createOrReplaceTempView("savedinfo_join_2")

# COMMAND ----------

savedinfo_join_1 = spark.sql(f"""      
select *,
        CASE WHEN si.ModifiedCFPL = 1 OR si.DoubleModified = 1  THEN 'Not Saved'
                                                                  ELSE si.OperationalSaved
                END AS FinalOperationalSaved
            ,   CASE WHEN SI.ModifiedCFPL = 1 OR SI.DoubleModified = 1    THEN NULL
                                                                                ELSE SI.OperationalSavedDate
                END AS FinalOperationalSavedDate  
            ,   CASE WHEN SI.ModifiedCFPL = 1 OR SI.DoubleModified = 1  THEN 'Not Saved'
                                                                                ELSE SI.RiskSaved
                END AS FinalRiskSaved
            ,   CASE WHEN SI.ModifiedCFPL = 1 OR SI.DoubleModified = 1    THEN NULL
                                                                                ELSE SI.RiskSavedDate
                END AS FinalRiskSavedDate      
from savedinfo_join_2 si
""")
   
savedinfo_join_1.createOrReplaceTempView("savedinfo_join_1")

# COMMAND ----------

savedinfo_join = spark.sql(f"""      
        select *,
        cast(case 
                when (savedinfo1.InflowTargetDate is not null or savedinfo1.cFPLInflow = 1 or savedinfo1.BucketCategoryWithMateriality > 0) 
                and FinalRiskSaved = 'Saved' then savedinfo1.SoMOutstandingTotalBalance 
                when FinalOperationalSaved='Saved' then savedinfo1.SoMOutstandingTotalBalance 
        end as string) as SavedBalance,  
		cast(case 
                when savedinfo1.InflowTargetDate is not null 
                or savedinfo1.cFPLInflow = 1 
                or savedinfo1.BucketCategoryWithMateriality > 0 then FinalRiskSaved 
                else FinalOperationalSaved 
                end as string) as SavedFlag, 
		cast(case 
                when savedinfo1.InflowTargetDate is not null 
                or savedinfo1.cFPLInflow = 1 
                or savedinfo1.BucketCategoryWithMateriality > 0 then FinalRiskSavedDate 
                else FinalOperationalSavedDate 
                end as date) as SavedDate,
		cast(case 
                when savedinfo1.cFPLInflow = 1 then 'Proactive NPE Inflow' 
                else null 
                end as string) as InflowType

        from savedinfo_join_1 savedinfo1 
""")
   
savedinfo_join.createOrReplaceTempView("savedinfo_join")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### LowLTVModification Calculation

# COMMAND ----------

corestatichistorical = spark.sql(
f'''
SELECT 
    RestructuringDate,
    OldExposureKey,
    NewExposureKey,
    CDI,
    Category,
    PartyTaxRegistrationNumber,
    AgreementKey,
    RestructuringAmount,
    HaircutAmount
FROM  `{env}-cdh`.reference.corestatichistoricalmodificationbeforemigration
'''
)

corestatichistorical.createOrReplaceTempView("corestatichistorical")

# COMMAND ----------

assignmentpoollowltvcampaign = spark.sql(
f'''
SELECT 
    ExposureKey
FROM  `{env}-cdh`.externalfiles.extassignmentpoollowltvcampaign
'''
)

assignmentpoollowltvcampaign.createOrReplaceTempView("assignmentpoollowltvcampaign")

# COMMAND ----------

lpfx_corestatichistorical_union = spark.sql('''
select 
    RestructuringDate, 
    OldExposureKey,
    NewExposureKey,
    RestructuringType as Category
from extlpfxb364
union 
select 
    RestructuringDate, 
    OldExposureKey,
    NewExposureKey,
    Category
from corestatichistorical
''')

lpfx_corestatichistorical_union.createOrReplaceTempView("lpfx_corestatichistorical_union")

# COMMAND ----------

lpfx_corestatichistorical = spark.sql('''
    WITH RankedHist AS (
    select 
        RestructuringDate, 
        OldExposureKey,
        NewExposureKey,
        Category,
        rank() over(partition by OldExposureKey, NewExposureKey order by RestructuringDate desc) as Rank
    from lpfx_corestatichistorical_union
    )

    select 
        RestructuringDate, 
        OldExposureKey,
        NewExposureKey,
        Category
    from RankedHist
    where Rank = 1
''')

lpfx_corestatichistorical.createOrReplaceTempView("lpfx_corestatichistorical")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Final query with resulted DataFrame

# COMMAND ----------

final_df = spark.sql(f"""
WITH party_join AS (
     select distinct 
        PartyKey,
        BusinessDate,
        SourceSystem,
        PartyType,
        TaxRegistrationNumber,
        DeceasedFlag,
        CDI,
        Watchlist,
        IsWholeshaleFlag,
        StaffFlag,
        OCWStatus,
        ConnectedBorrowerFlag
    from party   
    where rank = 1                 
), 
dexposure_join AS (
    select distinct
        GBV,
        ExposureKey,
        BusinessDate,
        Bucket,
        SourceSystem
    from dexposure                     
),
lpsagreementdebtor_join AS (
    select distinct
        AgreementKey,
        CDI,
        OperationalLegalStatusLevel3
    from lpsagreementdebtor                     
),
casecode_join AS (
    select distinct 
        Queue, 
        WatchlistFlag, 
        QueueCode, 
        CustCode, 
        DebtCode, 
        BusinessDate
    from casecode                    
),
casecodeB_join AS (
    select distinct 
        coalesce(p.TaxRegistrationNumber, p.CDI) TaxCDI, 
        cs.queue, 
        cs.CustCode, 
        cs.BusinessDate, 
        cs.DebtCode, 
        cs.SourceSystem
    from casecode cs
    inner join party p 
    on cs.CustCode = p.PartyKey                     
),     
exp1_join AS (
    select 
        sum(OnBalance+OffBalance) as ContractOnOffBalance,
        sum(OnBalance) as ContractOnBalance,
        AgreementKey,
        SourceSystem,
        BusinessDate
    from exposure
    where PartyRelationshipType='Primary Owner'
    group by AgreementKey, SourceSystem, BusinessDate                    
),    
exp2_join AS (
    select
        sum(ex.OnBalance) as CustomerOn,
        sum(ex.OnBalance+ex.OffBalance) as CustomerOnOffBalance,
        max(exSoM.Bucket) as MaxCustomerOperationalBucket,
        min(exSoM.Bucket) as MinCustomerOperationalBucket,
        sum(ex.OnBalance) as CustomerLowBalance,
        ex.BusinessDate,
        ex.SourceSystem,
        ex.Customer
    from exposure ex
    left join `{env}-cdh`.primary.exposure exSoM on exSoM.ExposureKey = ex.ExposureKey 
    and exSoM.BusinessDate = (select PreviousMonthLastWorkingDate from `{env}-cdh`.reference.ref_calendar where CalendarDate = {run_date!r})
    and ex.customer =  COALESCE(NULLIF(CASE WHEN exsom.PartyTaxRegistrationNumber RLIKE '^[0]+$' THEN '' ELSE exsom.PartyTaxRegistrationNumber END , ''),exsom.CDI)

    where ex.PartyRelationshipType='Primary Owner'
    group by ex.Customer, ex.BusinessDate, ex.SourceSystem                     
),                     
settlement_join AS (
    select distinct 
        SettlementStatus,
        ExposureKey,
        BusinessDate,
        SourceSystem,
        SettlementStatusCode,
        SettlementType,
        BucketCode
    from settlementexposure 
    where SettlementStatusCode in (5093,5092)                    
),                     
realestateresearch_join AS (
    select distinct 
        coalesce(p.TaxRegistrationNumber, p.CDI) TaxCDI, res.OutcomeCode, res.BusinessDate, res.SourceSystem,
                rank() over(partition by coalesce(p.TaxRegistrationNumber, p.CDI) order by res.RealEstateResearchDate desc, case when res.OutcomeCode = '6949' then 3 when res.OutcomeCode = '6950' then 2 when res.OutcomeCode = '6951' then 1 end desc) Rnk -- Negative: 6050, Positive: 6049, Pending: 6051
    from  realestateresearch res
    inner join party p
    on p.PartyKey = res.PartyKey and p.BusinessDate = res.BusinessDate
    and   res.CancelledDate is null
    and   res.CancelledFlag = 0
    and   res.RealEstateResearchStatusCode is not null                   
),                     
product_join AS (
    select 
        ProductKey, 
        ProductDescription,
        SubProductCode,
        SubProduct,
        BankingUnitCode,
        BusinessDate,
        SourceSystem, 
        ProductCode
    from product                
),                     
interaction_join AS (
    select distinct 
        ExposureKey, 
        InteractionTypeKey, rank() over(partition by ExposureKey order by InteractionDate desc, InteractionTypeKey desc) as Rank
    from interaction
    where InteractionTypeKey in ('152', '545', '620', '621', '622', '623', '624')                    
),                     
interaction1_join AS (
    select 
        ExposureKey, 
        count( distinct InteractionDate ) PositiveToModifyCount
    from interaction
    where   InteractionDate <= last_day(dateadd(month, -1, {run_date!r}))
            and InteractionTypeKey in  ('486','201','50','500',	'514',	'516',	'518',	'521',	'524',
    '525',	'529',	'531',	'533',	'534',	'536',	'54',	'547',	'578',	'58',	'62',	
    '620',	'624',	'64',	'67',	'70',	'75',	'77',	'98',	'6008',	'6014',	'6015',	
    '6018',	'6020',	'6026',	'6029',	'6033',	'6048',	'6049',	'6050',	'6216',	'6070',	'486',	'488',	'489')
    group by ExposureKey                    
),                     
interaction2_join AS (
    select 
        ExposureKey, 
        count( distinct InteractionDate ) PositiveToModifyCount
    from interaction
    where   InteractionDate <= {run_date!r} and InteractionDate > last_day(dateadd(month, -1, {run_date!r}))
            and InteractionTypeKey in  ('486','201','50','500',	'514',	'516',	'518',	'521',	'524',	
    '525',	'529',	'531',	'533',	'534',	'536',	'54',	'547',	'578',	'58',	'62',	
    '620',	'624',	'64',	'67',	'70',	'75',	'77',	'98',	'6008',	'6014',	'6015',	
    '6018',	'6020',	'6026',	'6029',	'6033',	'6048',	'6049',	'6050',	'6216',	'6070',	'486',	'488',	'489')
    group by ExposureKey                     
),                     
interaction3_join AS (
    select 
        PartyKey, 
        count( distinct InteractionDate ) PositiveToModifyCount
    from interaction
    where   InteractionDate <= last_day(dateadd(month, -1, {run_date!r}))
            and InteractionTypeKey in  ('486','201','50','500',	'514',	'516',	'518',	'521',	'524',	
    '525',	'529',	'531',	'533',	'534',	'536',	'54',	'547',	'578',	'58',	'62',	
    '620',	'624',	'64',	'67',	'70',	'75',	'77',	'98',	'6008',	'6014',	'6015',	
    '6018',	'6020',	'6026',	'6029',	'6033',	'6048',	'6049',	'6050',	'6216',	'6070',	'486',	'488',	'489')
    group by PartyKey                     
),                     
interaction4_join AS (
    select 
        PartyKey, 
        count( distinct InteractionDate ) PositiveToModifyCount
    from interaction
    where   InteractionDate <= {run_date!r} and InteractionDate > last_day(dateadd(month, -1, {run_date!r}))
            and InteractionTypeKey in  ('486','201','50','500',	'514',	'516',	'518',	'521',	'524',	
    '525',	'529',	'531',	'533',	'534',	'536',	'54',	'547',	'578',	'58',	'62',	
    '620',	'624',	'64',	'67',	'70',	'75',	'77',	'98',	'6008',	'6014',	'6015',	
    '6018',	'6020',	'6026',	'6029',	'6033',	'6048',	'6049',	'6050',	'6216',	'6070',	'486',	'488',	'489')
    group by PartyKey                   
),                     
interaction5_join AS (
    select 
        ExposureKey, 
        count( distinct InteractionDate ) RPCCount
    from interaction
    where   InteractionDate <= last_day(dateadd(month, -1, {run_date!r}))
            and isrpcflag = true
    group by ExposureKey                     
),                     
interaction6_join AS (
    select 
        ExposureKey, 
        count( distinct InteractionDate ) RPCCount
    from interaction
    where   InteractionDate <= {run_date!r} and InteractionDate > last_day(dateadd(month, -1, {run_date!r}))
            and isrpcflag = true
    group by ExposureKey                    
),                     
interaction7_join AS (
    select 
        PartyKey, 
        count( distinct InteractionDate ) RPCCount
    from interaction
    where   InteractionDate <= last_day(dateadd(month, -1, {run_date!r}))
            and isrpcflag = true
    group by PartyKey                     
),                     
interaction8_join AS (
    select 
        PartyKey, 
        count( distinct InteractionDate ) RPCCount
    from interaction
    where   InteractionDate <= {run_date!r} and InteractionDate > last_day(dateadd(month, -1, {run_date!r}))
            and isrpcflag = true
    group by PartyKey                     
),                     
interaction9_join AS (
    select distinct 
        ExposureKey, 
        PromiseToPayAmount, 
        rank() over(partition by ExposureKey order by InteractionDate desc, PromiseToPayAmount desc) Rank
    from interaction
    where   InteractionDate <= last_day(dateadd(month, -1, {run_date!r}))
            and ispromiseflag = true                    
),                     
interaction10_join AS (
    select distinct 
        ExposureKey, 
        PromiseToPayAmount, 
        rank() over(partition by ExposureKey order by InteractionDate desc, PromiseToPayAmount desc) Rank
    from interaction
    where   InteractionDate <= {run_date!r} and InteractionDate > last_day(dateadd(month, -1, {run_date!r}))
            and ispromiseflag = true                    
),                     
interaction11_join AS (
    select 
        ExposureKey, 
        count( distinct InteractionDate ) PromiseToPayCount
    from interaction
    where   InteractionDate <= last_day(dateadd(month, -1, {run_date!r}))
            and ispromiseflag = true
    group by ExposureKey                    
),                     
interaction12_join AS (
    select 
        ExposureKey, 
        count( distinct InteractionDate ) PromiseToPayCount 
    from interaction
    where   InteractionDate <= {run_date!r} and InteractionDate > last_day(dateadd(month, -1, {run_date!r}))
            and ispromiseflag = true
    group by ExposureKey                    
),                     
interaction13_join AS (
    select 
        PartyKey, 
        count( distinct InteractionDate ) PromiseToPayCount
    from interaction
    where   InteractionDate <= last_day(dateadd(month, -1, {run_date!r}))
            and ispromiseflag = true
    group by PartyKey                     
),                     
interaction14_join AS (
    select 
        PartyKey, 
        count( distinct InteractionDate ) PromiseToPayCount
    from interaction
    where   InteractionDate <= {run_date!r} and InteractionDate > last_day(dateadd(month, -1, {run_date!r}))
            and ispromiseflag = true
    group by PartyKey                    
),                     
interaction15_join AS (
    select 
        ExposureKey, 
        count( distinct InteractionDate ) ContactCount
    from interaction
    where   InteractionDate <= last_day(dateadd(month, -1, {run_date!r}))
            and (ContactTypeCode = '1048' or ContactTypeCode = '1049')
    group by ExposureKey                    
),                     
interaction16_join AS (
    select 
        ExposureKey, 
        count( distinct InteractionDate ) ContactCount
    from interaction
    where   InteractionDate <= {run_date!r} and InteractionDate > last_day(dateadd(month, -1, {run_date!r}))
            and (ContactTypeCode = '1048' or ContactTypeCode = '1049')
    group by ExposureKey                    
),                     
interaction17_join AS (
    select 
        PartyKey, 
        count( distinct InteractionDate ) ContactCount
    from interaction
    where   InteractionDate <= last_day(dateadd(month, -1, {run_date!r}))
            and (ContactTypeCode = '1048' or ContactTypeCode = '1049')
    group by PartyKey                     
),                     
interaction18_join AS (
    select 
        PartyKey, 
        count( distinct InteractionDate ) ContactCount
    from interaction
    where   InteractionDate <= {run_date!r} and InteractionDate > last_day(dateadd(month, -1, {run_date!r}))
            and (ContactTypeCode = '1048' or ContactTypeCode = '1049')
    group by PartyKey                    
),                     
paymentjumptotalperimeter_join AS (
    select distinct 
        PaymentJumpMonth, 
        ExposureKey, 
        BusinessDate
    from paymentjumptotalperimeter 
    where BusinessDate = {run_date!r} -- (select max (BusinessDate) from paymentjumptotalperimeter where BusinessDate <= {run_date!r})                     
),                     
unionfile_join AS (
    select distinct
        IFRSGrossCarryingAmount,
        EBAStatus,
        FPLUnderProbationFlag,
        ProbationStartDate,
        HFSFlag,
        StateGuaranteeFlag,
        ECAAssetClass,
        ExposureSegmentation,
        NonCoreFlag,
        ExposureKey,
        ProductCode,
        ExposureSourceSystemKey,
        AcMaintenServiceBranchCode,
        BucketCategoryNMateriality,
        BucketCategoryWithMateriality,
        ExposureNumber,
        e.SnapshotDate,
        u.UTP_IND,
        UTPTypeCode,
        cast(case
                when e.SnapshotDate = LAST_DAY(ADD_MONTHS({run_date!r}, 0))
                then 1
                else 0
        end as boolean) as SoMUnionFileExistenceFlag
    from extunionfile e
    inner join (select max(SnapshotDate) SnapshotDate from extunionfile where SnapshotDate <= {run_date!r}) e1
    on e.SnapshotDate = e1.SnapshotDate
    --inner join (select max(BusinessDate) BusinessDate, SnapshotDate  from extunionfile group by SnapshotDate) e2
    --on e.BusinessDate = e2.BusinessDate and e2.SnapshotDate = e1.SnapshotDate  
    inner join ref_calendar rfc
    on e.SnapshotDate = try_cast(rfc.CalendarDate as date)
    left join extfile_utp u
    on e.ExposureKey = u.AC_CODE 
    and try_cast(u.UTP_ID as int) = try_cast(e.UTPTypeCode as int)
    and u.BusinessDate = e.SnapshotDate
    left join `{env}-cdh`.reference.utpdescriptions udesc
    on udesc.UTPDescription = u.UTP_IND
    where e.UTPTypeCode is not null            
),                     
unionfile1_join AS (
    select distinct 
        EBAStatus, 
        CollateralCapTotalTangible, 
        ExposureKey , 
        e.SnapshotDate, 
        FPLUnderProbationFlag,
        ExposureNumber,
         case
            when sum( case when EBAStatus IN ('FPL') and FPLUnderProbationFlag = 1 then 1 else 0 end) over (partition by BorrowerKey) >= 1 then 'CFPE'
            when sum( case when EBAStatus IN ('PL','FPL') then 1 else 0 end) over (partition by BorrowerKey) >= 1 then 'PE'
            else 'NPE'
        end EBAStrategyBorrower,
        BorrowerKey
    from extunionfile e
    inner join (select max(SnapshotDate) SnapshotDate from extunionfile where SnapshotDate <= {run_date!r}) e1
    on e.SnapshotDate = e1.SnapshotDate
    --inner join (select max(BusinessDate) BusinessDate, SnapshotDate  from extunionfile group by SnapshotDate) e2
    --on e.BusinessDate = e2.BusinessDate and e2.SnapshotDate = e1.SnapshotDate                    
),                     
unionfile2_join AS (
    select  distinct 
        -- case
        --     when sum( case when EBAStatus IN ('FPL') and FPLUnderProbationFlag = 1 then 1 else 0 end) over (partition by BorrowerKey) >= 1 then 'CFPE'
        --     when sum( case when EBAStatus IN ('PL','FPL') then 1 else 0 end) over (partition by BorrowerKey) >= 1 then 'PE'
        --     else 'NPE'
        -- end EBAStrategyBorrower, 
        BorrowerKey,
        max(BucketCategoryNMateriality) over (partition by BorrowerKey) as MaxBucketCategoryNMateriality  ,
        min(BucketCategoryNMateriality)  over (partition by BorrowerKey) as MinBucketCategoryNMateriality,
        e.SnapshotDate
        from extunionfile e
        inner join (select max(SnapshotDate) SnapshotDate from extunionfile where SnapshotDate <= {run_date!r}) e1
        on e.SnapshotDate = e1.SnapshotDate
        --inner join (select max(BusinessDate) BusinessDate, SnapshotDate  from extunionfile group by SnapshotDate) e2
        --on e.BusinessDate = e2.BusinessDate and e2.SnapshotDate = e1.SnapshotDate                    
),                     
unionfile4_join AS (
    select distinct
        FPLUnderProbationFlag,
        ExposureKey,
        EBAStatus,
        e.SnapshotDate,
        ExposureNumber
    from extunionfile e
    inner join (select max(SnapshotDate) SnapshotDate from extunionfile
                 where SnapshotDate <= IF({run_date!r} = (select CurrentMonthLastWorkingDate from ref_calendar where CalendarDate = {run_date!r}),last_day(dateadd(month, -1, {run_date!r})) ,{run_date!r} )) e1
    on e.SnapshotDate = e1.SnapshotDate
    --inner join (select max(BusinessDate) BusinessDate, SnapshotDate  from extunionfile group by SnapshotDate) e2
    -- on e.BusinessDate = e2.BusinessDate and e2.SnapshotDate = e1.SnapshotDate                     
),                     
unionfile5_join AS (
    select 
        AgreementKey, 
        e.SnapshotDate, 
        sum(try_cast(IFRSGrossCarryingAmount as double)) ContractGBVBalance
    from extunionfile e
    inner join (select max(SnapshotDate) SnapshotDate from extunionfile where SnapshotDate <= {run_date!r}) e1
    on e.SnapshotDate = e1.SnapshotDate
    --inner join (select max(BusinessDate) BusinessDate, SnapshotDate  from extunionfile group by SnapshotDate) e2
    --on e.BusinessDate = e2.BusinessDate and e2.SnapshotDate = e1.SnapshotDate
    group by AgreementKey,  e.SnapshotDate                    
),                     
unionfile6_join AS (
    select 
        e.BorrowerKey, 
        e.SnapshotDate, 
        sum(try_cast(IFRSGrossCarryingAmount as double)) CustomerGBV
    from extunionfile e 
    inner join (select max(SnapshotDate) SnapshotDate from extunionfile where SnapshotDate <= {run_date!r}) e1
    on e.SnapshotDate = e1.SnapshotDate
    --inner join (select max(BusinessDate) BusinessDate, SnapshotDate  from extunionfile group by SnapshotDate) e2
    --on e.BusinessDate = e2.BusinessDate and e2.SnapshotDate = e1.SnapshotDate
    group by e.BorrowerKey, e.SnapshotDate                     
), 
fasttrack_join AS (
    select distinct 
        FastTrackFlag, 
        ExposureKey, 
        BusinessDate
    from extfasttrack
    where BusinessDate = {run_date!r} 
),                    
PayPrevious12Months_join AS (
    select  
        ExposureKey, 
        sum(try_cast(PaymentAmount as double)) SumPayments
    from payment
    where PaymentActualDate <= dateadd(month, -1, {run_date!r}) 
    and PaymentActualDate >= dateadd(month, -12, {run_date!r})
    group by ExposureKey                  
),                     
PayCurrentMonth_join AS (
    select 
        ExposureKey, 
        sum(try_cast(PaymentAmount as double)) SumPayments
    from payment
    where PaymentActualDate <= {run_date!r} and PaymentActualDate > last_day(dateadd(month, -1, {run_date!r}))
    group by ExposureKey                    
),                     
dod_join AS (
        select distinct 
            ExposureKey, 
            cast(DODDPDStartDate as date) DODDPDStartDate, 
            BusinessDate, 
            cast(DODDPDEndDate as date) DODDPDEndDate
        from dod_dpd_fcy  
        where BusinessDate = {run_date!r}                         
),                     
lpfx_join AS (
        select distinct 
            lpfx.RestructuringType, 
            lpfx.NewExposureKey, 
         A.RestructuringDate as LastModification, --   CAST(CONCAT(SUBSTRING(a.RestructuringDate, 7, 4), '-', SUBSTRING(a.RestructuringDate, 4, 2), '-', SUBSTRING(a.RestructuringDate, 1, 2)) AS DATE) LastModification,
            lpfx.RestructuringTypeCode,
            lpfx.RestructuringProductKey,
            case when RestructuringType in ('Ρύθμιση FT','Παράταση δόσης') or RestructuringTypeCode = 'BL' then 'Extension'
               when RestructuringType         like 'A%' then 'A'
               when RestructuringType         like 'B%' then 'B'
               when RestructuringType         like 'C%' then 'C'
               when RestructuringType         like 'D%' then 'D'
               --when RestructuringType = 'ST'          then 'D1 Small Ticket'
               when RestructuringProductKey in ('4015','4016','6078','6079','7363','7362') then 'Δικαστικές Ρυθμίσεις N3869'
               else 'Συμβατικά'
            end as LastMod
        from (select * 
            from extlpfxb364 
            WHERE BusinessDate = (select min(BusinessDate) from `{env}-cdh`.externalfiles.extlpfxb364 where BusinessDate > {run_date!r})
            ) lpfx
        inner join (
            select 
                NewExposureKey, 
                max(RestructuringDate) RestructuringDate
            from extlpfxb364 
            WHERE BusinessDate = (select min(BusinessDate) from `{env}-cdh`.externalfiles.extlpfxb364 where BusinessDate > {run_date!r})
            group by NewExposureKey
        ) a
        on  a.NewExposureKey = lpfx.NewExposureKey 
            and a.RestructuringDate = lpfx.RestructuringDate
        where lpfx.RestructuringType NOT IN ('ΠΕΕ 47 Wholesale', 'Calypso Proactive Management')                    
),                     
lpfx2_join AS (
    select 
        count( distinct RestructuringDate) as  CountOfModifications,
        AgreementKey
    from extlpfxb364
    where try_cast(RestructuringDate as date) <= {run_date!r}
    group by AgreementKey                    
),  
NoFModificationsCoreStatic_join AS (
    select 
        count( distinct RestructuringDate) as  CountOfModifications,
        AgreementKey
    from corestatichistorical
    where try_cast(RestructuringDate as date) <= {run_date!r}
    group by AgreementKey                    
),   
exSoM_join AS (
    select distinct
        ExposureKey,
        Bucket
    from `{env}-cdh`.primary.exposure ex
    INNER JOIN ref_calendar rc 
    WHERE  TRY_CAST(rc.CalendarDate AS DATE) = (
         SELECT TRY_CAST(PreviousMonthLastWorkingDate AS DATE)
	         FROM ref_calendar
         WHERE CalendarDate = {run_date!r}
      )
    and businessdate = TRY_CAST(rc.CalendarDate AS DATE) 
    and sourcesystem = 'qca'
    and PortfolioName = 'Core Retail'                 
),            
l3869_join AS (
    select distinct  
        e.ExposureKey, 
        e.CDI, 
        ul.UnderLawType Law, 
        ul.CalcLawStatus, 
        ul.CourtOutcome,
        e.BusinessDate
    from exposure e
    join underlaw ul on e.CDI = ul.CDI
    where e.PartyRelationshipType = 'Primary Owner' and ul.UnderLawType = 'N3869'                    
)
,                     
--l3869Total_join AS (
--    select distinct 
--        e.ExposureKey, 
--        ul.UnderLawType Law, 
--        ul.CalcLawStatus, 
--        e.BusinessDate,
--        rank() over (partition by e.ExposureKey order by case when ul.CalcLawStatus = 'Positive Court Decision' then 1 
--                                                                 when ul.CalcLawStatus = 'Pending Court Decision'  then 2 
--                                                                                                                   else 3 end asc) --Rank
--    from exposure e
--    join underlaw ul on e.CDI = ul.CDI
--    where ul.UnderLawType = 'N3869'                   
--),                     
assignment_join AS (
    select distinct 
        a.ExposureKey, 
        a.ChannelCode, 
        a.BusinessDate, 
        a.AgentPartyKey, 
        a.SourceSystem , 
        a.AttributionChannel,
        a.AgentLoginName,
        a.ExternalCompanyPartyKey
    from assignment a
    inner join exposure e on e.PartyKey = a.PartyKey and e.ExposureKey = a.ExposureKey and a.BusinessDate = e.BusinessDate and a.SourceSystem = e.SourceSystem
    where   a.AssignmentType = 'Channel Assignment' 
            and e.PartyRelationshipType = 'Primary Owner'                
),                     
reportunified_join AS (
    select distinct 
        BusinessDate,
        ExposureKey, 
        ApplicationNumber ActiveApplicationNumber, 
        CAST(CONCAT(SUBSTRING(CONCAT('20', ApplicationNumber), 1, 4), '-', SUBSTRING(CONCAT('20', ApplicationNumber), 5, 2), '-', SUBSTRING(CONCAT('20', ApplicationNumber), 7, 2)) AS DATE) AS ActiveApplicationDate,
        case when CommonApplicationStep like 'ΔΔΟΛΤ%' then ReceiptDateCurrentStep end ActiveApplicationCreditReviewStepDate,
        case 
	        when Channel in ('ALPHA BANK ΥΠΗΡΕΣΙΕΣ ΕΝΗΜΕΡΩΣΕΩΣ ΟΦΕΙΛΕΤΩΝ','CALL CENTER - ALPHA BANK ΥΠΗΡΕΣΙΕΣ ΕΝΗΜΕΡΩΣΕΩΣ','CEPAL - INHOUSE', 'CEPAL - INHOUSE L.F.',
                                 'ΔΔΟΛΤ - ΑΙΤΗΣΕΙΣ ΡΥΘΜΙΣΕΩΝ - Mitigating Actions','ΔΔΟΛΤ - ΑΙΤΗΣΕΙΣ ΡΥΘΜΙΣΕΩΝ - ΔΕΚ',
                                 'ΔΔΟΛΤ - ΑΙΤΗΣΕΙΣ ΡΥΘΜΙΣΕΩΝ - ΕΙΔΙΚΟΥ ΧΕΙΡΙΣΜΟΥ')     then 'IRT'
            when Channel LIKE '%ΚΑΤΑΣΤΗΜΑ%'   then 'IRT'
            when Channel like '%Δ.Γ.%'        then 'Legal Agencies'
            when Channel like '%ΚΕΝΤΡΟ%'      then 'Hub RMs'
            when Channel in ('ΔΔΚΛΤ II')     then 'Hub RMs'
        end ActiveApplicationChannel,
        UnifiedApplicationStatus
    from reportunified 
    where UnifiedApplicationStatus = 'Ενεργή' and FirstDateReceiptValuation is not null                    
),
lpfx_nallc_join AS (
    select distinct 
        NewExposureKey, 
        1 NotAllowedToCure
    from (
        select 
            NewExposureKey, 
            RestructuringDate, 
            RestructuringType, 
            rank() over (partition by NewExposureKey order by RestructuringDate desc) as Rank 
        from extlpfxb364 
            WHERE BusinessDate = (select min(BusinessDate) from `{env}-cdh`.externalfiles.extlpfxb364 where BusinessDate > {run_date!r})
    ) a 
    where   RestructuringType in ('A1','B3','B3 AC', 'A1 AC', 'A2', 'A2 AC') 
            and a.Rank = 1               
), 
utpind_join AS (
    select
        e.ExposureNumber,
        e.SnapshotDate,
        1 UTP
    from extunionfile e
    inner join (
        select
            AC_CODE
        from extfile_utp
        group by AC_CODE
        having MIN(UTP_DRTN_CLCN) = 12) utp
    on utp.AC_CODE = e.ExposureKey
    inner join (select max(SnapshotDate) SnapshotDate from extunionfile where SnapshotDate <= {run_date!r}) e1
    on e.SnapshotDate = e1.SnapshotDate
    --inner join (select max(BusinessDate) BusinessDate, SnapshotDate  from extunionfile group by SnapshotDate) e2
    --on e.BusinessDate = e2.BusinessDate and e2.SnapshotDate = e1.SnapshotDate
    where   (e.HFSFlag = 0 or e.HFSFlag is null)
            and e.ExposureSegmentation > 3
            and e.UTPType > 1            
), 
gefyra_join AS (
  select distinct 
    a.*,  
    rank() over(partition by a.Account_Number order by a.StartDate desc) as Rank from (
      select 
        gi.Account_Number, 
        gi.SubsidyCategory SubsidyCategory, 
        gi.Balance_L2_3112 as BalanceL2, 
        gi.Discretion_Start_Date StartDate, 
        gi.Discretion_End_Month_updated BridgeEndOfMonthSubsidyDate, 'Bridge1' as BridgeProgramm
      from extfile_implementations_gefyra_i_31_01_2023 gi

      union all
      select gii.Account_Number, gii.SubsidyCategory, gii.Balance_L2_3112 BalanceL2, gii.Discretion_Start_Date StartDate, gii.Discretion_End_Month BridgeEndOfMonthSubsidyDate, 'Bridge2' as BridgeProgramm
      from extfile_implementations_gefyra_ii_31_01_2023 gii
    ) a
), dunderlaw_join AS (
    select distinct 
        CDI, 
        SourceSystem, 
        CourtDate 
    from underlaw
), ltvmodified_calc_join AS (
    select distinct 
    lpfx_corestatichistorical.NewExposureKey,
    mod.ModsModificationDate,
    lpfx_corestatichistorical.Category,
    lpfx_corestatichistorical.RestructuringDate, 
    RANK() OVER (PARTITION BY lpfx_corestatichistorical.NewExposureKey ORDER BY lpfx_corestatichistorical.RestructuringDate DESC) AS RowNum
    from (  
        select NewExposureKey, max(RestructuringDate) as ModsModificationDate
        from assignmentpoollowltvcampaign a1
        left join extlpfxb364 m
        on a1.ExposureKey = m.OldExposureKey
        group by NewExposureKey
        ) mod
    left join lpfx_corestatichistorical
    on mod.NewExposureKey = lpfx_corestatichistorical.NewExposureKey
), are_future_join AS (
    select 
        are.AuctionDate AS FutureAuctionDate,
        ROW_NUMBER() OVER (PARTITION BY are.OriginDebtorTaxRegistrationNumber ORDER BY are.AuctionDate DESC) AS RowNum,
        are.OriginDebtorTaxRegistrationNumber
    from auctionrealestate are
    WHERE are.AuctionDate > {run_date!r}
), are_past_join AS (
    select 
        are.AuctionDate AS PastAuctionDate,
        are.AuctionStatusCode AS PastAuctionResult,
        ROW_NUMBER() OVER (PARTITION BY are.OriginDebtorTaxRegistrationNumber ORDER BY are.AuctionDate DESC) AS RowNum,
        are.OriginDebtorTaxRegistrationNumber
    from auctionrealestate are   
    WHERE  are.AuctionDate <= {run_date!r}
),  latest_ltv_join as (
    select distinct
        e.LTV,
        e.ExposureNumber,
        e.SnapshotDate
    from extunionfile e
    inner join (
        -- Find the latest SnapshotDate where LTV is not null before or on {run_date!r}
        select max(SnapshotDate) as LatestSnapshotDate
        from extunionfile
        where SnapshotDate <= {run_date!r}
        and try_cast(LTV as int) is not null
    ) latest
    on e.SnapshotDate = latest.LatestSnapshotDate
    inner join ref_calendar rfc
    on e.SnapshotDate = try_cast(rfc.CalendarDate as date)
    where e.UTPTypeCode is not null
),risk_bucket_calc_join AS (
select
    max(ebacalc.RiskBucketCalc) as MaximumCustomersCalculatedRiskBucket,
    min(ebacalc.RiskBucketCalc) as MinimumCustomersCalculatedRiskBucket,
    ex.BusinessDate,
    ex.SourceSystem,
    ex.Customer
from exposure ex
left join ebacalc_join ebacalc
on ex.ExposureKey = ebacalc.ExposureKey
group by ex.Customer, ex.BusinessDate, ex.SourceSystem
)

SELECT  DISTINCT 
        cast(Exposure.BusinessDate as date) as BusinessDate,
        cast(case when gefyra.Account_Number is not null then 'Bridge Perimeter'  else null end as string) as BridgePerimeter,
        try_cast(replace(replace(gefyra.BalanceL2, '.',''), ',','.') as decimal(19,2)) as BalanceL2,
        cast(gefyra.SubsidyCategory as string) as SubsidyCategory,
        cast(gefyra.BridgeEndOfMonthSubsidyDate as string) as BridgeEndOfMonthSubsidyDate,
        cast(gefyra.BridgeProgramm as string) as BridgeProgrammBridgeIBridgeII,
        cast(Null as string) as BridgeStatus,
        cast(savedinfo.ECAAssetClass as string) as ECAAssetClass,
        cast(cast(replace(u.IFRSGrossCarryingAmount, ',','') as decimal(19,4)) as decimal(19,4))  as IFRSGrossCarryingAmt,
        cast(case
            when u4.FPLUnderProbationFlag  = 1 then 'CFPL'
            else u4.EBAStatus
        end as string) as EBAStatusPreviousMonth,
        cast(case
            when u.FPLUnderProbationFlag  = 1 then 'CFPL'
            else u.EBAStatus
        end as string) as EBAStatus,
        cast(u.HFSFlag as string) as HFS,
        try_cast(
            CASE 
                WHEN u.EBAStatus = 'FNPL' THEN DATE_ADD(try_cast(u.ProbationStartDate AS DATE), 365)
                WHEN u.EBAStatus = 'CPFL' THEN DATE_ADD(try_cast(u.ProbationStartDate AS DATE), 730)
                ELSE try_cast(u.ProbationStartDate AS DATE) 
            END AS DATE
        ) AS ProbationDateExit,
        try_cast(latest_ltv.LTV as decimal(19,10)) as LTV,
        cast(u.BucketCategoryWithMateriality as int) as MaterialBucket,
        cast(u.BucketCategoryNMateriality as int) as NonMaterialBucket,
        cast(case
            when u.StateGuaranteeFlag != 0 then 'Stated Guarantee'
            else null
        end as string) as StateGuaranteeFlag,
        cast(ExCurrentMonthRPC.RPCCount as int) as CurrentMonthRPC,
        cast(ExPrevMonthsRPC.RPCCount as int) as PreviousMonthsRPC,
        cast(case
            when CaseCode.Queue = 'SSE84_Loans' or u.ExposureSourceSystemKey = 'SSE' then u.ExposureSourceSystemKey
            else NULL
        end as string) as SSE,
        cast(CASE
                WHEN Party.DeceasedFlag = 'true' THEN 1
                ELSE 0
        END as boolean) AS DeceasedFlag,
        cast(CASE
            WHEN Exposure.ObjectiveStatusCode like '%Step up%' THEN Exposure.ObjectiveStatusDate
            ELSE NULL
        END as date) AS StepUpModificationDate, 
        cast(CASE
            WHEN Exposure.ObjectiveStatusCode LIKE '%SPL%' THEN Exposure.ObjectiveStatusDate
            ELSE NULL
        END as date) AS SplitModificationDate,
        cast(CASE
                WHEN Exposure.ObjectiveStatusCode LIKE '074-C%'  OR
                Exposure.ObjectiveStatusCode LIKE '074-D%' THEN Exposure.ObjectiveStatusDate
                ELSE NULL
        END as date) AS HaircutModificationDate,
        cast(CASE
                WHEN Exposure.ObjectiveStatusCode LIKE '074-C6%'  OR
                Exposure.ObjectiveStatusCode LIKE '074-D1 Campaign%' THEN Exposure.ObjectiveStatusDate
                ELSE NULL
        END as date) AS DPOModificationDate,
        cast(CASE
                WHEN Exposure.ObjectiveStatusCode LIKE '074-A[0-9]%' OR
                Exposure.ObjectiveStatusCode LIKE '074-B[0-9]%' OR
                Exposure.ObjectiveStatusCode LIKE '074-C[0-9]%' OR
                Exposure.ObjectiveStatusCode LIKE '074-D[0-9]%' THEN Exposure.ObjectiveStatusDate
                ELSE NULL
        END as date) AS AnchorModificationDate,
        cast(CASE
            WHEN Exposure.ObjectiveStatusCode LIKE '074-Παράταση δόσης%'  OR
            Exposure.ObjectiveStatusCode LIKE '074-Ρύθμιση FT-Προσαρμόζω την δόση μου%' THEN Exposure.ObjectiveStatusDate
            ELSE NULL
        END as date) AS PaymentJumpExtensionModificationDate,
        cast(CASE
            WHEN Exposure.ObjectiveStatusCode='074-Overdraft-ΚΑΝΟΝΙΚΟΣ' THEN 1
            ELSE 0
        END as boolean) AS OverdraftProduct,
        cast(CASE
                WHEN Exposure.DaysPastDue < 11 THEN 1
                ELSE 0
        END as boolean) AS AccountUnder11Dpds,
        cast(Exposure.AllBorrowersDeceasedFlag as boolean) AS AccountsWithAllBorrowersDeceased,
        cast(Exposure.PartyTaxRegistrationNumber as string) as AFM,
        cast(Party.CDI as string) AS CIF,
        cast(Exposure.AgreementKey as string) AS Contract,
        cast(Exposure.AssetSegment as string) AS BU,
        cast(Exposure.ExposureStatus as string) AS DenouncedStatus,  
        cast(Exposure.DueAmount as decimal(19,4)) AS DebtAmount,
        cast(Exposure.DueAmount as decimal(26,4)) AS DueDebtAmount,
        cast(CaseCode.Queue as string) AS QueueDescription,
        cast(Product.SubProduct as string) AS Product,
        cast(Exposure.SPV as string) AS SPVFlag,
        cast(Exposure.DaysPastDue as int) AS DaysPastDue,
        cast(Exposure.Bucket as int) AS OperationalBucket,
        cast(exSoM.Bucket as int) AS SoMOperationalBucket,
        cast(Exposure.InterestRate as decimal(19,6)) AS InterestRate,
        cast(CASE
                WHEN CaseCode.Queue LIKE '%Affluent%' THEN 'Affluent'
                WHEN CaseCode.Queue LIKE '%Gold%' THEN 'Gold Business'
                ELSE null
        END as string) AS AffluentGoldBusiness,
        cast(Exposure.NextInstallmentAmount as decimal(19,2)) AS InstallmentAmount,
        cast(Product.ProductCode as int) AS SubProductCode,
        cast(concat(Product.BankingUnitCode,'-',Product.ProductDescription) as string) AS SubProduct,
        cast(ExCurrentMonthPromiseToPay.PromiseToPayCount as int) as CurrentMonthPromiseToPay,
        cast(ExCurrentMonthPromiseToPayAmount.PromiseToPayAmount as int) as CurrentMonthPromiseToPayAmount,
        cast(ExPrevMonthsPromiseToPay.PromiseToPayCount as int) as PreviousMonthsPromiseToPay,
        cast(ExPrevMonthsPromiseToPayAmount.PromiseToPayAmount as int) as PrevMonthsPromiseToPayAmount,
        cast(ExCurrentMonthPositiveToModify.PositiveToModifyCount as int) as CurrentMonthPositiveToModify,
        cast(ExPrevMonthsPositiveToModify.PositiveToModifyCount as int) as PreviousMonthsPositiveToModify,
        cast(CASE
                WHEN Exposure.ObjectiveStatusCode LIKE '074-Ρύθμιση FT-Προσαρμόζω την δόση μου%'  OR
                Exposure.ObjectiveStatusCode LIKE '074-Παράταση δόσης Επιχειρη-ΚΑΝΟΝΙΚΟΣ%' THEN 'Special Modification'
                ELSE NULL
        END as string) AS SpecialModifications,
        cast(CASE
            WHEN Exposure.ObjectiveStatusCode LIKE '%SPL%' THEN 'Split Modified'
            ELSE NULL
        END as string) AS SplitModified,
        cast(CASE
            WHEN Exposure.ObjectiveStatusCode LIKE '074-C%' OR
            Exposure.ObjectiveStatusCode LIKE '074-D%' THEN 'Haircut Modified'
            ELSE NULL
        END as string) AS HaircutModified,
        cast(CASE
            WHEN Exposure.ObjectiveStatusCode LIKE '074-C6%'  OR
            Exposure.ObjectiveStatusCode LIKE '074-D1 Campaign%' THEN 'DPO Modified'
            ELSE NULL
        END as string) AS DPOModified,
        cast(CASE
            WHEN Exposure.ObjectiveStatusCode LIKE '074-A[0-9]%' OR
            Exposure.ObjectiveStatusCode LIKE '074-B[0-9]%' OR
            Exposure.ObjectiveStatusCode LIKE '074-C[0-9]%' OR
            Exposure.ObjectiveStatusCode LIKE '074-D[0-9]%' THEN 'Anchor Modified'
            ELSE NULL
        END as string) AS AnchorModified,
        cast(CASE
            WHEN Exposure.ObjectiveStatusCode LIKE '074-Παράταση δόσης%'  OR
            Exposure.ObjectiveStatusCode LIKE '074-Ρύθμιση FT-Προσαρμόζω την δόση μου%' THEN 'Payment Jump Extension Modification'
            ELSE NULL
        END as string) AS PaymentJumpExtensionModification,
        cast(CASE
            WHEN Exposure.ObjectiveStatusCode LIKE '%Calypso%' THEN 'Calypso Modification'
            ELSE NULL
        END as string) AS Calypso,
        cast(CASE
                WHEN Exposure.ObjectiveStatusCode LIKE '074-Ρύθμιση FT-Προσαρμόζω την δόση μου%'  OR
                    Exposure.ObjectiveStatusCode LIKE '074-Παράταση δόσης Επιχειρη-ΚΑΝΟΝΙΚΟΣ%' THEN Exposure.ObjectiveStatusDate
                ELSE NULL
        END as date) AS SpecialModificationsDate,
        cast(Party.StaffFlag as string) AS Staff,
        cast(CASE
                WHEN CaseCode.Queue IN ('WHL_PRODUCTS','WHL_NOTICE ADMIN','WHL _SP TREATMENT','Wholesale_No_Flag',
                    'WHL _SP_TREATMENT_CL','Galaxy_SPV67_Wholesale_g','Cosmos_SPV73W_Wholesale_c')  
                THEN 1
                ELSE 0
        END as boolean) AS Wholesale,
        cast(CASE 
                WHEN CaseCode2.Queue LIKE '%4469%' 
                THEN 1 
                ELSE 0 
        END AS boolean) as OCWCustomers,
        cast(Party.Watchlist as string) AS WatchList,
        cast(Exposure.IsGuaranteeFlag as boolean) AS GovernmentGuaranteed,
        cast(securedwithRE.SecuredAccount as boolean) as SecuredAccount,
        cast(case when sum(case when securedwithRE.SecuredAccount = 1  then 1 else 0 end) over (partition by Exposure.CDI) >= 1 then 1 else 0 end as boolean) as SecuredCustomer,
        cast(Party.IsWholeshaleFlag as boolean) AS CustomerWholesale,
        cast(Exposure.OutstandingTotalBalance as decimal(19,4)) AS FinalBalance,
        cast(Exposure.OutstandingTotalBalanceMLT as decimal(19,4)) AS MLTAmount,
        cast(Exposure.OutstandingTotalBalanceMLT as decimal(19,4))AS TotalBalance,
        cast(l3869.CalcLawStatus as string) as L3869Status,
        cast(CASE
                WHEN u.ECAAssetClass IN ('Non-revolving Consumer', 'Revolving Consumer') AND
                    (u.ExposureSourceSystemKey = 'SSE' AND u.ProductCode IN ('6016', '6031', '6900', '6902', '6903', '6904', '6905', '6906', '6907', '6908', '6909', '42033'))
                THEN 1
                ELSE 0
        END as boolean) AS AutoLoan,
        cast(Party.ConnectedBorrowerFlag as boolean) as ConnectedBorrowerFlag,
        cast(CASE
                WHEN u.StateGuaranteeFlag IN ('1','2','3','4') OR Exposure.IsGuaranteeFlag = 'true'  
                THEN 1
                ELSE 0
        END as boolean) AS Eggyhmena,
        cast(CASE
            WHEN (Exposure.ExposureKey IS NOT NULL AND
            (CaseCode.Queue='LEGAL - ΠΤΩΧΕΥΣΗ ΕΠΙΧΕΙΡΗΣΕΩΝ' OR
            LPSAgrDebtor.OperationalLegalStatusLevel3 IN ('ΕΚΠΟΙΗΣΗ ΠΕΡΙΟΥΣΙΑΚΩΝ','ΠΕΡΑΤΩΣΗ ΑΚΑΡΠΗ', 'ΜΗ ΕΠΑΛΗΘΕΥΣΗ ΑΠΑΙΤΗΣΕΩΝ ΤΡΑΠΕΖΗΣ','ΕΝΩΣΗ ΠΙΣΤΩΤΩΝ','ΑΠΟΡΡΙΨΗ ΑΙΤΗΣΗΣ ΠΤΩΧΕΥΣΗΣ','ΑΙΤΗΣΗ ΠΤΩΧΕΥΣΗΣ')))           
            THEN 1
            ELSE 0 
        END as boolean) AS BankruptcyFlag,
        cast(CASE
            WHEN (Exposure.ExposureKey IS NOT NULL AND
            (CaseCode.Queue='LEGAL - ΠΡΟΠΤΩΧΕΥΤΙΚΗ ΔΙΑΔΙΚΑΣΙΑ' OR     
            LPSAgrDebtor.OperationalLegalStatusLevel3 IN ('ΣΥΜΦΩΝΙΑ ΣΥΝΔΙΑΛΛΑΓΗΣ','ΣΥΜΦΩΝΙΑ ΑΝΑΔΙΟΡΓΑΝΩΣΗΣ','ΜΗ ΣΥΜΦΩΝΙΑ ΕΚΚΑΘΑΡΙΣΗΣ','ΜΗ ΣΥΜΦΩΝΙΑ ΛΟΙΠΩΝ ΠΡΟΠΤΩΧΕΥΤΙΚΩΝ ΔΙΑΔΙΚΑΣΙΩΝ','ΜΗ ΣΥΜΦΩΝΙΑ ΣΥΝΔΙΑΛΛΑΓΗΣ','ΜΗ ΣΥΜΦΩΝΙΑ ΑΝΑΔΙΟΡΓΑΝΩΣΗΣ','ΑΙΤΗΜΑ ΕΙΔΙΚΗΣ ΕΚΚΑΘΑΡΙΣΗΣ',
                     'ΑΙΤΗΜΑ ΛΟΙΠΩΝ ΠΡΟΠΤΩΧΕΥΤΙΚΩΝ ΔΙΑΔΙΚΑΣΙΩΝ','ΑΙΤΗΜΑ ΣΥΝΔΙΑΛΛΑΓΗΣ','ΑΙΤΗΜΑ ΑΝΑΔΙΟΡΓΑΝΩΣΗΣ','ΣΥΜΦΩΝΙΑ ΕΚΚΑΘΑΡΙΣΗΣ','ΣΥΜΦΩΝΙΑ ΛΟΙΠΩΝ ΠΡΟΠΤΩΧΕΥΤΙΚΩΝ ΔΙΑΔΙΚΑΣΙΩΝ'))

            )AND Exposure.AssetSegment='SB' 
            THEN 1
            ELSE 0 
        END as boolean) AS PrebankruptcyFlag,
        cast(Exposure.Currency as string) AS ContractCurrency,
        cast(CASE    WHEN Exposure.BranchCode = '0950' AND  u.AcMaintenServiceBranchCode = '950'
                THEN 1
                ELSE 0
        END as boolean) AS PrivateBanking,
        cast(CASE
                WHEN CaseCode.Queue in ('Emp_Confirmed_Fraud','Emp_Υλοποιημένες Συμβιβαστικές Εξοφλήσεις') THEN CaseCode.Queue
                ELSE NULL
        END as string) AS SpecialQueues,
        cast(cast(replace(u1.CollateralCapTotalTangible, ',', '') as numeric) as int) as CollateralValue,
        cast(case
                when Party.PartyType is null and Exposure.PartyTaxRegistrationNumber like '09%' then 'Organization'
                when Party.PartyType is null and Exposure.PartyTaxRegistrationNumber like '08%' then 'Organization'
                when Party.PartyType is null and Exposure.PartyTaxRegistrationNumber like '8%'  then 'Organization'
                when Party.PartyType is null and Exposure.PartyTaxRegistrationNumber like '9%'  then 'Organization'
                when Party.PartyType is not null then Party.PartyType
            else 'Individual'
        end as string) AS CustomerType,
        cast(qMap.CourseOfAction as string) AS COAFlag, ---!!!! join with Queues Mapping external file on Queues to retreive CourseOfAction
        cast(qMap.AssignmentSegmentDetailed as string) AS AssignmentSegmentDetailed, ---!!!! join with Queues Mapping external file on Queues to retreive AssignmentSegmentDetailed
        cast(assignment.ChannelCode as string) as AssignmentChannel,
        cast(securedwithRe.AccWithCollateralRE as boolean) as AccWithCollateralRE,
        cast(ebacalc.EBAStrategyCalc as string) as EBAStrategyCalc,
        cast(ebacalc.EBAStrategyCustomerCalc as string) as EBAStrategyCustomerCalc,
        cast(ebacalc.InflowTargetDateCalc as date) as InflowTargetDateCalc,
        cast(Null as string) as EligibleForSettlement,
        cast(case 
                when u.UTP_IND is null and u.UTPTypeCode is not null then 
                    case 
                        WHEN u.UTPTypeCode IN ('01', '1') THEN 'UTP_DR'
                        WHEN u.UTPTypeCode IN ('02', '2') THEN 'UTP_Balloon'
                        WHEN u.UTPTypeCode IN ('03', '3') THEN 'UTP_Grace_Period'
                        WHEN u.UTPTypeCode IN ('04', '4') THEN 'UTP_FonF'
                        WHEN u.UTPTypeCode IN ('06', '6') THEN 'UTP_Cured_FPL_30_facility'
                        WHEN u.UTPTypeCode IN ('08', '8') THEN 'UTP_Cured_FPL_Reforbearance_facility'
                        WHEN u.UTPTypeCode = '13' THEN 'UTP_Retail_Bankruptcy_Law'
                        WHEN u.UTPTypeCode = '14' THEN 'UTP_Debt_Forgiveness'
                        WHEN u.UTPTypeCode = '21' THEN 'UTP_Denouncement'
                        WHEN u.UTPTypeCode = '30' THEN 'UTP_Restructuring_Rejected'
                        WHEN u.UTPTypeCode = '31' THEN 'UTP_Restructuring_Not_Proceeded'
                        WHEN u.UTPTypeCode = '36' THEN 'UTP_POCI'
                        WHEN u.UTPTypeCode = '37' THEN 'UTP_Fraud'
                        WHEN u.UTPTypeCode = '38' THEN 'UTP_Deceased'
                        WHEN u.UTPTypeCode = '49' THEN 'EBA_NPL_facility_forbearance'
                        WHEN u.UTPTypeCode = '50' THEN 'EBA_FPL_facility_90'
                        WHEN u.UTPTypeCode = '51' THEN 'EBA_FPL_facility_UTP'
                        WHEN u.UTPTypeCode = '52' THEN 'EBA_FNPL_facility_forbearance'
                        WHEN u.UTPTypeCode = '53' THEN 'EBA_Contamination'
                        WHEN u.UTPTypeCode = '54' THEN 'EBA_PL_FPL_facility_forbearance_above90dpd'
                        WHEN u.UTPTypeCode = '55' THEN 'UTP_Business_Cards'
                    end
                else u.UTP_IND
            end as string) as UTPReason,
        u.SoMUnionFileExistenceFlag as SoMUnionFileExistenceFlag, 
        cast(u.UTPTypeCode as int) as UTPTypeCode,
        cast(dod.DODDPDStartDate as date) as DODDPDStartDate,
        cast(dod.DODDPDEndDate as date) as DODDPDEndDate,
        cast(ExPrevMonthsContact.ContactCount as int) as PreviousMonthsContacts,
        cast(coalesce(lpfx.LastMod, ltvmodified_calc.Category) as string) as LastModification,
        cast(coalesce(lpfx2.CountOfModifications,0) + coalesce(NoFModificationsCoreStatic.CountOfModifications,0) as int) as CountOfModifications,
        cast(Null as string) as LegalProcessStatusLevel1,
        cast(Null as string) as LegalProcessStatusLevel2,
        cast(Null as string) as OperationalLegalProcessStatus,
        cast(Null as date) as LegalProcessStatusChangeDate,
        cast(Exposure.ExposureKey as string) as Account,
        cast(CASE
            WHEN Exposure.ObjectiveStatusCode LIKE '%Step up%' THEN 'Step Up Modified'
            ELSE NULL
        END as string) AS StepUpModified,
        --cast(Exposure.ObjectiveStatusCode as string) as StepUpModified,
        cast(Exposure.OnBalance as decimal(19,2)) as OnBalance,
        cast(Exposure.OffBalance as decimal(19,2)) as OffBalance,
        cast(Exposure.ExposureStatusDate as date) as AccountStatusDate,
        cast(CASE
                WHEN Exposure.PartyTaxRegistrationNumber ='' THEN Party.CDI
                WHEN Exposure.PartyTaxRegistrationNumber IS NOT NULL THEN  Exposure.PartyTaxRegistrationNumber
                ELSE  Party.CDI
        END AS string) as Customer,
        cast(ltvmodified_calc.RestructuringDate as date) as LastModificationDate,
        cast(DATEDIFF(month,{run_date!r},TO_DATE(lpfx.LastModification, 'dd/MM/yyyy')) as int) MonthsSinceLastModification,
        cast(Null as string) as EligibleForHaircut,
        cast(Null as string) as NotInITPool,
        cast(Null as string) as FailUAT,
        cast(exp.ContractOnBalance as decimal(19,2)) as ContractOnBalance,
        cast(u5.ContractGBVBalance as double) as ContractGBVBalance,
        cast(exp.ContractOnOffBalance as decimal(19,2)) as ContractOnOffBalance,
        cast(u6.CustomerGBV as double) as CustomerGBV,
        cast(exp2.CustomerOnOffBalance as decimal(19,2)) as CustomerOnOffBalance,
        cast(exp2.CustomerOn as decimal(19,2)) as CustomerOn,
        --cast(exp2.MaxCustomerOperationalBucket as int) as MaxCustomerOperationalBucket,
        --cast(exp2.MinCustomerOperationalBucket as int) as MinCustomerOperationalBucket,

        CAST(
        CASE 
            WHEN Exposure.PartyRelationshipType = 'Primary Owner'
            THEN COALESCE(exp2.MaxCustomerOperationalBucket, 0)
            ELSE 0
        END
        AS INT) AS MaxCustomerOperationalBucket,

        CAST(
        CASE 
            WHEN Exposure.PartyRelationshipType = 'Primary Owner'
            THEN COALESCE(exp2.MinCustomerOperationalBucket, 0)
            ELSE 0
        END
        AS INT) AS MinCustomerOperationalBucket,

        cast(CASE
                WHEN Exposure.OnBalance < 200 THEN 1
                ELSE 0
        END AS decimal(19,2)) as AccountLowBalance,
        cast(CASE
                WHEN exp2.CustomerLowBalance < 2000 then 1
                ELSE 0
        END AS decimal(19,2)) as CustomerLowBalance,
        cast(CASE
                WHEN Exposure.OutstandingTotalBalanceMLT < 100 THEN 1
                ELSE 0
        END AS boolean) as AccountLowDebt,
        cast(case 
                when dod.DODDPDEndDate = (select PreviousMonthLastWorkingDate from ref_calendar where CalendarDate = {run_date!r}) then 0 
                when dod.DODDPDStartDate = (select PreviousMonthLastWorkingDate from ref_calendar where CalendarDate = {run_date!r}) then 1 
                else ebacalc.RiskBucketCalc 
        end as int)
        as RiskBucketCalc, 
        
        --cast(riskbucket.MaximumCustomersCalculatedRiskBucket as int) as MaximumCustomersCalculatedRiskBucket,
        --cast(riskbucket.MinimumCustomersCalculatedRiskBucket as int) as MinimumCustomersCalculatedRiskBucket,
        
        CAST(
        CASE 
            WHEN Exposure.PartyRelationshipType = 'Primary Owner'
            THEN COALESCE(riskbucket.MaximumCustomersCalculatedRiskBucket, 0)
            ELSE 0
        END
        AS INT) AS MaximumCustomersCalculatedRiskBucket,

        CAST(
        CASE 
            WHEN Exposure.PartyRelationshipType = 'Primary Owner'
            THEN COALESCE(riskbucket.MinimumCustomersCalculatedRiskBucket, 0)
            ELSE 0
        END
        AS INT) AS MinimumCustomersCalculatedRiskBucket,        

        cast(u2.MaxBucketCategoryNMateriality as int) as MaximumCustomerRiskBucket,
        cast(u2.MinBucketCategoryNMateriality as int) as MinimumCustomerRiskBucket,
        cast(ebacalc.EBAStatusCalc as string) as EBAStatusCalc,
        cast(u.FPLUnderProbationFlag as string) as FPLUnderProbationFlag,
        cast(case
                when u1.EBAStatus = 'PL' or u1.EBAStatus = 'FPL' then 'PE'
                when u1.EBAStatus = 'NPL' or u1.EBAStatus = 'FNPL' then 'NPE'
                when u1.FPLUnderProbationFlag = 1 then 'CFPL'
        end as string) as EBAStrategy,
        cast(u1.EBAStrategyBorrower as string) as CustomerEBAStrategy,
        cast(
            case 
                when dod.DODDPDEndDate <> '9999-12-31' then NULL
                when u.EBAStatus = 'FPL' and u.FPLUnderProbationFlag = 1 and dateadd(day, 30, dod.DODDPDStartDate) >= dod.BusinessDate then dateadd(day, 30, dod.DODDPDStartDate)
                when (u.EBAStatus <> 'FPL' or u.FPLUnderProbationFlag = 0) and dateadd(day, 90, dod.DODDPDStartDate) >= dod.BusinessDate then dateadd(day, 90, dod.DODDPDStartDate)
                else NULL
            end as date
        ) as InflowTargetDate,
        cast(case when lpfx_nallc.NotAllowedToCure is null then 0 else lpfx_nallc.NotAllowedToCure end as string) as NotAllowedToCure,
        cast(null as string) as PaymentJumpCategory, -- only null values based on Jira ticket PMT-878
        cast(PaymentJumpTotalPerimeter.PaymentJumpMonth as string) as PaymentJumpMonth,
        cast(TO_DATE(CONCAT(PaymentJumpTotalPerimeter.PaymentJumpMonth, '01'), 'yyyyMMdd') as date) as PaymentJumpMonthDate,
        cast(fasttrack.FastTrackFlag as string) as PaymentJumpExtension,
        cast(case 
                when Interaction.InteractionTypeKey = '621' then 'Payment Jump Criteria Not Met'
                else null 
        end as string) as PaymentJumpCriteriaNotMet,
        cast(reportunified.ActiveApplicationDate as date) as ActiveApplicationDate,
        cast(reportunified.ActiveApplicationCreditReviewStepDate as date) as ActiveApplicationCreditReviewStepDate,
        cast(reportunified.ActiveApplicationChannel as string) as ActiveApplicationChannel,
        cast(case  
                when  Settlement.SettlementType <> 'Νόμος 3869 – Προσωρινή Διαταγή'
                and Settlement.SettlementStatusCode in (5093,5092)
                and Settlement.BucketCode <= 2 then 1 else 0
        end as boolean) as ActiveLegalSettlementsFlag,
        cast(PayCurrentMonth.SumPayments as double) as CurrentMonthPayments,
        cast(PayPrevious12Months.SumPayments as double) as Previous12MonthsPayments,
        cast(ExCurrentMonthContact.ContactCount as int) as CurrentMonthContacts, 
        cast(case
                when ExCurrentMonthContact.ContactCount != 0 then 'Current Month Contacted Account'
                else null
        end as string) as CurrentMonthContactedAccount,
        cast(case
                when PCurrentMonthContact.ContactCount != 0 then 'Current Month Contacted Customer'
                else null
        end as string) as CurrentMonthContactedCustomer,
        cast(case
                when ExPrevMonthsContact.ContactCount != 0 then 'Previous Months Contacted Account'
                else null
        end as string) as PreviousMonthsContactedAccount,
        cast(case
                when PPrevMonthsContact.ContactCount != 0 then 'Previous Months Contacted Customer'
                else null
        end as string) as PreviousMonthsContactedCustomer,
        cast(case
                when ExCurrentMonthRPC.RPCCount != 0 then 'Current Month Rpc Account'
                else null
        end as string) as CurrentMonthRPCAccount,
        cast(case
                when PCurrentMonthRPC.RPCCount != 0 then 'Current Month Rpc Customer'
                else null
        end as string) as CurrentMonthRPCCustomer,
        cast(case
                when ExPrevMonthsRPC.RPCCount != 0 then 'Previous Months Rpc Account'
                else null
                end as string) as PreviousMonthsRPCAccount,
        cast(case
                when PPrevMonthsRPC.RPCCount != 0 then 'Previous Months Rpc Customer'
                else null
        end as string) as PreviousMonthsRPCCustomer,
        cast(case
                when ExCurrentMonthPromiseToPay.PromiseToPayCount != 0 then 'Current Month Promise To Pay Account'
                else null
        end as string) as CurrentMonthPromiseToPayAccount,
        cast(case
                when PCurrentMonthPromiseToPay.PromiseToPayCount != 0 then 'Current Month Promise To Pay Customer'
                else null
        end as string) as CurrentMonthPromiseToPayCustomer,
        cast(case
                when ExPrevMonthsPromiseToPay.PromiseToPayCount != 0 then 'Previous Months Promise To Pay Account'
                else null
        end as string) as PreviousMonthsPromiseToPayAccount,
        cast(case
                when PPrevMonthsPromiseToPay.PromiseToPayCount != 0 then 'Previous Months Promise To Pay Customer'
                else null
        end as string) as PreviousMonthsPromiseToPayCustomer,
        cast(case
                when ExCurrentMonthPositiveToModify.PositiveToModifyCount != 0 then 'Current Month Positive To Modify Account'
                else null
        end as string) as CurrentMonthPositiveToModifyAccount,
        cast(case
                when PCurrentMonthPositiveToModify.PositiveToModifyCount != 0 then 'Current Month Positive To Modify Account'
                else null
        end as string) as CurrentMonthPositiveToModifyCustomer,
        cast(case
                when ExPrevMonthsPositiveToModify.PositiveToModifyCount != 0 then 'Previous Months Positive To Modify Account'
                else null
        end as string) as PreviousMonthsPositiveToModifyAccount,
        cast(case
                when PPrevMonthsPositiveToModify.PositiveToModifyCount != 0 then 'Previous Months Positive To Modify Customer'
                else null
        end as string) as PreviousMonthsPositiveToModifyCustomer,
        cast(reportunified.ActiveApplicationNumber as string) as ActiveApplicationNumber,
        cast(case when sum(case when securedwithRE.SecuredAccount = 1  then 1 else 0 end) over (partition by Exposure.AgreementKey) >= 1 then 1 else 0 end as boolean) as SecuredContract, 
        cast(securedwithRe.SecuredUnsecuredAccountWithCollateralRE as int) as SecuredUnsecuredAccountWithCollateralRE, 
        cast(securedwithRe.SecuredUnsecuredContractWithCollateralRE as int) as SecuredUnsecuredContractWithCollateralRE, 
        cast(securedwithRe.SecuredUnsecuredCustomerWithCollateralRE as int) as SecuredUnsecuredCustomerWithCollateralRE, 
        cast(RealEstateResearch.OutcomeCode as int) as eap,
        cast(RealEstateResearch.TaxCDI as string) as eapafm,
        cast(CaseCode.WatchlistFlag as string) as CustomerWatchlist,
        cast(Settlement.SettlementStatus as string) as Settlements,
        cast(Exposure.TreatmentBranch as string) as TreatmentBranch,
        cast(case 
            when f.EmployeeCategory is not null 
            then assignment.AgentPartyKey
        end as string) as AgentRM,
        cast(case 
            when f.EmployeeCategory is not null 
            then concat(f.EmployeeFirstName, ';', f.EmployeeLastName) 
        end as string) as AgentRMFullName,
        cast(case 
            when f.EmployeeCategory is null 
            then f1.EmployeeFullName
        end as string) as Agency,
        -- cast(case 
        --         when (savedinfo.InflowTargetDate is not null or savedinfo.cFPLInflow = 1 or savedinfo.BucketCategoryWithMateriality > 0) 
        --         and RiskSaved = 'Saved' then SoMOutstandingTotalBalance 
        --         when OperationalSaved='Saved' then SoMOutstandingTotalBalance 
        -- end as string) as SavedBalance,  
        cast(savedinfo.SavedBalance as string) as SavedBalance, 
        cast((CASE WHEN HFSFlag=1 THEN FinalOperationalSaved ELSE savedinfo.SavedFlag END) as string) as SavedFlag, 
        cast((CASE WHEN HFSFlag=1 THEN FinalOperationalSavedDate ELSE savedinfo.SavedDate END) as date) as SavedDate,
        cast(savedinfo.SavedReason as string) as SavedReason, 
     --   cast(savedinfo.InflowTypeOld as string) as InflowTypeOld, 
        cast(savedinfo.InflowType as string) as InflowType, 
		-- cast(case 
        --         when savedinfo2.cFPLInflow = 1 then 'Proactive NPE Inflow' 
        --         else null 
        --         end as string) as InflowType, 
		cast(savedinfo.FinalOperationalSaved as string) as FinalOperationalSaved,
		cast(savedinfo.FinalOperationalSavedDate as date) as FinalOperationalSavedDate,
		cast(savedinfo.FinalRiskSaved as string) as FinalRiskSaved,
		cast(savedinfo.FinalRiskSavedDate as date) as FinalRiskSavedDate,        
        -- cast(case 
        --         when savedinfo.InflowTargetDate is not null 
        --         or savedinfo.cFPLInflow = 1 
        --         or savedinfo.BucketCategoryWithMateriality > 0 then RiskSaved 
        --         else OperationalSaved 
        --         end as string) as SavedFlag, 
        -- cast(case 
        --         when savedinfo.OperationalSaved is not null 
        --         or savedinfo.RiskSaved is not null 
        --         or savedinfo.OutflowSaved is not null then SavedReason 
        -- end as string) as SavedReason, 
        cast(savedinfo.OperationalMLSaved as string) as OperationalMLSaved,
        cast(savedinfo.OperationalLMSSaved as string) as OperationalLMSSaved,
        cast(savedinfo.OperationalCMSSaved as string) as OperationalCMSSaved,
        cast(savedinfo.OperationalSBSaved as string) as OperationalSBSaved,
        cast(savedinfo.RiskSaved as string) as RiskSaved,
        cast(savedinfo.OutflowSaved as string) as OutflowSaved,
        cast(case when lpfx_nallc.NotAllowedToCure is null and utpind.UTP is null then 1 else 0 end as boolean) as PotentialPoolFNPLCurings,
        cast(Exposure.PartyRelationshipType AS string) AS RelationType,
        cast(Exposure.ObjectiveStatusCode as string) as ObjectiveStatusCode,
        cast(savedinfo.ExposureSegmentation as string) as ExposureSegmentation,
        cast(savedinfo.NonCoreFlag as string) as NonCoreFlag,
        cast(reportunified.UnifiedApplicationStatus as string) as ActiveApplicationStatus,
        cast(l3869.CourtOutcome as string) as CustomerStatusN3869,
        cast(dul.CourtDate as date) as CourtDate,
        cast(ltvmodified_calc.Category as string) as LastModificationDetailed,
        cast(are_future.FutureAuctionDate as date) as FutureAuctionDate,
        cast(are_past.PastAuctionDate as date) as PastAuctionDate,
        cast(are_past.PastAuctionResult as string) as PastAuctionResult,
                -- AttributionCOA calculation
        CAST(
            CASE 
                WHEN assignment.AttributionChannel = 'Outsourcing Collection' THEN 'Collection'
                WHEN assignment.AttributionChannel IN ('HUB RMs', 'IRT', 'Legal Agencies', 'Legal Agencies 3869') 
                    AND (CASE WHEN u.FPLUnderProbationFlag = 1 THEN 'CFPL' ELSE u.EBAStatus END) IN ('PL', 'CFPL', 'FPL') THEN 'Double Treatment'
                WHEN assignment.AttributionChannel = 'HUB RMs / Call Center' THEN 'Double Treatment'
                WHEN assignment.AttributionChannel IN ('HUB RMs', 'Call Center', 'Legal Agencies', 'Legal Agencies 3869') 
                    AND (COALESCE(CaseCode.Queue, qMap.SavedQueueDescription) LIKE '%DT%' 
                        OR COALESCE(CaseCode.Queue, qMap.SavedQueueDescription) LIKE '%Dou%') THEN 'Double Treatment'
                WHEN assignment.AttributionChannel IN ('HUB RMs', 'IRT', 'Legal Agencies', 'Legal Agencies 3869') THEN 'Restructuring'
                ELSE qMap.CourseOfAction
            END AS STRING
        ) AS AttributionCOA,
        cast(case when ltvmodified_calc.ModsModificationDate >= ltvmodified_calc.RestructuringDate then true else false end as boolean) as LowLTVModified,
        cast(u1.SnapshotDate as date) as SnapshotDate
from  exposure Exposure 
inner join party_join Party
on Exposure.PartyKey = Party.PartyKey
left join dexposure_join DExposure
on  DExposure.ExposureKey = Exposure.ExposureKey
left join lpsagreementdebtor_join LPSAgrDebtor
on  LPSAgrDebtor.AgreementKey = Exposure.AgreementKey
    and LPSAgrDebtor.CDI = Exposure.CDI
left join casecode_join CaseCode
on  CaseCode.CustCode = Exposure.PartyKey
    and cast(CaseCode.DebtCode as string)=cast(Exposure.ExposureSourceSystemKey as string)
left join casecodeB_join CaseCode2 
on  CaseCode2.CustCode = Exposure.PartyKey
    and coalesce(Exposure.PartyTaxRegistrationNumber, Exposure.CDI) = CaseCode2.TaxCDI
    and cast(CaseCode2.DebtCode as string)=cast(Exposure.ExposureSourceSystemKey as string)
left join exp1_join exp
on  exp.AgreementKey = Exposure.AgreementKey
left join exp2_join exp2
on  exp2.Customer = Exposure.Customer
left join settlement_join Settlement
on   Exposure.ExposureKey = Settlement.ExposureKey
left join  realestateresearch_join RealEstateResearch  
on  coalesce(Exposure.PartyTaxRegistrationNumber, Exposure.CDI) = RealEstateResearch.TaxCDI
    and RealEstateResearch.Rnk = 1 
left join product_join Product
on  Product.ProductKey = concat(Exposure.ProductKey)
left join interaction_join Interaction
on  Exposure.ExposureKey = Interaction.ExposureKey
    and Interaction.Rank = 1
left join interaction1_join ExPrevMonthsPositiveToModify
on  Exposure.ExposureKey = ExPrevMonthsPositiveToModify.ExposureKey
left join interaction2_join ExCurrentMonthPositiveToModify
on  Exposure.ExposureKey = ExCurrentMonthPositiveToModify.ExposureKey
left join interaction3_join PPrevMonthsPositiveToModify
on  Exposure.PartyKey = PPrevMonthsPositiveToModify.PartyKey
left join interaction4_join PCurrentMonthPositiveToModify
on  Exposure.PartyKey = PCurrentMonthPositiveToModify.PartyKey
left join interaction5_join ExPrevMonthsRPC
on  Exposure.ExposureKey = ExPrevMonthsRPC.ExposureKey
left join interaction6_join ExCurrentMonthRPC
on  Exposure.ExposureKey = ExCurrentMonthRPC.ExposureKey
left join interaction7_join PPrevMonthsRPC
on  Exposure.PartyKey = PPrevMonthsRPC.PartyKey
left join interaction8_join PCurrentMonthRPC
on  Exposure.PartyKey = PCurrentMonthRPC.PartyKey
left join interaction9_join ExPrevMonthsPromiseToPayAmount
on  Exposure.ExposureKey = ExPrevMonthsPromiseToPayAmount.ExposureKey 
and ExPrevMonthsPromiseToPayAmount.Rank = 1
left join interaction10_join ExCurrentMonthPromiseToPayAmount
on  Exposure.ExposureKey = ExCurrentMonthPromiseToPayAmount.ExposureKey
    and ExCurrentMonthPromiseToPayAmount.Rank = 1
left join interaction11_join ExPrevMonthsPromiseToPay
on  Exposure.ExposureKey = ExPrevMonthsPromiseToPay.ExposureKey
left join interaction12_join ExCurrentMonthPromiseToPay
on  Exposure.ExposureKey = ExCurrentMonthPromiseToPay.ExposureKey
left join interaction13_join PPrevMonthsPromiseToPay
on  Exposure.PartyKey = PPrevMonthsPromiseToPay.PartyKey
left join interaction14_join PCurrentMonthPromiseToPay
on  Exposure.PartyKey = PCurrentMonthPromiseToPay.PartyKey
left join interaction15_join ExPrevMonthsContact
on  Exposure.ExposureKey = ExPrevMonthsContact.ExposureKey
left join interaction16_join ExCurrentMonthContact
on  Exposure.ExposureKey = ExCurrentMonthContact.ExposureKey
left join interaction17_join PPrevMonthsContact
on  Exposure.PartyKey = PPrevMonthsContact.PartyKey
left join interaction18_join PCurrentMonthContact
on  Exposure.PartyKey = PCurrentMonthContact.PartyKey
left join paymentjumptotalperimeter_join PaymentJumpTotalPerimeter
on    Exposure.ExposureKey = PaymentJumpTotalPerimeter.ExposureKey
left join unionfile_join u
on u.ExposureNumber = Exposure.ExposureKey
left join unionfile1_join u1
on u1.ExposureNumber = Exposure.ExposureKey
left join unionfile2_join u2
on u2.BorrowerKey = Exposure.PartyKey
left join unionfile4_join u4
on u4.ExposureNumber = Exposure.ExposureKey
left join unionfile5_join u5 
on u5.AgreementKey = Exposure.AgreementKey
left join unionfile6_join u6 
on u6.BorrowerKey = Exposure.PartyKey
left join latest_ltv_join latest_ltv
on latest_ltv.ExposureNumber = Exposure.ExposureKey
left join fasttrack_join fasttrack
on Exposure.ExposureKey = fasttrack.ExposureKey
left join PayPrevious12Months_join PayPrevious12Months
on Exposure.ExposureKey = PayPrevious12Months.ExposureKey
left join PayCurrentMonth_join PayCurrentMonth
on Exposure.ExposureKey = PayCurrentMonth.ExposureKey
left join dod_join dod
on (
    (u.ECAAssetClass = 'Revolving Consumer' and dod.ExposureKey = Exposure.AgreementKey) 
    or 
    (u.ECAAssetClass != 'Revolving Consumer' and dod.ExposureKey = Exposure.ExposureKey)
)
left join lpfx_join lpfx
on      Exposure.ExposureKey = lpfx.NewExposureKey 
left join lpfx2_join   lpfx2
on      substring(Exposure.AgreementKey, 3, 50) = lpfx2.AgreementKey
left join NoFModificationsCoreStatic_join   NoFModificationsCoreStatic
on      substring(Exposure.AgreementKey, 3, 50) = NoFModificationsCoreStatic.AgreementKey
left join l3869_join l3869
on  Exposure.ExposureKey = l3869.ExposureKey
--left join l3869Total_join l3869Total
--on  Exposure.ExposureKey = l3869Total.ExposureKey
--    and l3869Total.Rank = 1
left join assignment_join assignment 
on  Exposure.ExposureKey = assignment.ExposureKey
    and Exposure.BusinessDate = assignment.BusinessDate 
left join firmstructure f 
on assignment.AgentLoginName = f.EmployeeUsername and assignment.SourceSystem = f.SourceSystem and assignment.BusinessDate = f.BusinessDate
left join firmstructure f1
on  assignment.ExternalCompanyPartyKey = f1.EmployeePartyKey 
    and assignment.SourceSystem = f1.SourceSystem 
    and assignment.BusinessDate = f1.BusinessDate 
    and f1.EmployeeCategory = 'External Company'
left join reportunified_join reportunified
on       Exposure.ExposureKey = reportunified.ExposureKey
         and Exposure.BusinessDate = reportunified.BusinessDate
left join securedwithRE_join securedwithRE
on       Exposure.ExposureKey = securedwithRE.ExposureKey
         and Exposure.BusinessDate = securedwithRE.BusinessDate
left join ebacalc_join ebacalc
on       Exposure.ExposureKey = ebacalc.ExposureKey
left join lpfx_nallc_join lpfx_nallc 
on Exposure.ExposureKey = lpfx_nallc.NewExposureKey
left join utpind_join utpind
on Exposure.ExposureKey = utpind.ExposureNumber
left join savedinfo_join savedinfo
on      savedinfo.ExposureKey = Exposure.ExposureKey 
        and Exposure.BusinessDate = savedinfo.BusinessDate
left join gefyra_join gefyra
on      gefyra.Account_Number = Exposure.ExposureKey
        and gefyra.Rank = 1
left join core_queusmapping qMap 
on  UPPER(CaseCode.Queue) = UPPER(qMap.SavedQueueDescription) 
    and qMap.ReferenceDate = (  select max(ReferenceDate) 
                                from core_queusmapping 
                                where ReferenceDate <= {run_date!r}
                            )    
left join exSoM_join exSoM
on exSoM.ExposureKey = Exposure.ExposureKey 
left join dunderlaw_join dul
on  Exposure.CDI = dul.CDI 
    and Exposure.SourceSystem = dul.SourceSystem  
left join ltvmodified_calc_join ltvmodified_calc
on ltvmodified_calc.NewExposureKey = Exposure.ExposureKey and ltvmodified_calc.RowNum = 1
left join are_future_join are_future 
on Exposure.PartyTaxRegistrationNumber = are_future.OriginDebtorTaxRegistrationNumber
and are_future.RowNum = 1
left join are_past_join are_past 
on Exposure.PartyTaxRegistrationNumber = are_past.OriginDebtorTaxRegistrationNumber
and are_past.RowNum = 1
left join risk_bucket_calc_join riskbucket
on  riskbucket.Customer = Exposure.Customer
and riskbucket.BusinessDate = Exposure.BusinessDate and riskbucket.SourceSystem = Exposure.SourceSystem
""")

# COMMAND ----------

final_df.createOrReplaceTempView("final_df")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write DataFrame to Delta Lake based on 'unity' value

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
