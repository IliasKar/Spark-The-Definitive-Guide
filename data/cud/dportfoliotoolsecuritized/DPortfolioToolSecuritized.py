# Databricks notebook source
# MAGIC %md 
# MAGIC ##### Import Libaries and Functions

# COMMAND ----------

import logging
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
# MAGIC * run_date : '2024-01-12'
# MAGIC * unity : True | False
# MAGIC
# MAGIC In case of Unity Catalog, the appropriate cluster should be attached

# COMMAND ----------

# dbutils.widgets.text("run_date", '2024-01-12')
# dbutils.widgets.text("table_name", 'DSecuritizedPortfolioTool')
# dbutils.widgets.text("unity", 'True')
# dbutils.widgets.text("run_date_paymentschedule", '2023-12-21')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Get the values of widgets and initialize the environment variable
# MAGIC Ensure that widgets are assigned the wanted values before proceeding with the next step.

# COMMAND ----------

# Takes the values of widgets
run_date = dbutils.widgets.get("run_date") 
# table_name = dbutils.widgets.get("table_name") 
table_name = 'DPortfolioToolSecuritized'
unity = dbutils.widgets.get("unity") 
# run_date_paymentschedule = dbutils.widgets.get("run_date_paymentschedule") 
# run_date_paymentschedule = '2023-12-21' # will be change in the future

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
    "ExposureKey string,"
    "ExposureKeyWithPrefix string,"
    "AssetClass string,"
    "AFM string,"
    "CDI string,"
    "AgreementKey string,"
    "L3869StatusCode string,"
    "L3869Status string,"
    "L3869Flag int,"
    "L3869CourtDate timestamp,"
    "InterbankFlag string,"
    "OutstandingTotalBalance double,"
    "OCWCustomerFlag boolean,"
    "ActiveLegalSettlementsFlag boolean,"
    "AllBorrowersDeceasedFlag string,"
    "FullyWrittenOffFlag boolean,"
    "OperationalBucket int,"
    "DenouncementStatus string,"
    "SecuredFlag boolean,"
    "DebtLegalProcessStatus string,"
    "DebtLPSDate date,"
    "SubProductDescription string,"
    "QueueDescription string,"
    "TotalDueAmount decimal(26,4),"
    "SubProductCode int,"
    "PositiveLoanEAPIndicator boolean,"
    "GBV double,"
    "TotalNoOfModifications int,"
    "NoOfModificationsNonGOrSProduct int,"
    "NewOrAlphaModificationProductType string,"
    "ApplicationStatus string,"
    "AccountImplementationStatus string,"
    "ActivationDate date,"
    "PayerNonPayer string,"
    "SPVName string,"
    "SPV string,"
    "ActualPayment decimal(19,2),"
    "KeptRate  decimal(19,10),"
    "NewExposureKey string,"
    "OldExposureKey string,"
    "ModificationDate date,"
    "TypeOfModification string,"
    "ModificationFlag boolean,"
    "RealEstateKey string,"
    "RealEstateCepalKey string,"
    "ActiveIndicator string,"
    "RealEstateStatusCode string,"
    "RealEstateSubCategory string,"
    "RealEstateSubCategoryDescription string,"
    "RealEstateCategory string,"
    "RealEstateUsageCode string,"
    "RealEstateAddress string,"
    "RealEstateArea string,"
    "RealEstateCity string,"
    "RealEstatePostalCode string,"
    "RealEstateLandArea double,"
    "RealEstateLandOwnership decimal(19,4),"
    "RealEstateFiscalValue decimal(19,4),"
    "RealEstateMarketValue decimal(19,4)," 
    "RealEstateMarketabilityCode string,"
    "RealEstateValuationType string,"
    "ValuationDate date,"
    "RealEstateHotelFlag boolean,"
    "OwnerPartyKey string,"
    "OwnershipType string,"
    "OwnershipPercentage double,"
    "OwnershipDate date,"
    "OwnershipStatus string,"
    "SecurityKey string,"
    "CollateralKey string,"
    "CollateralCategory string,"
    "CollateralSubCategory string,"
    "TypeOfNote string,"
    "CollateralAmount decimal(19,2),"
    "CollateralCurrency string,"
    "LienOrder int,"
    "CollateralStatus string,"
    "CollateralRegistrationDate date,"
    "RealEstateCollateralStatus string,"
    "Marketability int,"
    "Eligibility double,"
    "MarketabilityEligibilitySegment string,"
    "CollateralValueCREV double,"
    "AllocatedRev double,"
    "WholesaleIndicator boolean,"
    "LegalEntityIndicator string,"
    "DeceasedFlag boolean,"
    "CustWatchlist string,"
    "RelationType string,"
    "Borrower string,"
    "BorrowerTotalGBV double,"
    "LTVCalc double,"
    "EligiblePrenotedAsset string,"
    "BorrowerPayerExposureTotalGBV double,"
    "BorrowerCollateralValueCrev decimal(19,12),"
    "AgreementTotalGBV double,"
    "PaymentsLast12months decimal(15,2),"
    "InstallmentLast12Months decimal(38,7),"
    "PaymentsTrnxFlagging int,"
    "PaymentsAfterModification decimal(15,2),"
    "InstallmentAfterModification decimal(38,7),"
    "PostalCodeCommunicationAddress string,"
    "PostalCode string,"
    "LastRPCDate timestamp,"
    "RPCcreatedby string,"
    "ModificationsAfterCepalMigration int,"
    "SoMOperationalBucket int,"
    "AssignmentChannel string,"
    "AgentRM string,"
    "AgentRMFullName string,"
    "Agency string,"
    "ApplicationKey string,"
    "ApplicationDate timestamp,"
    "ImplementationDate date,"
    "BaseLTV double,"
    "FuturePaymentShockFlag boolean"
    ",PastPaymentShockFlag boolean"
    ",InstallmentBucket int"
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

# MAGIC     
# MAGIC %md 
# MAGIC ##### exposure

# COMMAND ----------

exposure = spark.sql(
f'''
SELECT 
    ExposureKey,
    PartyKey,
    PartyRelationshipType,
    AgreementKey,
    PortfolioKey,
    ExposureStatus,
    PartyTaxRegistrationNumber,
    AllBorrowersDeceasedFlag,
    OutstandingTotalBalance,
    CDI,
    PhysicalNumber,
    IsActiveFlag,
    ExposureStatusCode,
    BucketCode,
    Bucket,
    CaseCode,
    PortfolioName,
    ProductCode,
    ProductKey,
    BankingUnitCode,
    DueAmount,
    LastRestructuringDate,
    LastRestructuringType,
    ExposureSourceSystemKey,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.primary.exposure
WHERE   BusinessDate = {run_date!r}
'''
)

exposure.createOrReplaceTempView("exposure")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### party

# COMMAND ----------

party = spark.sql(
f'''
SELECT 
    Watchlist,
    DeceasedFlag,
    L3869InterbankingStatus,
    L3869CourtDate,
    L3869Status,
    PartyType,
    OCWStatus,
    PartyKey,
    CDI,
    IsWholeshaleFlag,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.primary.party
WHERE   BusinessDate = {run_date!r}
'''
)

party.createOrReplaceTempView("party")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### restructuring

# COMMAND ----------

restructuring = spark.sql(
f'''
SELECT 
    NewExposureKey,
    OldExposureKey,
    RestructuringDate,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.primary.restructuring
WHERE   BusinessDate = {run_date!r}
'''
)

restructuring.createOrReplaceTempView("restructuring")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### application

# COMMAND ----------

application = spark.sql(
f'''
SELECT 
    ApplicationImplementationStatus,
    ApplicationActivationDate,
    ApplicationKey,
    ImplementationDate,
    ApplicationDate,
    PartyKey,
    CreatedDate,
    QualcoAppDisbursedDate,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.primary.application
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'fldt'
'''
)

application.createOrReplaceTempView("application")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### portfolio

# COMMAND ----------

portfolio = spark.sql(
f'''
SELECT 
    ParentPortfolio,
    PortfolioName,
    unit_code_profits,
    AssignmentDate,
    prefix,
    Businessdate
FROM  `{env}-cdh`.primary.portfolio
WHERE   BusinessDate = {run_date!r}
'''
)

portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### dexposure

# COMMAND ----------

dexposure = spark.sql(
f'''
SELECT 
    AssetSegment,
    GBV,
    ExposureKeyQCAlpha,
    ExposureKey,
    FutureStepUpIndicator,
    PastStepUpIndicator,
    ExposureStatus,
    IsSecuredFlag,
    DueAmount, 
    DueExpense,
    AgreementKey,
    BucketCode,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.derivative.dexposure
WHERE   BusinessDate = {run_date!r}
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
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.primary.casecode
WHERE   BusinessDate = {run_date!r}
'''
)

casecode.createOrReplaceTempView("casecode")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### collateral

# COMMAND ----------

collateral = spark.sql(
f'''
SELECT
        CollateralAmount,
        CategoryCode,
        CollateralCurrency,
        StatusDate, -- col.CollateralDateApplied
        CollateralStatusCode,
        RealEstateCollateralStatus,
        SubCategory,
        InitialLegalOrder,
        CollateralKey, -- SecurityId
        MortgageTypeCode,
        CollateralKey, -- WeightId
        ownerpartykey,
        AgreementKey,
        RealEstateKey,
        SourceSystem,
        BusinessDate
FROM  `{env}-cdh`.primary.collateral
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'thesis' 
'''
)

collateral.createOrReplaceTempView("collateral")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### paymentschedule

# COMMAND ----------

paymentschedule = spark.sql(
f'''
SELECT 
    ExposureKey,
    InstallmentAmountPaid,
    InstallmentAmount,
    InstallmentDate,
    InstallmentType,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.derivative.dpaymentschedule
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'profits'
'''
)

paymentschedule.createOrReplaceTempView("paymentschedule")

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
# MAGIC ##### product

# COMMAND ----------

product = spark.sql(
f'''
SELECT 
    ProductDescription,
    ProductKey,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.primary.product
WHERE   BusinessDate = {run_date!r}
'''
)

product.createOrReplaceTempView("product")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### realestatebuilding

# COMMAND ----------

realestatebuilding = spark.sql(
f'''
SELECT 
    RealEstateActive,
    RealEstateAddress1,
    RealEstateArea,
    RealEstateCategory,
    RealEstateCity,
    RealEstateSubCategoryCode,
    RealEstateSubCategory,
    RealEstateFiscalValue,
    RealEstateHotelFlag,
    RealEstateLandArea,
    RealEstateLandOwnership,
    RealEstateMarketabilityCode,
    RealEstateMarketValue,
    RealEstateCepalKey,
    RealEstateKey,
    RealEstateStatus,
    RealEstateUsageCodeType,
    RealEstateEvaluationType,
    RealEstateEvaluationDate,
    RealEstatePostalCode,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.primary.realestatebuilding
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'thesis'
'''
)

realestatebuilding.createOrReplaceTempView("realestatebuilding")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### realestateinfo

# COMMAND ----------

realestateinfo = spark.sql(
f'''
SELECT 
    OwnerPartyKey,
    NotaryDate,
    OwnershipTypeCode,
    OwnershipPercentage,
    OwnershipStatusCode,
    RealEstateKey,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.primary.realestateinfo
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'thesis'
'''
)

realestateinfo.createOrReplaceTempView("realestateinfo")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### settlementexposure

# COMMAND ----------

settlementexposure = spark.sql(
f'''
SELECT 
    settlementType,
    settlementStatusCode,
    BucketCode,
    Bucket,
    ExposureKey,
    SubmissionDate,
    SourceSystem,
    Businessdate
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
    RealEstateResearchDate,
    PartyKey,
    CancelledDate,
    RealEstateResearchStatusCode,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.primary.realestateresearch
WHERE   BusinessDate = {run_date!r}
'''
)

realestateresearch.createOrReplaceTempView("realestateresearch")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### danalyticsmetrics

# COMMAND ----------

danalyticsmetrics = spark.sql(
f'''
SELECT 
    ExpCREV,
    PrimaryPartyCDI,
    ExposureKey,
    PayerFlagAccount,
    ExpAllocREV,
    CREVBase,
    AllocREVBase,
    AgreementKey,
    RealEstateKey,
    Businessdate
FROM  `{env}-cdh`.derivative.danalyticsmetrics
WHERE   BusinessDate = {run_date!r}
'''
)

danalyticsmetrics.createOrReplaceTempView("danalyticsmetrics")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### contactdetails

# COMMAND ----------

contactdetails = spark.sql(
f'''
SELECT 
    PartyKey,
    City,
    PostalCode,
    Street,
    CommunicationFlag,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.primary.contactdetails
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'profits'
'''
)

contactdetails.createOrReplaceTempView("contactdetails")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### interaction

# COMMAND ----------

interaction = spark.sql(
f'''
SELECT 
    ExposureKey,
    InteractionDate,
    Createdby,
    SourceSystem,
    Businessdate,
    IsRPCFlagCalc,
    CDI
FROM  `{env}-cdh`.primary.get_interactions_by_business_date({run_date!r})
WHERE  SourceSystem = 'qca'
'''
)

interaction.createOrReplaceTempView("interaction")

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
    ExternalCompanyPartyKey,
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
# MAGIC ##### underlaw

# COMMAND ----------

underlaw = spark.sql(
f'''
SELECT 
    InLawFlag,
    CDI,
    SourceSystem,
    Businessdate
FROM  `{env}-cdh`.derivative.dunderlaw
WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'qca'
'''
)

underlaw.createOrReplaceTempView("underlaw")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### fldt_unifiedloan

# COMMAND ----------

fldt_unifiedloan = spark.sql(
f'''
SELECT 
    AccountNumberWithPrefix,
    NumberOfPreviousModifications,
    loanid,
    Businessdate
FROM  `{env}-cdh`.landed.fldt_unifiedloan
WHERE   BusinessDate = {run_date!r}
'''
)

fldt_unifiedloan.createOrReplaceTempView("fldt_unifiedloan")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### fldt_loan

# COMMAND ----------

fldt_loan = spark.sql(
f'''
SELECT 
    AccountNumberWithPrefix,
    NumberOfPreviousModifications,
    id,
    Businessdate
FROM  `{env}-cdh`.landed.fldt_loan
WHERE   BusinessDate = {run_date!r}
'''
)

fldt_loan.createOrReplaceTempView("fldt_loan")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### biteam_avg_marketability_eligibility_contract_category

# COMMAND ----------

biteam_avg_marketability_eligibility_contract_category = spark.sql(
f'''
WITH portfolio_max_businessdate AS(
  SELECT PortfolioId, MAX(BusinessDate) as MaxBusinessDate
  FROM `{env}-ds`.aaas.marketability_eligibility
  WHERE BusinessDate <= {run_date!r}
  GROUP BY PortfolioId
),
biteam_avg_marketability_eligibility_contract_category AS (
SELECT 
    market.PortfolioId,
    Category,
    MedianeligibilitycREV,
    AvgMarketabilitycREV,
    CDI,
    AgreementNumber,
    Businessdate,
    row_number() OVER(PARTITION BY AgreementNumber, CDI ORDER BY IngestionTimestamp DESC) rn --keep the latest record for each agreement number, cdi
FROM  `{env}-ds`.aaas.marketability_eligibility market
JOIN portfolio_max_businessdate maxdate --keep the max businessdate data for each portfolio
ON market.PortfolioId = maxdate.PortfolioId
AND market.BusinessDate = maxdate.MaxBusinessDate
)
SELECT * FROM biteam_avg_marketability_eligibility_contract_category WHERE rn = 1;
'''
)

biteam_avg_marketability_eligibility_contract_category.createOrReplaceTempView("biteam_avg_marketability_eligibility_contract_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dlienanalysisltv

# COMMAND ----------

dlienanalysisltv = spark.sql(
f'''
SELECT 
    BaseLTV,
    AgreementKey
FROM  `{env}-cdh`.derivative.dlienanalysisltv 
WHERE   BusinessDate = {run_date!r}
'''
)

dlienanalysisltv.createOrReplaceTempView("dlienanalysisltv")

# COMMAND ----------

# MAGIC %md
# MAGIC #####payers_flag_algorithm_snapshot

# COMMAND ----------

payers_flag_algorithm = spark.sql(
f'''
SELECT 
    DISTINCT
    Account_Number_with_Prefix,
    Payer_Flag_Account
FROM  `{env}-ds`.aaas.payers_flag_algorithm
WHERE BusinessDate = (
    SELECT MAX(BusinessDate) AS SnapshotBusinessDate
    FROM `{env}-ds`.aaas.payers_flag_algorithm
    WHERE BusinessDate <= {run_date!r}
)
'''
)

payers_flag_algorithm.createOrReplaceTempView("payers_flag_algorithm")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### lpsagreementdebtor

# COMMAND ----------

lpsagreementdebtor = spark.sql(
f'''
SELECT 
    AgreementKey,
    CDI,
    OperationalLegalStatusLevel3,
    LegalProcessStatusLastChangeDate,
    BusinessDate
FROM  `{env}-cdh`.derivative.dlegalprocessstatusagreementdebtor
WHERE   BusinessDate = {run_date!r}
'''
)

lpsagreementdebtor.createOrReplaceTempView("lpsagreementdebtor")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Final query with resulted DataFrame
# MAGIC

# COMMAND ----------

final_df = spark.sql(f"""
WITH party_join AS (
    select distinct 
        PartyKey,
        SourceSystem,
        Watchlist ,
        DeceasedFlag ,
        L3869InterbankingStatus,
        L3869CourtDate ,
        L3869Status ,
        CDI ,
        IsWholeshaleFlag    
    from party   
    where SourceSystem = 'qca'                 
),
exqca_join AS (
    select distinct 
        AllBorrowersDeceasedFlag,
        cdi, 
        BusinessDate, 
        ExposureKey, 
        BucketCode,
        ExposureStatus
    from exposure
    where SourceSystem = 'qca'                    
),
lpsagreementdebtor_join AS (
    select distinct
        AgreementKey,
        CDI,
        OperationalLegalStatusLevel3,
        LegalProcessStatusLastChangeDate
    from lpsagreementdebtor                     
),
partyprof_join AS (
    select distinct 
        PartyKey,
        SourceSystem,
        PartyType,
        OCWStatus    
    from party                     
),
collateral_join AS (
    select distinct
        CollateralAmount,
        CategoryCode,
        CollateralCurrency,
        StatusDate, -- col.CollateralDateApplied
        CollateralStatusCode,
        RealEstateCollateralStatus,
        SubCategory,
        InitialLegalOrder,
        CollateralKey, -- SecurityId
        MortgageTypeCode,
        CollateralKey, -- WeightId
        ownerpartykey,
        AgreementKey,
        RealEstateKey,
        SourceSystem,
        BusinessDate
    from collateral                    
),
borGBV_join AS (
    select 
        coalesce(ex.PartyTaxRegistrationNumber,ex.cdi) as Borrower,
        ex.SourceSystem,
        sum(dex.GBV) GBV,
        ex.PortfolioName
    from dexposure dex
    inner join exposure ex
    on dex.ExposureKey = ex.ExposureKey and dex.BusinessDate = ex.BusinessDate and dex.SourceSystem = ex.SourceSystem
    where ex.SourceSystem = 'profits'
    and ex.PartyRelationshipType = 'Primary Owner'
    and ex.IsActiveFlag = true
    group by coalesce(ex.PartyTaxRegistrationNumber,ex.cdi),
            ex.SourceSystem, ex.PortfolioName                  
),
agrGBV_join AS (
    select  
        ex.AgreementKey,
        ex.SourceSystem,
        sum(dex.GBV) GBV
    from dexposure dex
    inner join exposure ex
    on dex.ExposureKey = ex.ExposureKey and dex.BusinessDate = ex.BusinessDate and dex.SourceSystem = ex.SourceSystem
    where ex.SourceSystem = 'profits'
    and ex.PartyRelationshipType = 'Primary Owner'
    and ex.IsActiveFlag = true
    group by  ex.AgreementKey,
            ex.SourceSystem                       
),
product_join AS (
    select distinct 
        ExposureKeyQCAlpha, 
        e.BusinessDate, 
        e.ExposureKey, 
        p.ProductDescription
    from dexposure d
    inner join exposure e
    on  d.ExposureKeyQCAlpha = e.ExposureKey 
        and d.BusinessDate = e.BusinessDate 
        and e.SourceSystem = 'qca' 
    left join product p
    on  p.ProductKey = concat(e.ProductKey)
        and p.SourceSystem = e.SourceSystem
),
application_join AS (
        select ApplicationKey, PartyKey, BusinessDate, ApplicationImplementationStatus, ApplicationActivationDate, CreatedDate, QualcoAppDisbursedDate
        from (
        select distinct 
            ApplicationKey, 
            PartyKey, 
            ApplicationImplementationStatus, 
            ApplicationActivationDate, 
            CreatedDate, 
            QualcoAppDisbursedDate, 
            BusinessDate, 
            rank() over (partition by PartyKey, BusinessDate order by CreatedDate desc) rank
        from application
        where SourceSystem = 'fldt'
        )
        where rank = 1                   
),
dexposure_join AS (
    select distinct 
        AssetSegment,
        GBV,
        FutureStepUpIndicator,
        PastStepUpIndicator,
        IsSecuredFlag,
        ExposureKey, 
        BucketCode,
        SourceSystem
    from dexposure                      
),
casecode_join AS (
    select distinct 
        e.cdi,
        e.ExposureKey,
        e.SourceSystem, 
        c.Queue
    from casecode c
    inner join exposure e
    on  c.CustCode = e.PartyKey
        and cast(c.DebtCode as string)=cast(e.ExposureSourceSystemKey as string)
    where c.SourceSystem in ('qca','qc')                      
),
paymentschedule_join AS (
    select  
        psc.ExposureKey, 
        psc.BusinessDate, 
        psc.SourceSystem,
        sum(psc.InstallmentAmountPaid) as ActualAmount,
        abs(case
                        when sum(psc.InstallmentAmount) = 0 THEN NULL
                        else sum(psc.InstallmentAmountPaid) / sum(psc.InstallmentAmount)
        end) as KeptRate,
        abs(sum(case when restr.RestructuringDate >= dateadd(month, -12, psc.BusinessDate) and restr.RestructuringDate <= psc.BusinessDate then 0 else psc.InstallmentAmount end )) as InstallmentLast12Months,
        abs(sum(case when restr.RestructuringDate >= dateadd(month, -12, psc.BusinessDate) and restr.RestructuringDate <= psc.BusinessDate and psc.InstallmentDate >= dateadd(month, 1, trunc(restr.RestructuringDate, 'MM')) then psc.InstallmentAmount else 0 end )) as InstallmentAfterModification
    from    paymentschedule psc
    left join ( select 
                    NewExposureKey,
                    BusinessDate,
                    SourceSystem,
                    max(RestructuringDate) RestructuringDate
                from  restructuring
                group by NewExposureKey, BusinessDate, SourceSystem
    ) restr
    on  restr.NewExposureKey = psc.ExposureKey 
        and restr.BusinessDate = psc.BusinessDate 
        and restr.SourceSystem = psc.SourceSystem
    where   psc.SourceSystem = 'profits' 
            and psc.InstallmentDate >= dateadd(month, -12, psc.BusinessDate)  
            and psc.InstallmentDate <= psc.BusinessDate
            and InstallmentType in  ('Installment', 'Instalment-Cash', 'Interest Request', 'CapitalRequest')
    group by psc.ExposureKey, psc.SourceSystem, psc.BusinessDate                    
),
settlementexposure_join AS (
    select distinct 
        SettlementType,
        SettlementStatusCode,
        BucketCode,
        Bucket,
        ExposureKey, 
        BusinessDate,
        SourceSystem
    FROM  `{env}-cdh`.primary.settlementexposure
    WHERE   BusinessDate = {run_date!r}
        AND SourceSystem = 'qca'
        and SettlementStatus in ('Running', 'Partially Fulfilled')
),
settlementexposure2_join AS (
        SELECT distinct 
              ExposureKey,
              max(SubmissionDate) SubmissionDate
        FROM  settlementexposure
        group by ExposureKey
),
intallmentbucketcalc_join AS (
    select ExposureKey, sum(InstallmentAmount) as InstallmentAmount
    from
    (
        SELECT 
            ExposureKey,
            InstallmentAmountPaid,
            InstallmentAmount,
            InstallmentDate,
            InstallmentType,
            SourceSystem,
            Businessdate
        FROM  `{env}-cdh`.derivative.dpaymentschedule
        WHERE   BusinessDate = {run_date!r}
                AND SourceSystem in ('profits', 'qca')
    )
    where InstallmentType in  ('Installment', 'Instalment-Cash', 'Interest Request', 'CapitalRequest')
    and month({run_date!r}) = month(InstallmentDate) and year({run_date!r}) = year(InstallmentDate)
    group by ExposureKey
),
collection_join AS (
    select   
        c.ExposureKey,
        sum(case 
                when GREATEST(restr.RestructuringDate, se.SubmissionDate) >= dateadd(month, -12, c.BusinessDate) and GREATEST(restr.RestructuringDate, se.SubmissionDate) <= c.BusinessDate 
                then 0 
                else c.Amount 
	        end 
        ) as PaymentsLast12months,
        count(  case 
                    when (GREATEST(restr.RestructuringDate, se.SubmissionDate) < dateadd(month, -12, c.BusinessDate) or GREATEST(restr.RestructuringDate, se.SubmissionDate) is null) and c.Amount > 10 then 1 
                end
        ) as PaymentsTrnxFlagging,
        sum(case 
                when GREATEST(try_cast(restr.RestructuringDate as date), try_cast(se.SubmissionDate as date)) >= dateadd(month, -12, c.BusinessDate) and GREATEST(try_cast(restr.RestructuringDate as date), try_cast(se.SubmissionDate as date)) <= c.BusinessDate and c.ActualDate >= dateadd(month, 1, trunc(GREATEST(try_cast(restr.RestructuringDate as date), try_cast(se.SubmissionDate as date)), 'MM'))  
                then c.Amount 
                else 0 
            end
        ) as PaymentsAfterModification
    from (
        SELECT distinct
              Amount,
              ActualDate,
              regexp_replace(ExposureKey, '^[A-Za-z]+', '') ExposureKey,
              PaymentType,
              Businessdate
        FROM  `{env}-cdh`.derivative.dcollection
        WHERE   BusinessDate = {run_date!r}
    ) c
    left join ( select 
                    regexp_replace(NewExposureKey, '^[A-Za-z]+', '') NewExposureKey,
                    cast(max(RestructuringDate) as date) RestructuringDate
                from  (SELECT 
                            NewExposureKey,
                            RestructuringDate
                        FROM  restructuring
                        WHERE   BusinessDate = {run_date!r}
                        and SourceSystem = 'profits'
                        )   
                group by NewExposureKey
    ) restr
    on  restr.NewExposureKey = c.ExposureKey 
    left join (
        SELECT distinct 
                  regexp_replace(ExposureKey, '^[A-Za-z]+', '') ExposureKey,
                  cast(max(SubmissionDate) as date) SubmissionDate
            FROM  settlementexposure
            group by ExposureKey
    ) se
    on  se.ExposureKey = c.ExposureKey
    where   c.ActualDate >= dateadd(month, -12, c.BusinessDate)  
            and c.ActualDate <= c.BusinessDate
            and c.PaymentType not like 'Ποσό κατάταξης%'
    group by c.ExposureKey
),
realestatebuilding_join AS (
    select distinct
        RealEstateActive, -- ActiveIndicator
        RealEstateAddress1, -- Address
        RealEstateArea,
        RealEstateCategory,
        RealEstateCity,
        RealEstateSubCategoryCode, -- Code
        RealEstateSubCategory, -- Description
        RealEstateFiscalValue,
        RealEstateHotelFlag,
        RealEstateLandArea,
        RealEstateLandOwnership,
        RealEstateMarketabilityCode,
        RealEstateMarketValue,
        RealEstateCepalKey, -- or reb.RealEstateIsEAPFlag as PropertyKey
        RealEstateKey, -- PropertyId
        RealEstateStatus,
        RealEstateUsageCodeType,
        RealEstateEvaluationType,
        RealEstateEvaluationDate,
        RealEstatePostalCode,
        SourceSystem
    from realestatebuilding                     
),
restructuring_join AS (
    select 
        NewExposureKey, 
        OldExposureKey,
        SourceSystem,
        max(RestructuringDate) RestructuringDate
    from  restructuring
    where try_cast(RestructuringDate as date) <= BusinessDate   
    group by NewExposureKey, OldExposureKey, SourceSystem             
),
realestateinfo_join AS (
    select distinct
        OwnerPartyKey, -- OwnerId
        NotaryDate,
        OwnershipTypeCode,
        OwnershipPercentage, -- Percentage
        OwnershipStatusCode, -- Status
        RealEstateKey,
        SourceSystem
    from realestateinfo                    
),
eap_join AS (
    select 
        b.ExposureKey,
        max(a.RealEstateResearchDate) as RealEstateResearchDate
    from realestateresearch a
    inner join exposure b
    on  a.PartyKey=b.PartyKey
        and a.SourceSystem=b.SourceSystem
    where   a.SourceSystem ='qca'
            and a.CancelledDate is null
            and a.RealEstateResearchStatusCode is not null
            and a.RealEstateResearchStatusCode not in ('6946','6940')
    group by b.ExposureKey                  
),
contactdetails_join AS (
  select distinct  
        PartyKey, 
        City, 
        PostalCode,
        concat(Street,';',City,';',PostalCode) as FullAddress,
        SourceSystem, 
        CommunicationFlag
  from contactdetails 
  where CommunicationFlag = 1 -- and FullAddress is not null and trim(FullAddress) <> ''                    
),
interaction_join AS (
    SELECT
        i.CDI,
        MAX(i.InteractionDate) AS LastRPCDate,
        FIRST(f.EmployeeFullName) AS RPCcreatedby
    FROM interaction i
    LEFT JOIN firmstructure f
        ON i.CreatedBy = f.EmployeePartyKey
    WHERE i.IsRPCFlagCalc = true
    GROUP BY i.CDI                  
),
port_join AS (
    select 
        AgreementKey, 
        count(distinct LastRestructuringDate) as ModificationsAfterCepalMigration
    from exposure e
    inner join portfolio
    on  portfolio.Portfolioname = e.PortfolioName
        and portfolio.AssignmentDate <= e.LastRestructuringDate
        and e.BusinessDate = portfolio.BusinessDate
    where   SourceSystem = 'profits'
            and e.LastRestructuringDate is not null
            and e.BusinessDate >= portfolio.AssignmentDate
    group by AgreementKey               
),
underlaw_join AS (
    select distinct 
        CDI, 
        InLawFlag
    from underlaw                  
),
danalyticsmetrics_join AS (
    select distinct 
        ExpCREV,
        CREVBase,
        PrimaryPartyCDI,
        ExposureKey,
        PayerFlagAccount,
        AllocREVBase,
        AgreementKey,
        RealEstateKey
    from danalyticsmetrics                     
),
AllocAgrRev_join AS (
    select distinct  
        sum(dan.AllocREVBase) as AllocatedCollateralValueREV,
        dan.agreementkey
    from  danalyticsmetrics dan
    WHERE dan.BusinessDate = {run_date!r}
    group by dan.AgreementKey                 
),
BorColCrev_join AS (
    select  
        coalesce(e.PartyTaxRegistrationNumber,e.cdi) as Borrower, 
        e.SourceSystem,
        e.PortfolioName,
        sum(d.CREVBase) as TotalCREV
    from (
        select distinct CDI, PartyTaxRegistrationNumber, BusinessDate, SourceSystem, PortfolioKey, PortfolioName  
        from exposure
        where SourceSystem = 'profits'
        ) e 
    inner join danalyticsmetrics d
    on e.CDI = d.PrimaryPartyCDI 
    inner join      portfolio port
    on              e.PortfolioKey=port.unit_code_profits
    and             e.businessDate=port.businessDate
    and             (ParentPortfolio in ('COSMOS','GALAXY'))
    group by coalesce(e.PartyTaxRegistrationNumber,e.cdi), e.SourceSystem, e.PortfolioName                      
),
BorPayerGBV_join AS (
        select 
            coalesce(ex.PartyTaxRegistrationNumber,ex.cdi) as Borrower,
            ex.SourceSystem,
            sum(dex.GBV) GBV
        from dexposure dex
        inner join exposure ex
        on  dex.ExposureKey = ex.ExposureKey 
            and dex.SourceSystem = ex.SourceSystem
        inner join payers_flag_algorithm payers_flag
        on ex.ExposureKey = payers_flag.Account_Number_with_Prefix 
        where   ex.SourceSystem = 'profits' 
                and payers_flag.Payer_Flag_Account = 'Payer'
        group by coalesce(ex.PartyTaxRegistrationNumber,ex.cdi), ex.SourceSystem                    
),
biteam_avg_marketability_eligibility_contract_category_join AS (
    select distinct 
        Category, 
        MedianeligibilitycREV, 
        AvgMarketabilitycREV, 
        CDI, 
        AgreementNumber
    from  biteam_avg_marketability_eligibility_contract_category                    
),
AgrbiREV_join AS (
    select 
        ExpAllocREV, 
        RealEstateKey, 
        AgreementKey
    from danalyticsmetrics
    group by AgreementKey, ExpAllocREV, RealEstateKey                   
),
fl_join AS (
    select distinct 
        coalesce(b.AccountNumberWithPrefix,a.AccountNumberWithPrefix) as AccountNumberWithPrefix,
        coalesce(b.NumberOfPreviousModifications,a.NumberOfPreviousModifications) as NumberOfPreviousModifications
    from fldt_loan a
    left join fldt_unifiedloan b
    on  a.id = b.loanid               
),
assignment_join AS (
    select distinct 
        a.ExposureKey, 
        a.ChannelCode, 
        a.BusinessDate, 
        a.AgentPartyKey, 
        a.SourceSystem , 
        a.ExternalCompanyPartyKey
    from assignment a
    inner join exposure e 
    on  e.PartyKey = a.PartyKey 
        and e.ExposureKey = a.ExposureKey 
        and a.SourceSystem = e.SourceSystem
    where   a.AssignmentType = 'Channel Assignment' 
            and e.PartyRelationshipType = 'Primary Owner'                
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
    and sourcesystem = 'profits'              
), 
dltv_join AS (
        select distinct 
                BaseLTV,
                AgreementKey
        from dlienanalysisltv             
),
payers_flag_algorithm_join AS (
        select distinct 
            Account_Number_with_Prefix,
            Payer_Flag_Account
        from payers_flag_algorithm
)
SELECT  
    cast(ex.BusinessDate AS date) AS BusinessDate,
    cast(substring(ex.ExposureKey, 4, 50) AS string) AS ExposureKey,
    cast(ex.ExposureKey AS string) AS ExposureKeyWithPrefix,
    cast(case when dex.AssetSegment='NULL' then null else dex.AssetSegment end AS string) AS AssetClass,
    cast(ex.PartyTaxRegistrationNumber AS string) AS AFM,
    cast(ex.CDI AS string) AS CDI,
    cast(ex.AgreementKey AS string) AS AgreementKey,
    cast(p.L3869Status AS string) AS L3869StatusCode,
    cast(
        case
            when p.L3869Status = '01' then 'Αίτηση για πληροφόρηση'
            when p.L3869Status = '02' then 'Έκπτωση από το Νόμο'
            when p.L3869Status = '03' then 'Συμβιβαστική Εξόφληση'
            when p.L3869Status = '04' then 'Αίτημα δικαστικού συμβιβασμού'
            when p.L3869Status = '05' then 'Αίτημα δικαστικού συμβιβασμού-Ασφαλιστικά Μέτρα'
            when p.L3869Status = '06' then 'Παραίτηση'
            when p.L3869Status = '07' then 'Δικαστική Ρύθμιση'
            when p.L3869Status = '08' then 'Μη τήρηση απόφασης – διαδικασία έκπτωσης'
            when p.L3869Status = '09' then 'Απόρριψη αιτήματος'
            when p.L3869Status = '10' then 'Δικαστική Εξόφληση'
            when p.L3869Status = '11' then 'Ματαίωση'
            when p.L3869Status = '12' then 'Ολοκλήρωση Δικαστικής Ρύθμισης'
            when p.L3869Status = '13' then 'Συμβιβαστική Ρύθμιση'
            when p.L3869Status = '20' then 'Προσωρινή Απόφαση (θετική)'
            when p.L3869Status = '99' then 'Παύση από 3869 (3870)'
            else null
        end as string) as L3869Status,
    cast(
        case when p.L3869Status in ('04','07','08','12','20') then 1 else 0
        end as int) as L3869Flag,
    cast(p.L3869CourtDate AS timestamp) AS L3869CourtDate,
    cast(p.L3869InterbankingStatus AS string) AS InterbankFlag,
    cast(ex.OutstandingTotalBalance AS double) AS OutstandingTotalBalance,
    cast(case when pprof.OCWStatus = 1 then 1
              else 0
    end as boolean) AS OCWCustomerFlag,
    cast(case  when  settl.SettlementType <> 'Νόμος 3869 – Προσωρινή Διαταγή'
                    and   settl.SettlementStatusCode in (5093,5092) -- ('Partially Fulfilled','Running')
                    and   settl.Bucket <= 2 then 1 else 0
    end as boolean) as ActiveLegalSettlementsFlag,
    cast(exqca.AllBorrowersDeceasedFlag AS string) AS AllBorrowersDeceasedFlag,
    cast(case 
        when ex.ExposureStatusCode = 4 and ex.PartyRelationshipType = 'Primary Owner' then 'True' 
        else 'False' 
    end as boolean) AS FullyWrittenOffFlag,
    cast(ex.Bucket AS int) AS OperationalBucket,
    cast(exqca.ExposureStatus AS string) AS DenouncementStatus,
    cast(dex.IsSecuredFlag AS boolean) AS SecuredFlag,
    cast(LPSAgrDebtor.OperationalLegalStatusLevel3 AS string) AS DebtLegalProcessStatus,
    cast(LPSAgrDebtor.LegalProcessStatusLastChangeDate AS date) AS DebtLPSDate,
    cast(prd.ProductDescription AS string) AS SubProductDescription,
    cast(cc.Queue AS string) AS QueueDescription,
    cast(ex.DueAmount AS decimal(26,4)) AS TotalDueAmount,
    cast(ex.ProductCode AS int) AS SubProductCode,
    cast(case   when  EAP.ExposureKey is not null then 1 
                else 0  
    end as boolean) as PositiveLoanEAPIndicator,
    cast(dex.GBV AS double) AS GBV,
    cast(mod.ModificationsAfterCepalMigration as int) as TotalNoOfModifications,
    cast(fl.NumberOfPreviousModifications as int) as NoOfModificationsNonGOrSProduct,
    -- cast(case
    --    when port.AssignmentDate >= ex.LastRestructuringDate and ex.LastRestructuringDate is not null and ex.LastRestructuringDate != '1753-01-01' --then 'New'
    --    when ex.LastRestructuringDate is not null and ex.LastRestructuringDate != '1753-01-01' then 'Alpha Modified'
    --    else Null
    --end as string) as NewOrAlphaModificationProductType,
    cast(case   when pd.Category like '%Non Modified%' then 'Alpha Modified' 
                when pd.Category like '%G/S Modified%' then 'New'
                when pd.Category like '%new%' then 'New'
                else null
    end as string) as NewOrAlphaModificationProductType,
    cast(case 
            when ap.ApplicationImplementationStatus is null then null 
            when ap.ApplicationImplementationStatus = 'Active' then 'Active'
            else 'Not Active'
    end as string) as ApplicationStatus,
    cast(case 
        when restr.NewExposureKey is not null then 'Implemented' 
        else 'Not Implemented' 
    end as string) AS AccountImplementationStatus,
    cast(ap.ApplicationActivationDate AS date) AS ActivationDate,
    cast(ex.PortfolioName as string) AS SPVName,
    cast(port.Prefix AS string) AS SPV,
    cast(payers_flag.Payer_Flag_Account as string) as PayerNonPayer,
    cast(ps.ActualAmount AS decimal(19,2)) AS ActualPayment,
    cast(ps.KeptRate AS decimal(19,10)) AS KeptRate,
    cast(restr.NewExposureKey as string) as NewExposureKey,
    cast(restr.OldExposureKey as string) as OldExposureKey,
    cast(GREATEST(try_cast(restr.RestructuringDate as date), try_cast(settl2.SubmissionDate as date)) as date) ModificationDate,
    cast(ex.LastRestructuringType AS string) AS TypeOfModification,
    cast(case  when settl.SettlementType<>'Νόμος 3869 – Προσωρινή Διαταγή'
                and settl.SettlementStatusCode in (5093,5092) 
            then 1
            when restr.NewExposureKey is not null 
            then 1
            else 0
    end as boolean) as ModificationFlag,
    cast(reb.RealEstateKey AS string) AS RealEstateKey,
    cast(reb.RealEstateCepalKey AS string) AS RealEstateCepalKey,
    cast(reb.RealEstateActive AS string) AS ActiveIndicator,
    cast(reb.RealEstateStatus AS string) AS RealEstateStatusCode,
    cast(reb.RealEstateSubCategory AS string) AS RealEstateSubCategory,
    cast(reb.RealEstateSubCategory AS string) AS RealEstateSubCategoryDescription,
    cast(reb.RealEstateCategory AS string) AS RealEstateCategory,
    cast(reb.RealEstateUsageCodeType AS string) AS RealEstateUsageCode,
    cast(reb.RealEstateAddress1 AS string) AS RealEstateAddress,
    cast(reb.RealEstateArea AS string) AS RealEstateArea,
    cast(reb.RealEstateCity AS string) AS RealEstateCity,
    cast(reb.RealEstatePostalCode AS string) AS RealEstatePostalCode,
    cast(reb.RealEstateLandArea AS double) AS RealEstateLandArea,
    cast(reb.RealEstateLandOwnership AS decimal(19,4)) AS RealEstateLandOwnership,
    cast(reb.RealEstateFiscalValue AS decimal(19,4)) AS RealEstateFiscalValue,
    cast(reb.RealEstateMarketValue AS decimal(19,4)) AS RealEstateMarketValue,
    cast(reb.RealEstateMarketabilityCode AS string) AS RealEstateMarketabilityCode,
    cast(reb.RealEstateEvaluationType AS string) AS RealEstateValuationType,
    cast(reb.RealEstateEvaluationDate AS date) AS ValuationDate,
    cast(reb.RealEstateHotelFlag AS boolean) AS RealEstateHotelFlag,
    cast(rei.OwnerPartyKey AS string) AS OwnerPartyKey,
    cast(rei.OwnershipTypeCode AS string) AS OwnershipType,
    cast(rei.OwnershipPercentage AS double) AS OwnershipPercentage,
    cast(rei.NotaryDate AS date) AS OwnershipDate,
    cast(rei.OwnershipStatusCode AS string) AS OwnershipStatus,
    cast(col.CollateralKey AS string) AS SecurityKey,
    cast(col.CollateralKey AS string) AS CollateralKey,
    cast(col.CategoryCode AS string) AS CollateralCategory,
    cast(col.SubCategory AS string) AS CollateralSubCategory,
    cast(col.MortgageTypeCode AS string) AS TypeOfNote,
    cast(col.CollateralAmount AS decimal(19,2)) AS CollateralAmount,
    cast(col.CollateralCurrency AS string) AS CollateralCurrency,
    cast(col.InitialLegalOrder AS int) AS LienOrder,
    cast(col.CollateralStatusCode AS string) AS CollateralStatus,
    cast(col.StatusDate AS date) AS CollateralRegistrationDate,
    cast(col.RealEstateCollateralStatus AS string) AS RealEstateCollateralStatus,
    cast(bavg.AvgMarketabilitycREV as int) AS Marketability,
    cast(bavg.MedianeligibilitycREV as double) AS Eligibility,
    cast(bavg.Category as string) AS MarketabilityEligibilitySegment,
    cast(biCREV_REV.CREVBase as double) as CollateralValueCREV,
    cast(biCREV_REV.AllocREVBase as double) as AllocatedRev,
    cast(p.IsWholeshaleFlag AS boolean) AS WholesaleIndicator,
    cast(pprof.PartyType AS string) AS LegalEntityIndicator,
    cast(p.DeceasedFlag AS boolean) AS DeceasedFlag,
    cast(p.Watchlist AS string) AS CustWatchlist,
    cast(ex.PartyRelationshipType AS string) AS RelationType,
    cast(coalesce(ex.PartyTaxRegistrationNumber, ex.cdi) as string) as Borrower,
    cast(BorGBV.GBV as double) as BorrowerTotalGBV,
    cast(case when rev.AllocatedCollateralValueREV != 0 then try_divide(AgrGBV.GBV, rev.AllocatedCollateralValueREV)
                 else null 
    end as double) AS LTVCalc,
    cast(case when trim(bavg.Category) = 'marketable & eligible' then 'Eligible Pre-noted Asset'
              else 'Non Eligible Pre-noted Asset'
    end as string) AS EligiblePrenotedAsset,
    cast(BorPayerGBV.GBV as double) as BorrowerPayerExposureTotalGBV,
    try_cast(BorColCrev.TotalCREV AS decimal(19,12)) AS BorrowerCollateralValueCrev, 
    cast(AgrGBV.GBV as double) as AgreementTotalGBV,
    cast(c.PaymentsLast12months as decimal(15,2)) as PaymentsLast12months,
    cast(ps.InstallmentLast12Months as decimal(38,7)) as InstallmentLast12Months,
    cast(c.PaymentsTrnxFlagging as int) as PaymentsTrnxFlagging,
    cast(c.PaymentsAfterModification as decimal(15,2)) as PaymentsAfterModification,
    cast(ps.InstallmentAfterModification as decimal(38,7)) as InstallmentAfterModification,
    cast(cd.FullAddress as string) as PostalCodeCommunicationAddress,
    cast(cd.PostalCode as string),
    cast(i.LastRPCDate as timestamp), 
    cast(i.RPCcreatedby as string),
    cast(mod.ModificationsAfterCepalMigration as int),
    cast(exSoM.Bucket as int) AS SoMOperationalBucket,
    cast(assignment.ChannelCode as string) as AssignmentChannel,
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
    cast(ap.ApplicationKey AS string) AS ApplicationKey,
    cast(ap.CreatedDate as timestamp) AS ApplicationDate,
    cast(ap.QualcoAppDisbursedDate as date) as ImplementationDate,
    cast(dltv.BaseLTV AS double),
    cast(case 
            when dex.FutureStepUpIndicator = 'Future' 
            then 1
            else 0
    end as boolean) as FuturePaymentShockFlag,
    cast(case 
            when dex.PastStepUpIndicator = 'Past' 
            then 1
            else 0
    end as boolean) as PastPaymentShockFlag
    ,cast(case when inst_calc.InstallmentBucket > ex.Bucket then ex.Bucket else inst_calc.InstallmentBucket end as int) as InstallmentBucket
from exposure ex
inner join portfolio port
on  ex.PortfolioKey=port.unit_code_profits
    and (ParentPortfolio in ('COSMOS','GALAXY') OR port.PortfolioName in ('DOMUS PORTFOLIO - RETAIL','MONZA PORTFOLIO - RETAIL','RHODIUM PORTFOLIO - RETAIL','GAIA I PORTFOLIO','GAIA II PORTFOLIO RETAIL','Athena Portfolio'))
inner join partyprof_join pprof
on  ex.PartyKey = pprof.PartyKey
    and ex.SourceSystem = pprof.SourceSystem
left join exqca_join exqca
on  ex.CDI = exqca.CDI 
    and substring(ex.ExposureKey, 4, 50) = exqca.ExposureKey
left join lpsagreementdebtor_join LPSAgrDebtor
on  LPSAgrDebtor.AgreementKey = ex.AgreementKey
    and LPSAgrDebtor.CDI = ex.CDI
left join party_join p
on  ex.CDI = p.CDI 
inner join product_join prd
on  prd.ExposureKey = substring(ex.ExposureKey, 4, 50)
left join `{env}-cdh`.reference.portfoliotoolproductdescriptions pd
on prd.ProductDescription = pd.SubProductDescription
left join application_join ap
on  ap.PartyKey = ex.CDI
left join dexposure_join dex
on  dex.ExposureKey = ex.ExposureKey 
    and dex.SourceSystem = ex.SourceSystem
left join BorGBV_join BorGBV
on  coalesce(ex.PartyTaxRegistrationNumber,ex.cdi) = BorGBV.Borrower
    and ex.SourceSystem = BorGBV.SourceSystem
    and BorGBV.PortfolioName = ex.PortfolioName
left join AgrGBV_join AgrGBV
on  ex.AgreementKey = AgrGBV.AgreementKey
    and ex.SourceSystem = AgrGBV.SourceSystem
left join casecode_join cc
on  cc.CDI = ex.CDI
    and case    
            when cc.SourceSystem = 'qca' then cc.ExposureKey = substring(ex.ExposureKey, 4, 50)
            else cc.ExposureKey = ex.ExposureKey
        end
left join collateral_join col
on  ex.AgreementKey = col.AgreementKey 
    and col.RealEstateCollateralStatus='Ενεργή'
left join realestatebuilding_join reb 
on  reb.RealEstateKey = col.RealEstateKey
left join realestateinfo_join rei  
on  rei.RealEstateKey = col.RealEstateKey
    and rei.OwnerPartyKey = col.ownerpartykey
left join  restructuring_join restr
on  restr.NewExposureKey = ex.ExposureKey 
    and restr.SourceSystem = ex.SourceSystem
left join collection_join c
on regexp_replace(ex.ExposureKey, '^[A-Za-z]+', '') = c.ExposureKey
left join paymentschedule_join ps
on  ps.ExposureKey = ex.ExposureKey 
    and ps.SourceSystem = ex.SourceSystem and ex.SourceSystem = 'profits' 
left join settlementexposure_join settl
on  substring(ex.ExposureKey, 4, 50) = settl.ExposureKey
left join settlementexposure2_join settl2
on  substring(ex.ExposureKey, 4, 50) = settl2.ExposureKey
left join eap_join EAP
on  ex.ExposureKey = substring(EAP.ExposureKey, 4, 50) 
left join danalyticsmetrics_join biCREV_REV -- biteam_biteamlienanalysispropertycontract_join
on  ex.AgreementKey = biCREV_REV.AgreementKey 
    and biCREV_REV.RealEstatekey = reb.RealEstateCepalKey
left join AgrbiREV_join AgrbiREV
on  ex.AgreementKey = AgrbiREV.AgreementKey 
    and AgrbiREV.RealEstateKey = reb.RealEstateCepalKey
left join contactdetails_join cd
on  ex.PartyKey = cd.PartyKey
left join interaction_join i 
on  ex.CDI = i.CDI
left join port_join mod
on  ex.AgreementKey = mod.AgreementKey
left join underlaw_join ul
on  ul.CDI = ex.CDI
left join danalyticsmetrics_join bi -- biteam_pf_final_output_join
on  bi.ExposureKey = ex.ExposureKey
left join fl_join fl
on ex.ExposureKey = fl.AccountNumberWithPrefix
left join BorColCrev_join BorColCrev
on  coalesce(ex.PartyTaxRegistrationNumber,ex.cdi) = BorColCrev.Borrower
    and ex.SourceSystem = BorColCrev.SourceSystem
    and ex.PortfolioName = BorColCrev.PortfolioName
left join BorPayerGBV_join BorPayerGBV
on  coalesce(ex.PartyTaxRegistrationNumber,ex.cdi)=BorPayerGBV.Borrower
    and ex.SourceSystem = BorPayerGBV.SourceSystem
left join biteam_avg_marketability_eligibility_contract_category_join bavg
on  trim(bavg.CDI) = ex.CDI
    and trim(bavg.AgreementNumber) = ex.AgreementKey
left join assignment_join assignment 
on  substring(ex.ExposureKey, 4, 50) = assignment.ExposureKey
left join firmstructure f 
on assignment.AgentPartyKey = f.EmployeeUsername and assignment.SourceSystem = f.SourceSystem and assignment.BusinessDate = f.BusinessDate
left join firmstructure f1
on  assignment.ExternalCompanyPartyKey = f1.EmployeePartyKey 
    and assignment.SourceSystem = f1.SourceSystem 
    and assignment.BusinessDate = f1.BusinessDate 
    and f1.EmployeeCategory = 'External Company'
left join dltv_join dltv
on  dltv.Agreementkey = ex.AgreementKey
left join AllocAgrRev_join rev
on rev.AgreementKey = ex.AgreementKey
left join (
    select d.ExposureKey, 
    case when i.InstallmentAmount > 0 then CAST((ROUND(((d.DueAmount - d.DueExpense)/i.InstallmentAmount),0))AS INT) END AS InstallmentBucket
    from intallmentbucketcalc_join i
    inner join dexposure d
    on  i.ExposureKey = d.ExposureKey
    where d.GBV > 0 and d.ExposureStatus not like 'Define Delay' 
    -- ExposureStatusCode	ExposureStatus
        --3	                 Define Delay
        --074-DENOUNCED	     Define Delay
) inst_calc
on inst_calc.ExposureKey = ex.ExposureKey
left join exSoM_join exSoM
on exSoM.ExposureKey = ex.ExposureKey 
left join payers_flag_algorithm_join payers_flag
on ex.ExposureKey = payers_flag.Account_Number_with_Prefix
where ex.SourceSystem = 'profits';
""")

# COMMAND ----------

final_df.createOrReplaceTempView("final_df")

# COMMAND ----------

# %sql
# select count(*) from final_df

# COMMAND ----------

# final_df.printSchema()

# COMMAND ----------

# spark.read.format("delta").load(derivative_table_path).printSchema()

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
