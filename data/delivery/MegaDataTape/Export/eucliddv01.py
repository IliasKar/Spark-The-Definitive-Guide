# Databricks notebook source
# DBTITLE 1,Initialization
# MAGIC %pip install openpyxl
# MAGIC %restart_python

# COMMAND ----------

# Import the module for export to excel
import sys
sys.path.append("../../01-utils")
 
from export_utils import export_data

# COMMAND ----------

# DBTITLE 1,Read parameters
PortfolioName = ["EUCLID"]
portfolio_str = ",".join([f"'{p}'" for p in PortfolioName])

env_prefix=dbutils.widgets.get("env_prefix")
last_eom_business_date = dbutils.widgets.get("last_eom_business_date")
output_dir = dbutils.widgets.get("output_dir")
output_file_prefix = dbutils.widgets.get("output_file_prefix")

print(f"last_eom_business_date: {last_eom_business_date}")
print(f"output_dir: {output_dir}")
print(f"output_file_prefix: {output_file_prefix}")
print(f"env_prefix: {env_prefix}")

# COMMAND ----------

from pyspark.sql.functions import col

def cast_decimal_to_float(df):
    return df.select([
        col(c).cast("float").alias(c) if t.startswith("decimal") else col(c)
        for c, t in df.dtypes
    ])

# COMMAND ----------

Active_Payment_Agreement = spark.sql(f""" 
SELECT DISTINCT 
    PaymentAgreementID AS PaymentAgreementID, 
    ExposureKey AS AccountNo, 
    ActiveExposureRestructureStartDate AS ActivePaymentArrangementStartDate, 
    MonthsOfCleanPayHistory AS MonthsOfCleanPayHistory, 
    GBV AS GBV, 
    OutstandingTotalBalance AS TotalExposure, 
    TotalPaymentAgreementPaid AS TotalPaymentAgreementPaid, 
    TotalPaymentAgreementDue AS TotalPaymentAgreementDue, 
    PaymentArrangementOrigination AS PaymentArrangementOrigination, 
    SubservicerFlag AS SubservicerFlag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan 
WHERE ReportingDate = '{last_eom_business_date}'  
AND PortfolioName IN ({portfolio_str})
AND SubServicerFlag in ('DV01','Common')
""")

Borrower = spark.sql(f""" 
SELECT DISTINCT 
    CDI AS BorrowerID, 
    PartyType AS BorrowerType, 
    L3869Flag AS L3869Flag, 
    CalcLawStatus AS L3869Status, 
    Gender AS Gender, 
    DeceasedFlag AS DeceasedFlag, 
    BirthDate AS BirthDate, 
    NationalityENG AS Nationality, 
    MaritalStatusENG AS MaritalStatus, 
    Nace3lvlDescr AS Industry, 
    PartyRegion AS BorrowerRegion, 
    PartyPostalCode AS BorrowerPostcode, 
    RightPersonContacts AS RightPersonContacts, 
    PositiveRightPersonContacts AS PositiveRightPersonContacts, 
    NumberofPromises AS NumberofPromises, 
    NumberofContactActivities AS NumberofContactActivities, 
    SubservicerFlag AS SubservicerFlag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.borrower 
WHERE ReportingDate = '{last_eom_business_date}'  
AND PortfolioName IN ({portfolio_str})
AND SubServicerFlag in ('DV01','Common')
""")

Borrower_Mapping = spark.sql(f""" 
SELECT DISTINCT 
    ExposureKey AS AccountNo, 
    CDI AS BorrowerID, 
    AgreementKey AS ContractID, 
    PartyRelationshipType AS RelationType, 
    isActiveFlag AS Activeflag, 
    SubservicerFlag AS SubservicerFlag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.borrowermapping 
WHERE ReportingDate = '{last_eom_business_date}'  
AND PortfolioName IN ({portfolio_str})
AND SubServicerFlag in ('DV01','Common')
""")

Collateral_Mapping = spark.sql(f""" 
SELECT DISTINCT 
    RealEstateCepalKey AS PropertyID, 
    OwnershipKey AS OwnershipID, 
    ExposureKey AS AccountNo, 
    AgreementKey AS ContractID, 
    DCollateralKey AS CollateralID, 
    CollateralAmount AS CollateralAmount, 
    CollateralType AS CollateralType, 
    CollateralTypeDescription AS CollateralTypeDescription, 
    InitialLegalOrder AS CollateralLien, 
    REMortgageRegistrationDate AS REMortgageRegistrationDate, 
    CollateralStatus AS CollateralStatus, 
    RealEstateActive AS PropertyStatus, 
    SubservicerFlag AS SubservicerFlag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateralrealestate 
WHERE ReportingDate = '{last_eom_business_date}'  
AND PortfolioName IN ({portfolio_str})
AND SubServicerFlag in ('DV01','Common')
""")

Collateral_Tape_NRE = spark.sql(f""" 
SELECT DISTINCT 
    DCollateralKey AS CollateralID, 
    CollateralType AS CollateralType, 
    CollateralTypeDescription AS CollateralTypeDescription, 
    CollateralAmount AS CollateralAmount, 
    SubservicerFlag AS SubservicerFlag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateral 
WHERE ReportingDate = '{last_eom_business_date}'  
AND PortfolioName IN ({portfolio_str}) 
AND CollateralType = 'Non-RE'
AND SubServicerFlag in ('DV01','Common')
""")

Collateral_Tape_RE = spark.sql(f""" 
SELECT DISTINCT 
    DCollateralKey AS CollateralID, 
    RealEstateCepalKey AS PropertyID, 
    RealEstateSubCategory AS PropertyType, 
    RealEstateAddress1 AS Address, 
    RealEstateCity AS City, 
    RealEstatePostalcode AS Postcode, 
    AreaENG AS Region, 
    Country AS Country, 
    RealEstateConstructionYear AS ConstructionYear, 
    OriginalValuationAmount AS OriginalValuationAmount, 
    OriginalValuationDate AS OriginalValuationDate, 
    OriginalValuationType AS OriginalValuationType, 
    LatestValuationAmount AS LatestValuationAmount, 
    LatestValuationDate AS LatestValuationDate, 
    LatestValuationType AS LatestValuationType, 
    RealEstateNetArea AS PropertySize, 
    RealEstateLandArea AS LandArea, 
    OtherArea AS OtherArea, 
    RealEstateActive AS PropertyStatus, 
    SubservicerFlag AS SubservicerFlag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateral 
WHERE ReportingDate = '{last_eom_business_date}'  
AND PortfolioName IN ({portfolio_str}) 
AND CollateralType = 'RE'
AND SubServicerFlag in ('DV01','Common')
""")

Collections = spark.sql(f""" 
SELECT DISTINCT 
    CollectionKey AS CollectionID, 
    ActualDate AS AllocationDate, 
    Amount AS AllocationAmount, 
    PaymentTypeGroup AS CollectionType, 
    ExposureKey AS AccountNo, 
    CDI AS BorrowerID, 
    PaymentAgreementID AS PaymentAgreementID, 
    SubservicerFlag AS SubservicerFlag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collections 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
AND SubServicerFlag in ('DV01','Common')
""")

Contact_History = spark.sql(f""" 
SELECT DISTINCT 
    InteractionKey AS ContactHistoryID, 
    ExposureKey AS AccountNo, 
    CDI AS BorrowerID, 
    InteractionDate AS ContactDate, 
    ContactType AS ContactType, 
    InteractionType AS ContactDescription, 
    SubservicerFlag AS SubservicerFlag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.contacthistory 
WHERE ReportingDate = '{last_eom_business_date}'  
AND PortfolioName IN ({portfolio_str})
AND SubServicerFlag in ('DV01','Common')
""")

Loan = spark.sql(f""" 
SELECT DISTINCT 
    PortfolioName AS SPV, 
    CDI AS BorrowerID, 
    AgreementKey AS ContractID, 
    ExposureKey AS AccountNo, 
    Currency AS Currency, 
    AssetSegment AS IFRSAssetClass, 
    IsSecuredFlag AS SecuredFlag, 
    BankOfOrigin AS BankOfOrigin, 
    ESMABankOfOrigin AS ESMABankOfOrigin, 
    OpenDate AS OriginationDate, 
    IssueDate AS ContractIssuanceDate, 
    OutstandingTotalBalance AS TotalExposure, 
    OutstandingTotalBalanceEur AS TotalExposureEur,
    TotalPrincipal AS Principal, 
    TotalPrincipalEur AS PrincipalEur,
    TotalInterest AS Interest, 
    TotalInterestEur AS InterestEur,
    TotalAccruals AS Accruals, 
    TotalAccrualsEur AS AccrualsEur,
    TotalExpense AS Expenses, 
    TotalCommissions AS Commissions, 
    GBV AS GBV,
    GBVEur AS GBVEur, 
    MaturityDate AS MaturityDate, 
    RepaymentFrequency AS RepaymentFrequency, 
    InterestRateType AS InterestRateType, 
    Baserate AS RefRate, 
    SpreadRate AS Spread, 
    Coupon AS Coupon, 
    N128Rate AS Levy, 
    PenaltyRate AS PenaltyInterestRate, 
    DaysPastDueFinal AS DaysPastDue, 
    EBAStatus AS EBAStatus, 
    PlatformNameFlag AS StateSubsidyFlag, 
    NoOfRestructures AS NoOfRestructures, 
    NoOfRestructuresESMA AS NoOfRestructuresESMA, 
    ActiveModificationProgramStatus AS ActiveModificationProgramStatus, 
    LastModificationActionType AS LastModificationActionType, 
    LastModificationDate AS LastModificationDate, 
    OperationalLegalStatusLevel3 AS LegalProcessStatus, 
    LegalProcessStatusLastChangeDate AS LegalProcessStatusDate, 
    DenouncedDate AS DenouncementDate, 
    DenouncementIndicator AS DenouncementIndicator, 
    ExtrajudicialLetterEverFlag AS ExtrajudicialLetterEverFlag, 
    ExtrajudicialLetterLastDate AS ExtrajudicialLetterLastDate, 
    PaymentOrderFlag AS PaymentOrderFlag, 
    PaymentOrderDate AS PaymentOrderDate, 
    CodeofConductStage AS CodeofConductStage, 
    L3869Flag AS L3869Flag, 
    L3869Status AS L3869Status, 
    L3869SubmissionDate AS L3869SubmissionDate, 
    L3869ProvisionalCourtDecisionDate AS L3869ProvisionalCourtDecisionDate, 
    L3869CourtDate AS L3869CourtDate, 
    PlatformName AS StateSubsidy, 
    NPEDate AS NPEDate, 
    PrincipalAtDefault AS PrincipalAtDefault, 
    PrincipalAtDefaultEur AS PrincipalAtDefaultEur,
    InterestAtDefault AS InterestAtDefault, 
    AmountDueAtDefault AS AmountDueAtDefault, 
    ABNPEDate AS ABNPEDate,
    ABNPEAmount AS ABNPEAmount,
    RightPersonContacts AS RightPersonContacts, 
    PositiveRightPersonContacts AS PositiveRightPersonContacts, 
    NumberofPromises AS NumberofPromises, 
    NumberofContactActivities AS NumberofContactActivities, 
    BaseLTV AS LTV, 
    AgreementCREV AS AgreementCREV, 
    AgreementCREVTLC AS AgreementCREVTLC, 
    AgreementREV AS AgreementREV, 
    IsActiveFlag AS OpenFlag, 
    CourtRegion AS CourtRegion, 
    DueAmount AS BalanceInArrears, 
    SubservicerFlag AS SubservicerFlag
     
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan 
WHERE ReportingDate = '{last_eom_business_date}'  
AND PortfolioName IN ({portfolio_str})
AND SubServicerFlag in ('DV01','Common')
""")

PA_Instalments = spark.sql(f""" 
SELECT DISTINCT 
    DPaymentScheduleKey AS InstallmentID, 
    PaymentAgreementID AS PaymentAgreementID, 
    InstallmentDate AS InstallmentDateDue, 
    InstallmentAmount AS InstallmentAmountDue, 
    InstallmentPaidDate AS InstallmentPaidDate, 
    InstallmentAmountPaid AS InstallmentAmountPaid, 
    InstallmentPaid AS InstallmentPaid, 
    ExposureRestructureOrigination AS PaymentArrangementOrigination, 
    ExposureKey AS AccountNo, 
    SubservicerFlag AS SubservicerFlag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.installments 
WHERE ReportingDate = '{last_eom_business_date}'  
AND PortfolioName IN ({portfolio_str})
AND SubServicerFlag in ('DV01','Common')
""")

Restructure_Mapping = spark.sql(f""" 
SELECT DISTINCT 
    CDI AS BorrowerID, 
    NewAgreementKey AS ContractID, 
    OldExposureKey AS RestructuredAccountNo, 
    NewExposureKey AS NewAccountNo, 
    RestructureType AS RestructureType, 
    SubservicerFlag AS SubservicerFlag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.restructuremapping 
WHERE ReportingDate = '{last_eom_business_date}'  
AND PortfolioName IN ({portfolio_str})
AND SubServicerFlag in ('DV01','Common')
""")

Restructuring_History = spark.sql(f""" 
SELECT DISTINCT 
    PaymentAgreementID AS PaymentAgreementID, 
    ExposureKey AS AccountNo, 
    ActiveExposureRestructureStartDate AS PaymentArrangementDate, 
    ExposureRestructureStatus AS PaymentArrangementStatus, 
    ExposureRestructureType AS PaymentArrangementType, 
    ProductDescription AS PaymentArrangementTypeDetailed, 
    PaymentArrangementOrigination AS PaymentArrangementOrigination, 
    SubservicerFlag AS SubservicerFlag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan 
WHERE ReportingDate = '{last_eom_business_date}'  
AND PortfolioName IN ({portfolio_str})
AND SubServicerFlag in ('DV01','Common')
""")

# COMMAND ----------

export_data(cast_decimal_to_float(Active_Payment_Agreement), "Active_Payment_Agreement", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Borrower), "Borrower", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Borrower_Mapping), "Borrower_Mapping", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collateral_Mapping), "Collateral_Mapping", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collateral_Tape_NRE), "Collateral_Tape_NRE", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collateral_Tape_RE), "Collateral_Tape_RE", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collections), "Collections", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Contact_History), "Contact_History", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(PA_Instalments), "PA_Instalments", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Loan), "Loan", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Restructure_Mapping), "Restructure_Mapping", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Restructuring_History), "Restructuring_History", last_eom_business_date, output_dir, output_file_prefix)
