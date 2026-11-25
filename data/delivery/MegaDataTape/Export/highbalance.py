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
PortfolioName = ["Galaxy II Funding DAC", "Cosmos Retail", "Orion X Securitisation DAC", "Galaxy Common Customers"]
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

Restructuring_History = spark.sql(f"""
SELECT DISTINCT 
    ActiveExposureRestructureStartDate AS PaymentArrangementDate, 
    ExposureKey AS AccountNo, 
    ExposureRestructureStatus AS PaymentArrangementStatus, 
    ExposureRestructureType AS PaymentArrangementType, 
    HighBalanceFlag AS IsHighBalance, 
    PaymentAgreementID AS PaymentAgreementID, 
    PaymentArrangementOrigination AS PaymentArrangementOrigination, 
    ProductDescription AS PaymentArrangementTypeDetailed 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

Property_ownership = spark.sql(f"""
SELECT DISTINCT 
    OwnerID AS OwnerID, 
    DCollateralKey AS CollateralID, 
    FullOwnershipFlag AS FullOwnershipFlag, 
    HighBalanceFlag AS IsHighBalance, 
    FieldValueENG AS OwnershipType, 
    OwnershipKey AS OwnershipID, 
    OwnershipPercentage AS Percentage, 
    RealEstateCepalKey AS PropertyID 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateralrealestate 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true 
    AND ownerid is not null
""")

Collateral_Tape_NRE = spark.sql(f"""
SELECT DISTINCT 
    CollateralAmount AS CollateralAmount, 
    CollateralType AS CollateralType, 
    CollateralTypeDescription AS CollateralTypeDescription, 
    DCollateralKey AS CollateralID, 
    HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateral 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true 
    AND CollateralType = 'Non-RE'
""")

Collateral_Tape_RE = spark.sql(f"""
SELECT DISTINCT 
    AreaENG AS Region, 
    Country AS Country, 
    DCollateralKey AS CollateralID, 
    HighBalanceFlag AS IsHighBalance, 
    LatestValuationAmount AS LatestValuationAmount, 
    LatestValuationDate AS LatestValuationDate, 
    LatestValuationType AS LatestValuationType, 
    OriginalValuationAmount AS OriginalValuationAmount, 
    OriginalValuationDate AS OriginalValuationDate, 
    OriginalValuationType AS OriginalValuationType, 
    OtherArea AS OtherArea, 
    RealEstateActive AS PropertyStatus, 
    RealEstateAddress1 AS Address, 
    RealEstateCepalKey AS PropertyID, 
    RealEstateCity AS City, 
    RealEstateConstructionYear AS ConstructionYear, 
    RealEstateLandArea AS LandArea, 
    RealEstateNetArea AS PropertySize, 
    RealEstatePostalcode AS Postcode, 
    RealEstateSubCategory AS PropertyType 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateral 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true 
    AND CollateralType = 'RE'
""")

Active_Payment_Agreement = spark.sql(f"""
SELECT DISTINCT 
    ActiveExposureRestructureStartDate AS ActivePaymentArrangementStartDate, 
    ExposureKey AS AccountNo, 
    GBV AS GBV, 
    HighBalanceFlag AS IsHighBalance, 
    MonthsOfCleanPayHistory AS MonthsOfCleanPayHistory, 
    OutstandingTotalBalance AS TotalExposure, 
    PaymentAgreementID AS PaymentAgreementID, 
    PaymentArrangementOrigination AS PaymentArrangementOrigination, 
    TotalPaymentAgreementDue AS TotalPaymentAgreementDue, 
    TotalPaymentAgreementPaid AS TotalPaymentAgreementPaid 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

Loan = spark.sql(f"""
SELECT DISTINCT 
    ABNPEAmount AS ABNPEAmount, 
    ABNPEDate AS ABNPEDate, 
    ActiveModificationProgramStatus AS ActiveModificationProgramStatus, 
    AgreementKey AS ContractID, 
    AgreementCREV AS AgreementCREV, 
    AgreementREV AS AgreementREV, 
    AmountDueAtDefault AS AmountDueAtDefault, 
    AssetSegment AS IFRSAssetClass, 
    BankOfOrigin AS BankOfOrigin, 
    BaseLTV AS LTV, 
    Baserate AS RefRate, 
    CDI AS BorrowerID, 
    CodeofConductStage AS CodeofConductStage, 
    Coupon AS Coupon, 
    CourtRegion AS CourtRegion, 
    Currency AS Currency, 
    DaysPastDueFinal AS DaysPastDue, 
    DenouncedDate AS DenouncementDate, 
    DenouncementIndicator AS DenouncementIndicator, 
    DueAmount AS BalanceInArrears, 
    EBAStatus AS EBAStatus, 
    ESMABankOfOrigin AS ESMABankOfOrigin, 
    ExposureKey AS AccountNo, 
    ExtrajudicialLetterEverFlag AS ExtrajudicialLetterEverFlag, 
    ExtrajudicialLetterLastDate AS ExtrajudicialLetterLastDate, 
    GBV AS GBV, 
    GBVEur AS GBVEur,
    InterestAtDefault AS InterestAtDefault, 
    InterestRateType AS InterestRateType, 
    IsActiveFlag AS OpenFlag, 
    HighBalanceFlag AS IsHighBalance, 
    IsSecuredFlag AS SecuredFlag, 
    IssueDate AS ContractIssuanceDate, 
    L3869CourtDate AS L3869CourtDate, 
    L3869Flag AS L3869Flag, 
    L3869ProvisionalCourtDecisionDate AS L3869ProvisionalCourtDecisionDate, 
    L3869Status AS L3869Status, 
    L3869SubmissionDate AS L3869SubmissionDate, 
    LastModificationActionType AS LastModificationActionType, 
    LastModificationDate AS LastModificationDate, 
    LegalProcessStatusLastChangeDate AS LegalProcessStatusDate, 
    OperationalLegalStatusLevel3 AS LegalProcessStatus, 
    MaturityDate AS MaturityDate, 
    N128Rate AS Levy, 
    NoOfRestructures AS NoOfRestructures, 
    NoOfRestructuresESMA AS NoOfRestructuresESMA, 
    NPEDate AS NPEDate, 
    NumberofContactActivities AS NumberofContactActivities, 
    NumberofPromises AS NumberofPromises, 
    OpenDate AS OriginationDate, 
    OutstandingTotalBalance AS TotalExposure, 
    OutstandingTotalBalanceEur AS TotalExposureEur,
    PaymentOrderDate AS PaymentOrderDate, 
    PaymentOrderFlag AS PaymentOrderFlag, 
    PenaltyRate AS PenaltyInterestRate, 
    PlatformName AS StateSubsidy, 
    PlatformNameFlag AS StateSubsidyFlag, 
    PositiveRightPersonContacts AS PositiveRightPersonContacts, 
    PrincipalAtDefault AS PrincipalAtDefault, 
    PrincipalAtDefaultEur AS PrincipalAtDefaultEur,
    RepaymentFrequency AS RepaymentFrequency, 
    RightPersonContacts AS RightPersonContacts, 
    TotalAccruals AS Accruals, 
    TotalAccrualsEur AS AccrualsEur,
    TotalExpense AS Expenses, 
    TotalInterest AS Interest, 
    TotalInterestEur AS InterestEur,
    TotalPrincipal AS Principal,
    TotalPrincipalEur AS TotalPrincipalEur

FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

Collection = spark.sql(f"""
SELECT DISTINCT 
    ActualDate AS AllocationDate, 
    Amount AS AllocationAmount, 
    CDI AS BorrowerID, 
    CollectionKey AS CollectionID, 
    ExposureKey AS AccountNo, 
    HighBalanceFlag AS IsHighBalance, 
    PaymentAgreementID AS PaymentAgreementID, 
    PaymentTypeGroup AS CollectionType 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collections 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

Contact_History = spark.sql(f"""
SELECT DISTINCT 
    CDI AS BorrowerID, 
    ContactType AS ContactType, 
    ExposureKey AS AccountNo, 
    InteractionKey AS ContactHistoryID, 
    InteractionType AS ContactDescription, 
    HighBalanceFlag AS IsHighBalance, 
    InteractionDate AS ContactDate 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.contacthistory 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

External_Lien_DB = spark.sql(f"""
SELECT DISTINCT 
    DCollateralKey AS CollateralID, 
    HighBalanceFlag AS IsHighBalance, 
    LastNameGR AS EntityOwnerName, 
    OwnershipKey AS OwnershipID, 
    PositionDate AS PositionDate, 
    RealEstateCepalKey AS PropertyID, 
    RELienInitialOrder AS Position, 
    RELienKey AS ClaimID, 
    RELienMortgageAmount AS Amount, 
    TaxRegistrationNumber AS EntityOwnerTaxID 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.externalliendb 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

Installments = spark.sql(f"""
SELECT DISTINCT 
    DPaymentScheduleKey AS InstallmentID, 
    ExposureKey AS AccountNo, 
    ExposureRestructureOrigination AS PaymentArrangementOrigination, 
    InstallmentAmount AS InstallmentAmountDue, 
    InstallmentAmountPaid AS InstallmentAmountPaid, 
    InstallmentDate AS InstallmentDateDue, 
    InstallmentPaid AS InstallmentPaid, 
    InstallmentPaidDate AS InstallmentPaidDate, 
    HighBalanceFlag AS IsHighBalance, 
    PaymentAgreementID AS PaymentAgreementID 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.installments 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

Restructure_Mapping = spark.sql(f"""
SELECT DISTINCT 
    CDI AS BorrowerID, 
    HighBalanceFlag AS IsHighBalance, 
    NewAgreementKey AS ContractID, 
    NewExposureKey AS NewAccountNo, 
    OldExposureKey AS RestructuredAccountNo, 
    RestructureType AS RestructureType 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.restructuremapping 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

Borrower = spark.sql(f"""
SELECT DISTINCT 
    BirthDate AS BirthDate, 
    CalcLawStatus AS L3869Status, 
    CDI AS BorrowerID, 
    DeceasedFlag AS DeceasedFlag, 
    Gender AS Gender, 
    L3869Flag AS L3869Flag, 
    MaritalStatusENG AS MaritalStatus, 
    NationalityENG AS Nationality, 
    NumberofContactActivities AS NumberofContactActivities, 
    NumberofPromises AS NumberofPromises, 
    PartyPostalCode AS BorrowerPostcode, 
    PartyRegion AS BorrowerRegion, 
    PartyType AS BorrowerType, 
    PositiveRightPersonContacts AS PositiveRightPersonContacts, 
    RightPersonContacts AS RightPersonContacts, 
    HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.borrower 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

Borrower_Mapping = spark.sql(f"""
SELECT DISTINCT 
    AgreementKey AS ContractID, 
    CDI AS BorrowerID, 
    ExposureKey AS AccountNo, 
    HighBalanceFlag AS IsHighBalance, 
    PartyRelationshipType AS RelationType, 
    isActiveFlag AS Activeflag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.borrowermapping 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

RE_Asset_Appraisals = spark.sql(f"""
SELECT DISTINCT 
    AppraisalAmount AS AppraisalAmount, 
    AppraisalCompanyName AS AppraisalCompanyName, 
    AppraisalDate AS AppraisalDate, 
    AppraisalID AS AppraisalID, 
    AppraisalType AS AppraisalType, 
    BuildingConditions AS BuildingConditions, 
    ConstructionStatus AS ConstructionStatus, 
    ForcedSaleValue AS ForcedSaleValue, 
    HighBalanceFlag AS IsHighBalance, 
    OccupancyStatus AS OccupancyStatus, 
    OpenMarketValue AS OpenMarketValue, 
    RealEstateCepalKey AS PropertyID, 
    TaxRegistrationNumber AS AppraisalCompanyTaxID 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.reassetappraisals 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

Loan_Sales = spark.sql(f"""
SELECT DISTINCT 
    EquivalentAmount AS TotalEquivalentAmount, 
    ExposureKey AS AccountNo, 
    ExposureSalesKey AS LoanSaleId, 
    ExposureSalesLastUpdatedDate AS FinalSaleDate, 
    ExposureSalesStatus AS LoanSalesDescription, 
    HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loansales 
WHERE ReportingDate = '{last_eom_business_date}' 
    AND PortfolioName IN ({portfolio_str}) 
    AND HighBalanceFlag = true
""")

# COMMAND ----------

export_data(cast_decimal_to_float(Loan), "Loan", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Borrower), "Borrower", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Borrower_Mapping), "Borrower_Mapping", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collateral_Tape_RE), "Collateral_Tape_RE", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collateral_Tape_NRE), "Collateral_Tape_NRE", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collection), "Collection", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Property_ownership), "Property_ownership", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Active_Payment_Agreement), "Active_Payment_Agreement", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Contact_History), "Contact_History", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(External_Lien_DB), "External_Lien_DB", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Loan_Sales), "Loan_Sales", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Installments), "Installments", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(RE_Asset_Appraisals), "RE_Asset_Appraisals", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Restructure_Mapping), "Restructure_Mapping", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Restructuring_History), "Restructuring_History", last_eom_business_date, output_dir, output_file_prefix)
