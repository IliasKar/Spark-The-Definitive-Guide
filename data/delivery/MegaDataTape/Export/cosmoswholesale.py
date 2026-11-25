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
PortfolioName = ["Cosmos Wholesale"]
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
    cast(GBV AS DECImal(18,2)) AS GBV, 
    cast(OutstandingTotalBalance AS decimal(18,2)) AS TotalExposure,
    cast(TotalPaymentAgreementPaid AS decimal(18,2)) AS TotalPaymentAgreementPaid,
    cast(TotalPaymentAgreementDue AS decimal(18,2)) AS TotalPaymentAgreementDue,
    PaymentArrangementOrigination AS PaymentArrangementOrigination 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
AND PaymentArrangementOrigination IS NOT NULL
""")

Loan = spark.sql(f"""
SELECT DISTINCT 
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
    CAST(OutstandingTotalBalance AS DECIMAL(18,2)) AS TotalExposure, 
    CAST(OutstandingTotalBalanceEur AS DECIMAL(18,2)) AS TotalExposureEur,
    CAST(TotalPrincipal AS DECIMAL(18,2)) AS Principal,
    CAST(TotalPrincipalEur AS DECIMAL(18,2)) AS PrincipalEur, 
    CAST(TotalInterest AS DECIMAL(18,2)) AS Interest,
    CAST(TotalInterestEur AS DECIMAL(18,2)) AS InterestEur, 
    CAST(TotalAccruals AS DECIMAL(18,2)) AS Accruals, 
    CAST(TotalAccrualsEur AS DECIMAL(18,2)) AS AccrualsEur,
    CAST(ifnull(TotalExpense,0) + ifnull(TotalCommissions,0) AS DECIMAL(18,2)) AS Expenses, 
    CAST(GBV AS DECIMAL(18,2)) AS GBV, 
    CAST(GBVEur AS DECIMAL(18,2)) AS GBVEur,
    MaturityDate AS MaturityDate, 
    RepaymentFrequency AS RepaymentFrequency, 
    InterestRateType AS InterestRateType, 
    CAST(Baserate AS DECIMAL(10,6)) AS RefRate, 
    CAST(SpreadRate AS DECIMAL(10,6)) AS Spread, 
    CAST(Coupon AS DECIMAL(10,6)) AS Coupon, 
    CAST(N128Rate AS DECIMAL(10,6)) AS Levy, 
    CAST(PenaltyRate AS DECIMAL(10,6)) AS PenaltyInterestRate, 
    DaysPastDueFinal AS DaysPastDue, 
    EBAStatus AS EBAStatus, 
    PlatformNameFlag AS StateSubsidyFlag, 
    NoOfRestructures AS NoOfRestructures, 
    NoOfRestructuresESMA AS NoOfRestructuresESMA, 
    ActiveModificationProgramStatus AS ActiveModificationProgramStatus, 
    LastModificationActionType AS LastModificationActionType, 
    LastModificationDate AS LastModificationDate, 
    OperationalLegalStatusLevel3 AS LegalProcessStatus, 
    cast(LegalProcessStatusLastChangeDate as date) AS LegalProcessStatusDate, 
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
    CAST(PrincipalAtDefault AS DECIMAL(18,2)) AS PrincipalAtDefault,
    CAST(PrincipalAtDefaultEur AS DECIMAL(18,2)) AS PrincipalAtDefaultEur, 
    CAST(InterestAtDefault AS DECIMAL(18,2)) AS InterestAtDefault, 
    CAST(AmountDueAtDefault AS DECIMAL(18,2)) AS AmountDueAtDefault, 
    ABNPEDate AS ABNPEDate, 
    ABNPEAmount AS ABNPEAmount, 
    RightPersonContacts AS RightPersonContacts, 
    PositiveRightPersonContacts AS PositiveRightPersonContacts, 
    NumberofPromises AS NumberofPromises, 
    NumberofContactActivities AS NumberofContactActivities, 
    BaseLTV AS LTV, 
    AgreementCREV AS AgreementCREV, 
    AgreementREV AS AgreementREV, 
    IsActiveFlag AS OpenFlag, 
    CourtRegion AS CourtRegion, 
    DueAmount AS BalanceInArrears 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
""")

Collections = spark.sql(f"""
SELECT DISTINCT 
    CollectionKey AS CollectionID, 
    ActualDate AS AllocationDate, 
    cast(Amount AS decimal(18,2)) AS AllocationAmount, 
    PaymentTypeGroup AS CollectionType, 
    ExposureKey AS AccountNo, 
    CDI AS BorrowerID, 
    PaymentAgreementID AS PaymentAgreementID
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collections 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
""")

Contact_History = spark.sql(f"""
SELECT DISTINCT 
    InteractionKey AS ContactHistoryID, 
    ExposureKey AS AccountNo, 
    CDI AS BorrowerID, 
    InteractionDate AS ContactDate, 
    ContactType AS ContactType, 
    NULL AS ContactReason,
    InteractionType AS ContactDescription, 
    NULL AS IsInbound 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.contacthistory 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
""")

External_Lien_DB = spark.sql(f"""
SELECT DISTINCT 
    RELienKey AS ClaimID, 
    OwnershipKey AS OwnershipID, 
    DCollateralKey AS CollateralID, 
    RealEstateCepalKey AS PropertyID, 
    LegalID,
    TaxRegistrationNumber AS EntityOwnerTaxID, 
    LastNameGR AS EntityOwnerName, 
    RELienInitialOrder AS Position, 
    PositionDate AS PositionDate, 
    CAST(RELienMortgageAmount AS DECIMAL(18,2)) AS Amount 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.externalliendb 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
""")

PA_Installments = spark.sql(f"""
SELECT DISTINCT 
    DPaymentScheduleKey AS InstallmentID, 
    PaymentAgreementID AS PaymentAgreementID, 
    InstallmentDate AS InstallmentDateDue, 
    CAST(InstallmentAmount AS DECIMAL(18,2)) AS InstallmentAmountDue, 
    InstallmentPaidDate AS InstallmentPaidDate, 
    CAST(InstallmentAmountPaid AS DECIMAL(18,2)) AS InstallmentAmountPaid, 
    InstallmentPaid AS InstallmentPaid, 
    ExposureRestructureOrigination AS PaymentArrangementOrigination, 
    ExposureKey AS AccountNo 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.installments 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
AND PaymentAgreementId IS NOT NULL
""")

Restructure_Mapping = spark.sql(f"""
SELECT DISTINCT 
    CDI AS BorrowerID, 
    NewAgreementKey AS ContractID, 
    OldExposureKey AS RestructuredAccountNo, 
    NewExposureKey AS NewAccountNo, 
    RestructureType AS RestructureType 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.restructuremapping 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
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
    NumberofContactActivities AS NumberofContactActivities 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.borrower 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
""")

Borrower_Mapping = spark.sql(f"""
SELECT DISTINCT 
    ExposureKey AS AccountNo, 
    CDI AS BorrowerID, 
    AgreementKey AS ContractID, 
    PartyRelationshipType AS RelationType, 
    isActiveFlag AS Activeflag 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.borrowermapping 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
""")

Collateral_Mapping = spark.sql(f"""
SELECT DISTINCT 
    RealEstateCepalKey AS PropertyID, 
    OwnershipKey AS OwnershipID, 
    ExposureKey AS AccountNo,  
    AgreementKey AS ContractID, 
    DCollateralKey AS CollateralID, 
    CAST(CollateralAmount AS DECIMAL(18,2)) AS CollateralAmount, 
    CollateralType AS CollateralType, 
    CollateralTypeDescription AS CollateralTypeDescription,  
    InitialLegalOrder AS CollateralLien,  
    REMortgageRegistrationDate AS REMortgageRegistrationDate, 
    CollateralStatus AS CollateralStatus, 
    RealEstateActive AS PropertyStatus  
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateralrealestate 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
""")

Property_Ownership = spark.sql(f"""
SELECT DISTINCT 
    OwnershipKey AS OwnershipID, 
    DCollateralKey AS CollateralID, 
    RealEstateCepalKey AS PropertyID, 
    OwnerID,  
    FieldValueENG AS OwnershipType, 
    FullOwnershipFlag,  
    CAST(OwnershipPercentage AS DECIMAL(20,6)) AS Percentage 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateralrealestate 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
AND ownerid IS NOT NULL
""")

Collateral_Tape_NRE = spark.sql(f"""
SELECT DISTINCT 
    DCollateralKey AS CollateralId,
    CollateralType AS CollateralType,
    CollateralTypeDescription AS CollateralTypeDescription,
    CAST(CollateralAmount AS DECIMAL(18,2)) AS CollateralAmount 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateral 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
AND CollateralType = 'Non-RE'
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
    CAST(LatestValuationAmount AS DECIMAL(18,2)) AS LatestValuationAmount, 
    LatestValuationDate AS LatestValuationDate, 
    LatestValuationType AS LatestValuationType, 
    CAST(RealEstateNetArea AS DECIMAL(18,2)) AS PropertySize, 
    CAST(RealEstateLandArea AS DECIMAL(18,2)) AS LandArea, 
    CAST(OtherArea AS DECIMAL(18,2)) AS OtherArea,
    RealEstateActive AS PropertyStatus 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateral 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
AND CollateralType = 'RE'
""")

Re_Asset_Appraisals = spark.sql(f"""
SELECT DISTINCT 
    AppraisalID AS AppraisalID, 
    RealEstateCepalKey AS PropertyID, 
    AppraisalType AS AppraisalType, 
    AppraisalDate AS AppraisalDate, 
    AppraisalAmount AS AppraisalAmount, 
    TaxRegistrationNumber AS AppraisalCompanyTaxID, 
    AppraisalCompanyName AS AppraisalCompanyName, 
    OccupancyStatus AS OccupancyStatus, 
    BuildingConditions AS BuildingConditions, 
    ConstructionStatus AS ConstructionStatus, 
    OpenMarketValue AS OpenMarketValue, 
    ForcedSaleValue AS ForcedSaleValue 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.reassetappraisals 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
""")

Loan_Sales = spark.sql(f"""
SELECT DISTINCT 
    ExposureSalesKey AS LoanSaleId, 
    ExposureSalesLastUpdatedDate AS FinalSaleDate, 
    CAST(EquivalentAmount AS DECIMAL(18,2)) AS TotalEquivalentAmount, 
    Description AS LoanSalesDescription, 
    ExposureKey AS AccountNo 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loansales 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
""")

Restructuring_History = spark.sql(f"""
SELECT DISTINCT 
    PaymentAgreementID AS PaymentAgreementID, 
    ExposureKey AS AccountNo, 
    ActiveExposureRestructureStartDate AS PaymentArrangementDate, 
    ExposureRestructureStatus AS PaymentArrangementStatus, 
    ExposureRestructureType AS PaymentArrangementType, 
    ProductDescription AS PaymentArrangementTypeDetailed, 
    ExposureRestructureOrigination AS PaymentArrangementOrigination 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
AND ActiveExposureRestructureStartDate IS NOT NULL
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
export_data(cast_decimal_to_float(PA_Installments), "PA_Installments", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Loan), "Loan", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(External_Lien_DB), "External_Lien_DB", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Property_Ownership), "Property_Ownership", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Re_Asset_Appraisals), "Re_Asset_Appraisals", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Loan_Sales), "Loan_Sales", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Restructuring_History), "Restructuring_History", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Restructure_Mapping), "Restructure_Mapping", last_eom_business_date, output_dir, output_file_prefix)

