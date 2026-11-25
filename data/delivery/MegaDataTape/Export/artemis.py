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
PortfolioName = ["ARTEMIS PORTFOLIO (ATTICA BANK)"]
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


Loan = spark.sql(f"""
SELECT DISTINCT 
    AgreementKey AS ContractID, 
    AssetSegment AS IFRSAssetClass, 
    BankOfOrigin AS BankOfOrigin, 
    Baserate AS RefRate, 
    CDI AS BorrowerID, 
    Coupon AS Coupon, 
    Currency AS Currency, 
    DaysPastDueFinal AS DaysPastDue, 
    DenouncedDate AS DenouncementDate, 
    DueAmount AS BalanceInArrears, 
    ESMABankOfOrigin AS ESMABankOfOrigin, 
    ExposureKey AS AccountNo, 
    GBV AS GBV, 
    GBVEur AS GBVEur,
    InterestRateType AS InterestRateType, 
    IsActiveFlag AS OpenFlag, 
    IsSecuredFlag AS SecuredFlag, 
    IssueDate AS ContractIssuanceDate, 
    L3869Flag AS L3869Flag, 
    L3869Status AS L3869Status, 
    MaturityDate AS MaturityDate, 
    N128Rate AS Levy, 
    OpenDate AS OriginationDate, 
    OutstandingTotalBalance AS TotalExposure, 
    OutstandingTotalBalanceEur AS TotalExposureEur,
    PenaltyRate AS PenaltyInterestRate, 
    PlatformName AS StateSubsidy, 
    PlatformNameFlag AS StateSubsidyFlag, 
    RepaymentFrequency AS RepaymentFrequency, 
    SpreadRate AS Spread, 
    TotalAccruals AS Accruals,
    TotalAccrualsEur AS AccrualsEur,
    TotalExpense AS Expenses, 
    TotalInterest AS Interest, 
    TotalInterestEur AS InterestEur,
    TotalPrincipal AS Principal,
    TotalPrincipal_Eur AS TotalPrincipal_EUR

FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str})
""")

Borrower = spark.sql(f"""
SELECT DISTINCT 
    BirthDate AS BirthDate, 
    CDI AS BorrowerID, 
    DeceasedFlag AS DeceasedFlag, 
    MaritalStatusENG AS MaritalStatus, 
    Nace3lvlDescr AS Industry, 
    NationalityENG AS Nationality, 
    PartyPostalCode AS BorrowerPostcode, 
    PartyRegion AS BorrowerRegion, 
    PartyType AS BorrowerType 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.borrower 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str})
""")

Borrower_Mapping = spark.sql(f"""
SELECT DISTINCT 
    AgreementKey AS ContractID, 
    CDI AS BorrowerID, 
    ExposureKey AS AccountNo, 
    isActiveFlag AS Activeflag, 
    PartyRelationshipType AS RelationType, 
    PortfolioName AS SPV 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.borrowermapping 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str})
""")

Collateral_Mapping = spark.sql(f"""
SELECT DISTINCT 
    AgreementKey AS ContractID, 
    CollateralAmount AS CollateralAmount, 
    CollateralStatus AS CollateralStatus, 
    CollateralType AS CollateralType, 
    CollateralTypeDescription AS CollateralTypeDescription, 
    DCollateralKey AS CollateralID, 
    InitialLegalOrder AS CollateralLien, 
    OwnershipKey AS OwnershipID, 
    RealEstateActive AS PropertyStatus, 
    RealEstateCepalKey AS PropertyID, 
    REMortgageRegistrationDate AS REMortgageRegistrationDate 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateralrealestate 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str})
""")

Collateral_Tape_RE = spark.sql(f"""
SELECT DISTINCT 
    AreaENG AS Region, 
    Country AS Country, 
    DCollateralKey AS CollateralID, 
    LatestValuationAmount AS LatestValuationAmount, 
    LatestValuationDate AS LatestValuationDate, 
    LatestValuationType AS LatestValuationType, 
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
AND CollateralType = 'RE'
""")

Collateral_Tape_NRE = spark.sql(f"""
SELECT DISTINCT 
    CollateralAmount AS CollateralAmount, 
    CollateralType AS CollateralType, 
    CollateralTypeDescription AS CollateralTypeDescription, 
    DCollateralKey AS CollateralID 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateral 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
AND CollateralType = 'Non-RE'
""")

Collections = spark.sql(f"""
SELECT DISTINCT 
    ActualDate AS AllocationDate, 
    Amount AS AllocationAmount, 
    CDI AS BorrowerID, 
    CollectionKey AS CollectionID, 
    ExposureKey AS AccountNo, 
    PaymentTypeGroup AS CollectionType 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collections 
WHERE ReportingDate = '{last_eom_business_date}'
AND PortfolioName IN ({portfolio_str})
""")

Property_Ownership = spark.sql(f"""
SELECT DISTINCT 
    OwnerID AS OwnerID, 
    DCollateralKey AS CollateralID, 
    FieldValueENG AS OwnershipType, 
    FullOwnershipFlag AS FullOwnershipFlag, 
    OwnershipKey AS OwnershipID, 
    OwnershipPercentage AS Percentage, 
    RealEstateCepalKey AS PropertyID 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateralrealestate 
WHERE ReportingDate = '{last_eom_business_date}' 
AND PortfolioName IN ({portfolio_str}) 
AND ownerid IS NOT NULL
""")

# COMMAND ----------

export_data(cast_decimal_to_float(Loan), "Loan", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Borrower), "Borrower", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Borrower_Mapping), "Borrower_Mapping", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collateral_Mapping), "Collateral_Mapping", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collateral_Tape_RE), "Collateral_Tape_RE", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collateral_Tape_NRE), "Collateral_Tape_NRE", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collections), "Collections", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Property_Ownership), "Property_Ownership", last_eom_business_date, output_dir, output_file_prefix)
