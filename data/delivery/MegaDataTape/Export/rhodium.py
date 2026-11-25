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
portfolio_str = "%RHODIUM%"

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

# DBTITLE 1,Perimeter filter - rhodium_deleted_agreements
# All tapes are filtered serially:
# 1. Perimeter exclusion table is registered as temp view
# 2. Loan is the master perimeter (LEFT ANTI JOIN on rhodium_deleted_agreements by ExposureKey)
# 3. Borrower_Mapping filters by Loan's ExposureKey
# 4. Borrower filters by Borrower_Mapping's CDI (ensures Loan<->Borrower consistency)
# 5. Collateral_Mapping filters by Loan's ExposureKey
# 6. Remaining tapes filter via Loan (ExposureKey) or Collateral_Mapping (DCollateralKey/RealEstateCepalKey)

# COMMAND ----------

# DBTITLE 1,0. Perimeter exclusion
rhodium_excluded = spark.sql(f"""
SELECT DISTINCT ExposureKey
FROM `{env_prefix}-cdh`.reference.rhodium_deleted_agreements
""")

rhodium_excluded.createOrReplaceTempView("rhodium_excluded")

# COMMAND ----------

# DBTITLE 1,1. Loan (master perimeter)
Loan = spark.sql(f"""
SELECT DISTINCT 
    CDI AS BorrowerID, 
    AgreementKey AS ContractID, 
    l.ExposureKey AS AccountNo, 
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
    AgreementCREV AS AllocatedCollatrealValueCREV, 
    AgreementREV AS AllocatedCollatrealValueREV, 
    IsActiveFlag AS OpenFlag, 
    CourtRegion AS CourtRegion, 
    DueAmount AS BalanceInArrears, 
    HighBalanceFlag AS IsHighBalance    
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan l
LEFT JOIN rhodium_excluded rex
    ON rex.ExposureKey = l.ExposureKey
WHERE l.ReportingDate = '{last_eom_business_date}' 
AND l.PortfolioName ilike ('{portfolio_str}')
AND rex.ExposureKey IS NULL
""")

Loan.createOrReplaceTempView("loan_filtered")

# COMMAND ----------

# DBTITLE 1,2. Borrower_Mapping (filters by Loan perimeter)
Borrower_Mapping = spark.sql(f"""
SELECT DISTINCT 
    bm.CDI AS BorrowerID, 
    bm.AgreementKey AS ContractID, 
    bm.ExposureKey AS AccountNo, 
    bm.PartyRelationshipType AS RelationType, 
    bm.isActiveFlag AS Activeflag, 
    bm.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.borrowermapping bm
INNER JOIN loan_filtered lf
    ON lf.AccountNo = bm.ExposureKey
WHERE bm.ReportingDate = '{last_eom_business_date}' 
AND bm.PortfolioName ilike ('{portfolio_str}')
""")

Borrower_Mapping.createOrReplaceTempView("borrower_mapping_filtered")

# COMMAND ----------

# DBTITLE 1,3. Borrower (only CDIs present in Borrower_Mapping)
Borrower = spark.sql(f"""
SELECT DISTINCT 
    b.CDI AS BorrowerID, 
    b.PartyType AS BorrowerType, 
    b.L3869Flag AS L3869Flag, 
    b.CalcLawStatus AS L3869Status, 
    b.Gender AS Gender, 
    b.DeceasedFlag AS DeceasedFlag, 
    b.BirthDate AS BirthDate, 
    b.NationalityENG AS Nationality, 
    b.MaritalStatusENG AS MaritalStatus, 
    b.Nace3lvlDescr AS Industry, 
    b.PartyRegion AS BorrowerRegion, 
    b.PartyPostalCode AS BorrowerPostcode, 
    b.RightPersonContacts AS RightPersonContacts, 
    b.PositiveRightPersonContacts AS PositiveRightPersonContacts, 
    b.NumberofPromises AS NumberofPromises, 
    b.NumberofContactActivities AS NumberofContactActivities, 
    b.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.borrower b
WHERE b.ReportingDate = '{last_eom_business_date}' 
AND b.PortfolioName ilike ('{portfolio_str}')
AND EXISTS (
        SELECT 1
        FROM borrower_mapping_filtered bmf
        WHERE bmf.BorrowerID = b.CDI
  )
""")

# COMMAND ----------

# DBTITLE 1,4. Collateral_Mapping (filters by Loan perimeter via AgreementKey + ExposureKey)
Collateral_Mapping = spark.sql(f"""
SELECT DISTINCT 
    cre.RealEstateCepalKey AS PropertyID, 
    cre.OwnershipKey AS OwnershipID, 
    cre.ExposureKey AS AccountNo, 
    cre.AgreementKey AS ContractID, 
    cre.DCollateralKey AS CollateralID, 
    cre.CollateralAmount AS CollateralAmount, 
    cre.CollateralType AS CollateralType, 
    cre.CollateralTypeDescription AS CollateralTypeDescription, 
    cre.InitialLegalOrder AS CollateralLien, 
    cre.REMortgageRegistrationDate AS REMortgageRegistrationDate, 
    cre.CollateralStatus AS CollateralStatus, 
    cre.RealEstateActive AS PropertyStatus, 
    cre.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateralrealestate cre
INNER JOIN loan_filtered lf
    ON lf.AccountNo = cre.ExposureKey
WHERE cre.ReportingDate = '{last_eom_business_date}' 
AND cre.PortfolioName ilike ('{portfolio_str}')
""")

Collateral_Mapping.createOrReplaceTempView("collateral_mapping_filtered")

# COMMAND ----------

# DBTITLE 1,5. Collateral_Tape_NRE (filters by Collateral_Mapping)
Collateral_Tape_NRE = spark.sql(f"""
SELECT DISTINCT 
    c.DCollateralKey AS CollateralID, 
    c.CollateralType AS CollateralType, 
    c.CollateralTypeDescription AS CollateralTypeDescription, 
    c.CollateralAmount AS CollateralAmount, 
    c.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateral c
INNER JOIN collateral_mapping_filtered cmf
    ON cmf.CollateralID = c.DCollateralKey
WHERE c.ReportingDate = '{last_eom_business_date}' 
AND c.PortfolioName ilike ('{portfolio_str}')
AND c.CollateralType = 'Non-RE'
""")

# COMMAND ----------

# DBTITLE 1,6. Collateral_Tape_RE (filters by Collateral_Mapping)
Collateral_Tape_RE = spark.sql(f"""
SELECT DISTINCT 
    c.DCollateralKey AS CollateralID, 
    c.RealEstateCepalKey AS PropertyID, 
    c.RealEstateSubCategory AS PropertyType, 
    c.RealEstateAddress1 AS Address, 
    c.RealEstateCity AS City, 
    c.RealEstatePostalcode AS Postcode, 
    c.AreaENG AS Region, 
    c.Country AS Country, 
    c.RealEstateConstructionYear AS ConstructionYear, 
    c.OriginalValuationAmount AS OriginalValuationAmount, 
    c.OriginalValuationDate AS OriginalValuationDate, 
    c.OriginalValuationType AS OriginalValuationType, 
    c.LatestValuationAmount AS LatestValuationAmount, 
    c.LatestValuationDate AS LatestValuationDate, 
    c.LatestValuationType AS LatestValuationType, 
    c.RealEstateNetArea AS PropertySize, 
    c.RealEstateLandArea AS LandArea, 
    c.OtherArea AS OtherArea, 
    c.RealEstateActive AS PropertyStatus, 
    c.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateral c
INNER JOIN collateral_mapping_filtered cmf
    ON cmf.CollateralID = c.DCollateralKey
WHERE c.ReportingDate = '{last_eom_business_date}' 
AND c.PortfolioName ilike ('{portfolio_str}')
AND c.CollateralType = 'RE'
""")

# COMMAND ----------

# DBTITLE 1,7. Collections (filters by Loan perimeter via ExposureKey)
Collections = spark.sql(f"""
SELECT DISTINCT 
    c.CollectionKey AS CollectionID, 
    c.ActualDate AS AllocationDate, 
    c.Amount AS AllocationAmount, 
    c.PaymentTypeGroup AS CollectionType, 
    c.ExposureKey AS AccountNo, 
    c.CDI AS BorrowerID, 
    c.GroupID AS GroupID, 
    c.PaymentAgreementID AS PaymentAgreementID, 
    c.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collections c
INNER JOIN loan_filtered lf
    ON lf.AccountNo = c.ExposureKey
WHERE c.ReportingDate = '{last_eom_business_date}' 
AND c.PortfolioName ilike ('{portfolio_str}')
""")

# COMMAND ----------

# DBTITLE 1,8. Contact_History (filters by Loan perimeter via ExposureKey)
Contact_History = spark.sql(f"""
SELECT DISTINCT 
    ch.InteractionKey AS ContactHistoryID, 
    ch.ExposureKey AS AccountNo, 
    ch.CDI AS BorrowerID, 
    ch.InteractionDate AS ContactDate, 
    ch.ContactType AS ContactType, 
    ch.InteractionType AS ContactDescription, 
    ch.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.contacthistory ch
INNER JOIN loan_filtered lf
    ON lf.AccountNo = ch.ExposureKey
WHERE ch.ReportingDate = '{last_eom_business_date}' 
AND ch.PortfolioName ilike ('{portfolio_str}')
""")

# COMMAND ----------

# DBTITLE 1,9. External_Lien_DB (filters by Collateral_Mapping via DCollateralKey)
External_Lien_DB = spark.sql(f"""
SELECT DISTINCT 
    el.RELienKey AS ClaimID, 
    el.OwnershipKey AS OwnershipID, 
    el.DCollateralKey AS CollateralID, 
    el.RealEstateCepalKey AS PropertyID, 
    el.TaxRegistrationNumber AS EntityOwnerTaxID, 
    el.LastNameGR AS EntityOwnerName, 
    el.RELienInitialOrder AS Position, 
    el.PositionDate AS PositionDate, 
    el.RELienMortgageAmount AS Amount, 
    el.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.externalliendb el
INNER JOIN collateral_mapping_filtered cmf
    ON cmf.CollateralID = el.DCollateralKey
WHERE el.ReportingDate = '{last_eom_business_date}' 
AND el.PortfolioName ilike ('{portfolio_str}')
""")

# COMMAND ----------

# DBTITLE 1,10. Loan_Sales (filters by Loan perimeter via ExposureKey)
Loan_Sales = spark.sql(f"""
SELECT DISTINCT 
    ls.ExposureSalesKey AS LoanSaleId, 
    ls.ExposureKey AS AccountNo, 
    ls.ExposureSalesStatus AS LoanSalesDescription, 
    ls.ExposureSalesLastUpdatedDate AS FinalSaleDate, 
    ls.EquivalentAmount AS TotalEquivalentAmount, 
    ls.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loansales ls
INNER JOIN loan_filtered lf
    ON lf.AccountNo = ls.ExposureKey
WHERE ls.ReportingDate = '{last_eom_business_date}' 
AND ls.PortfolioName ilike ('{portfolio_str}')
""")

# COMMAND ----------

# DBTITLE 1,11. PA_Installments (filters by Loan perimeter via ExposureKey)
PA_Installments = spark.sql(f"""
SELECT DISTINCT 
    i.DPaymentScheduleKey AS InstallmentID, 
    i.PaymentAgreementID AS PaymentAgreementID, 
    i.InstallmentDate AS InstallmentDateDue, 
    i.InstallmentAmount AS InstallmentAmountDue, 
    i.InstallmentPaidDate AS InstallmentPaidDate, 
    i.InstallmentAmountPaid AS InstallmentAmountPaid, 
    i.InstallmentPaid AS InstallmentPaid, 
    i.ExposureRestructureOrigination AS PaymentArrangementOrigination, 
    i.ExposureKey AS AccountNo, 
    i.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.installments i
INNER JOIN loan_filtered lf
    ON lf.AccountNo = i.ExposureKey
WHERE i.ReportingDate = '{last_eom_business_date}' 
AND i.PortfolioName ilike ('{portfolio_str}')
""")

# COMMAND ----------

# DBTITLE 1,12. Property_Ownership (filters by Collateral_Mapping via DCollateralKey)
Property_Ownership = spark.sql(f"""
SELECT DISTINCT 
    po.OwnershipKey AS OwnershipID, 
    po.DCollateralKey AS CollateralID, 
    po.RealEstateCepalKey AS PropertyID, 
    po.OwnerID AS OwnerID, 
    po.FieldValueENG AS OwnershipType, 
    po.FullOwnershipFlag AS FullOwnershipFlag, 
    po.OwnershipPercentage AS Percentage, 
    po.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.collateralrealestate po
INNER JOIN collateral_mapping_filtered cmf
    ON cmf.CollateralID = po.DCollateralKey
WHERE po.ReportingDate = '{last_eom_business_date}' 
AND po.PortfolioName ilike ('{portfolio_str}')
AND po.ownerid IS NOT NULL
""")

# COMMAND ----------

# DBTITLE 1,13. Re_Asset_Appraisals (filters by Collateral_Mapping via RealEstateCepalKey)
Re_Asset_Appraisals = spark.sql(f"""
SELECT DISTINCT 
    ra.AppraisalID AS AppraisalID, 
    ra.RealEstateCepalKey AS PropertyID, 
    ra.AppraisalType AS AppraisalType, 
    ra.AppraisalDate AS AppraisalDate, 
    ra.AppraisalAmount AS AppraisalAmount, 
    ra.TaxRegistrationNumber AS AppraisalCompanyTaxID, 
    ra.AppraisalCompanyName AS AppraisalCompanyName, 
    ra.OccupancyStatus AS OccupancyStatus, 
    ra.BuildingConditions AS BuildingConditions, 
    ra.ConstructionStatus AS ConstructionStatus, 
    ra.OpenMarketValue AS OpenMarketValue, 
    ra.ForcedSaleValue AS ForcedSaleValue, 
    ra.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.reassetappraisals ra
INNER JOIN collateral_mapping_filtered cmf
    ON cmf.PropertyID = ra.RealEstateCepalKey
WHERE ra.ReportingDate = '{last_eom_business_date}' 
AND ra.PortfolioName ilike ('{portfolio_str}')
""")

# COMMAND ----------

# DBTITLE 1,14. Restructure_Mapping (filters by Loan perimeter via ExposureKey)
Restructure_Mapping = spark.sql(f"""
SELECT DISTINCT 
    rm.CDI AS BorrowerID, 
    rm.NewAgreementKey AS ContractID, 
    rm.OldExposureKey AS RestructuredAccountNo, 
    rm.NewExposureKey AS NewAccountNo, 
    rm.RestructureType AS RestructureType, 
    rm.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.restructuremapping rm
INNER JOIN loan_filtered lf
    ON lf.AccountNo = rm.NewExposureKey
WHERE rm.ReportingDate = '{last_eom_business_date}' 
AND rm.PortfolioName ilike ('{portfolio_str}')
""")

# COMMAND ----------

# DBTITLE 1,15. Restructure_History (filters by Loan perimeter via ExposureKey)
Restructure_History = spark.sql(f"""
SELECT DISTINCT 
    l.PaymentAgreementID AS PaymentAgreementID, 
    l.ExposureKey AS AccountNo, 
    l.ActiveExposureRestructureStartDate AS PaymentArrangementDate, 
    l.ExposureRestructureStatus AS PaymentArrangementStatus, 
    l.ExposureRestructureType AS PaymentArrangementType, 
    l.ProductDescription AS PaymentArrangementTypeDetailed, 
    l.ExposureRestructureOrigination AS PaymentArrangementOriginationRestr, 
    l.HighBalanceFlag AS IsHighBalance 
FROM `{env_prefix}-cdh-delivery`.`mega-datatape`.loan l
INNER JOIN loan_filtered lf
    ON lf.AccountNo = l.ExposureKey
WHERE l.ReportingDate = '{last_eom_business_date}' 
AND l.PortfolioName ilike ('{portfolio_str}')
""")

# COMMAND ----------

# DBTITLE 1,Export
export_data(cast_decimal_to_float(Loan), "Loan", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Borrower), "Borrower", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Borrower_Mapping), "Borrower_Mapping", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collateral_Mapping), "Collateral_Mapping", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collateral_Tape_RE), "Collateral_Tape_RE", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collateral_Tape_NRE), "Collateral_Tape_NRE", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Collections), "Collections", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Contact_History), "Contact_History", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(External_Lien_DB), "External_Lien_DB", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Loan_Sales), "Loan_Sales", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(PA_Installments), "PA_Installments", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Property_Ownership), "Property_Ownership", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Re_Asset_Appraisals), "Re_Asset_Appraisals", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Restructure_Mapping), "Restructure_Mapping", last_eom_business_date, output_dir, output_file_prefix)
export_data(cast_decimal_to_float(Restructure_History), "Restructure_History", last_eom_business_date, output_dir, output_file_prefix)
