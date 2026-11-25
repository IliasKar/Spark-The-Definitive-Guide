# Platform Migration (PLTFRM dtransfer) — Knowledge Base

This document captures the patterns, mappings, and lessons learned from migrating on-prem `[PLTFRM].[dtransfer]` stored procedures to Databricks delivery notebooks.

---

## 1. Overview

The on-prem `dwhcore\PLTFRM` database has a `dtransfer` schema with stored procedures that populate tables consumed by IT systems. Each SP reads from `dwhcore\DWH` (the golden layer) and writes to `dtransfer.*` tables. These tables are uploaded to regulatory government platforms — **accuracy is critical**.

The cloud equivalent:
- **Source**: `{env}-cdh.primary.*`, `{env}-cdh.derivative.*`, `{env}-cdh.landed.*`
- **Target**: `{env}-cdh-delivery.it.*` (schema `it`)
- **Repo**: `gr.cepal.dna.de.delivery` under `platforms/` directory
- **Pattern**: One notebook per table, PySpark DataFrame API, overwrite mode
- **Branch**: `DT-842_create_pltfrm_customer`

---

## 2. Notebook Structure

Each notebook follows this pattern:

```
platforms/dtransfer_{TableName}/dtransfer_{TableName}.ipynb
```

### Cell 1: Parameters & Environment Setup
```python
dbutils.widgets.text("business_date", "", "Business Date (YYYY-MM-DD, empty=latest)")

import sys
sys.path.append("/Workspace/Shared")
import aws_utils

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, ...
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
env = aws_utils.get_env_prefix()

business_date_param = dbutils.widgets.get("business_date").strip()
if business_date_param:
    run_date = business_date_param
else:
    run_date = str(
        spark.table(f"`{env}-cdh`.primary.exposure")
        .filter(col("SourceSystem") == "profits")
        .agg({"BusinessDate": "max"})
        .collect()[0][0]
    )

run_date_lit = lit(run_date).cast("date")
```

### Cell 2: Markdown documentation (Table Mapping)

### Cell 3+: Load sources, transform, write

### Final Cell: Write to target
```python
target = f"`{env}-cdh-delivery`.it.dtransfer_{tablename}"
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
```

---

## 3. On-Prem → Cloud Table Mapping

### Core Tables

| On-Prem | Cloud | Notes |
|---|---|---|
| `DWH.prt.Party` | `{env}-cdh.primary.party` | Unified (Individual+Organization). Has CDI, PartyKey, PartyType, LegalNameGR, LastNameGR, FirstNameGR, PrimaryCommercialGR, BirthDate, TaxRegistrationNumber |
| `DWH.prt.PartyIdentification` | `{env}-cdh.primary.partyidentification` | Has PartyIdentificationType ('TaxNumber', 'IdentityCard', etc.), PartyIdentificationNumber, CDI |
| `DWH.ast.Asset` + `DWH.ast.AssetXPartyRltp` | `{env}-cdh.primary.exposure` | One row per asset-party relationship. Has PartyRelationshipType, ExposureKey, CDI, IsActiveFlag |
| `DWH.ast.Asset` (all assets) | `{env}-cdh.derivative.dexposure` | One row per exposure (Primary Owner only). Has ALL financial columns, ClosedDate, PortfolioName |
| `DWH.ast.LoanFinancials` | `{env}-cdh.derivative.dexposure` | All balance columns (Principal, Interest, DuePrincipal, etc.) |
| `DWH.ast.LoanMetrics` | `{env}-cdh.derivative.dexposure` | AssetSegment, LPS, LPSDate |
| `DWH.ast.LoanInterestBehaviour` | `{env}-cdh.derivative.dexposure` | InterestRateType, BaseRate, SpreadRate, N128Rate, PenaltyRate |
| `DWH.ast.Product` | `{env}-cdh.primary.product` | ProductKey, ProductDescription, SubProduct |
| `DWH.ast.AssetRelationships` | `{env}-cdh.primary.entityrelationships` + `{env}-cdh.primary.restructuring` | Master-Slave in entityrelationships; Old-New Loan in restructuring |
| `DWH.act.UnderLaw` | `{env}-cdh.primary.underlaw` | Filter: UnderLawType = 'N3869', InLawFlag = 1 |
| `DWH.trn.Collections` | `{env}-cdh.primary.collection` | FinalMatchedFlag=1 → final_reason = 'matched' |
| `DWH.act.LegalActivity` | `{env}-cdh.primary.interaction` | Filter: IsLegalFlag = true. Has InteractionTypeKey, InteractionType, InteractionDate |
| `DWH.ast.Settlement` | `{env}-cdh.primary.settlementexposure` | Active settlements per exposure |
| `DWH.ast.Modifications_PEE` | `{env}-cdh.primary.restructuring` | NumberOfRestructurings = COUNT per NewExposureKey |
| `DWH.ast.Collateral` | `{env}-cdh.derivative.dcollateral` | CollateralAmount, AgreementKey, CollateralStatus, SubCategory |
| `DWH.adm.PartyReferenceCodes` | CDI column in party/exposure tables | Denormalized |
| `DWH.adm.SourceExtraction` | `MAX(BusinessDate) FROM primary.exposure WHERE SourceSystem = 'profits'` | Standard pattern |
| `DWH.cls.Classification` | Absorbed into other tables | PartyType, PartyRelationshipType, PartyIdentificationType, etc. |
| `DWH.ref.Portfolio` | `{env}-cdh.primary.portfolio` | Has PortfolioId, PortfolioName, Prefix |
| `PLTFRM.rpt.ApplicationSubsidy` | `{env}-cdh.landed.rpt_applicationsubsidy` | Subsidy programs (Bridge1, Bridge2, L4605) |
| `DWH_STG.qca.*` | `{env}-cdh.landed.qca_*` | Same naming: DWH_STG.qca.FS_CASES_SNAP → landed.qca_fs_cases_snap |
| `DWH_STG.qc.*` | DECOMMISSIONED June 6, 2026 | Data migrated to QCA. Use qca_ landed tables instead |
| `primary.assignment` | `{env}-cdh.primary.assignment` | AssignmentDate = COALESCE(InternalAssignmentStartDate, ExternalAssignmentStartDate). Join on AgreementKey + CDI |

### Relationship Type Mapping (Exposure)

| On-Prem Classification.Description | Cloud PartyRelationshipType |
|---|---|
| `loan-Main Beneficiary Rltp` | `Primary Owner` |
| `Loan-Guarantor Rltp` | `Guarantor` |
| `Loan-Other Beneficiaries Rltp` | `Co-Owner` |

### Asset Relationship Type Mapping

| On-Prem | Cloud Source | Cloud Filter |
|---|---|---|
| Master - Slave Rltnp | `primary.entityrelationships` | EntityRelationshipType = 'Master - Slave Rltnp', SubType != 'WDN' |
| Restructured Loan - Writeoff Rltnp | `primary.entityrelationships` | EntityRelationshipType = 'Master - Slave Rltnp', SubType = 'WDN' |
| Restructuring: Old - New Loan | `primary.restructuring` | RestructuringType column |
| Old-New Loan QualcoAB | `primary.restructuring` | RestructuringType column |
| L1-L2 Subsidy Account | `primary.restructuring` | RestructuringType column |

### DWH_STG → Landed Table Naming Convention

`DWH_STG.{source}.{TableName}` → `landed.{source}_{tablename_lowercase}`

Examples:
- `DWH_STG.qca.FS_CASES_SNAP` → `landed.qca_fs_cases_snap`
- `DWH_STG.qca.FS_CASES_SNAP_EXTRA` → `landed.qca_fs_cases_snap_extra`
- `DWH_STG.qca.DIM_CASES_STATIC` → `landed.qca_dim_cases_static`
- `DWH_STG.qca.DIM_CUSTOMER_STATIC` → `landed.qca_dim_customer_static`
- `DWH_STG.qca.DIM_QUEUE` → `landed.qca_dim_queue`

**QC source system was decommissioned on June 6, 2026.** All data moved to QCA except:
- Settlements → moved to QCA and Profits
- Legal activities → moved to Arotron source system
- In cloud primary/derivative tables, all data is consolidated regardless of source

---

## 4. Critical Patterns & Lessons Learned

### Date Filtering — CRITICAL
- **Always use `lit(run_date).cast("date")`** when comparing with date columns in PySpark DataFrame API
- Bare string comparison (`col("BusinessDate") == run_date`) **silently returns 0 rows** due to type mismatch
- Store as: `run_date_lit = lit(run_date).cast("date")`

### Column Ambiguity in Joins — CRITICAL
- When joining `exposure` with `dexposure`, many columns have the same name (ExposureKey, AgreementKey, AssetSegment, MaturityDate, OriginationAmount, ProductKey, etc.)
- **Always alias columns from dexposure** with a `dx_` prefix in the select before joining:
  ```python
  dexposure.select(
      col("ExposureKey").alias("dx_ExposureKey"),
      col("AssetSegment").alias("dx_AssetSegment"),
      col("Maturitydate").alias("dx_MaturityDate"),
      ...
  )
  ```

### Party Table Coverage
- `primary.party` filtered by today's BusinessDate only has ~686K parties
- On-prem has ~805K because its party table uses SCD Type 2 (EffectiveDate/EndDate)
- **Solution**: Load party WITHOUT BusinessDate filter, deduplicate by CDI with priority: profits > qca > others, ordered by BusinessDate DESC
- This gives coverage matching on-prem (~805K)

```python
party_all = spark.table(f"`{env}-cdh`.primary.party")
w_party = Window.partitionBy("CDI").orderBy(
    when(col("SourceSystem") == "profits", 1)
    .when(col("SourceSystem") == "qca", 2)
    .otherwise(3),
    col("BusinessDate").desc()
)
party = (
    party_all.withColumn("_rn", row_number().over(w_party))
    .filter(col("_rn") == 1).drop("_rn")
)
```

### Exposure Perimeter
- `derivative.dexposure` only has `Primary Owner` relationship type — NOT suitable for customer perimeter
- `derivative.get_exposures_by_business_date()` function also only returns Primary Owner
- **Use `primary.exposure`** filtered by BusinessDate for the full perimeter (Guarantor, Primary Owner, Co-Owner)
- This matches on-prem count (805K distinct CDIs)

### Source System Filtering
- On-prem SPs do NOT filter by source system — they take all parties linked to assets
- Cloud must also include all source systems (profits, qca, thesis, arotron)
- Deduplicate by CDI with source system priority

### Column Name Mappings (Common Gotchas)
- On-prem `Party.PartyName` → Cloud `party.LegalNameGR` (NOT `PartyName`)
- On-prem `CustomerVat` → Cloud `party.TaxRegistrationNumber` (direct, no partyidentification join needed)
- On-prem `BirthDate` is timestamp → cast to date: `party["BirthDate"].cast("date")`
- On-prem `LPS` is string in dexposure (not date)
- On-prem `LPSDate` in dexposure is **string** type — cast to date

### N4605 Status (Verified)
```python
# From qca_fs_cases_snap_extra WHERE CEXT_DEDICATED_PROPERTY_01_NVARCHAR = 'ΝΑΙ'
# Join path: qca_fs_cases_snap → qca_fs_cases_snap_extra (on CASE_CODE)
#           → qca_dim_cases_static (on CASE_CODE) for CASE_ACCOUNT_FOR_SEARCH
#           → qca_dim_customer_static (on CUST_CODE = CUST_ID) for CUST_CIF (=CDI)
# Account = strip 3-char prefix if non-numeric: SUBSTRING(CASE_ACCOUNT_FOR_SEARCH, 4, 99)
```

### N3588 Status (Bankruptcy) — QC Decommissioned
- On-prem used `DWH_STG.qc.AT_CUSTOMER_CORE` + `LST_OF_VAL` for bankruptcy status
- **Cloud**: Use QCA queue-based lookup instead:
  - Table: `landed.qca_dim_queue`
  - Filter: `QUE_DESC IN ('LEGAL - ΠΡΟΠΤΩΧΕΥΤΙΚΗ ΔΙΑΔΙΚΑΣΙΑ', 'LEGAL - ΠΡΩΠΤΩΧΕΥΤΙΚΗ ΔΙΑΔΙΚΑΣΙΑ', 'ΠΤΩΧΕΥΣΗ', 'LEGAL - ΠΤΩΧΕΥΣΗ ΕΠΙΧΕΙΡΗΣΕΩΝ')`
  - Join: qca_fs_cases_snap → qca_dim_queue (on QUE_CODE = QUE_ID) → qca_dim_cases_static → qca_dim_customer_static

### IsL3869Flag (UnderLaw)
- Cloud: LEFT JOIN with `primary.underlaw` WHERE UnderLawType='N3869' AND InLawFlag=1
- Use `coalesce(col("IsL3869Flag"), lit(0))` for the final value

### Collateral Covered Amount
- Source: `derivative.dcollateral`
- Column: `CollateralAmount` (sum per AgreementKey)
- Filter active: `CollateralStatus IN ('ΕΝΕΡΓΗ','Ενεργή','Ενεργη','ενεργή','ενεργη','ΟΥΣΙΑΣ','Ενεργό','N/A')`
- Exclude personal guarantees: `SubCategory NOT IN ('Εταιρικές Εγγυήσεις','Προσωπική Εγγυηση Συγκ. Ποσού','Προσωπική Εγγύηση','Εγγύηση Ελληνικού Δημοσίου','Προσωπική Εγγύηση Συνόλου Σύμβασης','Ετ Εγγ Συγκ. Ποσού','Εγγύηση Ελλ. Δημοσ.','ΕΓΓΥΗΣΗ ΝΠΔΔ','ΕΓΓΥΗΣΗ ΕΤΕΑΝ','Εταιρικές Εγγυήσεις Συγκ. Ποσού','Προσωπικές Εγγυήσεις (Ορισμένου ποσού)')`

### AgreementRelationType
- Derived 1:1 from loan-level PartyRelationshipType:
  - Primary Owner → Agreement-Main Beneficiary Rltp
  - Guarantor → Agreement-Guarantor Rltp
  - Co-Owner → Agreement-Other Beneficiaries Rltp

### dexposure Financial Columns (Verified)
All balance/interest columns available directly in `derivative.dexposure`:

| On-Prem (LoanFinancials) | dexposure Column |
|---|---|
| OutstandingTotalBalance | OutstandingTotalBalance |
| OnBalance | OnBalance |
| OffBalance | OffBalance |
| GBV | GBV (double) |
| Principal | Principal |
| Interest | interest (lowercase!) |
| UnrealizedInterest | Unrealizedinterest |
| Expense | Expense |
| Comission | Commission (different spelling!) |
| Accrued | Accrued |
| DelinquentPrincipal | DuePrincipal |
| DelinquentInterest | Dueinterest |
| DelinquentUnrInterest | DueUnrealizedinterest |
| PenaltyInterest | Penaltyinterest |
| PenaltyUnrInterest | PenaltyUnrealizedinterest |
| DelinquentExpense | DueExpense |
| DelinquentComission | DueCommission |
| DelinquentAccrued | DueAccrued |
| DPD (PastDueDays) | DaysPastDue |
| OverdueDate | Overduedate |
| InterestRateType | InterestRateType |
| BaseRate | BaseRate |
| SpreadRate | SpreadRate |
| N128Rate | N128Rate |
| PenaltyRate | PenaltyRate |
| InterestDescription | interestDescription |
| BaseDescription | BaseDescription |
| FixInterestEndDate | FixInterestEndDate |
| Duration | InterestDuration (int) |
| HasActiveSettlementFlag | HasActiveSettlementFlag |
| DenouncedDate | DenouncedDate |
| BucketCode | BucketCode |
| SettlementBucketCode | SettlementBucketCode |
| LPS | LPS |
| LPSDate | LPSDate (string — cast to date!) |
| LastRestructuringDate | LastRestructuringDate |
| Currency | Currency |
| MechanismCode | MechanismCode |
| BankingUnitCode | BankingUnitCode |

---

## 5. Verification Strategy

These tables feed regulatory government platforms. Use this verification approach:

### Level 1: Row Count Match
```sql
-- Cloud
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT key_column) AS distinct_keys
FROM `{env}-cdh-delivery`.it.dtransfer_{table}

-- On-prem (via MCP mssql-itstaging)
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT key_column) AS distinct_keys
FROM [PLTFRM].[dtransfer].[{Table}]
```

### Level 2: Distribution Match
Compare counts by categorical columns (PortfolioId, relationship types, flags).

### Level 3: Column Completeness
Count non-null values per column — should match on-prem pattern.

### Level 4: Financial Totals
Sum key financial columns (OTB, OnBalance, Payments) — should be in same ballpark.

### Level 5: Sample Record Comparison
Pick specific accounts and compare field by field.

### On-Prem Reference Values (2026-06-30)

#### DebtStatic
- Total rows: 1,610,617
- Distinct loans: 1,043,997
- Distinct agreements: 632,828
- Primary Owner: 1,043,997 | Guarantor: 460,167 | Co-Owner: 106,453
- IsSlave: 538,733 | N4605: 8,234 | N3588: 649
- All columns populated: AccMechanism, PrdDescription, AssignmentDate, gd_lpucb

#### DebtDetail
- Total rows: 1,043,997
- Has settlement: 42,711 | Is restructured: 344,680
- Has LPS: 727,699 | Has legal activity: 595,652

#### DebtFinancial
- Total rows: 1,043,997
- Total OTB: €48.66B | Total OnBalance: €24.29B
- Total payments (12mo): €577.3M | Total secured: €24.97B

---

## 6. Deployment

1. Create feature branch from master in `gr.cepal.dna.de.delivery`
2. Add notebook under `platforms/dtransfer_{TableName}/dtransfer_{TableName}.ipynb`
3. Test in Databricks with `business_date` parameter
4. Run verification queries against on-prem
5. Push and create PR to `development` → `master`
6. After merge, configure Databricks workflow job with:
   - `business_date` parameter (empty for daily, specific date for backfill)
   - Git source pointing to master branch

---

## 7. On-Prem MSSQL Access

The on-prem data is accessible via MCP MSSQL server (ITStaging database on dwhcore):
- Configured at repo level: `.kiro/settings/mcp.json`
- Schema `dtransfer` contains the target tables populated by the SPs
- Use `mcp_mssql_itstaging_query` tool for read-only SELECT queries
- The DWH database tables are NOT directly queryable — only dtransfer output tables

---

## 8. Known Differences (V1 Accepted)

| Field | On-Prem | Cloud | Gap | Status |
|---|---|---|---|---|
| PartyId | DWH numeric int ID (69441) | **CDI** (e.g. `GLC0004267942`), string. Replaces earlier source-system PartyKey, which collided across source systems and silently mis-joined downstream | Different ID systems; CDI is unique and non-colliding. IT-confirmed string domain; consuming int columns must be widened (DDL tracked separately) | Accepted (DNA26-1716) |
| PartyIdQualco | Cross-reference CUST_ID (321117) | QCA PartyKey by CDI (2888397) | Different QCA identifiers | Accepted |
| ParentPartyId | DWH grouping int ID (18934) | **COALESCE(TaxRegistrationNumber, CDI)**, string | No equivalent numeric key; TaxRegistrationNumber serves the parent rollup, CDI fallback guarantees non-null. IT-confirmed string domain | Accepted (DNA26-1716) |
| Customer count | ~841K | ~805K (with all-history party) | ~36K parties not in cloud party table | Accepted |
| IsL3869Flag count | 90,489 | 88,052 (in perimeter) | ~2.4K timing difference | Accepted |
| AccountRelationType names | loan-Main Beneficiary Rltp | Primary Owner | Different naming convention | By design |
| CutOffOverdueDate | OverdueDate from snapshot 2026-02-19 | NULL | Needs business clarification | Pending |
| gd_lpucb | From profits_loan_details | NULL | Profits-specific, needs landed join | Pending |

---

## 9. Notebooks Status

| Notebook | SP | Status | On-Prem Rows |
|---|---|---|---|
| `dtransfer_Customer` | `CustomerSp` | ✅ Validated | ~841K |
| `dtransfer_AllAccounts` | `prep_AllAccounts` | Ready to test | ~3.2M |
| `dtransfer_AccountRelation` | `prep_AccountRelation` | Ready to test | ~747K |
| `dtransfer_AccountPayment` | `prep_AccountPayment` | Ready to test | ~217K |
| `dtransfer_VatSettlement` | `prep_VatSettlement` | Ready to test | ~76K |
| `dtransfer_Res` | `ResSp` | Ready to test (column names TBD) | ~37K |
| `dtransfer_Debt` | `DebtSp` | ✅ Validated (pending upstream fixes) | ~1.6M + 1.0M + 1.0M |
| `dtransfer_DebtAB` | `DebtABsp` | ✅ Validated | ~497K |

---

## 10. dtransfer_Debt — Lessons Learned

### ExposureKey Format Mismatch — CRITICAL

On-prem uses internal numeric `LoanId`/`AssetId` for all joins. Cloud uses `ExposureKey` (string with portfolio prefix). Different tables store ExposureKey in different formats:

| Table | ExposureKey Format | Example |
|---|---|---|
| `derivative.dexposure` | Prefix + number | `GLB989003064208101` |
| `primary.exposure` | Prefix + number | `GLB989003064208101` |
| `primary.interaction` | Bare number (no prefix) | `989003064208101` |
| `primary.settlementexposure` | Bare number (no prefix) | `989003064208101` |
| `primary.collection` | Mixed (both formats) | `GLB989003064208101` or `989003064208101` |
| `landed.qca_*` (CASE_ACCOUNT_FOR_SEARCH) | Bare number (stripped prefix) | `989003064208101` |

**Solution**: Use `substring(col("ExposureKey"), 4, 100)` to strip the 3-char prefix from dexposure/exposure when joining to tables with bare numbers. All portfolio prefixes are exactly 3 characters.

**WARNING**: Do NOT use `regexp_extract` in `.ipynb` JSON files. The pattern `r'\\d.*'` gets double-escaped in JSON to `r'\\\\d.*'`, which in Python becomes literal `\\d` (backslash + d) instead of the digit shorthand `\d`. Use `substring(col, 4, 100)` instead.

### Collections (Payments) — Two-Pass Join

`primary.collection` has ExposureKeys in both formats:
- **Prefixed** (~548K rows): direct match to dexposure
- **Bare number** (~770K rows): match via `substring(dexposure.ExposureKey, 4, 100)`

Bare-number collections are **confirmed non-core** (all from Galaxy, Cosmos, Orion, Pearl, etc.). The source system (profits) sends some collections without the portfolio prefix.

### Interaction (Legal Activity) — Cumulative Snapshot

`primary.interaction` is a **cumulative snapshot table** — each BusinessDate contains ALL interactions ever. Always use `MAX(BusinessDate)` from the table, NOT `run_date`, to get full history.

### Assignment — Portfolio-Level, Not Case-Level

On-prem's `AssignmentDate` comes from `DWH.ref.Portfolio.AssignmentDate` — the date the portfolio was transferred to the servicer. Use `primary.portfolio.AssignmentDate` for 100% coverage.

### IsRestructuredFlag — Use drestructuring

Use `derivativeactive.drestructuring` with `IsRestructureActive=1`, grouped by `NewExposureKey`. Do NOT derive from `dexposure.LastRestructuringDate` (populated for nearly all loans).

### gd_lpucb

Source: `landed.profits_loan_details.gd_lpucb`. Join on `account_number = ExposureKey` (both have prefix).

### N3588 (Bankruptcy)

Cannot be replicated in cloud. On-prem's 649 records came from QC process instances (wholesale borrowers loaded via external files). Not available in cloud.

### LPS — From DS Team (p-ds.aaas.lps)

LPS now comes from `derivativeactive.dlegalprocessstatusagreementdebtor` (computed by DS team from `p-ds.aaas.lps`). In DExposure, join via a standalone `lps_by_exposure` DataFrame on ExposureKey. Do NOT pass LPS through exposure_profits/exposure_qca DataFrames (causes lazy evaluation conflicts).

### SubProductDescription — Lazy Evaluation Bug (Fixed)

The `exposure_qca` temp view was registered with ALL QCA exposures for the product lookup, but later overwritten to Core Retail only. Due to Spark's lazy evaluation, `exposure_product_qca` SQL saw only Core Retail data. Fix: register as `exposure_qca_all` for the product lookup.

### HasActiveSettlementFlag — Multiple Fixes

1. **Coalesce case sensitivity** (CUD): `lit('Cancelled')` → `lit('cancelled')` in the fallback
2. **IsActiveFlag/isPrimarySettlementFlag** (lng-lnd-cup): Populated from `FK_CLC_SECOND_STS_PREV` in profits settlements
3. **regexp_extract → substring** (CUD): For profits settlement join
4. **Missing ExposureKey** (pending): 7,260 profits settlements have CASE_ACC_SYSTEM=19 (agreement-level) with NULL ExposureKey. Needs IT investigation.

---

## 11. Expected Verification Results (2026-06-30)

### DebtStatic (on-prem: 1,610,617)

| Metric | Expected Cloud | On-Prem | Diff |
|---|---|---|---|
| total_rows | ~1,608K | 1,610,617 | -0.1% |
| dist_accounts | ~1,043K | 1,043,997 | -0.1% |
| primary_owner | ~1,043K | 1,043,997 | -0.1% |
| guarantor | ~459K | 460,167 | -0.1% |
| co_owner | ~105K | 106,453 | -0.6% |
| n4605_count | ~8,212 | 8,234 | -0.3% |
| n3588_count | 0 | 649 | Accepted (external files) |
| has_assignment_date | 100% | 100% | ✅ |
| has_gd_lpucb | 100% | 100% | ✅ |

### DebtDetail (on-prem: 1,043,997)

| Metric | Expected Cloud | On-Prem | Status |
|---|---|---|---|
| total_rows | ~1,043K | 1,043,997 | ✅ |
| has_settlement_true | ~34-42K | 42,711 | Pending upstream fix |
| is_restructured | ~87K | 344,680 | Accepted (profits-only) |
| has_lps | ~700K | 727,699 | Pending DExposure LPS fix deployment |
| has_legal_activity | ~557K | 595,652 | ✅ (-6.4%) |
| has_settlement_code | ~12-26K | 42,644 | Pending upstream fix |

### DebtFinancial (on-prem: 1,043,997)

| Metric | Expected Cloud | On-Prem | Status |
|---|---|---|---|
| sum_otb | ~€48.59B | €48.66B | ✅ (-0.1%) |
| sum_on_balance | ~€24.50B | €24.29B | ✅ (+0.9%) |
| sum_off_balance | ~€15.34B | €15.34B | ✅ (0.0%) |
| sum_payments | ~€561M | €577M | ✅ (-2.9%) |
| with_payments | ~132K | 131,810 | ✅ (+0.1%) |
| sum_secured | ~€26.4B | €25.0B | Under investigation (+5.9%) |
| with_secured | ~421K | 422,123 | ✅ (-0.2%) |

### DebtAB (on-prem: 496,765)

| Metric | Expected Cloud | On-Prem | Diff |
|---|---|---|---|
| total_rows | ~495K | 496,765 | -0.3% |
| primary_owner | ~381K | 382,914 | -0.3% |
| guarantor | ~90K | 90,553 | -0.4% |
| co_owner | ~23K | 23,298 | -0.2% |

---

## 12. Cross-Platform Party Identity & Settlement Rework — Lessons (DNA26-1716)

Captured from the party-identity rule rollout, the Debt/VatSettlement reworks, and
the 2026-11-04 prod verification. (portfolio_cdi_bal learnings are intentionally
out of scope for this section.)

### 12.1 Party identity rule (AGREED with IT, applies to ALL platform tables)

On-prem `PartyId` / `ParentPartyId` are DWH internal **integers** that do NOT exist
in the cloud model. The earlier migration put the source-system `PartyKey` / `cust_id`
into `PartyId` — this is **WRONG and dangerous**: `PartyKey`/`cust_id` is only unique
*within one source system* and is reused across systems (e.g. `cust_id = 156659` maps
to 3 different real parties across profits/qca/thesis). Loaded into an int `PartyId`
column downstream it silently mis-joins (collisions) or joins to nothing.

**The rule (no schema changes, IT-confirmed string domain):**
- `PartyId` = **CDI** (globally unique, non-colliding, present on every table).
- `ParentPartyId` = **COALESCE(TaxRegistrationNumber, CDI)** (CDI fallback → never null).
- If a table has both a `CDI` and a `PartyId` column, populate both with the same CDI.
- If a table has only `PartyId`, populate it with CDI.
- No NULLs allowed in `PartyId` / `ParentPartyId` / `CDI`.
- Downstream joins are `Customer.PartyId = DebtStatic.PartyId` → now CDI=CDI, same domain.

**Consuming-side dependency (IT):** the on-prem `common.*.PartyId` columns are `int`;
they MUST be widened to string before loading the cloud output, or the load fails/
truncates on alphanumeric CDIs (e.g. `GLA0005899249`). Flag this in every release note.

### 12.2 Decouple tax resolution from the record (CRITICAL)

Do NOT use a tax-first dedup tie-break to pick the *whole* party record — it drags
names / BirthDate / PartyType / cust_id to a lower-priority source for any CDI whose
top-priority source has a blank/null tax (measured: ~34K CDIs affected).

**Pattern:** keep the canonical record dedup on source priority (profits>qca>other)
then latest date. Resolve the tax number with a **separate** CDI→tax lookup
(`party_tax`) used ONLY for `CustomerVat` / `ParentPartyId`. Left-join it in.

### 12.3 Party / tax dedup must be consistent across notebooks

All party_tax lookups (dtransfer_Customer, dtransfer_VatSettlement, and any other
platform table) MUST share the same **scope and ordering** or the same CDI resolves a
different tax/parent in different tables and cross-table reconciliation breaks:
- **Scope: run-date** (`BusinessDate = run_date_lit`). All-history adds only ~13 CDIs
  of extra tax coverage (measured) — not worth the inconsistency.
- **Ordering:** non-blank tax first → source priority (profits>qca>other) →
  `BusinessDate DESC` → `TaxRegistrationNumber` (deterministic final tie-break, so a
  CDI with several same-source/same-date rows resolves reproducibly).
- **Normalize blank tax:** `NULLIF(TRIM(TaxRegistrationNumber), '')` (or
  `when(trim(tax)!='' , trim(tax))`) BEFORE the fallback. `COALESCE('', CDI)` returns
  `''`, not CDI — blank/whitespace tax otherwise collapses many customers into one
  empty parent key (measured: 44,042 blank-tax winners).

### 12.4 Debt — single gated perimeter & run-date party scope

- DebtStatic party match = **LEFT SEMI on CDI** against `party` (drops exposures with
  unresolvable CDI, matching on-prem `INNER JOIN DWH.prt.Party`). `PartyId = exposure
  CDI` via `withColumn`. SEMI (not INNER) cannot duplicate rows if party dedup changes.
- **DebtDetail / DebtFinancial must be restricted to the DebtStatic LoanId set** (LEFT
  SEMI on `ExposureDerivativeID == debt_static.LoanId`). On-prem builds all three from
  one gated perimeter (zero orphans); cloud must too, or a LoanId-parity assert fails
  exactly when the party gate drops a row.
- **Party source = run-date scoped** in Debt/DebtAB (was all-history ~805K vs Customer
  run-date ~686K). A historical-only party row otherwise lets `DebtStatic.PartyId`
  orphan against `Customer.PartyId`. Verified: 0 orphans after alignment.

### 12.5 VatSettlement — loan-level AccountNumber & NOT NULL taxNumber

- On-prem `prep_VatSettlement` / `bi.sp_settlement` set `AccountNumber =
  SGKAsset.AssetKey1` reached via the **Settlement→Loan relationship** — i.e. the LOAN
  key, never the agreement key. Cloud `ExposureKey` is the analogue.
- **AccountNumber = ExposureKey (loan-level only).** Do NOT
  `COALESCE(ExposureKey, AgreementKey)`: the cloud `AgreementKey` is a dash-composite
  (`CSR7009902691-0000000002649116`) that is unmatchable against the on-prem account
  space (11-19 chars, no dashes). Filter `ExposureKey IS NOT NULL`.
- Agreements come later as a **separate `AgreementNumber` column** (per IT perimeter
  query: CASE_ACC_SYSTEM 4 → AccountNumber, 19 → AgreementNumber). Tracked follow-up.
- **taxNumber NOT NULL contract:** on-prem INNER-joins `PRC.ref.partyTaxNumber`, so any
  settlement with no resolvable tax is dropped (never NULL). Reproduce with: INNER join
  to party on CDI + tax-first dedup + blank→NULL + explicit `taxNumber IS NOT NULL`
  filter + a post-build assertion. The INNER join alone is NOT enough — the dedup winner
  can still carry a NULL tax if no row for that CDI has one.
- Post QC-decommission, some settlements lost their CDI linkage and are legitimately
  excluded (no cloud data). The ~35% lower VatSettlement row count vs on-prem is an
  on-prem-only coverage implementation, NOT a cloud data gap — do not treat as a bug.

### 12.6 Settlement participation — source from derivativeactive.dsettlementexposure

- On-prem settlement participation (`AssetRelationships.ParticipationPercentage`) is
  built from **BOTH** QC/Process (`DWH_STG.process.settlement_loan.participation_perc`,
  frozen post-decommission) and QCA (`FS_ARRANGEMENTS_CASES.SETC_RATIO`). On-prem's
  cdi_bal settlement perimeter is effectively 100% QC/Process.
- In cloud, `primaryactive.settlementexposure.ParticipationPercentage` is **NULL for
  profits (QC-migrated) settlements** — the profits curated notebook stubs it `lit(None)`;
  only the QCA curated notebook populates it (`setc_ratio`). Do NOT fabricate a default
  (e.g. COALESCE to 100) — it inflates vs the real on-prem ~21% average.
- **Use `derivativeactive.dsettlementexposure`**, which populates ParticipationPercentage
  for every row (verified 100% coverage, both sources). The scale is unified to **0-100
  percent** for both qca and profits → divide by 100 uniformly.
- Guard the contract: validate per source (fail on NULL, negative, max>100, or the
  pre-unification 0-1 scale) before aggregating, so a mixed-scale/incomplete snapshot
  can never be delivered silently.

### 12.7 Pre-write guards & in-notebook assertions (regulatory tables)

The write is an unconditional `overwrite`; a silently empty/partial result replaces a
good regulatory table. For each table add, BEFORE the write:
- empty-result check (**raise**, do NOT `dbutils.notebook.exit()` — exit terminates as
  SUCCESS and lets orchestration treat a rejected delivery as a good refresh);
- a day-over-day row-count floor (e.g. fail if >25% below the currently delivered table);
- narrow any target-read `except` to table-not-found only (via `getErrorClass() ==
  'TABLE_OR_VIEW_NOT_FOUND'`) so schema/catalog errors propagate instead of being
  treated as a first run;
- key invariants as assertions (non-null PartyId, cross-table LoanId-set parity, etc.).
  Place a check one step upstream of the gate that would make it vacuously pass (e.g.
  assert NULL CDI on the pre-gate frame, not after a SEMI join that already drops NULLs).

### 12.8 No hardcoded prod catalogs

Every notebook must resolve catalogs via `env = aws_utils.get_env_prefix()` and
f-strings (`` f"`{env}-cdh`..." ``, `` f"`{env}-cdh-delivery`..." ``). No literal
`p-cdh` / `d-cdh` in executable code OR in doc/markdown mapping cells. When promoting a
plain `spark.sql("""...""")` to use `{env}`, make it an f-string (`spark.sql(f"""...""")`)
or the placeholder will not interpolate.

### 12.9 Notebook JSON editing gotchas

- `.ipynb` `source` is a JSON array of lines; `str_replace` on the de-escaped view
  often will not match — edit via a small Python script that loads/modifies/dumps JSON.
- Some notebooks carry a **UTF-8 BOM** (e.g. dtransfer_DebtAB) — read with
  `encoding='utf-8-sig'`. Writing back without BOM is fine.
- Keep every non-last `source` element ending in `\n`; the last element has no trailing
  `\n`. A dangling `"\n"` element after a line without a newline is the fragile pattern
  that corrupts on the next hand-edit.
- `regexp_extract` double-escaping in JSON (see §10) — prefer `substring(col, 4, 100)`.

### 12.10 Verification approach & 2026-11-04 prod results

Three layers: (1) internal invariants (our own guards/asserts), (2) on-prem comparison
with every delta classified as expected-known or investigate, (3) review-driven risk
points that only a real run reveals (participation scale landed, party-gate drop size,
reference-date alignment). On-prem is a **reference, not ground truth** — it has known
issues; large unexplained deltas are "flag for discussion," not "cloud is wrong."

**2026-11-04 prod verification — all core correctness PASSED:**
- PartyId = CDI on 100% of customer/representatives/debt rows; 0 null/blank ParentPartyId.
- `DebtStatic.PartyId = Customer.PartyId` join: 0 orphans (IT's reported bug fixed; the
  `cust_id 156659` collision gone — the two parties now separated by distinct CDIs).
- Debt trio (Static/Detail/Financial) share exactly one LoanId set (1,045,884), 0 orphans.
- VatSettlement: 0 null/blank taxNumber, 0 dash accounts (loan-level).
- dsettlementexposure participation 100% populated both sources; 0 negatives, no 100x inflation.
- Financial totals within ~2-3% of on-prem (DebtFinancial OTB €47.7B vs €48.7B etc.).

**Accepted / explained deltas (not bugs):**
- Combo portfolios 18, 66, 71, 74, 78 absent in cloud (no dexposure rows — unused combos).
- Settlement coverage lower in cloud (credited only to parents present in output =
  on-prem MERGE `when matched`, plus QC-decommission migration).
- N4605 ~-16% (QCA-sourced status timing).
