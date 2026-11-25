# Analytic.bi.Loan → Cloud Migration: Analysis & Field Mapping

**Target:** `{env}-cdh-delivery.it.Loan`
**Notebook:** `LoanIT/Loan.ipynb`
**Ticket:** DNA26-1429
**On-prem source:** `[Analytic].[bi].[Loan]` (94 columns, ~3.22M rows at ReferenceDate 2026-09-18)

---

## 1. Overview

On-prem `bi.Loan` is **not** built by a single stored procedure. It is assembled by:

1. **SSIS package `Select_for_Loans`** (`books/Select_for_Loans.dtsx`) — builds `staging.LOANTEST` via a chain of staging tables:
   - `Truncate Tables` → truncates 5 staging tables
   - `LoanfinancialPartitionLoantest` (Execute SQL) — aggregates `DWH.ast.LoanFinancials` composite balances
   - `FirstJoinForLoantest` (data flow) — base loan attributes
   - `SecondJoinForLoantest` (data flow) — servicing / channel / strategy / queue
   - `ThirdJoinForLoantest` (data flow) — statuses / financials / euro conversions
   - `UnionJoinForLoantest` (Execute SQL) — First LEFT JOIN Second
   - `Load loantest` (Execute SQL) — Union INNER JOIN Third + LoanWorkable → `staging.LOANTEST`
2. **Enrichment SPs / loads** populate the remaining columns into `bi.Loan`:
   - `bi.sp_settlement` (`books/sp_settlement.sql`) → settlement columns
   - `bi.sp_Payment` → transaction-level `bi.Payment`, aggregated per loan
   - `DWH.act.LegalActivity`-based loads → legal-activity + extrajudicial columns

**Grain:** one row per **loan × party relationship** (Primary Owner + Guarantor + Co-Owner).

**Source-system split (2026-09-18):** QualcoAB 1,684,558 (52%) · Profits 1,524,788 (47%) · Arotron 11,293 (0.4%).

---

## 2. Cloud architecture decisions

- **Perimeter:** `primaryactive.exposure` (all relationship types). NOT `DExposure`, which is Primary-Owner-only.
- **Derivative-first:** the delivery notebook only reads/selects from `primary` + `derivative`. No new calculations in delivery, with two sanctioned exceptions (Expense sum, InstalmentFrequency formatting — both simple transforms over existing derivative columns).
- **Euro conversion:** `{env}-cdh.derivative.converttoeur(currency, amount, run_date)` (EUR passes through; other currencies converted — an improvement over on-prem, which left non-EUR NULL).
- **Balances:** always from `DExposure` (portfolio-aware OnBalance/OffBalance/GBV already computed there).
- **Target naming:** PascalCase `it.Loan`, mirroring the `it.CollateralsREAll` convention.
- **Keys (proposal a):** DWH numeric IDs replaced with cloud business keys (see mapping).

---

## 3. Full field mapping (94 on-prem columns)

Legend: ✅ direct · 🔧 euro UDF · 🧩 in-notebook aggregation · 🔗 join-derived · 🧮 in-delivery transform · ⏭️ dropped

### Identifiers & party

| bi.Loan | Cloud source | Type |
|---|---|---|
| PortfolioName | `exposure.PortfolioName` | ✅ |
| AccountNumber | `exposure.ExposureKey` | ✅ |
| AgreementNumber | `exposure.AgreementKey` | ✅ |
| PrimaryDebtorCDI | `exposure.CDI` | ✅ |
| TaxNumber | `DParty.TaxRegistrationNumber` (join CDI) | 🔗 |
| GroupCode / GroupName | `DParty.GroupCode / GroupName` (join CDI) | 🔗 |
| UnifiedCustomerId | NULL — no cloud equivalent (TaxNumber serves grouping) | ⏭️ |
| LoanId | `ExposureKey` (key substitution, proposal a) | ✅ |
| PartyId | `CDI` (key substitution) | ✅ |
| AgreementId | `AgreementKey` (key substitution) | ✅ |
| SettlementId | `DSettlementExposure.SettlementKey` | 🔗 |
| PortfolioId | `portfolio.PortfolioId` (join PortfolioName) | 🔗 |
| DebtNumber | `exposure.DebtNumber` | ✅ |

### Product & static

| bi.Loan | Cloud source | Type |
|---|---|---|
| ProductKey | `DExposure.ProductKey` | ✅ |
| ProductDesc | `primary.Product.ProductDescription` (join ProductKey) | 🔗 |
| SubProductDesc | `DExposure.SubProductDescription` | ✅ |
| AssetSegment | `DExposure.AssetSegment` | ✅ |
| Currency | `DExposure.Currency` | ✅ |
| OriginBank | `DExposure.OriginBank` | ✅ |
| OpenDate | `DExposure.Opendate` | ✅ |
| MaturityDate | `DExposure.Maturitydate` | ✅ |
| OriginationAmount | `DExposure.OriginationAmount` | ✅ |
| InitialAssignmentAmount | `DExposure.InitialAssignmentAmount` | ✅ |
| Tenor | `DExposure.ExposureTenor` | ✅ |
| SigningDate | `DExposure.ContractSigningDate` | ✅ (QCA + Profits x-ref) |
| AccountMechanism | `DExposure.Mechanism` | ✅ |
| InstalmentFrequency | `DExposure.InstalmentFrequency` (int) → `'{n}M'` | 🧮 |
| SPV | `portfolio.SPV` (join PortfolioName) | 🔗 |

### Balances (all from DExposure — derivative-first)

| bi.Loan | Cloud source | Type |
|---|---|---|
| OutstandingTotalBalance | `DExposure.OutstandingTotalBalance` | ✅ |
| OnBalance | `DExposure.OnBalance` | ✅ |
| OffBalance | `DExposure.OffBalance` | ✅ |
| GBV | `DExposure.GBV` | ✅ |
| BoGBalance | `DExposure.BogBalance` | ✅ |
| Principal | `DExposure.TotalPrincipal` | ✅ |
| Interest | `DExposure.TotalInterest` | ✅ |
| Accruals | `DExposure.TotalAccruals` | ✅ |
| Expense | `DExposure.TotalExpense + DueCommission` | 🧮 |
| OutstandingTotalBalanceEuro | converttoeur(OTB) | 🔧 |
| OnBalanceEuro | converttoeur(OnBalance) | 🔧 |
| OffBalanceEuro | converttoeur(OffBalance) | 🔧 |
| AccrualsEuro | converttoeur(TotalAccruals) | 🔧 |
| GBVEuro | converttoeur(GBV) | 🔧 |

### Status / bucket / PDD

| bi.Loan | Cloud source | Type |
|---|---|---|
| LoanStatus | `DExposure.ExposureStatus` | ✅ |
| LoanStatusValue | `DExposure.ExposureStatusCode` | ✅ |
| ObjectiveStatus | `DExposure.ObjectiveStatus` | ✅ (QCA + Profits x-ref) |
| ObjectiveStatusDate | `DExposure.ObjectiveStatusdate` | ✅ |
| IsActiveFlag | `DExposure.IsActiveFlag` | ✅ |
| ClosingDate | `DExposure.ClosedDate` | ✅ |
| DenouncedDate | `DExposure.DenouncedDate` | ✅ |
| DefaultDate | `DExposure.Defaultdate` | ✅ |
| SourceBucket | `DExposure.BucketCode` | ✅ |
| SourcePastDueDays | `DExposure.DaysPastDue` | ✅ |
| FinalBucket | settlement Bucket if settled else DExposure.BucketCode | 🔗 |
| PastDueDays | settlement DaysPastDue if settled else DExposure.DaysPastDue | 🔗 |
| BalanceArrear | DExposure.DueAmount when status Overdue; 0 when Normal; else NULL | 🧮 |
| BalanceDate | run_date | ✅ |

### Rates

| bi.Loan | Cloud source | Type |
|---|---|---|
| BaseRate | `DExposure.BaseRate` | ✅ |
| SpreadRate | `DExposure.SpreadRate` | ✅ |
| PenaltyRate | `DExposure.PenaltyRate` | ✅ |
| N128Rate | `DExposure.N128Rate` | ✅ |
| InterestRateType | `DExposure.InterestRateType` | ✅ |

### Settlement (from DSettlementExposure, primary + active)

| bi.Loan | Cloud source | Type |
|---|---|---|
| HasSettlement | `DSettlementExposure.IsActiveFlag` (primary settlement exists) | 🔗 |
| SettlementCode | `DSettlementExposure.SettlementKey` | 🔗 |
| SettlementAmount | `DSettlementExposure.ProposedAmount` | 🔗 |
| SettlementRemainingAmount | `DSettlementExposure.RemainAmount` | 🔗 |
| SettlementBucket | `DSettlementExposure.Bucket` | 🔗 |
| SettlementPastDueDays | `DSettlementExposure.DaysPastDue` | 🔗 |

### Payments (in-notebook aggregate of primary.payment)

| bi.Loan | Cloud source | Type |
|---|---|---|
| TotalPaymentAmountEuro | SUM(converttoeur(PaymentAmount)) per ExposureKey | 🧩 |
| Last3MPaymentAmountEuro | SUM over last 3 months from run_date | 🧩 |
| LastPaymentActualDate | MAX(PaymentActualDate) per ExposureKey | 🧩 |

### Servicing / channel / strategy

| bi.Loan | Cloud source | Type |
|---|---|---|
| Channel | `assignment` External-LF rule (ExternalCompanyFullName) + double-assignment guard | 🧮 |
| LegalChannel | `assignment` External-LF variant + double-assignment guard | 🧮 |
| AssignmentProcess | `assignment.AssignmentProcess` (nulled if double-assignment) | 🔗 |
| CustomerSegment | `DParty.Segment` (join CDI) | 🔗 |
| Tranche | CASE WHEN CustomerSegment LIKE '%-B%' THEN 'Tranche B' ELSE 'Tranche A' | 🧮 |
| StrategyStep | `casecode.StrategyStep` (AccountNumber bridge) | 🔗 |
| Strategy | `casecode.ParentStrategyStep` | 🔗 |
| StrategyArea | `casecode.StrategyType` | 🔗 |
| Queue | `casecode.Queue` (nulled if double-assignment) | 🔗 |
| QualcoCurrentlyOpenFlag | 'Yes'/'No' if open QCA case exists | 🧮 |

### Legal / LPS

| bi.Loan | Cloud source | Type |
|---|---|---|
| AccountLegalProcessStatus | `dlegalprocessstatusagreementdebtor.OperationalLegalStatusLevel3` (join AgreementKey+CDI) | 🔗 |
| PrimaryDebtorLegalProcessStatus | `dlegalprocessstatusdebtor.OperationalLegalStatusLevel3` (join CDI) | 🔗 |
| LegalProcessStatusName | same as AccountLegalProcessStatus | 🔗 |
| LegalActivityFlag | 'Yes'/'No' if legal interaction exists | 🧮 |
| LegalActivityDate | MAX(InteractionDate) per exposure (IsLegalFlag=true) | 🔗 |
| LegalActivityName | InteractionType of latest legal interaction | 🔗 |
| ExtrajudicialLetterAccountFlag | 'Yes'/'No' if extrajudicial-denunciation interaction exists | 🧮 |
| ExtrajudicialLetterAccountDate | MAX(InteractionDate) over ΕΞΩΔΙΚ ΚΑΤΑΓΓΕΛ types | 🔗 |

### Audit

| bi.Loan | Cloud source | Type |
|---|---|---|
| ReferenceDate | run_date | ✅ |
| Insert Date Time | current_timestamp (Europe/Athens) | ✅ |

### Dropped (empty in on-prem prod)

| bi.Loan | Reason |
|---|---|
| WorkableFlag / NonWorkableReason | 0 populated rows on-prem (LoanWorkable join yields nothing) |
| ExtrajudicialLetterPrimaryFlag / ExtrajudicialLetterPrimaryDate | 'No'/NULL for all 3.22M rows |
| GLXLegalProcessStatusName / GLXLPSDate | staging-only, never reach `bi.Loan` |

---

## 4. Verified findings (from on-prem data)

- **Expense** = Expense + DelinquentExpense + DelinquentComission (verified on samples). DueCommission is material: €96.7M across 13,249 loans, 99% in EUCLID Small-Business (266 loans).
- **Payments** source = `DWH.trn.Payments` (real payments), NOT `trn.Collections`. Verified 1:1 on PaymentId. Cloud = `primary.payment`.
- **Legal activity** = `DWH.act.LegalActivity`, MAX(ActivityDate) per loan, name from `ActivityType.ActivityName`, IsLegalFlag=true. Cloud = `primary.interaction`.
- **Extrajudicial** = `DWH.act.LegalActivity` filtered to the ΕΞΩΔΙΚ ΚΑΤΑΓΓΕΛ activity-type family (codes 574,575,577,578,579,672,994,1293,1297,2050,+legacy 989), MAX date per loan.
- **Settlement** = on-prem `bi.sp_settlement` (primary settlement per loan, DNA-6534) → cloud `DSettlementExposure` (isPrimarySettlementFlag). FinalBucket/PastDueDays confirmed as settlement-override of source.
- **ObjectiveStatus** — no gap: DExposure Profits pipeline carries the QCA ObjectiveStatus via the 1:1 QCA cross-reference; QCA pipeline uses its own. 118 shared distinct values between Profits/QCA (same classification domain).
- **Balances profits-only** — bi.Loan balance columns are populated for Profits only (QCA/Arotron null) both on-prem and in DExposure Total* fields → parity.

---

## 5. CONCERNS — read before deploying

> These are open risks. The notebook is **syntactically valid but has NOT been executed on a cluster or verified against on-prem values.**

### 5.1 Unverified at runtime (highest priority)
- **The notebook has never run.** Column-name, join-key, and type-cast issues will only surface on a real cluster run. Treat the first `dev`/`qa` run as a debugging pass, not a validation.
- **Column names are assumed** from the curated/DExposure notebook source, not from the live delivered tables. Fields I am least certain exist with these exact names:
  - `primary.casecode`: `ParentStrategyStep`, `StrategyType`, `CurrentlyOpenFlag`, `AccountNumber`, `StrategyStep`, `Queue`
  - `primary.assignment`: `ExternalCompanyFullName`, `Channel`, `AssignmentProcess`, `AssignmentKey`, `Segment`
  - `primary.portfolio`: `SPV` (may be `spv_qualco_AB` or similar)
  - `DParty`: `Segment`, `GroupCode`, `GroupName`
  - `primary.interaction`: `InteractionType`, `InteractionDate`, `IsLegalFlag`
  - `DSettlementExposure`: `Bucket`, `RemainAmount`, `DaysPastDue`, `isPrimarySettlementFlag`
  If any differ, the notebook errors on first run. Each should be confirmed against the actual table schema.

### 5.2 ExposureKey format mismatch (likely to bite)
- **CaseCode join** now uses the portfolio `Prefix` column to strip the ExposureKey dynamically: `substring(ExposureKey, length(pf_Prefix) + 1, 100)`. This handles variable-length prefixes correctly and is sourced from `primary.portfolio.Prefix`. The same approach should be applied to any future join where prefixed vs bare ExposureKeys need alignment.
- **Payment / assignment joins** on raw `ExposureKey`. If `primary.payment` (QCA) stores the bare number while `exposure` stores the prefixed key, these joins silently return no payments for QCA loans. The migration steering documents this exact class of bug.

### 5.3 Grain / fan-out risks
- **CaseCode** is deduplicated by `AccountNumber` only. On-prem also partitioned by PortfolioId and picked one row via ROW_NUMBER ordered by CaseCode. If one AccountNumber maps to multiple casecodes, the arbitrary dedup may pick a different row than on-prem.
- **Assignment** is deduplicated by `ExposureKey` (dropDuplicates), which arbitrarily picks one assignment row. The double-assignment guard nulls the fields when count>1, but for count=1 the row choice should be deterministic.
- **`.distinct()` removed for the verification run.** Any row-count inflation from a non-1:1 lookup will now be visible in the output row count (rather than silently collapsed). Compare the row count against on-prem `bi.Loan`; if inflated, identify the fan-out lookup before re-adding distinct.

### 5.4 Value-logic uncertainties
- **Channel / LegalChannel External-LF rule** is reproduced from the on-prem SecondJoin SQL, but the on-prem `e.LoanId` double-assignment subquery had specific portfolio/vendor filters (`vendor_code_qualco is not null AND IsSingleTicketFlag=0`) that are NOT reproduced — I only reproduce the count>1 guard. This may differ from on-prem for edge cases.
- **QualcoCurrentlyOpenFlag** on-prem also depended on `assetreferencecodes_AssetIdQualco IS NOT NULL` (returns NULL, not 'No', when the loan has no QCA cross-reference). My version returns 'Yes'/'No' only. Minor semantic difference.
- **Tranche** depends on `CustomerSegment LIKE '%-B%'`. CustomerSegment now comes from `DParty.Segment` (QCA-derived), whereas on-prem it came from `prt.CustomerCategory`. If the segment string format differs, Tranche classification will differ.
- **BalanceArrear** on-prem used `LoanStatus.Value` (2 → Delinquent, 1/0 → 0). I mapped this to `ExposureStatusCode` (2 → DueAmount, 0/1 → 0). The status-code domains must actually align (on-prem Value vs cloud ExposureStatusCode).

### 5.5 Coverage gaps (accepted, but document for consumer)
- **SigningDate / ObjectiveStatus**: QCA-sourced (+ Profits via cross-reference). Arotron (0.4%) will be NULL. On-prem had them for all.
- **CustomerSegment / GroupCode / GroupName / StrategyStep / Queue**: QCA/profits-partial coverage matching on-prem population percentages, but exact per-loan parity is unverified.
- **Legal activity date type**: on-prem `LegalActivityDate` is `datetime2`; I cast to `timestamp`. `interaction.InteractionDate` type should be confirmed.

### 5.6 Not addressed
- **No column-comments / schema-definition step** (unlike DExposure). The table schema is inferred from the write. If the IT consumer needs a fixed schema/DDL, that must be added.
- **No verification queries** written yet. Level 1–5 verification (row count, distribution, completeness, financial totals, sample records) against on-prem `bi.Loan` still to be done.
- **Arotron balances**: DExposure Total* are profits-only; Arotron loans (0.4%) will have null Principal/Interest/Accruals/Expense. Matches on-prem (Arotron had these null too) but worth confirming.

---

## 6. Recommended next steps

1. Confirm the ~25 assumed column names against live table schemas (Section 5.1).
2. Resolve the ExposureKey prefix/bare-number handling per source system for casecode/payment/assignment joins (Section 5.2).
3. Test-run in `dev` (or `qa`) as a Databricks workflow; fix runtime errors.
4. Write and run verification queries vs on-prem `bi.Loan` (row count, source-system split, financial totals, sample records).
5. Review the value-logic uncertainties (Section 5.4) with the business/on-prem owner.
