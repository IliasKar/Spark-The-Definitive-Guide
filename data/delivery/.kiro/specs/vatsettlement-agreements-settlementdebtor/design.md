# Design — VatSettlement Agreements + SettlementDebtor

## Overview

Three design areas across two layers:

**Upstream (cup + cud repos) — Design 0 (prerequisite for Design 2):**
0. Curate the settlement case/phase fields + debtor relations + applicant, and add an
   `isPromiseActive` flag, so delivery can read everything from curated/derivative.

**Delivery (`gr.cepal.dna.de.delivery`, `platforms/`):**
1. Extend `dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` — add agreement
   rows (new `AgreementNumber` column), include all source systems, always exclude
   banking portfolios, perimeter by `isPromiseActive` (interim).
2. New notebook `dtransfer_SettlementDebtor/dtransfer_SettlementDebtor.ipynb` →
   `{env}-cdh-delivery.it.dtransfer_settlementdebtor` at settlement × debtor grain,
   reading curated/derivative (NOT landed), with a `HasActiveSettlementFlag`.

**ARCHITECTURE RULE:** delivery notebooks never read `landed.*`. All SettlementDebtor
fields come from curated/derivative after Design 0.

Conventions (per steering §2, §12): `{env}` via `aws_utils.get_env_prefix()`;
`run_date_lit = lit(run_date).cast("date")`; run-date-scoped, deterministic party
dedup; pre-write guards that **raise** (never `dbutils.notebook.exit` for a rejected
result); no hardcoded catalogs.

---

## Design 0 — Upstream curation (cup + cud)

### 0.1 Case/phase fields on `primaryactive.settlementexposure` (cup)
In `src/jobs/curated_notebooks/settlement_exposure_profits.ipynb`:
- `clc_case` is already loaded; `case_id`, `CREATE_TYPE`, `CASE_CUSTOMER_ID` are already
  in `clc_case_selected_fields`. ADD `CASE_DESCRIPTION`, `CASE_ARRIVAL_DT`.
- NEW extract `profits_generic_detail` filtered `PARAMETER_TYPE='CLCRN'`, joined on
  `SERIAL_NUM = clc_case.CREATE_TYPE` → `DESCRIPTION`.
- NEW extract `profits_prfsettlements`, deduped to latest per `CASE_SETTL_PROMISE_ID` by
  `MAX(SCHEDULED_DATE)` (ROW_NUMBER/rank), joined on PAY_PROMISE_ID → `CASE_OPER_STATUS`,
  `CLC_JUST_REASON`.
- Final select adds the 6 columns with the FIXED name mapping:

  | New column | Source | Type |
  |---|---|---|
  | `PromiseCaseID` | `clc_case.CASE_ID` (trim) | string |
  | `PromiseCasePurpose` | `generic_detail.DESCRIPTION` (CLCRN) | string |
  | `PromiseCaseDescription` | `clc_case.CASE_DESCRIPTION` (trim) | string |
  | `PromiseCaseArrivalDate` | `clc_case.CASE_ARRIVAL_DT` | date |
  | `SettlementPhase` | `prfsettlements.CASE_OPER_STATUS` (trim) | string |
  | `SettlementJustification` | `prfsettlements.CLC_JUST_REASON` (trim) | string |

- NULL for non-profits rows. Redis `curated.json` schema for `settlementexposure` updated
  with the 6 columns; delta table altered accordingly.
- Type-safety: `try_cast`/trim on CREATE_TYPE↔SERIAL_NUM and promise-id joins.

### 0.2 `isPromiseActive` on the derivative (cud)
In `DSettlementExposure` / `SettlementEvaluation.py`:
- Add `isPromiseActive` (int) to `dsettlementexposure` = 1 when the settlement maps to a
  `clc_pay_promises` row with `TABLE_STATUS='1'` and `clc_case.CASE_ACC_SYSTEM IN (4,19)`.
  Independent of `IsActiveFlag`; leave `IsActiveFlag` and its dedup/status logic unchanged.
- For qca settlements (no profits clc), `isPromiseActive` follows the existing active
  convention or NULL (decide in implementation; platforms reporting is profits-driven for
  these fields).

### 0.3 Debtor relations + applicant — NO upstream curation (decision DNA26-1804)
- NO curated `settlementdebtorrelation` table. The debtor fan-out is read directly from
  `landed.profits_vdebtorsrelations` in the delivery notebook (Design 2) — scoped exception.
- Applicant = `dsettlementexposure.CDI` (already the applicant via
  `clc_case.CASE_CUSTOMER_ID` → `customer_rf_codes.CUSTOMER_ID_NO`; 99.0% = IT). No new
  applicant curation. 242 NULL-CDI settlements = known limitation.
- Net: R0 = 0.A (6 case/phase fields) + 0.B (`isPromiseActive`) only.

### 0.4 Deployment order
Design 0 (cup curated → redis → cud derivative) ships and runs BEFORE Design 2. Design 1
(VatSettlement) depends on 0.2 (`isPromiseActive`); Design 2 depends on 0.A + 0.B.

---

## Design 1 — dtransfer_VatSettlement (agreements)

### 1.1 Source & perimeter
Source: `{env}-cdh.primary.settlementexposure` (or `derivative.dsettlementexposure` if
`isPromiseActive` lives only there), filtered:
- `BusinessDate = run_date_lit`
- `isPromiseActive = 1` (INTERIM, per R0.B / R1-5) — NOT `IsActiveFlag`. If IT confirms
  the deduped definition, switch to `IsActiveFlag = 1`.
- `ExposureKey IS NOT NULL OR AgreementKey IS NOT NULL` (loan OR agreement rows)
- ALL source systems (remove the implicit profits-only effect).

### 1.2 Loan vs agreement branch (Option 1)
Per row:
- `AccountNumber`   = `ExposureKey`  (NULL when agreement-keyed)
- `AgreementNumber` = `AgreementKey` (NULL when loan-keyed)

Implemented as:
```python
col("ExposureKey").alias("AccountNumber"),
col("AgreementKey").alias("AgreementNumber"),
```
No COALESCE, no synthesized key. QC→QCA 1→N settlements (ExposureKey present, no
AgreementKey) naturally become loan rows — `AccountNumber` set, `AgreementNumber` NULL.

### 1.3 Banking exclusion (always)
Join `portfolio` on `PortfolioName` (already present for PortfolioId lookup); add:
```python
.filter(~col("PortfolioId").isin(31, 32, 60, 92, 87))
```
Define the banking list as a named constant `BANKING_PORTFOLIO_IDS = [31,32,60,92,87]`
at the top of the notebook with a comment (Core Retail, Core Wholesale, Artemis/Attica,
Jewel, Galene).

### 1.4 taxNumber NOT NULL contract (unchanged, applies to agreement rows too)
Keep the existing party/tax lookup, aligned with steering §12.3:
- `party_tax` from `{env}-cdh.primary.party`, **run-date scoped**
  (`BusinessDate = run_date_lit`), deduped per CDI by: non-blank tax first
  (`when(trim(TaxRegistrationNumber)!='' , 0).otherwise(1)`), source priority
  (profits>qca>other), `BusinessDate DESC`, then `TaxRegistrationNumber` (deterministic).
- Selected tax normalized blank→NULL.
- INNER join settlement→party_tax on CDI.
- `.filter(col("taxNumber").isNotNull())`.
- Post-build assertion: 0 NULL taxNumber rows.

Note: agreement rows carry a CDI on `settlementexposure` (verified for profits agreement
rows), so the INNER-join-on-CDI contract works for them identically to loan rows.

### 1.5 Output columns
`AccountNumber, AgreementNumber, SettlementType, SettlementCode, PortfolioId,
taxNumber, datadate`. (AgreementNumber is the ONE new column; consuming-side DDL must
add it — coordination item.)

### 1.6 Pre-write guards
- empty-result → `raise ValueError`
- day-over-day row-count floor (e.g. >25% drop vs current delivered → raise)
- write `overwrite` to `{env}-cdh-delivery.it.dtransfer_vatsettlement`.

### 1.7 Expected row-count impact (quantify during implementation)
The change adds two populations and removes one, relative to the current
loan-level-profits output:
- **+ ~8,199 agreement rows** — profits CASE_ACC_SYSTEM=19 (ExposureKey NULL,
  AgreementKey present), emitted with `AgreementNumber` set.
- **+ ~1,559 QC→QCA migrated settlements** — currently-active 1→N settlements now
  included because the source-system restriction is removed; under Option 1 they
  appear as loan rows (`AccountNumber = ExposureKey`, `AgreementNumber` NULL), not as
  agreements. (These expand to multiple rows — ~4,744 exposure rows across the 1,559
  settlements — since each covered exposure is its own row.)
- **− banking rows** — now always excluded (31/32/60/92/87).

These counts are pre-contract (before the taxNumber INNER join + non-null filter, which
may drop a few), so the final delivered count must be **measured** during implementation
and recorded for the floor guard and verification. Do NOT assume parity with the old
loan-level-profits count.

---

## Design 2 — dtransfer_SettlementDebtor (new table)

### 2.1 Target
`{env}-cdh-delivery.it.dtransfer_settlementdebtor`, overwrite, grain = one row per
(settlement, debtor). PascalCase column names (delivery convention). Depends on Design 0.

### 2.2 Sources — curated/derivative, PLUS one scoped landed read
- `{env}-cdh.derivative.dsettlementexposure` (or `primary.settlementexposure`): SettlementKey,
  SourceSystem (filter `profits`), ExposureKey, AgreementKey, **CDI (= applicant)**,
  SettlementStatus, CreatedDate, PortfolioKey, the R0.A fields (`PromiseCaseID`,
  `PromiseCasePurpose`, `PromiseCaseDescription`, `PromiseCaseArrivalDate`, `SettlementPhase`,
  `SettlementJustification`) + R0.B `isPromiseActive`.
- `{env}-cdh.landed.profits_vdebtorsrelations` — DIRECT read (scoped exception): DebtorCode
  (CDI), DebtorRelation (RELATION_CODE), DebtorVatNumber (AFM_NO).
- `{env}-cdh.primary.portfolio` for banking-exclusion lookup (by PortfolioKey).

### 2.3 Build
1. The IT `LastSettlement` MAX(SCHEDULED_DATE) dedup is ALREADY applied in R0.A — the
   curated `SettlementPhase`/`SettlementJustification` carry the latest prfsettlements row.
2. Perimeter: filter `isPromiseActive = 1` AND `SourceSystem='profits'` (encodes IT's
   `TABLE_STATUS='1'` + `CASE_ACC_SYSTEM IN (4,19)`).
3. CasePurpose/CaseCode/CaseDescription/CaseStartDate come directly from the R0.A fields.
4. CustomerCode (applicant) = `dsettlementexposure.CDI` — no `profits_other_id` read.
5. Debtor fan-out: join `landed.profits_vdebtorsrelations` per case level — loan rows on
   `ACCOUNT_NUMBER` = ExposureKey, agreement rows on `CONTRACT_NUMBER` = AgreementKey (keys
   match DIRECTLY, no prefix-strip). MANDATORY `SELECT DISTINCT` on the IT column set
   (raw ~69K → ~36.5K distinct). Pin the landed read to the same BusinessDate as the
   settlement source.
6. HasActiveSettlementFlag (2.6): 1 where DebtorCode = `dsettlementexposure.CDI` (applicant)
   and an active-primary settlement exists for that key+CDI.

### 2.4 Columns
ReferenceDate (run_date/BusinessDate), CaseCode (`PromiseCaseID`), CasePurpose
(`PromiseCasePurpose`), CaseLevel (19→'Σε επίπεδο σύμβασης', 4→'Σε επίπεδο Δανείου';
derive from ExposureKey/AgreementKey presence), CaseDescription (`PromiseCaseDescription`),
CaseStartDate (`PromiseCaseArrivalDate`), SettlementCode (SettlementKey),
SettlementCreateDate (CreatedDate), SettlementStatus, SettlementPhase,
SettlementJustification, AccountNumber (ExposureKey, loan), AgreementNumber (AgreementKey,
agreement), CustomerCode, DebtorCode, DebtorRelation, DebtorVatNumber,
HasActiveSettlementFlag (see 2.6), datadate/BusinessDate.

### 2.5 Banking exclusion
Use the `PortfolioKey`/PortfolioId already present on the curated/derivative rows;
`PortfolioId NOT IN (31,32,60,92,87)`. (No landed CASE_UNIT→portfolio resolution needed —
the curated rows already carry the portfolio.)

### 2.6 HasActiveSettlementFlag — translated from IT's QC query to live cloud data

IT's rule (QC/`process`-sourced EXISTS): flag=1 iff there is an active, primary
settlement on the debtor's loan account for that debtor's CDI. QC is decommissioned →
re-express against `{env}-cdh.derivativeactive.dsettlementexposure`.

**Column mapping (QC → cloud):**

| IT QC query | Cloud (dsettlementexposure) | Notes |
|---|---|---|
| `process.settlement_loan` + `process.settlement` | `derivativeactive.dsettlementexposure` | one table, live |
| `s.is_active = 1` | `IsActiveFlag = 1` | |
| `s.status NOT IN (5 cancelled/rejected)` | — (subsumed by `IsActiveFlag = 1`) | cloud active statuses are only IMPLEMENTED/Running/PENDING; the QC cancelled strings never appear on active rows (verified). Optional defensive `SettlementStatus NOT LIKE 'Cancelled%'`. Confirm with IT. |
| `sl.isPrimarySettlement = 1` | `isPrimarySettlementFlag = true` | populated 99.9% |
| `sl.account_number = <loan AccountNumber>` | loan rows: `ExposureKey` = AccountNumber; agreement rows: `AgreementKey` = AgreementNumber | dsettlementexposure carries BOTH keys on every row (verified), so agreement rows are matchable |
| `sl.cdi = TRIM(oi.ID_NO)` | `CDI` = DebtorCode | the row's debtor CDI |

**Implementation:** build a distinct set of active+primary (account/agreement, CDI)
tuples from `dsettlementexposure` and LEFT-SEMI / left-join it onto the SettlementDebtor
rows, keying loan rows by (ExposureKey, CDI) and agreement rows by (AgreementKey, CDI);
flag = 1 where matched, else 0. Computed for BOTH loan and agreement rows (R3-5).

**Verify in implementation:** the status-collapse assumption (no active cloud settlement
carries a cancelled status) on the run date, and `isPrimarySettlementFlag` coverage.
IT to confirm the status mapping.

### 2.7 Type-safety & guards
- `try_cast`/`trim` on any residual curated key joins (per steering curated gotchas).
  The landed type/casing hazards (CASE_CUSTOMER_ID int vs CUST_ID, FKGD_HAS_TYPE string
  '13', unit codes) are handled upstream in Design 0, not here.
- Pre-write guards: empty-result raise, row-count floor.
- Dedup: `.distinct()` and/or explicit (settlement, debtor) uniqueness assertion to
  guard the grain (the two debtor-relation branches are resolved in R0.C; still assert
  grain here).

---

## Verification (both tables, per steering §5 + §12.10)

Layer 1 (internal): row counts, grain uniqueness, taxNumber non-null (VatSettlement),
AccountNumber/AgreementNumber mutual exclusivity, banking portfolios absent, (settlement,
debtor) uniqueness (SettlementDebtor).

Layer 2 (on-prem, via MSSQL MCP): compare VatSettlement agreement count to
CASE_ACC_SYSTEM=19 active (~8,199); AgreementNumber ⊆ on-prem ClcCase.CASE_ACC_NUMBER;
SettlementDebtor row/debtor counts vs the IT query run on-prem. On-prem is a reference,
not ground truth — classify deltas.

Layer 3 (risk points): banking rows truly excluded; QC→QCA loan rows present; agreement
rows have non-null taxNumber; the two VDebtorsRelations branches don't double-count.

No Databricks execution is done by the spec author; a QA run for a pinned reference date
is required before release (record counts, reconciliations).

---

## Deployment / coordination
- **Design 0 (cup + cud + redis) ships first** and must run in prod before Design 2.
  Order: cup `settlement_exposure_profits` + new debtor/applicant curated notebooks →
  redis `curated.json` schema → cud `DSettlementExposure`/`SettlementEvaluation`
  (`isPromiseActive`) → delivery notebooks.
- Add tasks to the platforms Databricks job for the new SettlementDebtor notebook.
- Consuming-side DDL (IT): new `AgreementNumber` column on VatSettlement target; new
  `SettlementDebtor` table.
- PR flow per repo: feature branch → development → master (per steering §6).

## Open items carried to tasks
- **Perimeter definition** (`isPromiseActive` vs `IsActiveFlag`) — **IT** (interim uses
  `isPromiseActive`).
- R0.C: final name/home of the curated debtor-relations table + applicant field.
- R3 / 2.6: `HasActiveSettlementFlag` boolean definition (**IT**).
- Design 0 is the gating dependency; delivery Design 2 cannot ship until it lands.
