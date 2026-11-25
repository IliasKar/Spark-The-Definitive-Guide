# Spec Input — VatSettlement Agreements + new SettlementDebtor table

Seed for spec-driven development. Captures the requirement, the discussion
outcomes, and the data investigation done this session. Target repo:
`gr.cepal.dna.de.delivery`, `platforms/` directory. No hardcoded catalogs — use
`{env}` via `aws_utils.get_env_prefix()`.

Status: **awaiting IT answer on the full agreement perimeter** (see §1.3). For now,
proceed with the agreed interim scope (profits CASE_ACC_SYSTEM=19 only).

---

## Requirement 1 — Add agreement-level settlements to VatSettlement

### 1.1 Current state
`platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` →
`{env}-cdh-delivery.it.dtransfer_vatsettlement`. Currently **loan-level only**:
`AccountNumber = ExposureKey`, filter `ExposureKey IS NOT NULL`. Columns:
`AccountNumber, SettlementType, SettlementCode, PortfolioId, taxNumber, datadate`.

### 1.2 Requirement
Include **agreement-level** settlements via a **separate `AgreementNumber` column**
(do NOT fold agreement keys into `AccountNumber`), aligned with IT's perimeter query
split by `CASE_ACC_SYSTEM`:
- CASE_ACC_SYSTEM = 4  → `AccountNumber`   (loan key)
- CASE_ACC_SYSTEM = 19 → `AgreementNumber` (contract/agreement key)

Target shape: existing columns + new `AgreementNumber` (NULL for loan rows,
populated for agreement rows; `AccountNumber` NULL for agreement rows).

### 1.3 Interim scope decision (agreed) + open IT question
**INTERIM (build now):** include ONLY the **profits CASE_ACC_SYSTEM=19** agreement
settlements — measured **8,199 active settlements / 8,200 active rows** (1 row per
settlement). These are source = profits, `ExposureKey IS NULL AND AgreementKey IS
NOT NULL`.

**OPEN (awaiting IT):** whether to also include the **QC→QCA migrated 1→N settlements**
(see §1.5). Do not build this until IT confirms.

### 1.4 Why the earlier COALESCE approach was reverted
`AccountNumber = COALESCE(ExposureKey, AgreementKey)` was reverted: the cloud
`AgreementKey` is a dash-composite (`CSR7009902691-0000000002649116`, ~30 chars,
sometimes Greek text) that does not exist in the on-prem AccountNumber space (11-19
chars, no dashes) → unmatchable. On-prem `prep_VatSettlement` / `bi.sp_settlement`
set `AccountNumber = SGKAsset.AssetKey1` via the Settlement→Loan relationship (always
a LOAN key, never an agreement key).

**RESOLVED — AgreementNumber format/source:** `AgreementNumber` =
`settlementexposure.AgreementKey` taken directly, no transformation. Traced to source:
`landed.profits_clc_case.CASE_ACC_NUMBER` (CASE_ACC_SYSTEM=19) is ALREADY the 30-char
dash-composite (e.g. `CSR7006885191-0000000002879103`); the cup-repo notebook
`settlement_exposure_profits.ipynb` sets `AgreementKey = CASE_ACC_NUMBER when
CASE_ACC_SYSTEM=19` and passes it through unchanged (verified: all 6,568 active cloud
AgreementKeys exist in clc_case CASE_ACC_NUMBER, 0 missing). This is the SAME field IT's
query emits as `[Agreement Number] = cc.CASE_ACC_NUMBER`, so cloud and on-prem agree 1:1.
The dash-composite was only "unmatchable" when wrongly placed in `AccountNumber` (which
on-prem keys from loan `AssetKey1`); as its own `AgreementNumber` column it matches
on-prem exactly. No IT format confirmation needed for the profits population.

### 1.5 Perimeter investigation — two distinct "agreement" populations (DATA-VERIFIED)
Verified on prod `{env}=p`, reference date ~2026-11-04:

| Population | Active | Representation in settlementexposure | Source |
|---|---|---|---|
| **A. profits CASE_ACC_SYSTEM=19** (agreement-keyed) | 8,199 settlements / 8,200 rows | `ExposureKey IS NULL` + `AgreementKey` (dash-composite) | profits ONLY (0 qca/process) |
| **B. QC→QCA migrated 1→N** (one settlement, many exposures) | 1,559 settlements / 4,744 rows (1,489 multi-exposure) | `ExposureKey` present, multiple rows per SettlementKey, **NO AgreementKey** | qca ONLY (0 profits/process) |

Key facts:
- Population A is **profits-only** and **1 row per settlement**.
- Population B has **no agreement key** — it is detectable only by `SettlementKey`
  grouping where the settlement spans >1 ExposureKey (NumOfaccounts>1). Authoritative
  source for the QC→QCA mapping: `{env}-cdh.reference.settlement_migration_snapshot`
  (`NewSourceSystem='qca'`, has `SettlementKey`, `SettlementKeyQCSor`, `NumOfaccounts`,
  `CDI`, `ExposureKey`, `IsActiveFlag`).
- `settlement_migration_snapshot.IsActiveFlag` is a FROZEN migration-time flag. Current
  active status must come from `settlementexposure` (current BusinessDate). Of 13,461
  snapshot-active QC→QCA settlements, only **1,559 are still active today** (all present
  in `settlementexposure` under qca); the rest have since closed.
- Non-banking QC→QCA active multi-account settlements by portfolio: Mercury 4,702,
  Cell 1,112, Orbit 530, Pearl 313, Light 27 (snapshot-active counts).
- **Banking portfolios to EXCLUDE** (IT): 31 Core Retail, 32 Core Wholesale,
  60 ARTEMIS (Attica Bank), 92 JEWEL, 87 GALENE. profits has no banking portfolios,
  so profits-only excludes banking automatically; QCA includes Core Retail (banking)
  which must be explicitly excluded if population B is ever included.

### 1.6 VatSettlement constraints to preserve
- **taxNumber NOT NULL contract**: INNER join to party on CDI, tax-first dedup,
  blank→NULL normalization, non-null filter, post-build assertion (on-prem INNER-joins
  PRC.ref.partyTaxNumber). Agreement rows must honor this too.
- **No portfolio filter** in VatSettlement (matches on-prem `prep_VatSettlement`, which
  filters only isActive + party-relationship type + AccountNumber NOT NULL). The only
  portfolio effect is the PortfolioName INNER join (lookup integrity). Banking is
  excluded only implicitly (profits has none) — if population B is added, add an
  explicit banking exclusion.
- Pre-write guards (empty-result raise, row-count floor), `{env}` catalogs, party dedup
  consistent with other platform tables (run-date scoped, deterministic).

---

## Requirement 2 — New table: dtransfer.SettlementDebtor (settlement × debtor grain)

### 2.1 Why a new table
The debtor-binding data (settlement applicant + involved debtors with Relation/VAT) is
at **settlement × debtor** grain — fits neither DebtDetail (loan grain) nor
dtransfer.Customer (one row per customer). Forcing it into either breaks the grain.
→ New delivery table `{env}-cdh-delivery.it.dtransfer_settlementdebtor` at
settlement × debtor grain.

### 2.2 Source tables (all confirmed present in cloud landed)
| IT query source (ITStaging.profits) | Cloud landed |
|---|---|
| ClcCase | `landed.profits_clc_case` |
| ClcPayPromises | `landed.profits_clc_pay_promises` |
| PrfSettlements | `landed.profits_prfsettlements` |
| GenericDetail | `landed.profits_generic_detail` |
| Customer | `landed.profits_customer` |
| OtherId | `landed.profits_other_id` |
| VDebtorsRelations | `landed.profits_vdebtorsrelations` |

### 2.3 Target columns (from IT query)
Reference Date, Case Code, Case Purpose, Case Level (19→'Σε επίπεδο σύμβασης',
4→'Σε επίπεδο Δανείου'), Case Description, Case Start Date, Settlement Code,
Settlement Create Date, Settlement Status, Settlement Phase, Settlement Justification,
Account Number (CASE_ACC_SYSTEM=4), Agreement Number (CASE_ACC_SYSTEM=19),
Customer Code (= TRIM(OtherId.ID_NO), type 13 = settlement applicant),
Debtor Code (COALESCE(vdl.CDI, vdc.CDI)), Debtor Relation (RELATION_CODE),
Debtor Vat Number (AFM_NO).

### 2.4 IT reference query (authoritative logic)
Latest settlement per promise via `PrfSettlements` MAX(SCHEDULED_DATE); ClcCase ⋈
ClcPayPromises on CASE_ID; LEFT joins to PrfSettlements (status/phase/justification),
GenericDetail (CLCRN purpose), Customer, OtherId (type 13 applicant), and
VDebtorsRelations twice — vdl on CASE_ACC_SYSTEM=4 & CASE_ACC_NUMBER=Account_Number,
vdc on CASE_ACC_SYSTEM=19 & CASE_ACC_NUMBER=CONTRACT_NUMBER. Filter
`cpp.TABLE_STATUS='1'` and `CASE_ACC_SYSTEM IN (4,19)`. (Full SQL retained in the
ticket / IT message.)

Implementation notes:
- Reproduce the `LastSettlement` MAX(SCHEDULED_DATE) dedup in PySpark/Spark SQL.
- OtherId filter `FKGD_HAS_TYPE = '13'` (column casing/type may differ in landed —
  verify; use `try_cast`/trim as needed, per curated gotchas).
- VDebtorsRelations is joined on different key per case level (Account_Number for loan,
  CONTRACT_NUMBER for contract).

---

## Requirement 3 — HasActiveSettlementFlag in dtransfer.SettlementDebtor

Add a column `HasActiveSettlementFlag` computed per the IT query logic, and it must
take into account `[Customer Code] = TRIM(OtherId.ID_NO)` — i.e. the flag reflects
whether the **debtor is bound into the settlement** (applicant presence), not merely
that a settlement exists. Exact boolean rule to be finalized against IT's definition
during requirements (confirm the precise condition combining settlement
status/phase and the applicant binding).

---

## Open questions for the spec (requirements phase)

1. (R1) Full agreement perimeter: profits CASE_ACC_SYSTEM=19 only (interim), or also the
   QC→QCA 1→N population? If the latter — AgreementNumber value for key-less QCA rows,
   and explicit banking exclusion.
2. (R1) Exact `AgreementNumber` format expected by IT (vs the dash-composite cloud key).
3. (R1/R2) Target schema changes on the consuming side (new AgreementNumber column;
   new SettlementDebtor table) — DDL coordination with IT.
4. (R3) Precise HasActiveSettlementFlag boolean definition incl. the applicant-binding
   condition.
5. (R2) Grain/dedup confirmation: one row per (settlement, debtor); handling of NULL
   debtor rows and the two VDebtorsRelations branches.

## Reference artifacts
- Notebook: `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb`
- On-prem SPs: `[PLTFRM].[dtransfer].[prep_VatSettlement]`, `[Analytic].[bi].[sp_settlement]`
- Authoritative QC→QCA map: `{env}-cdh.reference.settlement_migration_snapshot`
- Steering: `.kiro/steering/platform-migration-checklist.md` §12 (party identity,
  settlement participation, guards, QC decommission context)
