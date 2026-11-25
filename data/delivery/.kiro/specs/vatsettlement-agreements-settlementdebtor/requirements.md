# Requirements — VatSettlement Agreements + SettlementDebtor

## Introduction

This spec extends the PLTFRM `dtransfer` cloud delivery with (1) agreement-level
settlements in `dtransfer_vatsettlement`, (2) a new settlement × debtor grain table
`dtransfer_settlementdebtor`, and (3) a `HasActiveSettlementFlag` on that new table
that reflects whether a debtor is bound into the settlement.

Delivery scope is the `gr.cepal.dna.de.delivery` repo, `platforms/` directory, PySpark
notebooks writing to `{env}-cdh-delivery.it.*`. Catalogs resolved via
`aws_utils.get_env_prefix()` (no hardcoded prod catalogs). These tables feed
regulatory platforms — accuracy is critical.

### ARCHITECTURE RULE — delivery reads curated/derivative only, never landed
Delivery notebooks MUST read from the curated (`primaryactive` / `primary`) and
derivative (`derivativeactive`) areas only. They MUST NOT read from `landed.*`. The
SettlementDebtor fields that today exist only in `landed.profits_*` (case metadata,
settlement phase/justification, debtor relations, applicant) MUST first be curated
upstream in the cup repo (`gr.cepal.dna.de.lng-lnd-cup`) and, where needed, carried into
the derivative in the cud repo (`gr.cepal.dna.de.cud`). This upstream work is
**Requirement 0** and is a prerequisite for Requirements 2 and 3.

### Perimeter / activity decision (interim, pending IT)
IT's settlement perimeter query filters `ClcPayPromises.TABLE_STATUS = '1'`, which is a
*current-pay-promise* flag, NOT a settlement-activity flag. It therefore includes
settlements that IT's own `PrfSettlements.CASE_SETTL_STATUS` marks `INACTIVE`, and keeps
multiple settlements per account (no dedup). The derivative `dsettlementexposure.IsActiveFlag`
is stricter (excludes completed/"final" statuses and auto-cancels all-but-newest per
(ExposureKey, CDI)). On 2026-11-06 the IT perimeter = 23,722 settlements, of which 21,392
are `IsActiveFlag=1` and 2,330 are present-but-inactive (0 missing). See
`books/SettlementPerimeter_Analysis_for_IT.md`.

**INTERIM DECISION (pending IT answer):** introduce a NEW flag
`dsettlementexposure.isPromiseActive` that encodes IT's `TABLE_STATUS='1'` perimeter
logic, and use `isPromiseActive` (NOT `IsActiveFlag`) for platforms settlement reporting
(VatSettlement agreement perimeter, SettlementDebtor perimeter, and the
VDebtorsRelations-based debtor join). This is tracked in Requirement 0.

### Agreed scope (Option 1)
- `AgreementNumber` = `settlementexposure.AgreementKey` directly (resolved: it is the
  on-prem `ClcCase.CASE_ACC_NUMBER` dash-composite, identical to IT's `[Agreement Number]`).
  An agreement row = one that has an `AgreementKey` (i.e. `ExposureKey IS NULL AND
  AgreementKey IS NOT NULL`). In practice these are the profits CASE_ACC_SYSTEM=19
  population (~8,199 active).
- **No source-system restriction** — qca settlements are included. The QC→QCA 1→N
  migrated settlements have NO AgreementKey (they appear as loan-level rows, one per
  ExposureKey), so under Option 1 they simply flow through as normal **loan rows**
  (`AccountNumber = ExposureKey`, `AgreementNumber = NULL`); they are included in the
  extract but not keyed as agreements. No synthesized AgreementNumber.
- **Banking portfolios are ALWAYS excluded** (unconditional rule, independent of source
  system): 31 Core Retail, 32 Core Wholesale, 60 ARTEMIS/Attica, 92 JEWEL, 87 GALENE.

### Background references
- Spec input: `books/Spec_Input_VatSettlement_Agreements_and_SettlementDebtor.md`
- Steering: `.kiro/steering/platform-migration-checklist.md` (§12 party identity,
  settlement participation, guards; §7 on-prem MSSQL access)
- On-prem SPs: `[PLTFRM].[dtransfer].[prep_VatSettlement]`, `[Analytic].[bi].[sp_settlement]`
- IT perimeter query for SettlementDebtor (ClcCase/ClcPayPromises/PrfSettlements/...)

---

## Requirement 0 — Upstream curation of settlement case/debtor data (cup + cud repos)

**User story:** As data engineering, I want the settlement case metadata, phase/
justification, debtor relations, and applicant curated into `primaryactive.settlementexposure`
(and carried into `derivativeactive.dsettlementexposure` where needed), plus a new
`isPromiseActive` flag, so that the delivery SettlementDebtor and VatSettlement notebooks
can source every field from curated/derivative and never read `landed.*`.

Repos: `gr.cepal.dna.de.lng-lnd-cup` (curated notebook `settlement_exposure_profits.ipynb`
and a new debtor-relations curated notebook) and `gr.cepal.dna.de.cud`
(`DSettlementExposure` / `SettlementEvaluation`). Redis schema updates in
`gr.cepal.dna.de.redis`. Field-by-field equivalence was verified against both settlement
tables on 2026-11-06; see §"Field equivalence findings" below.

#### Acceptance Criteria

**0.A — Case metadata on `primaryactive.settlementexposure` (6 new fields).**
1. `settlement_exposure_profits.ipynb` SHALL add the following columns to
   `primaryactive.settlementexposure` with this exact name mapping, sourced from the
   landed tables it already reads (clc_case is already loaded; CREATE_TYPE, case_id,
   CASE_CUSTOMER_ID already selected):

   | New curated column | Landed source | Notes |
   |---|---|---|
   | `PromiseCaseID` | `clc_case.CASE_ID` | string (e.g. `MZR999990046026270-19.1`); do NOT reuse the existing int `CaseCode` column |
   | `PromiseCasePurpose` | `generic_detail.DESCRIPTION` WHERE `PARAMETER_TYPE='CLCRN' AND SERIAL_NUM = clc_case.CREATE_TYPE` | new CLCRN lookup join |
   | `PromiseCaseDescription` | `clc_case.CASE_DESCRIPTION` | add to `clc_case_selected_fields` |
   | `PromiseCaseArrivalDate` | `clc_case.CASE_ARRIVAL_DT` | add to `clc_case_selected_fields`; cast to date |
   | `SettlementPhase` | `prfsettlements.CASE_OPER_STATUS` | new `profits_prfsettlements` extract (not curated anywhere today), latest per promise via MAX(SCHEDULED_DATE) |
   | `SettlementJustification` | `prfsettlements.CLC_JUST_REASON` | same new extract |

2. The `prfsettlements` join SHALL select the latest row per `CASE_SETTL_PROMISE_ID` by
   `MAX(SCHEDULED_DATE)` (reproduce IT's `LastSettlement` CTE), joined to the settlement
   on PAY_PROMISE_ID = SettlementKey.
3. These 6 fields SHALL be populated for the profits settlement perimeter and left NULL
   for non-profits rows (qca settlements have no profits clc case).
4. The Redis `curated.json` schema for `settlementexposure` SHALL be updated to include
   the 6 new columns; the delta table SHALL be altered/recreated accordingly.

**0.B — `isPromiseActive` flag.**
5. A new column `isPromiseActive` (int/boolean) SHALL be added to
   `derivativeactive.dsettlementexposure` (and, if required for the curated-only read
   path, mirrored on `primaryactive.settlementexposure`).
6. `isPromiseActive = 1` SHALL encode IT's perimeter rule: the settlement corresponds to
   a `ClcPayPromises` row with `TABLE_STATUS = '1'` and `CASE_ACC_SYSTEM IN (4, 19)`.
   This flag is INDEPENDENT of `IsActiveFlag` (it does not apply the status-"final"
   exclusion nor the newest-per-exposure auto-cancel dedup).
7. The existing `IsActiveFlag` SHALL be left unchanged (its dedup/status semantics remain
   for other consumers). `isPromiseActive` is additive.

**0.C — Debtor relations + applicant (DECISION: no new curated table).**
8. **DECISION (DNA26-1804):** do NOT create a curated `primaryactive.settlementdebtorrelation`
   table. The debtor-relations fan-out is only meaningful for the profits-only IT
   settlement query; general debtor relations not tied to settlements are of no interest to
   the curated/derivative layers. The delivery `dtransfer_settlementdebtor` notebook reads
   `landed.profits_vdebtorsrelations` DIRECTLY — a deliberate, documented exception to the
   "delivery never reads landed" rule, scoped to this one source. See R2.
9. The settlement **applicant** SHALL be taken from `dsettlementexposure.CDI` — which is
   already the applicant (derived from `clc_case.CASE_CUSTOMER_ID` via `customer_rf_codes`,
   verified = IT applicant CDI for 23,477/23,722 = 99.0%). NO new curated applicant field is
   required. KNOWN LIMITATION: 242 perimeter settlements have NULL `dsettlementexposure.CDI`
   (customer_rf_codes LEFT-join miss) — handle/flag these (open item with IT).
10. Therefore R0.C adds NOTHING to the curated/derivative layer. R0 reduces to 0.A (6 case/
    phase fields) + 0.B (`isPromiseActive`).

---

### Field equivalence findings (verified 2026-11-06, profits)
Against `primaryactive.settlementexposure` AND `derivativeactive.dsettlementexposure`:
- `CaseCode` (int) column EXISTS in both but is **0% populated for profits** (100% qca).
  Profits `CASE_ID` is a string → must go to new `PromiseCaseID`, not `CaseCode`.
- `ContractNumber`, `Comments`, `SubmissionDate`, `StartingCaseDenounceStatus`: 0% for profits.
- `StatusCategory` / `CalculatedSettlementStatus` exist and are populated but are the
  DE-normalized status — NOT equivalent to the raw profits `CASE_OPER_STATUS`
  (SettlementPhase) or `CLC_JUST_REASON` (SettlementJustification).
- No equivalent column for CasePurpose, CaseDescription, CaseArrivalDate.
- Conclusion: all 6 case/phase fields are genuinely new (confirmed field-by-field).

---

## Requirement 1 — Agreement-level settlements in dtransfer_vatsettlement

**User story:** As IT consuming the VatSettlement extract, I want contract-level
(agreement) settlements delivered in a dedicated `AgreementNumber` column alongside the
loan-level `AccountNumber`, so that settlements keyed at CASE_ACC_SYSTEM=19 are not lost
and are not conflated with loan keys.

#### Acceptance Criteria
1. WHEN the notebook runs THEN the output table SHALL include a new column
   `AgreementNumber` in addition to the existing columns (`AccountNumber`,
   `SettlementType`, `SettlementCode`, `PortfolioId`, `taxNumber`, `datadate`).
2. WHERE a settlement is loan-level (has `ExposureKey`) THE SYSTEM SHALL set
   `AccountNumber = ExposureKey` and `AgreementNumber = NULL` (unchanged behavior).
3. WHERE a settlement is agreement-keyed (`ExposureKey IS NULL` AND `AgreementKey IS
   NOT NULL`) THE SYSTEM SHALL set `AgreementNumber = settlementexposure.AgreementKey`
   (the dash-composite, taken unchanged) and `AccountNumber = NULL`.
4. WHERE a settlement has an `ExposureKey` (including QC→QCA 1→N settlements, which have
   no AgreementKey) THE SYSTEM SHALL set `AccountNumber = ExposureKey` and
   `AgreementNumber = NULL`. The perimeter SHALL include `ExposureKey IS NOT NULL OR
   AgreementKey IS NOT NULL`.
5. THE SYSTEM SHALL include settlements by the agreed platforms-perimeter flag. INTERIM
   (pending IT): use `isPromiseActive = 1` (R0.B) for the settlement perimeter, NOT
   `IsActiveFlag`. (If IT confirms the stricter deduped definition is desired instead,
   switch to `IsActiveFlag = 1`.) Source remains `primary.settlementexposure` /
   `derivative.dsettlementexposure` — never landed.
6. THE SYSTEM SHALL include all source systems (no profits/qca restriction). QC→QCA
   migrated 1→N settlements are included as their loan-level rows; no synthesized
   AgreementNumber is created for them.
7. THE SYSTEM SHALL preserve the taxNumber NOT NULL contract for ALL rows (loan and
   agreement): INNER join to party on CDI, tax-first run-date-scoped dedup with
   blank→NULL normalization and `TaxRegistrationNumber` final tie-break, an explicit
   `taxNumber IS NOT NULL` filter, and a post-build assertion that 0 rows have NULL
   taxNumber.
8. THE SYSTEM SHALL ALWAYS exclude banking portfolios, regardless of source system:
   PortfolioId NOT IN (31 Core Retail, 32 Core Wholesale, 60 ARTEMIS/Attica, 92 JEWEL,
   87 GALENE). This is an unconditional rule (not merely a consequence of including qca).
9. THE SYSTEM SHALL NOT hardcode catalog names; it SHALL use `{env}` throughout.
10. THE SYSTEM SHALL fail the run (raise, not `dbutils.notebook.exit`) if the final
    result is empty, and SHALL apply a day-over-day row-count floor guard before the
    overwrite.
11. WHEN verified against on-prem for a reference date THEN the agreement-row count
    SHALL reconcile to the CASE_ACC_SYSTEM=19 active population (~8,199), and
    `AgreementNumber` values SHALL be a subset of on-prem `ClcCase.CASE_ACC_NUMBER`
    (CASE_ACC_SYSTEM=19).

---

## Requirement 2 — New table dtransfer_settlementdebtor (settlement × debtor grain)

**User story:** As IT, I want a new delivery table at settlement × debtor grain binding
each settlement to its applicant and involved debtors (with relation and VAT), because
this data fits neither DebtDetail (loan grain) nor Customer (one row per customer).

#### Acceptance Criteria
1. THE SYSTEM SHALL create/write `{env}-cdh-delivery.it.dtransfer_settlementdebtor` at
   one row per (settlement, debtor), reproducing IT's reference query semantics.
2. THE SYSTEM SHALL source all fields EXCEPT the debtor fan-out from curated/derivative:
   the settlement + case fields + applicant CDI + keys from `derivative.dsettlementexposure`
   / `primary.settlementexposure` (including the R0.A fields `PromiseCaseID`,
   `PromiseCasePurpose`, `PromiseCaseDescription`, `PromiseCaseArrivalDate`,
   `SettlementPhase`, `SettlementJustification` and the R0.B `isPromiseActive`; applicant =
   `dsettlementexposure.CDI`). The debtor relations (DebtorCode/Relation/VatNumber) SHALL be
   read DIRECTLY from `landed.profits_vdebtorsrelations` — the single, documented landed
   exception (R0.C-8). Depends on R0.A + R0.B landing.
3. The "latest settlement per promise" semantics (IT's `LastSettlement` MAX(SCHEDULED_DATE))
   SHALL be satisfied upstream in R0.A (the curated `SettlementPhase`/`SettlementJustification`
   already reflect the latest prfsettlements row), so the delivery notebook reads the
   already-resolved fields.
4. THE SYSTEM SHALL emit the columns: ReferenceDate, CaseCode (= `PromiseCaseID`),
   CasePurpose (= `PromiseCasePurpose`), CaseLevel (19→'Σε επίπεδο σύμβασης',
   4→'Σε επίπεδο Δανείου'; derivable from ExposureKey/AgreementKey presence),
   CaseDescription (= `PromiseCaseDescription`), CaseStartDate (= `PromiseCaseArrivalDate`),
   SettlementCode (SettlementKey), SettlementCreateDate (CreatedDate), SettlementStatus,
   SettlementPhase, SettlementJustification, AccountNumber (loan: ExposureKey),
   AgreementNumber (agreement: AgreementKey), CustomerCode (= `dsettlementexposure.CDI`,
   the applicant), DebtorCode (CDI), DebtorRelation (RELATION_CODE), DebtorVatNumber (AFM_NO).

   **SettlementStatus mapping (INTERIM decision):** IT's `[Settlement Status]` =
   `PrfSettlements.CASE_SETTL_STATUS` (values ACTIVE/INACTIVE/OVERDUE/DELAYED) is a
   DIFFERENT field from the derivative's `SettlementStatus` (IMPLEMENTED/PENDING FOR
   IMPLEMENTATION/NOT FULFILLED/FULFILLED). For now THE SYSTEM SHALL map `SettlementStatus`
   to the derivative `dsettlementexposure.SettlementStatus`. The raw `CASE_SETTL_STATUS`
   is NOT curated (only in landed `profits_prfsettlements`); revisit if IT requires the raw
   value (would add a 7th R0.A field from the same prfsettlements extract).

   **CustomerCode coverage (verified):** `dsettlementexposure.CDI` = IT's `OtherId` type-13
   `ID_NO` in 99.94% of profits customers (373/644,731 differ). Acceptable substitute;
   residual = the 242 NULL-CDI settlements (known limitation).

   All 18 IT-query columns are thus sourced: 16 directly + CaseLevel (derived) +
   SettlementStatus (interim mapping). HasActiveSettlementFlag (R3) is additive.
5. THE SYSTEM SHALL join `landed.profits_vdebtorsrelations` per case level: loan rows
   (ExposureKey / sys=4) on `ACCOUNT_NUMBER`; agreement rows (AgreementKey / sys=19) on
   `CONTRACT_NUMBER`. Keys match the settlement keys DIRECTLY (no prefix-strip — verified
   100%: 14,924/14,924 loan, 6,731/6,731 agreement). THE SYSTEM SHALL apply `SELECT
   DISTINCT` on the IT column set and assert the (settlement, debtor, relation) grain — the
   raw fan inflates (~69K rows → ~36.5K distinct) because a contract/account recurs across
   cases/promises and vdebtors carries repeat rows.
6. THE SYSTEM SHALL scope the perimeter by `isPromiseActive = 1` (R0.B), which encodes
   IT's `TABLE_STATUS='1'` + `CASE_ACC_SYSTEM IN (4,19)` rule — replacing the direct
   landed filter.
7. THE SYSTEM SHALL ALWAYS exclude banking portfolios (31, 32, 60, 92, 87), consistent
   with R1 — using the PortfolioKey/PortfolioId already on the curated/derivative rows.
8. THE SYSTEM SHALL handle any residual type/casing differences safely (try_cast/trim)
   on curated keys.
9. THE SYSTEM SHALL use `{env}` catalogs and apply empty-result + row-count-floor
   pre-write guards (raise on failure).

---

## Requirement 3 — HasActiveSettlementFlag on dtransfer_settlementdebtor

**User story:** As IT, I want a `HasActiveSettlementFlag` per (settlement, debtor) row
indicating whether that specific debtor is actively bound, as the primary settlement,
to an active settlement on the row's account — so it reflects the debtor↔settlement
binding, not merely that a settlement exists.

IT provided the rule as a QC/`process`-sourced EXISTS (process.settlement_loan +
process.settlement). QC is decommissioned; the rule is re-expressed against **live cloud
data** (`derivativeactive.dsettlementexposure`). Column mapping and the status-filter
collapse are in design D2.6.

#### Acceptance Criteria
1. THE SYSTEM SHALL add an int/boolean `HasActiveSettlementFlag` column to
   `dtransfer_settlementdebtor`.
2. THE flag SHALL be 1 WHERE there EXISTS a row in
   `{env}-cdh.derivativeactive.dsettlementexposure` such that ALL hold:
   - `IsActiveFlag = 1`
   - `isPrimarySettlementFlag = true` (IT's `sl.isPrimarySettlement = 1`)
   - the settlement's account matches the SettlementDebtor row's account: for loan rows
     (CASE_ACC_SYSTEM=4) match on `ExposureKey` = AccountNumber; for agreement rows
     (CASE_ACC_SYSTEM=19) match on `AgreementKey` = AgreementNumber (both keys are
     populated on dsettlementexposure — verified)
   - `dsettlementexposure.CDI` = the row's DebtorCode (IT's `sl.cdi = TRIM(oi.ID_NO)`)
   ELSE 0.
3. THE SYSTEM SHALL source this from `derivativeactive.dsettlementexposure` (live),
   NOT the decommissioned `process.*` QC tables.
4. THE status-exclusion in IT's query (NOT IN the 5 cancelled/rejected statuses) is
   subsumed by `IsActiveFlag = 1` in the cloud model (active cloud settlements carry
   only IMPLEMENTED/Running/PENDING, never those cancelled statuses — verified). A
   defensive `SettlementStatus NOT LIKE 'Cancelled%'` MAY be kept as a guard. This
   status-collapse mapping SHALL be confirmed with IT.
5. THE flag SHALL be computed for BOTH loan and agreement rows (agreement rows matched
   via AgreementKey), since dsettlementexposure populates both keys.

---

## Open items (to resolve in design / with IT)
- **Perimeter definition (KEY, awaiting IT):** should platforms reporting mirror the IT
  query perimeter (all `TABLE_STATUS='1'` pay-promises incl. settlements IT itself marks
  INACTIVE, no dedup) via `isPromiseActive`, OR use the deduped active set via
  `IsActiveFlag`? Interim build uses `isPromiseActive`. See
  `books/SettlementPerimeter_Analysis_for_IT.md` and the 17 genuine disagreements noted there.
- R0.C: final name/home of the curated debtor-relations table and applicant field.
- R3: precise `HasActiveSettlementFlag` boolean definition (**awaiting IT**).
- R2/R1: consuming-side schema changes (new `AgreementNumber` column; new
  `SettlementDebtor` table) — DDL coordination with IT.
- R2: dedup/grain edge cases — NULL debtor rows, the two debtor-relation branches,
  settlement-with-no-applicant handling.

## Resolved decisions
- **Layering (refined DNA26-1804):** delivery reads curated/derivative, EXCEPT the debtor
  fan-out which reads `landed.profits_vdebtorsrelations` directly (one deliberate, scoped
  exception — no curated `settlementdebtorrelation` table). All other fields (perimeter,
  keys, applicant CDI, case/phase) come from curated/derivative (R0.A + R0.B).
- **Feasibility VERIFIED (2026-11-10):** cloud reproduction of the IT grain =
  **36,519 distinct (settlement, debtor, relation)** / 23,853 settlements, **0 null-debtor
  rows**. Two independent cloud methods (landed-reproduction vs
  dsettlementexposure+vdebtors) agree EXACTLY (36,519 = 36,519). IT (ITStaging, post-ETL)
  = 36,592 / 23,906 — the 73-row delta is reference-date drift (IT includes brand-new
  settlements not yet in the cloud snapshot), not a join defect.
- **Applicant** = `dsettlementexposure.CDI` (already the applicant, 99.0% = IT); no new
  applicant curation. 242 NULL-CDI settlements are a known limitation.
- **6 case/phase fields** added to `primaryactive.settlementexposure` with the fixed
  name mapping: PromiseCaseID, PromiseCasePurpose, PromiseCaseDescription,
  PromiseCaseArrivalDate, SettlementPhase, SettlementJustification (R0.A).
- **`isPromiseActive`** flag added to the derivative to encode IT's `TABLE_STATUS='1'`
  perimeter; used for platforms reporting instead of `IsActiveFlag` (interim).
- R1 agreement perimeter: **Option 1** — AgreementNumber = AgreementKey when present;
  all source systems included; QC→QCA 1→N settlements flow through as loan-level rows
  (no synthesized key).
- Banking portfolios (31/32/60/92/87) are **always excluded** across R1 and R2
  (unconditional, independent of source system).
- R1 AgreementNumber source/format: `settlementexposure.AgreementKey` directly (=
  on-prem `ClcCase.CASE_ACC_NUMBER`, verified 1:1).

## Non-goals
- Synthesizing an AgreementNumber for key-less QC→QCA 1→N settlements (they ride through
  as loan rows per Option 1).
- Changing the existing `IsActiveFlag` semantics (isPromiseActive is additive).
- portfolio_cdi_bal changes (separate work).
- Changes to the on-prem side or the consuming platform DDL (coordination only).
