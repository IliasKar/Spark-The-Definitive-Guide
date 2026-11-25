# Settlement Debtor & Perimeter — Knowledge Base (DNA26-1804)

Captures the investigation, data findings, and decisions for the VatSettlement
agreements + new `dtransfer_settlementdebtor` work, plus the settlement active-perimeter
analysis. Companion to `platform-migration-checklist.md`. Reference date for all
verified numbers: **2026-11-06** unless noted (the latest date where landed, curated,
and derivative coexisted at analysis time; later checks use 2026-11-10).

These tables feed regulatory government platforms — accuracy is critical. The delivery
`dtransfer_*` notebooks read **curated (`primary*`) + derivative (`derivative*`)** only,
NEVER `landed.*` — with ONE deliberate, scoped exception documented in §7.

---

## 1. The IT settlement perimeter query (authoritative)

IT defines the profits settlement perimeter via a query on ITStaging `profits`:
- `ClcCase cc` INNER JOIN `ClcPayPromises cpp` ON `CASE_ID`
- Filter: `cpp.TABLE_STATUS = '1'` AND `cc.CASE_ACC_SYSTEM IN (4, 19)`
- LEFT JOINs: `PrfSettlements` (latest per promise by MAX(SCHEDULED_DATE)), `GenericDetail`
  (CLCRN purpose), `Customer`, `OtherId` (type 13 = applicant), `VDebtorsRelations`
  twice (loan on Account_Number, contract on CONTRACT_NUMBER).
- Grain: settlement × debtor (SELECT DISTINCT). ~36,391 rows / 23,722 settlements.

### CRITICAL — `TABLE_STATUS='1'` is NOT a settlement-activity flag
`ClcPayPromises.TABLE_STATUS='1'` means "the pay-promise record is current," NOT "the
settlement is active." The perimeter therefore INCLUDES settlements that IT's own
`PrfSettlements.CASE_SETTL_STATUS` marks `INACTIVE`, and keeps multiple settlements per
account (NO dedup). Of the 23,722 IT-perimeter settlements, ~12,443 are `INACTIVE` by IT's
own status. Do not assume the IT perimeter = active settlements.

---

## 2. Perimeter vs `dsettlementexposure.IsActiveFlag` (the core gap)

On 2026-11-06, of the 23,722 IT-perimeter settlements:
- **23,722 present** in `derivativeactive.dsettlementexposure` (profits) — **0 missing**.
- **21,392 have `IsActiveFlag = 1`**; **2,330 are present but `IsActiveFlag = 0`**.

The 2,330 are inactive for two reasons, both in `SettlementEvaluation.py` (cud repo),
neither matching IT's perimeter definition:
1. **Status exclusion (462):** `StatusCategory IN ('pending approval','cancel','final',
   'rejected','working')`. For profits, `FULFILLED/NOT FULFILLED/LOAN CLOSED → 'final'`
   → inactive. (`PENDING FOR IMPLEMENTATION → 'approved'`, `IMPLEMENTED → 'running'` stay active.)
2. **Auto-cancel dedup (1,868):** only the newest settlement per `(ExposureKey, CDI)`
   stays active; older ones are forced `IsActiveFlag=0`. 1,546 carry a stamped
   `AutoCancelledDate`; 322 do NOT (because the dedup winner has a NULL `ImplementationDate`,
   so the stamp resolves to NULL even though the row is correctly flagged inactive).

Cross-check: cloud-inactive agrees with IT's own `CASE_SETTL_STATUS='INACTIVE'` in
2,313 of 2,330 (99.3%). Only **17** are genuine disagreements (cloud-inactive but IT
ACTIVE/OVERDUE). Full analysis: `books/SettlementPerimeter_Analysis_for_IT.md`.

### Decision — introduce `isPromiseActive` (INTERIM, pending IT)
A NEW flag `isPromiseActive` is to be added to `dsettlementexposure` (cud) encoding IT's
rule (`TABLE_STATUS='1'` + `CASE_ACC_SYSTEM IN (4,19)`), INDEPENDENT of `IsActiveFlag`
(no status-final exclusion, no dedup). Platforms settlement reporting (VatSettlement
perimeter, SettlementDebtor perimeter) uses `isPromiseActive`, NOT `IsActiveFlag`.
`IsActiveFlag` and its semantics are left unchanged for other consumers.
OPEN with IT: confirm whether the regulatory output wants the full pay-promise perimeter
(`isPromiseActive`) or the deduped active set (`IsActiveFlag`).

---

## 3. How `dsettlementexposure` is populated (cud: DSettlementExposure + SettlementEvaluation)

`dsettlementexposure` is built in `gr.cepal.dna.de.cud/DSettlementExposure/`:
- `DSettlementExposure.ipynb` joins curated `se` = `primaryactive.settlementexposure`,
  `ev` = `derivativeinternal.settlementevaluation_{run_suffix}`, `sl` =
  `derivativeinternal.settlementloan_{run_suffix}` (the two internal tables are run-scoped,
  dropped after the run — not queryable later).
- `SettlementEvaluation.py` builds `ev` (status/flags/dates) and `sl` (keys).

### `IsActiveFlag`
Pulled from `ev.IsActiveFlag` (NOT recomputed in the notebook). Set in
`SettlementEvaluation.py` by the two rules in §2. `StatusCategory` is mapped from
`SettlementStatus` via `STATUS_TO_CATEGORY`.

### `ExposureKey` / `AgreementKey` — two-path cross-lookup, both always populated
Both come from `sl` (settlementloan), built as a union of two paths against
`src_exposure` = **all dexposures ever** (`derivativeactive.dexposure` UNION
`derivativearchive.dexposure`, latest snapshot per ExposureKey up to run_date):
- **Exposure path** (qca + profits-with-ExposureKey): `ExposureKey` from the settlement;
  `AgreementKey` LEFT-looked-up from `src_exposure` by ExposureKey.
- **Agreement path** (profits without ExposureKey): `AgreementKey` from the settlement;
  `ExposureKey` LEFT-looked-up from `src_exposure` by AgreementKey.
Result: on the perimeter, BOTH keys are 100% non-null (40,000 rows / 23,722 settlements,
0 null either column on 2026-11-06). The archive union is why coverage is complete here
even when raw `primary.exposure` is not (§5).

### `CDI` — it is the APPLICANT, not the primary owner
`dsettlementexposure.CDI` ← `se.CDI` (passthrough) ← curated
`settlement_exposure_profits.ipynb`:
`CDI = customer_rf_codes.CUSTOMER_ID_NO`, joined on
`clc_case.CASE_ACC_NUMBER = ACCOUNT_NUMBER AND CASE_ACC_SYSTEM = ACCOUNT_SYSTEM AND
clc_case.CASE_CUSTOMER_ID = CUSTOMER_CODE` (LEFT join).
The key is `CASE_CUSTOMER_ID` = the case customer = the **settlement applicant** (same
field IT uses for `[Customer Code]`). There is NO relation-type filter — it is NOT
"primary owner." Verified: `dsettlementexposure.CDI` = IT applicant CDI for 23,477/23,722
(99.0%); 242 NULL (LEFT-join miss); 3 differ. In divergent applicant≠owner cases, ds.CDI
followed the applicant, never the owner.
Single CDI per settlement, repeated across all its exposure rows (23,479/23,722 single-CDI).

### SettlementKey is NOT globally unique
The same `SettlementKey` can appear under BOTH `profits` AND `qca` for completely
different exposures/CDIs/portfolios (e.g. SettlementKey 24073 on 2026-11-10: profits
IMR… vs qca CEL…). ALWAYS key on `(SettlementKey, SourceSystem)`. For platforms we care
only about `SourceSystem = 'profits'`.

---

## 4. Debtors vs applicant — the fan-out

For a settlement, the IT query returns one row per **debtor** (from `VDebtorsRelations`),
repeating the single **applicant** on each row. These are different concepts:
- **Applicant** = one person per settlement = `dsettlementexposure.CDI` (= `CASE_CUSTOMER_ID`).
- **Debtors** = all borrowers on the account/contract = `VDebtorsRelations` (CDI,
  RELATION_CODE, AFM_NO). Relation codes: OWNER-CONTR, CO-OWNER-CONTR, GUARANTOR-CONTR,
  REPRESENTS-OWNER-CONTR, GUARANTOR-LOAN, CO-OWNER-LOAN.

Worked example — profits settlement code 24073 (2026-11-10), contract
`IMR0010/202800002236559`, applicant `IMR6883975`:
| debtor_cdi | relation | is applicant? |
|---|---|---|
| IMR2293279 | CO-OWNER-CONTR | no |
| IMR6883975 | OWNER-CONTR | **yes (= applicant)** |
| IMR6925852 | CO-OWNER-CONTR | no |
IT returns 3 rows; `dsettlementexposure` has 1 row (CDI=IMR6883975, the applicant).
The applicant happens to be the OWNER here, but that is incidental — the applicant is
sourced from `CASE_CUSTOMER_ID`, not a relation type.

### HasActiveSettlementFlag — exactly one debtor row per settlement
The flag marks the debtor who is the bound/applicant party: `HasActiveSettlementFlag = 1`
where the debtor's CDI = the settlement's bound CDI (`dsettlementexposure.CDI`, i.e. the
applicant) AND there is an active primary settlement (`IsActiveFlag=1`,
`isPrimarySettlementFlag=true`) on that account/agreement for that CDI. For 24073, only
the `IMR6883975` (owner = applicant) row flags 1; the two co-owners flag 0.

---

## 5. `primary.exposure` coverage of the settlement perimeter (do NOT rely on it alone)

Matching settlement keys to `primary.exposure` by natural key (loan→ExposureKey,
agreement→AgreementKey), 2026-11-06:
- Loan (sys=4): 12,844 / 14,924 present (**86%**) — 2,080 missing.
- Agreement (sys=19): 2,093 / 6,731 present (**31%**) — 4,638 missing.

Root cause: `exposure_profits.ipynb` (cup) builds the profits exposure perimeter via an
INNER-join chain (`loan_details ⋈ agreement ⋈ loan_relation_agreement_relation ⋈
customer`); settlement-backed accounts failing any of those are dropped. It is NOT a
relation-type exclusion — when a loan IS present, ~99% of its debtors are too.

### Use `derivative.get_exposures_by_business_date(date, include_migrated)` for full coverage
Signature: `(business_date DATE, include_migrated BOOLEAN)`. Unions `dexposure` +
`derivativearchive.dexposure` (latest per ExposureKey), so it recovers migrated/closed
exposures. Coverage of the settlement perimeter (2026-11-10):
- `include_migrated = true`: **100%** loan (14,924) AND agreement (6,731).
- `include_migrated = false`: 100% loan, 97.6% agreement (6,570/6,731).
Use `include_migrated = true` for complete coverage.
CAVEAT: this function returns **Primary Owner only** (one row per exposure) — good for the
exposure/agreement spine and primary-owner attributes, but it does NOT carry co-owners or
guarantors. For the full borrower set use `VDebtorsRelations`.

NOTE: `dsettlementexposure` already resolves both keys at 100% internally (§3) using the
same archive union, so the exposure/agreement spine can be taken straight from
`dsettlementexposure` without calling this function.

---

## 6. Field-by-field: what curated/derivative carries vs what is new

Verified on both `primaryactive.settlementexposure` and `derivativeactive.dsettlementexposure`
(profits, 2026-11-06):
- `CaseCode` (int) column EXISTS but is **0% populated for profits** (100% qca). Profits
  `CASE_ID` is a string (`MZR999990046026270-19.1`) → cannot use the int `CaseCode`.
- `ContractNumber`, `Comments`, `SubmissionDate`: 0% for profits.
- `StatusCategory` / `CalculatedSettlementStatus` exist/populated but are DE-normalized —
  NOT equivalent to raw profits `CASE_OPER_STATUS` (phase) / `CLC_JUST_REASON`.

### 6 new case/phase fields on `primaryactive.settlementexposure` (R0.A, cup repo)
Add in `settlement_exposure_profits.ipynb` (which already reads clc_case and already
selects `case_id`, `CREATE_TYPE`, `CASE_CUSTOMER_ID`). FIXED name mapping:
| New curated column | Landed source | Notes |
|---|---|---|
| `PromiseCaseID` | `clc_case.CASE_ID` | string; NOT the int `CaseCode` |
| `PromiseCasePurpose` | `generic_detail.DESCRIPTION` (PARAMETER_TYPE='CLCRN', SERIAL_NUM=CREATE_TYPE) | new CLCRN lookup |
| `PromiseCaseDescription` | `clc_case.CASE_DESCRIPTION` | add to clc_case fields |
| `PromiseCaseArrivalDate` | `clc_case.CASE_ARRIVAL_DT` | add to clc_case fields; cast date |
| `SettlementPhase` | `prfsettlements.CASE_OPER_STATUS` | NEW `profits_prfsettlements` extract, latest per CASE_SETTL_PROMISE_ID by MAX(SCHEDULED_DATE) |
| `SettlementJustification` | `prfsettlements.CLC_JUST_REASON` | same new extract |
NULL for non-profits. Redis `curated.json` schema update + delta alter required.
`profits_prfsettlements` is NOT curated anywhere today.

---

## 7. Decision: dtransfer_settlementdebtor reads profits_vdebtorsrelations DIRECTLY

DECISION (DNA26-1804): do NOT create a curated `primaryactive.settlementdebtorrelation`
table. Rationale: the debtor-relations fan-out is only meaningful for the profits-only IT
settlement query; general debtor relations not tied to settlements are of no interest to
the curated/derivative layers. Therefore the delivery notebook
`platforms/dtransfer_SettlementDebtor/dtransfer_SettlementDebtor.ipynb` reads
`landed.profits_vdebtorsrelations` DIRECTLY — a deliberate, documented exception to the
"delivery never reads landed" rule, scoped to this one source.

Everything ELSE in the notebook still comes from curated/derivative:
- Perimeter + settlement/case fields + applicant CDI + keys → `dsettlementexposure` /
  `primary.settlementexposure` (incl. the R0.A fields + `isPromiseActive`).
- Only the debtor fan-out (CDI / RELATION_CODE / AFM_NO) → `profits_vdebtorsrelations`.

### Feasibility (verified on cloud, 2026-11-10)
Reproducing the IT grain with `dsettlementexposure` (perimeter/keys/applicant) +
`profits_vdebtorsrelations` (debtors) yielded **36,519 distinct (settlement, debtor,
relation)** tuples (vs IT ~36,391, ~0.4% — reference-date drift) and **0 null-debtor rows**
(100% key match: loan→ACCOUNT_NUMBER, agreement→CONTRACT_NUMBER, verified earlier at
14,924/14,924 and 6,731/6,731).
Key-format alignment: settlement keys match `VDebtorsRelations` DIRECTLY — no
prefix-strip needed (loan ACCOUNT_NUMBER and agreement CONTRACT_NUMBER both match the
clc CASE_ACC_NUMBER verbatim).

### MUST apply DISTINCT (grain guard)
The raw fan produces ~69,074 rows that collapse to 36,519 distinct — a contract/account
appears across multiple cases/promises and `VDebtorsRelations` carries repeat rows. The
notebook MUST `SELECT DISTINCT` on the IT column set (as IT's query does) and assert the
(settlement, debtor, relation) grain.

### KNOWN LIMITATION — QC→QCA migrated settlements NOT covered
`dtransfer_settlementdebtor` is **profits-only** by construction: the perimeter comes from
the profits clc tables (`clc_case`/`clc_pay_promises`), and the debtor fan-out from
`profits_vdebtorsrelations`. The historic QC→QCA migrated 1→N settlements still active
(**~1,559 settlements**, qca-sourced — see `platform-migration-checklist` / VatSettlement
Population B, authoritative map `reference.settlement_migration_snapshot`) have NO profits
clc case and NO `profits_vdebtorsrelations` rows, so they are **NOT included** in this
table. This mirrors IT's own query, which is profits-only (`TABLE_STATUS='1'` on the
profits `ClcPayPromises`). If these migrated settlements must appear in SettlementDebtor,
that is a separate requirement needing a qca-sourced debtor path (open item with IT).

---

## 7b. dtransfer_settlementdebtor — full column coverage (all 18 IT columns)

| IT query column | Delivery column | Source |
|---|---|---|
| Reference Date | ReferenceDate | run_date / BusinessDate |
| Case Code | CaseCode | `dsettlementexposure.PromiseCaseID` (R0.A) |
| Case Purpose | CasePurpose | `PromiseCasePurpose` (R0.A) |
| Case Level | CaseLevel | derived (AgreementKey→σύμβασης / ExposureKey-only→Δανείου) |
| Case Description | CaseDescription | `PromiseCaseDescription` (R0.A) |
| Case Start Date | CaseStartDate | `PromiseCaseArrivalDate` (R0.A) |
| Settlement Code | SettlementCode | `dsettlementexposure.SettlementKey` |
| Settlement Create Date | SettlementCreateDate | `dsettlementexposure.CreatedDate` |
| Settlement Status | SettlementStatus | **INTERIM:** `dsettlementexposure.SettlementStatus` (see note) |
| Settlement Phase | SettlementPhase | `PromiseCasePhase`→`SettlementPhase` (R0.A) |
| Settlement Justification | SettlementJustification | `SettlementJustification` (R0.A) |
| Account Number | AccountNumber | `dsettlementexposure.ExposureKey` (loan rows) |
| Agreement Number | AgreementNumber | `dsettlementexposure.AgreementKey` (agreement rows) |
| Customer Code (applicant) | CustomerCode | `dsettlementexposure.CDI` (= applicant, 99.94% = ID_NO) |
| Debtor Code | DebtorCode | `profits_vdebtorsrelations.CDI` (direct read) |
| Debtor Relation | DebtorRelation | `profits_vdebtorsrelations.RELATION_CODE` (direct) |
| Debtor Vat Number | DebtorVatNumber | `profits_vdebtorsrelations.AFM_NO` (direct) |
| (additive) | HasActiveSettlementFlag | derived (§4 / R3) |

### INTERIM note — Settlement Status semantics mismatch
IT's `[Settlement Status]` = `PrfSettlements.CASE_SETTL_STATUS` (values
ACTIVE / INACTIVE / OVERDUE / DELAYED — the pay-promise lifecycle status). The derivative's
`SettlementStatus` is a DIFFERENT field (IMPLEMENTED / PENDING FOR IMPLEMENTATION /
NOT FULFILLED / FULFILLED — the settlement business status). DECISION (DNA26-1804, interim):
use the derivative `SettlementStatus`. The raw `CASE_SETTL_STATUS` is NOT curated (only
landed `profits_prfsettlements`); if IT requires the raw value, add it as a 7th R0.A field
(same prfsettlements extract as Phase/Justification). Revisit with IT.

### CustomerCode verified
`dsettlementexposure.CDI` (applicant) = `OtherId` type-13 `ID_NO` for 644,357/644,731 =
**99.94%** of profits customers (373 differ). Acceptable substitute; residual = 242
NULL-CDI settlements.

---

## 8. VatSettlement agreements (R1) — recap

- Add a dedicated `AgreementNumber` column (do NOT fold into `AccountNumber`).
  `AgreementNumber = settlementexposure.AgreementKey` directly (= on-prem
  `ClcCase.CASE_ACC_NUMBER` sys=19, dash-composite e.g. `CSR7006885191-0000000002879103`,
  verified 1:1). Loan rows: `AccountNumber = ExposureKey`, `AgreementNumber = NULL`.
- Perimeter by `isPromiseActive = 1` (INTERIM, §2), all source systems.
- QC→QCA 1→N migrated settlements (ExposureKey present, no AgreementKey) flow through as
  loan rows; no synthesized agreement key (Option 1).
- Banking portfolios ALWAYS excluded (unconditional): 31 Core Retail, 32 Core Wholesale,
  60 ARTEMIS/Attica, 92 JEWEL, 87 GALENE.
- taxNumber NOT NULL contract preserved for agreement rows too (INNER join to party on
  CDI, run-date-scoped tax-first dedup, blank→NULL, non-null filter, post-build assertion).

---

## 9. Reference values & gotchas

- Reference date: pin landed + curated + derivative to the SAME BusinessDate. At analysis
  time the derivative lagged primary/landed by a few days; cross-date comparison created
  phantom "missing" rows (43 that vanished when aligned to 2026-11-06).
- IT perimeter 2026-11-06: 23,722 settlements; 2026-11-04-ish earlier runs showed 23,765
  (date drift).
- ITStaging cross-table joins (ClcCase⋈…⋈VDebtorsRelations) are SLOW / can hang — prefer
  reproducing on the cloud/landed side; use ITStaging only for small, targeted lookups.
- The MSSQL MCP rejects queries not starting with `SELECT` (inline CTEs as derived tables,
  not leading `WITH`).
- `profits_other_id` applicant: `FKGD_HAS_TYPE='13'` → `ID_NO`. NOT recoverable from
  `primary.partyidentification` (that notebook types on `FKGH_HAS_TYPE='OIDTP'` and drops
  `FKGD_HAS_TYPE`). But the applicant CDI is available directly as `dsettlementexposure.CDI`
  (§3), so no new applicant curation is needed (modulo the 242 NULL-CDI settlements).

---

## 10. Open items with IT / business

1. Perimeter definition: `isPromiseActive` (full pay-promise perimeter, incl. IT-INACTIVE)
   vs `IsActiveFlag` (deduped active). Interim = `isPromiseActive`.
2. The 17 genuine cloud-inactive-but-IT-ACTIVE/OVERDUE disagreements.
3. `HasActiveSettlementFlag` precise boolean definition (applicant-binding condition).
4. The 242 settlements with NULL `dsettlementexposure.CDI` (applicant unresolved).
5. QC→QCA migrated 1→N settlements still active (~1,559) are NOT in
   `dtransfer_settlementdebtor` (profits-only table, §7). Confirm whether they must be
   included (would need a qca-sourced debtor path).
6. Settlement Status semantics: interim maps to derivative `SettlementStatus`
   (IMPLEMENTED/PENDING/…) not IT's raw `CASE_SETTL_STATUS` (ACTIVE/INACTIVE/OVERDUE).
7. Consuming-side DDL: new `AgreementNumber` column on VatSettlement; new
   `dtransfer_settlementdebtor` table.

---

## 11. Branches / artifacts

- Branch: `DNA26-1804` (off master) — SettlementDebtor + VatSettlement agreements work.
- Spec: `.kiro/specs/vatsettlement-agreements-settlementdebtor/{requirements,design,tasks}.md`
  (untracked at time of writing).
- Analysis doc for IT: `books/SettlementPerimeter_Analysis_for_IT.md`.
- Spec input: `books/Spec_Input_VatSettlement_Agreements_and_SettlementDebtor.md`.
- Upstream repos: `gr.cepal.dna.de.lng-lnd-cup` (curated `settlement_exposure_profits.ipynb`),
  `gr.cepal.dna.de.cud` (`DSettlementExposure`/`SettlementEvaluation.py` — `isPromiseActive`),
  `gr.cepal.dna.de.redis` (`curated.json` schema).
