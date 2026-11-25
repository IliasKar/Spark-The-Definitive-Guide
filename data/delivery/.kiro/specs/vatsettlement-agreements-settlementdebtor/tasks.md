# Tasks — VatSettlement Agreements + SettlementDebtor

Implementation plan. Each task cites the requirements (R#) / design (D#) it fulfils.

**Ordering:** the upstream curation (tasks 0.x, cup + cud + redis) is a PREREQUISITE.
- VatSettlement (tasks 1–2) depends only on 0.2 (`isPromiseActive`).
- SettlementDebtor (tasks 3–6) depends on 0.1 + 0.2 + 0.3 being deployed in prod.

---

## Upstream curation (cup + cud + redis) — PREREQUISITE

- [ ] 0.1 Add 6 case/phase fields to `primaryactive.settlementexposure` (cup)
  - In `gr.cepal.dna.de.lng-lnd-cup` `src/jobs/curated_notebooks/settlement_exposure_profits.ipynb`:
    add `CASE_DESCRIPTION`, `CASE_ARRIVAL_DT` to `clc_case_selected_fields` (case_id,
    CREATE_TYPE, CASE_CUSTOMER_ID already present).
  - Add a `profits_generic_detail` extract (filter `PARAMETER_TYPE='CLCRN'`), join on
    `SERIAL_NUM = clc_case.CREATE_TYPE`.
  - Add a `profits_prfsettlements` extract, dedup latest per `CASE_SETTL_PROMISE_ID` by
    MAX(SCHEDULED_DATE), join on PAY_PROMISE_ID.
  - Final select the 6 columns with the FIXED mapping: `PromiseCaseID` (CASE_ID),
    `PromiseCasePurpose` (CLCRN DESCRIPTION), `PromiseCaseDescription` (CASE_DESCRIPTION),
    `PromiseCaseArrivalDate` (CASE_ARRIVAL_DT, date), `SettlementPhase` (CASE_OPER_STATUS),
    `SettlementJustification` (CLC_JUST_REASON). NULL for non-profits.
  - `try_cast`/trim on CREATE_TYPE↔SERIAL_NUM and promise-id joins.
  - _Requirements: R0.A-1..R0.A-3 · Design D0.1_

- [ ] 0.2 Redis schema + delta alter for the 6 columns (redis + cup)
  - Update `gr.cepal.dna.de.redis` `curated.json` `settlementexposure` schema with the 6
    new columns; coordinate the delta table alter/recreate.
  - _Requirements: R0.A-4 · Design D0.1_

- [ ] 0.3 Add `isPromiseActive` to the derivative (cud)
  - In `gr.cepal.dna.de.cud` `DSettlementExposure`/`SettlementEvaluation.py`: add
    `isPromiseActive` (int) = 1 when the settlement maps to a `clc_pay_promises` row with
    `TABLE_STATUS='1'` and `clc_case.CASE_ACC_SYSTEM IN (4,19)`. Independent of
    `IsActiveFlag`; do NOT change `IsActiveFlag` or its dedup/status logic.
  - Add the column to the `dsettlementexposure` schema/DDL (and mirror on curated if the
    delivery read path needs it there).
  - _Requirements: R0.B-5..R0.B-7 · Design D0.2_

- [ ] 0.4 (REMOVED) No curated debtor-relations table / applicant field
  - DECISION DNA26-1804: debtor fan-out is read directly from
    `landed.profits_vdebtorsrelations` in the delivery notebook (task 4); applicant =
    `dsettlementexposure.CDI`. Nothing to build upstream for debtors/applicant.
  - _Requirements: R0.C-8..R0.C-10 · Design D0.3_

- [ ] 0.5 QA the upstream outputs (cup/cud)
  - Verify the 6 fields populate for the profits perimeter; `isPromiseActive=1` count
    reconciles to the IT perimeter (~23.7K on a pinned date).
  - _Requirements: R0.A, R0.B · Design D0.4_

---

## VatSettlement — add agreements + banking exclusion

- [ ] 1. Extend the perimeter and add the AgreementNumber branch (needs 0.3)
  - In `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb`, change the
    settlement filter to `isPromiseActive = 1 AND (ExposureKey IS NOT NULL OR AgreementKey
    IS NOT NULL)`, all source systems. (INTERIM: `isPromiseActive`, not `IsActiveFlag`;
    switch if IT chooses the deduped definition.)
  - Select `col("ExposureKey").alias("AccountNumber")` and
    `col("AgreementKey").alias("AgreementNumber")` (no COALESCE, no synthesized key).
  - _Requirements: R1-1, R1-2, R1-3, R1-4, R1-5, R1-6 · Design D1.1, D1.2, D1.5_

- [ ] 2. Always-exclude banking portfolios + guards + verify
  - Add `BANKING_PORTFOLIO_IDS = [31, 32, 60, 92, 87]` constant (comment the names);
    filter `~col("PortfolioId").isin(*BANKING_PORTFOLIO_IDS)` after the portfolio join.
  - Keep the taxNumber contract intact (run-date party_tax, deterministic dedup,
    blank→NULL, non-null filter, post-build assertion). Confirm it holds for agreement
    rows (CDI present).
  - Add pre-write guards: raise on empty result; day-over-day row-count floor. Keep
    `{env}` catalogs.
  - Measure and record the delivered row count (loan + agreement, post-contract,
    post-banking-exclusion) for the floor guard and the release note.
  - _Requirements: R1-7, R1-8, R1-9, R1-10, R1-11 · Design D1.3, D1.4, D1.6, D1.7_

---

## SettlementDebtor — new table

- [ ] 3. Scaffold the notebook and load sources (needs 0.1 + 0.3 i.e. 0.A + 0.B)
  - Create `platforms/dtransfer_SettlementDebtor/dtransfer_SettlementDebtor.ipynb` with
    the standard cell-1 params/env setup (`business_date` widget, `aws_utils`,
    `run_date`, `run_date_lit`).
  - Load: `derivative.dsettlementexposure` (or `primary.settlementexposure`) filtered to
    `SourceSystem='profits'` incl. R0.A fields + `isPromiseActive` + CDI (applicant);
    `landed.profits_vdebtorsrelations` (DIRECT — scoped exception, debtor fan-out only);
    `primary.portfolio` for banking exclusion (by PortfolioKey). Pin all reads to the same
    BusinessDate.
  - _Requirements: R2-1, R2-2 · Design D2.1, D2.2_

- [ ] 4. Build the settlement × debtor result
  - Scope perimeter: `isPromiseActive = 1 AND SourceSystem='profits'`. Case fields read
    from the resolved R0.A columns; applicant (CustomerCode) = `dsettlementexposure.CDI`.
  - Debtor fan-out: join `profits_vdebtorsrelations` per case level — loan on
    `ACCOUNT_NUMBER` = ExposureKey, agreement on `CONTRACT_NUMBER` = AgreementKey (DIRECT
    key match, no prefix-strip). → DebtorCode/DebtorRelation/DebtorVatNumber.
  - Emit all columns per D2.4 (PascalCase): CaseCode=`PromiseCaseID`,
    CasePurpose=`PromiseCasePurpose`, CaseDescription=`PromiseCaseDescription`,
    CaseStartDate=`PromiseCaseArrivalDate`, SettlementPhase, SettlementJustification, plus
    CaseLevel mapping and the AccountNumber/AgreementNumber split.
  - MANDATORY `SELECT DISTINCT` on the IT column set (raw ~69K → ~36.5K distinct); assert
    (settlement, debtor, relation) grain.
  - _Requirements: R2-3, R2-4, R2-5, R2-6, R2-8 · Design D2.3, D2.4, D2.7_

- [ ] 5. Banking exclusion + grain guard + write
  - Filter `PortfolioId NOT IN (31,32,60,92,87)` using the PortfolioKey already on the
    curated/derivative rows (no landed CASE_UNIT resolution).
  - Assert (settlement, debtor) grain uniqueness; add empty-result raise + row-count
    floor; overwrite to `{env}-cdh-delivery.it.dtransfer_settlementdebtor`.
  - Measure and record the delivered row/debtor counts.
  - _Requirements: R2-7, R2-9 · Design D2.5, D2.7_

- [ ] 6. HasActiveSettlementFlag (live-cloud translation of IT's QC rule)
  - Build a distinct set of (account-key, CDI) tuples from
    `{env}-cdh.derivativeactive.dsettlementexposure` where `IsActiveFlag = 1` AND
    `isPrimarySettlementFlag = true` (optional defensive `SettlementStatus NOT LIKE
    'Cancelled%'`), carrying both `ExposureKey` and `AgreementKey`.
  - Flag = 1 where the SettlementDebtor row matches: loan rows on
    (ExposureKey = AccountNumber, CDI = DebtorCode); agreement rows on
    (AgreementKey = AgreementNumber, CDI = DebtorCode). Else 0. Compute for BOTH row types.
  - Document the QC→cloud mapping (design D2.6) in the notebook.
  - Verify on the run date: no active settlement carries a cancelled status (status
    collapse holds); isPrimarySettlementFlag coverage.
  - NOTE: non-blocking — implementable now on live data. One item to CONFIRM with IT
    (not blocking the build): that collapsing their 5-status exclusion into
    `IsActiveFlag = 1` is acceptable.
  - _Requirements: R3-1..R3-5 · Design D2.6_

---

## Verification & deployment

- [ ] 7. Verify both tables (QA run, pinned reference date)
  - Layer 1 internal: VatSettlement — taxNumber non-null, AccountNumber/AgreementNumber
    mutual exclusivity, no banking portfolios, agreement count ≈ 8,199, qca loan rows
    present. SettlementDebtor — (settlement, debtor) uniqueness, no banking portfolios.
  - Layer 2 on-prem (MSSQL MCP): AgreementNumber ⊆ on-prem ClcCase.CASE_ACC_NUMBER
    (sys=19); SettlementDebtor counts vs IT query on-prem. Classify deltas.
  - _Requirements: R1-11, R2 · Design Verification_

- [ ] 8. Wire the job + coordinate DDL + PR
  - Add the SettlementDebtor notebook to the platforms Databricks job.
  - Coordinate consuming-side DDL with IT: new `AgreementNumber` column on the
    VatSettlement target; new `SettlementDebtor` table. (Both are cross-team; do not
    ship ahead of the target DDL.)
  - Feature branch → development → master (steering §6). Update steering §12.5 with the
    final VatSettlement agreement behavior and the new table.
  - _Requirements: R1, R2 · Design Deployment_

---

## Blocking summary
- **Upstream curation (tasks 0.1–0.5) is the gating prerequisite.** Delivery tasks 3–6
  (SettlementDebtor) cannot ship until R0 (6 case/phase fields + `isPromiseActive` +
  curated debtor-relations + applicant) is deployed in prod. Task 1–2 (VatSettlement)
  only needs 0.3 (`isPromiseActive`).
- **Perimeter definition is awaiting IT** (interim: `isPromiseActive`, encoding IT's
  TABLE_STATUS='1' perimeter incl. settlements IT marks INACTIVE and no dedup). See
  `books/SettlementPerimeter_Analysis_for_IT.md`. If IT chooses the deduped set, switch
  the delivery filters to `IsActiveFlag = 1`.
- HasActiveSettlementFlag (task 6): the rule is translated to live `dsettlementexposure`;
  one NON-blocking confirmation with IT (collapsing the 5-status exclusion into the
  active flag). Buildable once R0 lands.
