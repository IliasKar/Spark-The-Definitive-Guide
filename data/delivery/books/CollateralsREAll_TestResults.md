# CollateralsREAll — Dev Test Results

**Ticket:** DNA26-1357
**Notebook:** `RCG/CollateralsREAll/CollateralsREAll.ipynb`
**Output table:** `` `d-cdh-delivery`.it.collateralsreall ``
**Migrates:** on-prem SP `[bi].[sp_CollateralsRE]` → `[Analytic].[bi].[CollateralsREAll]`
**Environment:** databricks-dev (`d-cdh`), cluster `0506-121850-11rwc2ls`
**Run date tested:** `2026-09-03` (latest common date across all dev source tables)
**Executed:** 2026-09-11 · run_id `586647256596271` · **result: SUCCESS**

On-prem is one business day behind dev, so the comparison baseline is on-prem `2026-09-01`. Absolute counts are compared as close approximations; structural and record-level checks are exact.

---

## 1. Execution

The notebook ran end-to-end on the dev cluster and wrote the output table with no errors. It was invoked with `run_date=2026-09-03`; the `run_date` widget default (latest `BusinessDate`) was not exercised in this run.

## 2. Schema — PASS

Output has all **24 columns** in the on-prem order and types, including the original on-prem spelling `IntialLegalOrder` (preserved intentionally for the external consumer).

| # | Column | Type | | # | Column | Type |
|---|---|---|---|---|---|---|
| 0 | PropertyKey | string | | 12 | OwnershipId | int |
| 1 | CDI | string | | 13 | OwnershipPercentage | decimal |
| 2 | AgreementNumber | string | | 14 | CollateralId | string |
| 3 | IntialLegalOrder | int | | 15 | CollateralREStatus | string |
| 4 | Currency | string | | 16 | CollateralREStatusChangeDate | date |
| 5 | CollateralAmountEuro | decimal | | 17 | collateral_status_en | string |
| 6 | CollateralAmount | decimal | | 18 | CollateralStatusId | int |
| 7 | primaryCDI | string | | 19 | OwnershipStatusId | int |
| 8 | CollateralKey | string | | 20 | AssetAmount | decimal |
| 9 | ThesisCollateralID | string | | 21 | DateApplied | date |
| 10 | PortfolioId | int | | 22 | ReferenceDate | date |
| 11 | PortfolioName | string | | 23 | ETLLoadingDate | date |

## 3. Volume & distinct keys — PASS

| Metric | Dev (2026-09-03) | On-prem (2026-09-01) | Delta |
|---|---|---|---|
| Total rows | 349,193 | 349,232 | −39 (−0.01%) |
| Distinct collaterals (ThesisCollateralID) | 233,946 | 233,966 | −20 |
| Distinct properties (PropertyKey) | 207,227 | 207,247 | −20 |
| Distinct agreements | 151,057 | 151,080 | −23 |
| Distinct CDI | 148,832 | 148,855 | −23 |

All within ~0.01% — differences attributable to the one-day snapshot offset and SCD timing.

## 4. Transformation checks — PASS

| Check | Result |
|---|---|
| Status split (rows) | active 306,346 / inactive 42,847 — matches on-prem pattern |
| Status split (distinct collaterals) | active 214,165 / inactive 28,845 |
| `CollateralStatusId` reconstruction | 11157 = 321,512 · 11212 = 27,681 · **0 NULL** |
| `OwnershipStatusId` reconstruction | **0 NULL** (all 11006 / 11009) |
| `CollateralId = ThesisCollateralID` | 100% of rows |
| `PortfolioId` populated | **0 missing** |
| `primaryCDI` populated | **0 NULL** (INNER join to exposure Primary Owner) |
| `CDI` NoCDI fallback | 211 rows (148 active + 63 inactive) — expected for owners without a CDI |
| `ReferenceDate` | 2026-09-03 (= run_date) |
| `ETLLoadingDate` | 2026-09-11 (= run day) |
| Greek `CollateralREStatus` preserved | Yes (e.g. `Ενεργή`, `Ρευστοποιημένη`) |

## 5. Record-level spot-checks — PASS

Every field matched on-prem across EUR rows, inactive rows, multi-ownership rows, NoCDI-fallback rows, and multiple portfolios.

- **`GLBCH424956`** (Thesis 424956, Galaxy II, PortfolioId 20) — 2 rows (properties CH230779 / CH230780, OwnershipId 310138 / 310139). Notable correctness: property CH230779 has `CollateralREStatus = Ρευστοποιημένη` (→ `collateral_status_en = inactive`) while `CollateralStatusId = 11157` (the collateral-level status is still active). The two status fields are sourced independently and reproduced exactly.
- **EUCLID 338709 / 338710** (PortfolioId 67) — PropertyKey `NBG400579309_1` / `CH181565`, CDI, amounts and status ids all match. Collateral 338710 correctly emits two ownership rows (250839 → CDI `EUC5011313506`, 250840 → CDI `EUC1149596503`), preserving the multi-ownership grain.
- **NoCDI fallback 116288 / 116289** (Galaxy IV, PortfolioId 21) — `CDI = 'NoCDI - PropertyOwnerId = 88515'` / `'= 57113'` exactly as on-prem, with `primaryCDI = GLC0002200108` present.

## 6. Known issue / follow-up

**`CollateralAmountEuro` is NULL for all non-EUR rows in this dev run (1,060 rows).**

- **Cause:** `d-cdh.reference.exchangerates` has **no FX rate for 2026-09-03** (it holds 2026-09-07 and 2026-09-09). The cloud UDF `derivative.converttoeur` does an **exact `rate_date` match** and returns NULL when no rate exists for that date. EUR amounts pass through unchanged.
- **Not a code defect:** the UDF was verified working on a date with FX data — `converttoeur('CHF', 946000, DATE'2026-09-09')` = `1,005,954.91`. In prod, 2026-09-01 rates exist and conversion succeeded (`CHF → 671,186.93` in an earlier check).
- **Consistency with source:** on-prem `bi.convertToEuro` likewise left non-EUR `CollateralAmountEuro` NULL, so behaviour matches the legacy output.
- **Design note for follow-up:** because the UDF requires an exact-date rate, non-EUR amounts will be NULL whenever the FX feed is missing for the run date (weekends, holidays, feed gaps). If the external consumer requires non-EUR conversion on every run, consider falling back to the most recent available rate on/before `run_date`. Tracked as a follow-up; no change made pending consumer requirement.

## 7. Verdict

The notebook runs successfully on dev and reproduces the on-prem `CollateralsREAll` output faithfully: schema, volumes, distinct-key counts, status splits, reconstructed status ids, and record-level values all align within snapshot-timing tolerance. The only open item is the non-EUR `CollateralAmountEuro` FX-availability behaviour (Section 6), which mirrors the legacy source and depends on FX-feed completeness rather than notebook logic.
