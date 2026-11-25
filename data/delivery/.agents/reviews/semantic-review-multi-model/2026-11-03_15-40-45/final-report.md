# Final consolidated issues report — CDI-based party identity across dtransfer tables and uc_id/CDI balance fan-out in portfolio_cdi_bal (4ecf465..5fdbe10)

Issues are sorted from highest to lowest combined importance/confidence.

---

## 1. Settlement statement cannot execute (type mismatches)

**Severity: High** · *(cluster: bal_over_settlement — see also #2, #3, #11)*

- **User impact:** The reworked parent-grain settlement logic has never run. `bal_over_settlement` stays unpopulated in `portfolio_cdi_bal`, so consumers see no settlement balance at all for the new grain.
- **Issue:** In `portfolio_cdi_bal/portfolio_cdi_bal.ipynb`, the settlement query compares `se.IsActiveFlag = true` while `primaryactive.settlementexposure.IsActiveFlag` is `INT`, and `pf.processSettlementFlag = 1` while `primaryactive.portfolio.processSettlementFlag` is `BOOLEAN`. Both raise `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` against the live schemas. The lines pre-date this change but sit inside the statement the commit rewrites.
- **Recommendation:** Use `se.IsActiveFlag = 1` and `pf.processSettlementFlag = true` (or cast explicitly), then re-run the settlement block end to end before shipping.

## 2. Settlement amounts are ~100x on-prem

**Severity: High** · *(cluster: bal_over_settlement — see also #1, #3, #11)*

- **User impact:** Delivered settlement balances are inflated by two orders of magnitude (cloud avg €1,037,833 vs on-prem €10,294; sum €1.56B vs €190.4M) on a regulatory feed.
- **Issue:** The settlement aggregation in `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` multiplies `ParticipationPercentage * SettlementAmount`, but `settlementexposure.ParticipationPercentage` ranges 0–100 (percent), where the on-prem SP expression consumes a fraction.
- **Recommendation:** Divide the percentage by 100 (or confirm the exact on-prem factor with the SP owner) before the sum, and reconcile the resulting total against on-prem's €190.4M.

## 3. Settlement coverage collapses on NULL participation percentage

**Severity: High** · *(cluster: bal_over_settlement — see also #1, #2, #11)*

- **User impact:** Only 1,586 rows end up with a settlement amount against 18,493 on-prem — roughly 91% of settlement balances silently go missing from the delivered table.
- **Issue:** 23,170 of 49,900 active settlements (46%) have a NULL `ParticipationPercentage`, which NULLs the whole `ParticipationPercentage * SettlementAmount` product (`SettlementAmount` itself is never NULL). 7,699 of 9,204 parent groups end up NULL in `portfolio_cdi_bal/portfolio_cdi_bal.ipynb`.
- **Recommendation:** Decide the intended default (e.g. `COALESCE(ParticipationPercentage, 100)`) or fix the upstream population of the column, then verify the populated-row count against on-prem's 18,493.

## 4. Fan-out can overwrite the target with an empty or partial result

**Severity: High** · *(cluster: validation gaps — see also #12, #13, #14)*

- **User impact:** A CDI-resolution failure can replace a good `portfolio_cdi_bal` table with an empty or silently truncated one while the run reports success. Balances can also disappear without a trace.
- **Issue:** `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` guards only `base_count == 0` (line ~301). `cdi_map` filters out NULL CDIs (line ~253) and is INNER joined to `base` (line ~363), so `base` can be populated from tax data while the final frame is empty, and exposures with a NULL CDI in `base` have their balances dropped by that join with no reconciliation. The write is an unconditional overwrite.
- **Recommendation:** Validate `cdi_map` and the final frame before the overwrite: require non-zero rows, assert the parent-grain balance total equals the fanned-out total, and add a date-over-date row/fan-out threshold.

## 5. VatSettlement overwrites a knowingly incomplete perimeter

**Severity: High** · *(cluster: VatSettlement — see also #8)*

- **User impact:** The delivered VAT settlement table is a full replacement carrying ~35% fewer rows than on-prem (~49.7K vs 76,455), so IT consumes a materially incomplete regulatory dataset with no signal that it is short.
- **Issue:** `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` documents the gap at line ~77 and still overwrites the target at line ~220. The gap decomposes into two separate causes that are not recorded: 33,487 distinct active settlements vs 37,406 on-prem (-10.5%) and 1.49 accounts per settlement vs 2.04 (-27%) — i.e. missing settlement↔account links, not the active-status filter.
- **Recommendation:** Record the decomposition in the notebook and either resolve the missing settlement↔account links or obtain explicit business acceptance plus enforced row-count/distinct-settlement thresholds that block the overwrite when coverage drops below the approved floor.

## 6. PartyId value domain no longer fits the consuming schema

**Severity: High** · *(cluster: party identity — see also #9, #15)*

- **User impact:** Loads into the on-prem targets will fail or truncate: `dtransfer.Customer.PartyId`, `DebtStatic.PartyId` and `DebtAB.PartyId` are `int NOT NULL`, while 2,041,720 of 2,293,224 cloud party rows (89%) have a CDI that is not int-castable and up to 23 characters long.
- **Issue:** `platforms/dtransfer_Customer/dtransfer_Customer.ipynb`, `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` and `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` now publish `PartyId = CDI` and `ParentPartyId = COALESCE(TaxRegistrationNumber, CDI)` (60,319 rows have a non-int-castable tax number). The agreement with IT exists only as a notebook comment, with no corresponding DDL change tracked. `PRC.operation.portfolio_cdi_bal` is already wide enough (`uc_id nvarchar(30)`, `CDI nvarchar(40)`), so the problem is confined to the three int columns.
- **Recommendation:** Track the widening of the three `PartyId` columns (and `ParentPartyId`) as a schema change on the consuming side in a durable artefact (ticket/DDL script), and do not ship the identity change ahead of it.

## 7. Reference-date lag means the new grain is never delivered

**Severity: High**

- **User impact:** `portfolio_cdi_bal` reports a clean run while the delivered table keeps its old shape with no `uc_id` column — the whole change is a silent no-op in production.
- **Issue:** `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` derives the reference date from `primaryactive.exposure` (max 2026-11-02) but reads balances from `derivative.dexposure` (max 2026-10-30), so `base` is empty and the notebook exits at its safety check. The date derivation is pre-existing, but it blocks this change from taking effect.
- **Recommendation:** Pin the reference date to the table actually read (`derivative.dexposure`) or pass it in explicitly as a parameter, and confirm the new grain reaches the target.

## 8. VatSettlement AccountNumber emits keys outside the domain IT matches on

**Severity: High** · *(cluster: VatSettlement — see also #5)*

- **User impact:** 8,147 newly added rows carry account numbers IT cannot match against anything, and the pre-existing loan-level rows are keyed differently from on-prem — the extra rows add noise rather than coverage.
- **Issue:** `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` sets `AccountNumber = COALESCE(ExposureKey, AgreementKey)`. Every `AgreementKey` is a dash composite (`CSR7009902691-0000000002649116`), while the on-prem target holds 71,509 distinct account numbers of 11–19 characters with zero dashes, 45,546 of them carrying a 3-char portfolio prefix. Cloud `ExposureKey` in `settlementexposure` is bare numeric where on-prem is prefixed. `nvarchar(100)` means nothing truncates, so the mismatch is silent.
- **Recommendation:** Agree the expected key format with IT, then normalise both branches (strip/compose the agreement key, add the portfolio prefix to loan-level keys) before including the agreement-level rows.

## 9. uc_id provenance disagrees with the published ParentPartyId

**Severity: High** · *(cluster: party identity — see also #6, #11, #15)*

- **User impact:** The parent keys in two deliverables cannot be joined for 3,063 CDIs, and those customers do not roll up with their siblings, so their parent-level balances are understated.
- **Issue:** `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` derives `uc_id` from `dexposure.PartyTaxRegistrationNumber`, which is NULL for 3,063 CDIs whose `primary.party.TaxRegistrationNumber` is populated. Those rows become their own parent, while `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` publishes the tax number as `ParentPartyId` for the same CDIs.
- **Recommendation:** Derive `uc_id` from the party table (or coalesce `dexposure.PartyTaxRegistrationNumber` through `primary.party.TaxRegistrationNumber`) so both deliverables emit the same parent key.

## 10. Output column order does not match the on-prem table

**Severity: Medium**

- **User impact:** Under a positional load, two financial columns swap silently, putting settlement amounts in the initial-assignment column and vice versa on a regulatory table.
- **Issue:** The final select in `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` ends `..., off_balance_EUR, initial_assign_bal_EUR, bal_over_settlement`, while `PRC.operation.portfolio_cdi_bal` ends `..., off_balance_EUR, bal_over_settlement, initial_assign_bal_EUR`. Both are decimals, so no type error surfaces.
- **Recommendation:** Reorder the final select to match the target DDL, and confirm with the ingestion owner whether the load is positional or by name.

## 11. Settlement parent mapping drops settlements that on-prem keeps

**Severity: Medium** · *(cluster: bal_over_settlement / party identity — see also #3, #9)*

- **User impact:** €129.1M of settlement balance (2,492 of 20,203 in-perimeter rows, 7.6%) never reaches a parent, including roughly €48.1M belonging to parents that are present in the output.
- **Issue:** Because `cdi_map` in `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` is built from `dexposure`, the INNER join cannot resolve a settlement whose CDI holds no exposure in that portfolio. On-prem resolves the parent through `DWH.prt.Party`, so it still credits the parent when the settling party has no loans.
- **Recommendation:** Build the CDI→uc_id map from the party table (with the standard dedup to keep it unique per CDI), which also resolves #9.

## 12. The party existence gate drops rows without logging

**Severity: Medium** · *(cluster: validation gaps — see also #4, #13, #14)*

- **User impact:** A party-coverage regression upstream would quietly shrink a regulatory table with nothing in the run output to show it happened.
- **Issue:** `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` and `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` replace the party LEFT JOIN with a `left_semi` gate but never report how many rows it removes. On current data the effect is negligible (45 null CDIs, zero unresolvable CDIs at 2026-11-02), which is exactly why a future regression would go unnoticed.
- **Recommendation:** Print pre/post counts around the gate and fail (or warn) when the drop exceeds a small threshold.

## 13. No uniqueness guard on the new (portfolio_code, CDI) grain

**Severity: Medium** · *(cluster: validation gaps — see also #4, #12, #14)*

- **User impact:** A CDI could be delivered twice under two different parents with two different balance sets, double-counting that customer in the consumed table.
- **Issue:** `uc_id` is computed per exposure row in `portfolio_cdi_bal/portfolio_cdi_bal.ipynb`, so a CDI whose `PartyTaxRegistrationNumber` is populated on some exposures and NULL on others lands under two parents. Verified clean today (385,324 map rows for 385,324 distinct pairs), but nothing enforces it — unlike `dtransfer_VatSettlement`, which gained an assertion in this same range.
- **Recommendation:** Add an assertion that `(portfolio_code, CDI)` is unique in the final frame before the write.

## 14. No executable parity or regression gate for the changed semantics

**Severity: Medium** · *(cluster: validation gaps — see also #4, #12, #13)*

- **User impact:** Every correctness claim behind this change rests on manual one-off queries, so a future edit can break a regulatory deliverable without any check failing.
- **Issue:** The only programmatic contract check added in the range is the null-`taxNumber` assertion in `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb`. Unchecked: `PartyId == CDI`, DebtStatic/DebtAB row retention, fan-out cardinality, parent-grain vs fanned balance reconciliation, and VatSettlement coverage.
- **Recommendation:** Add in-notebook assertions for these invariants, or a repeatable validation notebook run before the delivery writes.

## 15. Dedup tie-break diverges between VatSettlement and Customer

**Severity: Medium** · *(cluster: party identity — see also #6, #9)*

- **User impact:** The same customer can carry a tax number in `dtransfer_vatsettlement` and a NULL `CustomerVat` in `dtransfer_customer`, and the winning row may come from a lower-trust source system in one table than the other.
- **Issue:** `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` ranks a non-null `TaxRegistrationNumber` ahead of the standard source-system priority, an ordering not applied in `platforms/dtransfer_Customer/dtransfer_Customer.ipynb`. 401,036 CDIs have both null and non-null tax rows, the upper bound on the divergence.
- **Recommendation:** Use one dedup ordering across both notebooks (preferably a shared helper), or document explicitly why the two tables resolve the same CDI differently.

## 16. Delivered shape of portfolio_cdi_bal changes without consumer notice

**Severity: Medium**

- **User impact:** Consumers get an extra column and the same parent balances repeated across member CDIs, so naive row-level sums rise about 3% (€50.57B fanned vs €47.45B at parent grain) — anyone aggregating the table without grouping by parent will report inflated totals.
- **Issue:** `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` splits the single `CDI` key into `uc_id` plus `CDI` and overwrites the target with the new grain. This matches the on-prem SP (on-prem's €51.05B double-counts the same way), but it is a breaking shape change for anything reading the cloud table today.
- **Recommendation:** Notify consumers of the new column and the fan-out grain before the overwrite ships, and state in the notebook docs that row-level sums must be grouped by `uc_id`.

## 17. Steering checklist is stale and now circularly referenced

**Severity: Low**

- **User impact:** Anyone following the documented migration rules implements the old identity mapping and validates against obsolete row counts.
- **Issue:** `.kiro/steering/platform-migration-checklist.md` still records `PartyId = source-system PartyKey` and hardcoded expected counts, while `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` now defers to that checklist for "the current expected count" — a circular reference until the checklist is updated.
- **Recommendation:** Update the checklist in this change with the CDI identity rule, the `ParentPartyId` fallback, and the current expected counts.

## 18. Broken markdown table in the VatSettlement documentation cell

**Severity: Low**

- **User impact:** The mapping documentation renders incorrectly, and the last three mappings read as loose text instead of table rows.
- **Issue:** In `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb`, the perimeter-gap blockquote is inserted between rows of the mapping table, terminating the table early.
- **Recommendation:** Move the blockquote above or below the table.
