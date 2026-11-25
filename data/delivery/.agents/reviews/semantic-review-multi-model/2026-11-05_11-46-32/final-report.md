# Final consolidated issues report — CDI party identity, party-gated Debt perimeter, and parent-grain portfolio_cdi_bal (origin/DNA26-1716_platforms..HEAD)

Issues are sorted from highest to lowest combined importance/confidence.

---

## 1. Debt/DebtAB party gate can silently deliver zero rows

**Severity: High**

- **User impact:** A bad or partial `primary.party` load overwrites four regulatory tables (DebtStatic, DebtDetail, DebtFinancial, DebtAB) with zero rows while the job reports SUCCESS. IT uploads an empty submission and nothing in the pipeline flags it.
- **Issue:** In `dtransfer_Debt.ipynb` and `dtransfer_DebtAB.ipynb` the former `LEFT JOIN party` became a `LEFT SEMI` gate, so row survival now depends on party coverage. The instrumentation computes `_drop_pct` and prints a warning above 1%, then writes regardless. There is no empty-result check and no row-count floor. Every assert added in this branch passes on empty input: the LoanId-parity assert compares three empty sets, and the NULL-`PartyId` asserts find no rows. `portfolio_cdi_bal` received exactly these guards in the same branch; the Debt notebooks did not.
- **Recommendation:** Port the `portfolio_cdi_bal` pre-write guards into both Debt notebooks: raise on empty output and enforce a day-over-day row-count floor (the 25% floor used there is a reasonable starting point). Given the regulatory consumer, promote the >1% gate-drop warning to a raise, or make the tolerated percentage an explicit parameter.

## 2. LoanId parity check collects roughly three million keys into the driver

**Severity: High** · *(cluster: Debt cross-table parity — see also #12, #20)*

- **User impact:** Driver memory pressure or an outright driver failure immediately before three regulatory overwrites, turning a correct run into a failed or aborted delivery at production cardinality.
- **Issue:** The write cell of `dtransfer_Debt.ipynb` calls `.collect()` on the distinct `LoanId` of `debt_static`, `debt_detail` and `debt_financial` and holds all three Python sets at once. At the expected ~1.04M loans per table with 64-character derivative IDs, that is hundreds of MB of Python string objects plus set overhead, for a comparison that is naturally distributable.
- **Recommendation:** Keep the check in Spark. Build the three distinct-ID DataFrames and prove equality with `left_anti` joins (or `exceptAll`) plus `limit(1).count()` in both directions, asserting on the counts rather than on materialized sets.

## 3. VatSettlement resolves tax numbers from all party history

**Severity: High** · *(cluster: party snapshot scope — see also #7)*

- **User impact:** The same CDI can carry a different tax number in `dtransfer_VatSettlement` than in `dtransfer_Customer` and `portfolio_cdi_bal` for the same business date, and a historical/backfill run can emit a tax number that did not exist on the requested date. Cross-deliverable reconciliation on the IT side fails.
- **Issue:** `dtransfer_VatSettlement.ipynb` builds `party_all` from `primary.party` with no `BusinessDate` predicate and ranks all history by tax presence, source priority, then descending date, while Customer and `portfolio_cdi_bal` restrict their `party_tax` lookup to the run date. The candidate sets differ, so the in-code comment claiming the dedup is "kept IN SYNC with dtransfer_Customer" is materially false even though the sort expressions match.
- **Recommendation:** Filter the VatSettlement party source to `run_date_lit` and reuse the same normalized tax-first lookup as Customer. If all-history resolution is deliberate, record it as an explicit business exception with a backfill test showing why cross-table disagreement is acceptable, and correct the comment.

## 4. Negative settlement participation is measured but never rejected

**Severity: High** · *(cluster: settlement participation contract — see also #5, #8)*

- **User impact:** Negative participation percentages flow into the delivered `bal_over_settlement`, understating settled balances in a regulatory submission with no visible symptom.
- **Issue:** In `portfolio_cdi_bal.ipynb`, the `_part_check` loop computes and prints `neg_cnt` but asserts only the null count and the maximum-value condition. A source with one negative row and one row above 1.0 passes every assertion, and the negative amount is then included in `SUM((ParticipationPercentage / 100.0) * SettlementAmount)`.
- **Recommendation:** Add `assert r["neg_cnt"] == 0` (with the source name in the message) before the settlement aggregation, alongside the existing null and scale assertions.

## 5. Participation scale is inferred from a per-source daily maximum

**Severity: High** · *(cluster: settlement participation contract — see also #4, #8)*

- **User impact:** The check is unsound in both directions. A source still on the 0-1 fractional scale that contains one outlier above 1.0 passes, and every amount in it is then divided by 100 — a 100x understatement of `bal_over_settlement`, delivered silently. Conversely, a legitimate 0-100 source whose values all fall at or below 1% is rejected and the whole delivery fails.
- **Issue:** `portfolio_cdi_bal.ipynb` gates on `assert (r["max_pp"] is None) or (float(r["max_pp"]) > 1.0)` per source. `max > 1` is not an invariant of a 0-100 percentage domain, and `max > 1` is not sufficient evidence of one either.
- **Recommendation:** Validate against an explicit upstream scale/version contract (source-level metadata or an agreed column semantic) rather than inferring the unit from one day's maximum. If a heuristic must remain as a backstop, make it distributional (share of rows ≤ 1, or the median) instead of max-based.

## 6. Nothing verifies that the party_tax lookup resolved anything

**Severity: High** · *(cluster: tax lookup robustness — see also #9, #10, #11)*

- **User impact:** `portfolio_cdi_bal` silently changes grain. With an empty or stale party snapshot, every `uc_id` collapses to its own CDI, parent roll-up disappears entirely (~322K parents become ~385K single-member parents), and balances are delivered without the aggregation IT expects. All four pre-write guards still pass.
- **Issue:** `party_tax` in `portfolio_cdi_bal.ipynb` is scoped to `BusinessDate = '{datadate}'` on `primary.party`, while `datadate` comes from `primaryactive.exposure`. If the two zones are not loaded in lockstep the lookup returns nothing, `COALESCE(pt.TaxRegistrationNumber, e.CDI)` degrades to CDI, and the guards remain satisfied: non-empty passes, reconciliation passes (the aggregate is internally consistent under a 1:1 map), one-row-per-CDI passes, and the row count rises slightly so the floor passes.
- **Recommendation:** Assert that `party_tax` is non-empty and that `distinct(uc_id) / distinct(CDI)` stays within a band around the verified ~0.84 ratio, raising before the overwrite if it does not.

## 7. Customer and Debt build from different party scopes, producing orphan PartyId values

**Severity: High** · *(cluster: party snapshot scope — see also #3)*

- **User impact:** `DebtStatic.PartyId` joined to `Customer.PartyId` — the join IT actually performs — returns orphans. A CDI can survive the Debt party gate on the strength of a historical party row and have no row in the delivered Customer table.
- **Issue:** `dtransfer_Debt.ipynb` and `dtransfer_DebtAB.ipynb` read `spark.table(f"`{env}-cdh`.primary.party")` with no `BusinessDate` filter (~805K parties), while `dtransfer_Customer.ipynb` filters to `run_date_lit` (~686K). The new cross-table assert covers `LoanId` parity between the three Debt tables only; nothing checks `PartyId` against the Customer deliverable.
- **Recommendation:** Settle on one party scope across the deliverables, or add an explicit assert that `DebtStatic.PartyId ⊆ Customer.PartyId` for the run date and fail the delivery when it does not hold.

## 8. An empty settlement snapshot passes every guard

**Severity: Medium** · *(cluster: settlement participation contract — see also #4, #5, #17)*

- **User impact:** If `dsettlementexposure` has no qualifying rows for the date, every delivered row gets a NULL `bal_over_settlement` and the delivery is accepted as a clean refresh.
- **Issue:** In `portfolio_cdi_bal.ipynb`, an empty `_part_check` means the validation loop executes no assertions at all. Settlement amounts are nullable enrichments, so the non-emptiness, grain, reconciliation and row-floor guards are all indifferent to their absence.
- **Recommendation:** Require a non-empty settlement perimeter before aggregation, and compare the observed source set and row counts against the expected sources or the previous successful run.

## 9. Tax number becomes a grouping key with no plausibility bound

**Severity: Medium** · *(cluster: tax lookup robustness — see also #6, #10, #11)*

- **User impact:** A placeholder or shared tax value merges unrelated customers into one `uc_id`, sums their balances, and fans the combined total out to every member CDI; the same value merges them under one `ParentPartyId` in Customer. Reconciliation passes because the aggregate is internally consistent.
- **Issue:** `portfolio_cdi_bal.ipynb` asserts `cdi_map` uniqueness — one CDI maps to one `uc_id` — but nothing bounds the other direction, how many CDIs a single `uc_id` may absorb. The tax number is used as the parent key without any format validation.
- **Recommendation:** Add a maximum-CDIs-per-`uc_id` guard (or a top-N report that raises past a threshold) and validate the tax-number format before it is promoted to a grouping key.

## 10. The party_tax ordering is not a total order

**Severity: Medium** · *(cluster: tax lookup robustness — see also #6, #9, #11)*

- **User impact:** A CDI with several same-source, same-date rows carrying different non-blank tax numbers gets an arbitrary winner, so `CustomerVat`, `ParentPartyId` and `uc_id` can change between runs for unchanged input.
- **Issue:** The `party_tax` window in `dtransfer_Customer.ipynb` (and its SQL equivalent in `portfolio_cdi_bal.ipynb`) orders by tax presence, source priority, then `BusinessDate DESC`. Ties beyond that are unresolved, and `row_number` offers no stability guarantee across evaluations.
- **Recommendation:** Append a deterministic final tie-break to the `ORDER BY` — `TaxRegistrationNumber` itself is sufficient and makes the choice reproducible.

## 11. party_tax is uncached and re-evaluated four times

**Severity: Medium** · *(cluster: tax lookup robustness — see also #6, #10)*

- **User impact:** Combined with the non-total ordering, two evaluations can disagree and the `base ⋈ cdi_map` inner join on `(PortfolioId, uc_id)` loses affected parents. The reconciliation guard then raises, so the symptom is a non-deterministic job failure rather than bad data, but the delivery is blocked for no real reason.
- **Issue:** In `portfolio_cdi_bal.ipynb`, `party_tax` is registered as a lazy temp view and never cached, so it is evaluated independently in `base`, `cdi_map`, the initialization-balance subquery and `settlements`.
- **Recommendation:** Materialize `party_tax` once (cache, or write to a temporary Delta table) so all four consumers read the same resolved mapping.

## 12. A NULL LoanId breaks the parity assert and blocks all three Debt writes

**Severity: Medium** · *(cluster: Debt cross-table parity — see also #2)*

- **User impact:** One NULL key fails the delivery of DebtStatic, DebtDetail and DebtFinancial together, with an assertion message that does not point at the cause.
- **Issue:** In `dtransfer_Debt.ipynb`, a NULL `ExposureDerivativeID` enters `_ls_static` as `None` but can never match the `left_semi` gate used to build detail and financial, so the three sets can never be equal. The assert sits before all three writes.
- **Recommendation:** Reject NULL IDs explicitly with their own assertion and message before the parity comparison, so the diagnosis is unambiguous.

## 13. The portfolio_cdi_bal zero-row path still exits as SUCCESS

**Severity: Medium**

- **User impact:** The most likely real failure — upstream `dexposure` missing for `datadate` — is reported as a clean run, so orchestration treats a skipped delivery as a good refresh and stale data stays in place unnoticed.
- **Issue:** Step 3 of `portfolio_cdi_bal.ipynb` still uses `dbutils.notebook.exit("No data retrieved from base calculation. ...")` on `base_count == 0`, while the Step 7 guards added in this branch deliberately raise for exactly this reason.
- **Recommendation:** Make the Step 3 empty-base path raise, consistent with the Step 7 guards and the rationale the branch already documents.

## 14. Reconciliation guard uses an absolute €1 tolerance and covers one column

**Severity: Medium**

- **User impact:** A good delivery can fail non-deterministically, and drift in four other delivered amount columns goes unchecked.
- **Issue:** In `portfolio_cdi_bal.ipynb`, `assert recon_diff < 1.0` compares two double sums over hundreds of thousands of values, computed through different aggregation paths and partition orders, on a total in the tens of billions — float summation error at that magnitude is not reliably below €1. The guard also reconciles only `Total_balance_EUR`; `on_balance_EUR`, `off_balance_EUR`, `initial_assign_bal_EUR` and `bal_over_settlement` are unverified.
- **Recommendation:** Switch to a relative tolerance keyed to the parent total (for example 1e-9 of the total, with a small absolute floor) and extend the reconciliation to the remaining amount columns.

## 15. dtransfer_AllAccounts still hardcodes the p-cdh catalog

**Severity: Medium**

- **User impact:** A QA or dev run of `dtransfer_AllAccounts` reads production data, which contradicts the branch's stated objective that no hardcoded production catalog values remain anywhere.
- **Issue:** `platforms/dtransfer_AllAccounts` calls `` `p-cdh`.primary.get_all_exposures `` in executable code, on the line above an f-string that already uses `{env}`. Pre-existing and outside this diff, but it leaves the objective unmet at repo level.
- **Recommendation:** Replace the literal with the `{env}` prefix from `aws_utils.get_env_prefix()`, and grep the repo for remaining executable `p-cdh` literals before release.

## 16. No execution evidence for the new logic

**Severity: Medium**

- **User impact:** The identity rule, settlement source switch, percentage scaling, fan-out totals and Debt driver behaviour reach a regulatory delivery unproven. Local validation (diff check, JSON parse, Python AST parse) does not analyse Spark SQL or source data.
- **Issue:** All five modified notebooks have empty outputs and zero execution counts, and no automated test exercises the new behaviour.
- **Recommendation:** Run the notebooks in QA for a representative date and compare row counts, distinct keys, tax/parent mappings, settlement totals and Debt cross-table LoanId parity against the documented on-prem controls. Include explicit negative and empty participation cases, and run Debt at production-scale cardinality after the driver collection is replaced.

## 17. Unmatched settlements are dropped with no logging

**Severity: Low** · *(cluster: settlement participation contract — see also #8)*

- **User impact:** Settlement amounts can disappear from the delivery with no trace, which is the same invisibility that allowed the previous `cdi_map` reparenting bug to survive undetected.
- **Issue:** Step 6 of `portfolio_cdi_bal.ipynb` drops settlements whose parent is absent from the output via the LEFT join. That matches on-prem's `MERGE ... WHEN MATCHED` semantics, but neither the dropped row count nor the dropped amount is reported.
- **Recommendation:** Log the count and summed amount of unmatched settlements, and raise if either exceeds an agreed threshold.

## 18. VatSettlement row drops are not instrumented

**Severity: Low**

- **User impact:** Settlements silently vanish from the deliverable with no count or percentage in the run log, so post-QC-decommission CDI linkage loss — the cause the code comment itself names — is invisible.
- **Issue:** `dtransfer_VatSettlement.ipynb` changed the party join from LEFT to `inner` and added `.filter(col("taxNumber").isNotNull())`. Both are needed (the inner join proves CDI existence, the filter catches a dedup winner with no tax number), but unlike the Debt gates neither reports what it removed.
- **Recommendation:** Print the before/after counts and drop percentage for both the inner join and the tax filter, matching the Debt gate instrumentation.

## 19. Row-count floor is skipped when the catalog or schema is missing

**Severity: Low**

- **User impact:** A configuration error (wrong catalog or schema) is treated as a first run, so the day-over-day floor check is bypassed and a suspect delivery is written unguarded.
- **Issue:** The Guard 3 handler in `portfolio_cdi_bal.ipynb` classifies `AnalysisException` by substring; the bare `"NOT FOUND"` test also matches `SCHEMA_NOT_FOUND` and `CATALOG_NOT_FOUND`, which set `prev_count = None`.
- **Recommendation:** Match on the Unity Catalog error class for a missing table only, and let schema/catalog errors propagate.

## 20. dtransfer_Debt recomputes the DebtStatic DAG about ten times

**Severity: Low** · *(cluster: Debt cross-table parity — see also #2)*

- **User impact:** Long, costly runs for the largest deliverable, and a wider window in which an upstream change can shift results between recomputations.
- **Issue:** Nothing in `dtransfer_Debt.ipynb` is cached, and `debt_static` is referenced by its own `count()`, the PartyId assert, both semi-join gates, the parity collect and the write — each one re-expanding a window over `primary.exposure ⋈ dexposure ⋈ portfolio` plus five enrichment joins and a `distinct()`.
- **Recommendation:** Gate the detail and financial tables on `debt_static_gated.select("LoanId")` instead of the fully enriched `debt_static` (the enrichment joins are all LEFT and cannot drop loans), and cache that gated key set.

## 21. The NULL-PartyId asserts cannot fail

**Severity: Low**

- **User impact:** False confidence in an invariant that is never actually tested, at the cost of a full extra pass over the pipeline in each notebook.
- **Issue:** `dtransfer_Debt.ipynb` and `dtransfer_DebtAB.ipynb` assert that no row has a NULL `PartyId`, but `PartyId` is `exp_CDI` and `exp_CDI` already survived a `LEFT SEMI` join on `exp_CDI = pty_CDI`, which NULL cannot satisfy.
- **Recommendation:** Drop both asserts, or move the check upstream of the party gate where a NULL CDI is actually possible.

## 22. portfolio_cdi_bal schema change needs an explicit consumer note

**Severity: Low**

- **User impact:** Positional readers break: the table gains a `uc_id` column and `bal_over_settlement` / `initial_assign_bal_EUR` swap positions. Separately, `bal_over_settlement` and the balance columns repeat across every CDI of a multi-CDI parent, so a consumer summing them across rows double-counts.
- **Issue:** The repetition is the intended on-prem behaviour being reproduced, not a defect, but combined with the new column and the reordering it is a consumer-visible contract change that has to be stated rather than inferred.
- **Recommendation:** Confirm the IT-facing release note covers the new `uc_id` column, the column reordering, and the parent-grain repetition of balance and settlement amounts.

## 23. Stale d-cdh references in portfolio_cdi_bal documentation

**Severity: Low**

- **User impact:** A reader of the notebook is told the job reads a specific environment's table while the code resolves the catalog from `env`, inviting a wrong assumption during the next change.
- **Issue:** The markdown in `portfolio_cdi_bal.ipynb` still names `d-cdh.reference.initializationexposureinfo` twice (design-decisions list and mapping table) after the code was parameterized.
- **Recommendation:** Update both references to the `{env}`-prefixed form.
