# Final consolidated issues report — CDI party identity, derivative settlement sourcing and pre-write guards for the PLTFRM dtransfer delivery (origin/DNA26-1716_platforms..HEAD)

Issues are sorted from highest to lowest combined importance/confidence.

---

## 1. ParticipationPercentage divided by 100 for all sources while production is still mixed-scale

**Severity: High** · *(cluster: settlement participation contract — see also #7)*

- **User impact:** Settlement amounts delivered to the regulatory platform are understated by 100x for every profits-sourced settlement. On the measured perimeter that is ~€1.38M delivered against ~€138.2M actual, roughly 88% of the perimeter settlement total missing from `bal_over_settlement`. Nothing in the run signals the error.
- **Issue:** `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` (settlement query, `(ParticipationPercentage / 100.0) * SettlementAmount`) re-sources settlements from `derivativeactive.dsettlementexposure` and assumes the unified 0–100 contract. Production does not yet provide it: profits rows are on a 0–1 scale (all 14,717 perimeter rows exactly `1.0000`; 31,992–39,726 active rows table-wide with max `1.0000`), qca rows are 0–100. The notebook comment asserts the unified scale as fact, so a reader cannot tell the code is ahead of the data.
- **Recommendation:** Make the upstream `dsettlementexposure` scale unification a hard release gate for this notebook, including rerun sequencing for the business date consumed here, and do not run or release before it is deployed and verified. Add a per-`SourceSystem` range check before aggregation (e.g. `MAX(pp) > 1 AND MAX(pp) <= 100`) that fails the run, and log per-source min/max/distribution so a mixed-scale snapshot can never be delivered silently.

## 2. Three notebooks resolve the CDI→tax number three different ways

**Severity: High** · *(cluster: party identity resolution — see also #4)*

- **User impact:** `dtransfer_Customer.ParentPartyId` and `portfolio_cdi_bal.uc_id` are documented as the same parent identifier but disagree for thousands of customers (3,063 exposed CDIs measured with different parent keys), so balances roll up under one parent key while the customer record is delivered under another. Reconciliation between the two delivered tables is impossible.
- **Issue:** `dtransfer_Customer` ranks party rows filtered to `BusinessDate = run_date` with ordering [tax non-null, source priority, BusinessDate desc]; `dtransfer_VatSettlement` applies the same ordering over all history; `portfolio_cdi_bal.party_tax` uses all history with [source priority, BusinessDate desc] and no tax tie-break. The `party_tax` comment claims "the same source priority as dtransfer_Customer" and the design note claims `uc_id` "agrees with dtransfer_Customer.ParentPartyId"; neither matches runtime behaviour. 401,061 CDIs in `primary.party` have both null-tax and non-null-tax rows, bounding the exposure.
- **Recommendation:** Extract one CDI→tax lookup with a single documented date scope and deterministic tie-breakers, and reuse it (or a persisted shared mapping) in `dtransfer_Customer`, `dtransfer_VatSettlement` and `portfolio_cdi_bal`. Remove or correct the "in sync"/"agrees with" comments, and measure the residual divergence count before shipping.

## 3. Blank and whitespace-only tax numbers bypass the CDI fallback

**Severity: High**

- **User impact:** Customers are delivered with an empty `ParentPartyId`, VatSettlement rows with an empty `taxNumber`, and in `portfolio_cdi_bal` all blank-tax customers in a portfolio collapse into a single empty `uc_id` — their balances are summed into one fictitious parent and then repeated onto every member CDI. 44,042 blank-tax winners were found under the new Customer ranking, 9,188 of them inside the current exposure perimeter.
- **Issue:** `coalesce(TaxRegistrationNumber, CDI)` in `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` (party and representative branches) and throughout `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` (`party_tax`, `base`, `cdi_map`, settlement query) treats `''` and `'   '` as present, as does the `taxNumber isNotNull` filter in `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb`. The new uniqueness and balance-reconciliation asserts accept this state: each CDI still maps to one parent and the grand total is conserved.
- **Recommendation:** Normalize with `NULLIF(TRIM(TaxRegistrationNumber), '')` before ranking, and use the normalized column in every fallback, filter and join. Assert that delivered `ParentPartyId`, `uc_id` and `taxNumber` are non-blank, and reject any empty parent key associated with more than one CDI.

## 4. Customer tax tie-break silently changes the canonical party row

**Severity: High** · *(cluster: party identity resolution — see also #2)*

- **User impact:** Delivered customer attributes — `LegalNameGR`/`LastNameGR`/`FirstNameGR`, `PrimaryCommercialGR`, `BirthDate`, `PartyType` and `PartyKey`→`cust_id` — change for customers whose highest-priority source row has no tax number, because the whole record now comes from a lower-priority source. The tax-first ranking changed the winning source for 34,094 CDIs on the current-date party population.
- **Issue:** `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` ranks the entire party record by [tax non-null, source priority, BusinessDate desc] to obtain the tax number, so attribute provenance follows the tax tie-break. The commit framing presents this purely as a dedup alignment. The `BusinessDate desc()` tie-break is also inert there, since the frame contains a single date.
- **Recommendation:** Keep the canonical Customer row on the established source-priority/latest policy and resolve the tax number through the separate CDI→tax lookup from #2, so parent identity and attribute provenance are decoupled. If the attribute change is intended, measure and record the attribute churn and get it confirmed before shipping.

## 5. Pre-write guards fail open and rejected deliveries report success

**Severity: High**

- **User impact:** A rejected delivery (empty result, >25% row drop) ends the notebook green, so the orchestration and every downstream dependency see a successful refresh while the regulatory table still holds the previous day's data. A transient read error on the target silently disables the regression check and lets a bad overwrite through.
- **Issue:** In `portfolio_cdi_bal/portfolio_cdi_bal.ipynb`, Guards 1 and 3 reject via `dbutils.notebook.exit(...)`, which Databricks treats as successful termination. Guard 3's `try: prev_count = spark.table(target_fqn).count() / except Exception: prev_count = None` is scoped in the comment to "missing table = first run" but swallows permission errors, metastore timeouts and malformed-table errors as well.
- **Recommendation:** Raise a task-failing exception for rejected deliveries, including the old/new dates and counts in the message. Narrow the exception handler to the specific table-not-found condition so a genuine first run still passes and every other target-read failure stops the write. If `notebook.exit` must stay anywhere, confirm the Databricks workflow surfaces the exit message as an alert.

## 6. Party gate warns while the LoanId parity assert aborts, and relationship-level loss goes undetected

**Severity: High**

- **User impact:** Two opposite policies for the same upstream condition. A CDI-coverage blip above the 1% tolerance either blocks the entire Debt delivery (all three tables, no override) or, when loans keep at least one valid relationship, publishes DebtStatic/DebtAB with missing guarantor and co-owner rows while every check passes.
- **Issue:** `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` and `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` print a non-blocking warning above a 1% party-gate drop and still overwrite. Three cells later `dtransfer_Debt` asserts strict `LoanId` set equality across `debt_static`/`debt_detail`/`debt_financial`, but `debt_detail` and `debt_financial` come from `dexposure` with no party gate — so the assert hard-fails exactly in the case the 1% threshold exists to tolerate. The LoanId assert is also blind to relationship-only loss, and `dtransfer_DebtAB` has no relationship-count guard at all.
- **Recommendation:** Pick one policy: either gate `debt_detail`/`debt_financial` with the same LEFT SEMI join so parity holds by construction and the warning remains the single control, or make the parity check threshold-based. Either way, make the party-gate threshold block delivery (or publish it to a quality gate that does) and validate relationship-type distributions, not just distinct LoanIds.

## 7. Null, negative and out-of-range participation values are unvalidated

**Severity: Medium** · *(cluster: settlement participation contract — see also #1)*

- **User impact:** A NULL participation makes `SUM` skip that settlement entirely, so the delivered amount is understated with no trace; a negative participation produces a negative settlement amount that no guard rejects. Production already carries both: three null profits rows and qca values down to `-0.0100`.
- **Issue:** The settlement query in `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` documents "no COALESCE default is needed - the column is fully populated", which is load-bearing on an upstream property the notebook does not check, and applies no sign or range bound.
- **Recommendation:** Fail the run on null participation inside the perimeter, and decide explicitly whether negative participation is valid — if it is, document why; if not, assert it away. Pair this with the per-source range check from #1.

## 8. String PartyId/ParentPartyId depends on target DDL widening with no gate in this repo

**Severity: Medium**

- **User impact:** If the notebooks reach prod before the consuming int columns are widened, the platform upload fails on column type rather than on content, blocking the delivery entirely.
- **Issue:** `PartyId` moves from NULL (Customer) / source-system `PartyKey` (Debt, DebtAB) to the alphanumeric CDI, and `ParentPartyId` from NULL to `COALESCE(TaxRegistrationNumber, CDI)`. `.kiro/steering/platform-migration-checklist.md` records IT sign-off on the string domain and notes the int→string widening is "tracked separately" — a cross-team dependency with nothing in this repo enforcing the ordering.
- **Recommendation:** Confirm the target DDL widening is deployed before these notebooks run in prod, and record the dependency in the release checklist alongside the `dsettlementexposure` dependency from #1.

## 9. No date-scoped execution or output reconciliation accompanies the change

**Severity: Medium**

- **User impact:** Correctness of regulatory outputs rests on reasoning and read-only spot queries. Any of the data states above could already be wrong in the delivered tables without anyone knowing until the platform rejects or mis-reports.
- **Issue:** Verification is JSON validity, cell compilation and ad-hoc prod queries. No fixed-date run of the five changed notebooks against a non-production target exists, and no output-level comparison of keys, distributions or financial totals against the accepted on-prem baselines. `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` also replaced its hardcoded expected count (`1,608,266`) with a pointer to the migration checklist, while the checklist row still carries pre-gate figures — removing the only number a reader could check.
- **Recommendation:** Run all changed notebooks for one pinned business date against a non-prod target and apply the repository's verification levels: parent/CDI cardinality, blank and null key counts, per-source participation distributions, Debt/DebtAB relationship-type counts, settlement and balance totals, and on-prem vs cloud row counts. Refresh the checklist with the post-gate expected numbers.

## 10. Debt, DebtAB and VatSettlement overwrite regulatory tables with no pre-write guard

**Severity: Medium**

- **User impact:** One bad upstream day empties or halves any of these three regulatory tables and the delivery replaces good data without complaint — the same failure mode `portfolio_cdi_bal` now has three guards against.
- **Issue:** `platforms/dtransfer_Debt`, `platforms/dtransfer_DebtAB` and `platforms/dtransfer_VatSettlement` all end in an unconditional `.mode("overwrite")` with no empty-result check and no row-count floor, despite identical blast radius to `portfolio_cdi_bal`.
- **Recommendation:** Extend the empty-result and row-count-floor guards to all three notebooks, implemented as job-failing errors per #5.

## 11. Reconciliation guards catch shrinkage but not row inflation

**Severity: Medium**

- **User impact:** A fan-out regression that duplicates CDI rows inside a parent passes every guard and delivers inflated row counts — and, for any consumer that sums across rows, inflated balances.
- **Issue:** In `portfolio_cdi_bal/portfolio_cdi_bal.ipynb`, Guard 2 rolls the fanned output back with `MAX(Total_balance_EUR)` per `(portfolio_code, uc_id)`, so duplicate CDI rows net to a zero reconciliation diff. Guard 3 only triggers below 75% of the previous count, so a doubling passes both.
- **Recommendation:** Add a `COUNT(DISTINCT (portfolio_code, CDI))` versus total-row-count assertion on the final frame, and consider an upper bound in the row-count guard as well as the existing floor.

## 12. VatSettlement perimeter reduction is not quantified or guarded

**Severity: Medium**

- **User impact:** The delivered row count moves off the documented ~76K with no recorded new expectation, so an unrelated future drop in the same table is indistinguishable from this intended narrowing.
- **Issue:** `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` narrows twice in this diff — loan-level settlements only (agreement-level `CASE_ACC_SYSTEM=19` excluded, tracked as a follow-up) and LEFT→INNER party join plus a non-null tax filter. Both are argued in the notebook, but no new expected count is recorded in the migration checklist and the write remains an unconditional overwrite.
- **Recommendation:** Measure the post-change row count for a pinned date, record it in `.kiro/steering/platform-migration-checklist.md`, and add a row-count floor derived from it.

## 13. `base` and `cdi_map` disagree on NULL CDIs, turning one bad row into a hard abort

**Severity: Medium**

- **User impact:** A single NULL-CDI exposure with a non-zero balance makes the balance reconciliation fail and aborts the whole `portfolio_cdi_bal` run — the opposite of the clean-skip behaviour the other guards implement for comparable conditions.
- **Issue:** In `portfolio_cdi_bal/portfolio_cdi_bal.ipynb`, `base` and `cdi_map` are built from the same `portfolio ⋈ dexposure ⋈ party_tax` query, but `cdi_map` filters `e.CDI IS NOT NULL` and `base` does not. A NULL-CDI exposure lands in `base` under a NULL `uc_id` with its balance counted, has no `cdi_map` row, and is dropped by the Step 4 inner join; `assert recon_diff < 1.0` then fires. The perimeter has zero NULL-CDI rows today, so this is latent.
- **Recommendation:** Apply the `CDI IS NOT NULL` filter to `base` as well, and surface the excluded count as an explicit metric rather than letting it destabilize the reconciliation.

## 14. LoanId parity check collects ~3M keys to the driver and misreports mismatches

**Severity: Medium**

- **User impact:** Avoidable driver-memory and serialization pressure immediately before three large overwrites, on frames that are never cached, so the run is slower and more fragile than it needs to be. When Financial alone diverges, the failure message reports zero differences and gives the operator nothing to act on.
- **Issue:** `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` builds three Python sets via `.select("LoanId").distinct().collect()` (~1.04M keys each, held simultaneously), on an uncached DAG that `debt_static` already recomputes for the gate counts, the row-count print, the NULL-`PartyId` assert and the write. The assert's error text compares only Static and Detail.
- **Recommendation:** Replace the sets with bidirectional `LEFT ANTI` counts (or `exceptAll`) against one canonical set, collecting only mismatch counts and a bounded sample, report both Detail and Financial relationships, and cache `debt_static`.

## 15. Several new assertions cannot fail by construction

**Severity: Low**

- **User impact:** The branch reads as if it added validation coverage where none exists, so a real regression in these conditions ships unnoticed — at the cost of a full extra recomputation of each frame.
- **Issue:** `debt_static.filter(col("PartyId").isNull())` in `dtransfer_Debt` and the equivalent in `dtransfer_DebtAB` run after a LEFT SEMI join on that same CDI column, which cannot match NULL. `vatsettlement_df.filter(col("taxNumber").isNull())` directly follows `.filter(col("taxNumber").isNotNull())`. The `cdi_map` "one `uc_id` per CDI" check tests a value that is a function of CDI, since `party_tax` is already deduped to one row per CDI.
- **Recommendation:** Move each check one step earlier where it can actually fail — pre-gate NULL CDI on `debt_static_base`, unresolvable-tax CDIs on the party frame, and a pre-dedup `cdi_map` variant — or delete them.

## 16. `portfolio_cdi_bal` is pinned to production catalogs

**Severity: Low**

- **User impact:** The notebook cannot be exercised in dev or QA without editing it, which is precisely why the issues above went undetected until review and why #9 cannot be satisfied as the code stands.
- **Issue:** `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` hardcodes `p-cdh`/`p-cdh-delivery`, and the new `party_tax` and `dsettlementexposure` reads extend that hardcoding rather than parameterizing it.
- **Recommendation:** Switch to `aws_utils.get_env_prefix()` as the `platforms/dtransfer_*` notebooks already do.

## 17. Whole-file notebook JSON reformat and implicit line joins

**Severity: Low**

- **User impact:** Reviewers and future editors cannot see the real change in the diff, and the next hand-edit can corrupt a cell's source without any syntax error.
- **Issue:** `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` and `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` were reformatted whole-file (one-line JSON objects expanded, trailing newline dropped, BOM removed from DebtAB), inflating the diff around the substantive edits. One `source` element in `dtransfer_Debt` now lacks its trailing `\n` and relies on the following element being `"\n"`.
- **Recommendation:** Restore the trailing `\n` on that `source` element, and keep notebook JSON formatting changes in a separate commit from behavioural changes so the semantic diff stays readable.
