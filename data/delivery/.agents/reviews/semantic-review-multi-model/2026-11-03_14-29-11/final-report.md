# Final consolidated issues report — Hoist collection-invoicing migration plus the cross-platform party-identity rule for dtransfer deliverables (HEAD)

Issues are sorted from highest to lowest combined importance/confidence.

---

## 1. Hoist invoicing reads settlement debt from the latest snapshot, not the reporting date

**Severity: High** · *(cluster: as-of-date correctness in HoistInvoicing — see also #10)*

- **User impact:** Any backfill or re-run of an older reporting month delivers settlement balances and active-settlement flags that did not exist in that month, and the same `business_date` can produce different numbers after each new settlement load. The invoicing figures Hoist is billed on are not reproducible.
- **Issue:** `HoistInvoicing/hoist_collection_invoicing.ipynb:336-344` derives `dse_bd` as the unrestricted `MAX(BusinessDate)` of `derivativeactive.dsettlementexposure`, unconstrained by `business_date`, the previous-month payment window, or each payment's `PreviousBusinessDay`. Every other financial attribute in the notebook (exposure, assignment) is correctly pinned to `PreviousBusinessDay`, so settlement debt is the single field leaking future state. During review the production settlement maximum was 2026-10-30, meaning a `business_date=2026-06-30` run would carry October values.
- **Recommendation:** Resolve settlement data at `PreviousBusinessDay` from the historical derivative table. If no historical view exists, implement an explicit latest-snapshot-`<= run_date` rule, document the approximation, and get business acceptance for it.

## 2. VatSettlement can still emit a null taxNumber into a NOT NULL target column

**Severity: High** · *(cluster: dtransfer_VatSettlement rewrite — see also #6, #12, #13)*

- **User impact:** Rows land in the delivered VatSettlement extract with an empty tax number in a column IT treats as mandatory, risking rejected uploads or silently unidentifiable settlements.
- **Issue:** `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` switches the party join to INNER and the added markdown claims this mirrors on-prem's INNER JOIN on `PRC.ref.partyTaxNumber` so `taxNumber` can never be null. The join only requires the CDI to exist in `primary.party`. The dedup winner is chosen by source-system priority then `BusinessDate DESC` — not by tax-number presence — so 8 of 23,402 settlement CDIs resolve to a winner with a null `TaxRegistrationNumber` even though another party row for the same CDI carries one. On-prem `VatSettlement.taxNumber` is `nvarchar(200) NOT NULL` with zero nulls across 76,455 rows.
- **Recommendation:** Push tax-number presence into the party dedup window ordering (prefer non-null `TaxRegistrationNumber`), and add an explicit `taxNumber IS NOT NULL` filter or post-write assertion so the contract is enforced rather than assumed.

## 3. portfolio_cdi_bal emits uc_id as a second copy of the CDI column

**Severity: High** · *(cluster: portfolio_cdi_bal uc_id — see also #15)*

- **User impact:** The delivered table has two identical columns and no real portfolio-scoped CDI, so consumers cannot reconcile a row to a customer the way they can on-prem, and the new column does not actually replace on-prem's parent identifier.
- **Issue:** In `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` the `base` CTE already aliases `COALESCE(e.PartyTaxRegistrationNumber, e.CDI) AS CDI` and groups by that expression, so the added `b.CDI AS uc_id` and the pre-existing `b.CDI AS CDI` emit the same string. On-prem `[PRC].[operation].[portfolio_cdi_bal]` holds these as distinct columns at a different grain: 390,430 rows, one per `(portfolio_code, CDI)`, 390,430 distinct CDIs and 322,498 distinct `uc_id` values as a rollup attribute. Because the cloud query aggregates at parent grain there is no CDI left to emit.
- **Recommendation:** Group `base` by `(PortfolioId, CDI)` and carry `COALESCE(PartyTaxRegistrationNumber, CDI)` as a non-grouping attribute aliased `uc_id`, reproducing both the on-prem grain and the two distinct columns. If the parent grain is intentional, rename the duplicate and document that cloud `CDI` is not a CDI. Column position is fine either way: `uc_id` before `CDI` matches on-prem ordering.

## 4. PartyId changes from a numeric domain to an alphanumeric CDI without documented IT sign-off

**Severity: High** · *(cluster: party identity value domain — see also #5)*

- **User impact:** Consumers that read `PartyId` as an integer break outright on values like `GLC0004267942`. This affects every party-bearing dtransfer deliverable at once, and `DebtStatic`/`DebtAB` have no separate `CDI` column, so the CDI lands in exactly the column a consumer is most likely to cast.
- **Issue:** `platforms/dtransfer_Customer/dtransfer_Customer.ipynb`, `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` and `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` now source `PartyId` from CDI. On-prem `Customer.PartyId`, `Representatives.PartyId`, `DebtStatic.PartyId` and `DebtAB.PartyId` are all `int NOT NULL`, and current cloud values are numeric strings (1,621,642 of 1,621,689 DebtStatic rows match `^[0-9]+$`; 492,839 of 492,839 for DebtAB). The Delta writes will not fail because the delivery tables are STRING-typed, so the change is silent. The team obtained an explicit IT confirmation for a string `uc_id` in `portfolio_cdi_bal`; no equivalent confirmation is recorded for these four columns.
- **Recommendation:** Obtain and record the same explicit IT confirmation covering `Customer.PartyId`, `Representatives.PartyId`, `DebtStatic.PartyId` and `DebtAB.PartyId` before release. Note that the grain itself is sound — on-prem Customer already has 806,161 distinct `PartyId` against 805,937 distinct CDI, so portfolio-scoped CDIs match the existing cardinality.

## 5. ParentPartyId is a string with meaningful leading zeros where on-prem is int

**Severity: Medium** · *(cluster: party identity value domain — see also #4)*

- **User impact:** A consumer casting `ParentPartyId` to an integer fails on CDI fallback values and silently drops leading zeros on tax numbers such as `094014760`, quietly merging or mis-keying customer groups.
- **Issue:** `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` fills `ParentPartyId` for both the customer and representatives selects with `COALESCE(TaxRegistrationNumber, CDI)`, per the agreed rule. On-prem `Customer.ParentPartyId` and `Representatives.ParentPartyId` are `int`. The substitution itself measures well — on-prem `ParentPartyId` yields 639,113 distinct groups against 639,103 for the substitute with 639,113 distinct pairs, so the substitute never splits a parent group and merges only 10 — and the key agrees between `party.TaxRegistrationNumber` and `dexposure.PartyTaxRegistrationNumber` for all 585,481 dexposure CDIs.
- **Recommendation:** Confirm with IT that `ParentPartyId` is consumed as a string, and state the leading-zero requirement in the notebook documentation so a later numeric cast is not introduced downstream.

## 6. VatSettlement still covers ~35% fewer rows than on-prem while the documentation now reads as parity

**Severity: Medium** · *(cluster: dtransfer_VatSettlement rewrite — see also #2, #12, #13)*

- **User impact:** A third of the settlements IT expects are missing from the delivered extract, and whoever runs the verification pass is likely to read the result as a pass because the notebook text claims on-prem alignment.
- **Issue:** The `COALESCE(ExposureKey, AgreementKey)` expansion in `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` lifts the perimeter from roughly 41.7K to about 49.7K rows (49,900 active settlements, less 242 null-CDI rows dropped by the INNER join, less portfolio and party join misses) against 76,455 on-prem rows spanning 37,406 distinct `SettlementCode`s. The rewritten mapping markdown presents the cloud logic as mirroring on-prem, including the INNER join rationale.
- **Recommendation:** Investigate the residual gap (which settlement classes on-prem includes that the cloud perimeter does not) and change the markdown to record ~49.7K vs 76,455 as a known, quantified gap rather than parity.

## 7. No execution-level verification or post-write assertions accompany these regulatory outputs

**Severity: Medium**

- **User impact:** Defects of exactly the kind listed in this report reach the government-platform uploads and the Hoist invoicing file undetected, because nothing in the change demonstrates the outputs were produced and compared.
- **Issue:** The diff contains no Databricks execution result, regression test, or checked-in validation output for either the new Hoist report or the identity migration. JSON parsing and cell compilation cover notebook integrity only — they cannot exercise Spark column resolution, Unity Catalog permissions, Volume writes, report grain, or the joins. Specifically unverified: Hoist parity with the on-prem workbook, historical replay determinism, that delivered `uc_id` and `CDI` differ, that `taxNumber` is non-null after the write, that DebtStatic/DebtDetail/DebtFinancial share a LoanId set, and post-change row counts for DebtStatic, DebtAB and VatSettlement.
- **Recommendation:** Run the migration-checklist comparisons against an on-prem month and retain the output: row counts, `(ExposureKey, PayDate)` uniqueness for Hoist, payment and debt totals, null distributions, and representative record comparisons; run the same `business_date` twice to prove deterministic source selection. Add the cheap post-write assertions (uc_id ≠ CDI, taxNumber NOT NULL, sibling LoanId set equality) so the invariants are enforced rather than assumed.

## 8. Hoist export filename carries a duplicated prefix

**Severity: Medium** · *(cluster: Hoist export artifacts — see also #9)*

- **User impact:** The delivered file is named `hoistinvoicing_hoistinvoicing_<date>_<timestamp>.xlsx`, which may not match the filename contract the consumer or email automation expects.
- **Issue:** `HoistInvoicing/hoist_collection_invoicing.ipynb:592` calls `export_data(output_df, output_file_prefix, run_date, output_dir, output_file_prefix)`, supplying the same value as both the helper's prefix and the report name. The helper composes filenames as `{output_file_prefix}_{name}_{date}`.
- **Recommendation:** Pass a distinct logical report name, or set the configured prefix in `HoistInvoicing/config/live/hoist_collection_invoicing.json` to produce the exact filename the consumer expects, and confirm that contract.

## 9. Re-runs accumulate multiple current-looking export artifacts in the same date folder

**Severity: Medium** · *(cluster: Hoist export artifacts — see also #8)*

- **User impact:** After a retry, the date folder holds several timestamped workbooks that all look valid, so downstream delivery can pick up the superseded run instead of the corrected one.
- **Issue:** `HoistInvoicing/hoist_collection_invoicing.ipynb:587-588` creates the output directory but nothing archives or removes existing artifacts before writing the new timestamped file, so every retry for the same `run_date` adds another file.
- **Recommendation:** Apply the repository's archive/cleanup behaviour before writing, or publish one deterministic final filename per report date so only the latest run is deliverable.

## 10. Hoist portfolio mapping resolves from the globally latest snapshot

**Severity: Medium** · *(cluster: as-of-date correctness in HoistInvoicing — see also #1)*

- **User impact:** A portfolio rename after the fact can change the perimeter of an already-delivered historical report, so a re-run no longer reproduces what was sent.
- **Issue:** `HoistInvoicing/hoist_collection_invoicing.ipynb:157-159` resolves the fixed portfolio IDs from the unrestricted latest portfolio snapshot rather than a snapshot at or before `run_date`. Lower risk than #1 because portfolio names change rarely, but the mechanism is identical.
- **Recommendation:** Resolve the portfolio map at or before `run_date`, or assert and document that the ID-to-name mapping is immutable.

## 11. The new party filter applies to DebtStatic only, not to DebtDetail and DebtFinancial

**Severity: Medium**

- **User impact:** DebtDetail or DebtFinancial can carry loans that DebtStatic does not, breaking the cross-table join IT relies on. On-prem guarantees all three tables share the same LoanId set (1,048,759 each, zero orphans).
- **Issue:** In `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` DebtStatic now requires a party-resolvable CDI via the INNER join, while DebtDetail and DebtFinancial are built straight from `dexposure` joined to portfolio with no party constraint. Measured on prod for 2026-10-30 the filter is nearly inert — 47 of 3,741,503 exposure rows have a null CDI, zero exposure CDIs are missing from the all-history party dedup, and no loan loses all its DebtStatic rows — so the invariant holds by data accident, not by construction, and nothing would surface it breaking. Separately, the join contributes nothing but row elimination: `PartyId` is taken from the party side of an equality on CDI, so it always equals the exposure's own CDI. The same pattern is in `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb`.
- **Recommendation:** Express the operation as `left_semi` with `col("exp_CDI").alias("PartyId")` so the intent is explicit and a future change in the party dedup cannot duplicate rows, and either apply the same CDI constraint to DebtDetail/DebtFinancial or add an assertion that the three LoanId sets match.

## 12. VatSettlement AccountNumber now mixes bare and portfolio-prefixed key formats

**Severity: Medium** · *(cluster: dtransfer_VatSettlement rewrite — see also #2, #6, #13)*

- **User impact:** The format of `AccountNumber` depends on which branch produced the row, so IT cannot apply one parsing rule when resolving the column against account identifiers.
- **Issue:** The `COALESCE(ExposureKey, AgreementKey)` change in `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` is well motivated — 8,147 of 49,900 active settlements have no `ExposureKey` — but all 8,147 agreement-level values are portfolio-prefixed while essentially all 41,753 loan-level values are bare (only 2 prefixed). On-prem mixes formats too (45,546 prefixed vs 30,909 bare of 76,455, prefixes like `PRA`/`CEL`/`GLA`/`MRC`/`ORB` at lengths 17-19, bare at 14-16), so the bare-`ExposureKey` question pre-dates this change, but routing a second consistently-prefixed key space into the same column compounds it. Duplicate keys are not a new risk: on-prem already has 76,455 rows over 72,474 distinct `(AccountNumber, SettlementCode)` pairs.
- **Recommendation:** Confirm with IT which format each row class should carry, then normalise both branches to that rule rather than letting the source key format decide.

## 13. The VatSettlement change is uncommitted and would be left out of a push

**Severity: Medium** · *(cluster: dtransfer_VatSettlement rewrite — see also #2, #6, #12)*

- **User impact:** The VatSettlement perimeter and party-join fixes silently do not ship, so the delivered table keeps its old behaviour while the rest of the identity rule goes live.
- **Issue:** `platforms/dtransfer_Customer`, `dtransfer_Debt`, `dtransfer_DebtAB` and `portfolio_cdi_bal` are committed in `0837d63` and `edc9b64`; `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` is still modified in the working tree.
- **Recommendation:** Commit the VatSettlement edit (after addressing #2, #6 and #12) so it travels with the rest of the identity change.

## 14. Stale verified row-count comment in the DebtStatic base SQL

**Severity: Low**

- **User impact:** The next verification run anchors on a baseline that no longer describes the query, so a genuine regression could be read as expected.
- **Issue:** `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` still carries `# Use SQL approach (verified: gives 1,608,266 matching on-prem)` above the party join. That figure was measured when the join was LEFT; the current table holds 1,621,689 rows and the new INNER join subtracts 47.
- **Recommendation:** Update the comment to the post-change expected count, or drop the number and point to the verification checklist instead.

## 15. portfolio_cdi_bal uc_id documentation states the wrong type and the wrong parity measure

**Severity: Low** · *(cluster: portfolio_cdi_bal uc_id — see also #3)*

- **User impact:** Anyone validating the new column against on-prem compares the wrong things and concludes it matches when it does not.
- **Issue:** The added documentation bullet in `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` describes on-prem `uc_id` as an int — the column is `nvarchar(60)`, and the int is the source `ParentPartyId`. The recorded 362,267 vs 361,959 parity check measures a grouping count, not the table's 390,430-row portfolio-CDI grain.
- **Recommendation:** Correct both claims and state the on-prem grain (390,430 rows, 390,430 distinct CDIs, 322,498 distinct `uc_id`) as the comparison baseline.

## 16. PartyIdQualco is overridden to null although its source is available

**Severity: Low**

- **User impact:** IT receives an empty Qualco identifier in Customer and Representatives even though the value is present in the pipeline.
- **Issue:** Both the customer and representatives selects in `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` join `party_qca`, which carries `PartyIdQualco`, then override it with `lit(None).cast("string")`. The null literal pre-dates this change, but this commit's purpose is applying the party-identity rule and this is the third identity column whose source is already in the DataFrame.
- **Recommendation:** Confirm the null is a deliberate scope decision; if not, populate it from the joined `party_qca` value.
