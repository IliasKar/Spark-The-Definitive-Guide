# Run-date party identity and regulated balance delivery hardening

## Summary

This branch replaces source-local party keys with CDI across Customer, Debt, and DebtAB, while deriving parent identity independently from a run-date-scoped non-blank tax lookup. It aligns all three Debt outputs to the same party-gated loan perimeter and rebuilds `portfolio_cdi_bal` at parent grain before fanning balances out to individual CDIs. The balance notebook is now environment-aware and adds pre-write controls around output size, grain, and reconciliation. Several new controls are incomplete or unsafe at production scale, so the branch should not ship unchanged.

**Watch for:** **confirmed** negative settlement participation is measured but never rejected; **confirmed** an empty settlement snapshot bypasses the new contract checks; **confirmed** Debt parity collects roughly three million LoanId values into Python driver memory; and **confirmed** VatSettlement still resolves tax from all history while the other new parent mappings are run-date scoped.

**Verdict**: NEEDS_CHANGES

## High-level view

The identity design separates record provenance from parent resolution. Customer names and demographic fields continue to come from the source-priority winner, while only `CustomerVat` and `ParentPartyId` use a tax-first lookup. `portfolio_cdi_bal` applies the same run-date tax rule, preventing tax completeness from silently changing the source of unrelated customer attributes.

VatSettlement does not participate in that common snapshot rule. **Confirmed:** it scans all party history and can select a tax value recorded after the requested business date, despite its new comment saying the ordering is kept in sync with Customer.

DebtStatic applies the party gate once and DebtDetail/DebtFinancial inherit its LoanId set, which makes the warning-only coverage policy compatible with cross-table parity. **Confirmed:** the final parity check abandons Spark for three driver-side Python sets of about 1.04 million IDs each, creating avoidable memory pressure and a plausible production driver failure.

`portfolio_cdi_bal` now aggregates at `(PortfolioId, uc_id)`, fans each parent out to its member CDIs, and resolves settling parties directly through `party_tax`. The output guards protect the overwrite from empty balance results, fan-out duplication, reconciliation drift, and a large row-count collapse. **Confirmed:** the settlement-specific validation neither rejects negative percentages nor rejects an empty settlement perimeter, and its `max > 1` scale heuristic rejects a valid 0–100 snapshot whose observed values all happen to be at or below one percent.

`portfolio_cdi_bal` now resolves source and target catalogs from `env`; grep found no executable `p-cdh` literal. **Confirmed from the available artifacts:** there is still no Databricks execution evidence for the new source, joins, driver behavior, or regulatory totals; those behaviors cannot be established through local syntax validation.

<details>
<summary>Issues (6)</summary>

1. **Run-date tax mismatch** — **Confirmed:** VatSettlement selects tax from all party history while Customer and `portfolio_cdi_bal` use the requested date. Reuse the same run-date `party_tax` lookup or document and test an explicit exception.
2. **Driver-side LoanId materialization** — **Confirmed:** Debt parity collects about 1.04 million distinct IDs from each of three outputs into simultaneous Python sets. Replace this with distributed anti-join or symmetric-difference checks.
3. **Negative participation accepted** — **Confirmed:** `neg_cnt` is calculated but never asserted, so negative percentages can reduce delivered settlement totals. Reject any `neg_cnt > 0` before aggregation.
4. **Empty settlement snapshot passes** — **Confirmed:** an empty `_part_check` executes no assertions and allows every `bal_over_settlement` to become null. Require a non-empty expected perimeter and validate expected source presence or prior-run coverage.
5. **Scale heuristic rejects valid percentages** — **Confirmed:** requiring each source’s daily maximum to exceed 1 rejects valid 0–100 data when all observed values are at most 1%. Validate an explicit upstream scale/version contract instead of inferring units from the maximum.
6. **No runtime verification evidence** — **Confirmed from available artifacts:** syntax checks pass, but no Databricks run validates the new Spark SQL, totals, or production-scale behavior. Execute representative QA reconciliation and failure-path tests before release.

</details>

<details>
<summary>Details</summary>

### Canonical attributes are decoupled from parent identity

The branch introduces two independent selections over the run-date party snapshot. The canonical party row is selected by source priority for names, dates, types, and `cust_id`; a second `party_tax` relation ranks non-blank normalized tax values before source priority and supplies only `CustomerVat` and `ParentPartyId`. This preserves the intended provenance of non-tax attributes while allowing a lower-priority source to fill a missing tax number.

The same parent-key expression is used in `portfolio_cdi_bal`, so Customer and balance delivery agree on `COALESCE(non_blank_tax, CDI)` for a given run date. The resulting data flow is:

```text
run-date primary.party
    ├── source-priority record ──> names / type / birth date / cust_id
    └── tax-first lookup ─────────> CustomerVat / ParentPartyId / uc_id
```

This also makes blank and whitespace-only tax values fall back to CDI rather than collapsing unrelated customers onto an empty parent key. The commits explicitly address earlier review points around blank-tax normalization, catalog parameterization, and fail-as-success overwrite guards; those resolved points are not re-raised here.

### VatSettlement can use future tax data on historical runs

**Confirmed:** the modified VatSettlement code builds `party_all` without a `BusinessDate` predicate, ranks all history by tax presence, source, and descending date, and then joins that winner to a settlement selected for `run_date`. Customer and `portfolio_cdi_bal` instead restrict `party_tax` to their selected date. A historical VatSettlement run can therefore emit a tax number that did not exist on the requested date, or disagree with Customer for the same CDI and date.

The new “kept in sync” comment is materially false even though the sort expressions look similar; the candidate sets differ. VatSettlement should filter party rows to `run_date_lit` and reuse the same normalized tax-first rule, or the all-history behavior should be established as an explicit business exception with a backfill test showing why cross-table disagreement is acceptable.

The separate decision to emit only loan-level settlements is clearly encoded: `ExposureKey` is mandatory and agreement-only rows are excluded rather than being forced into the AccountNumber domain. Agreement delivery remains a documented follow-up rather than being silently mixed into this schema.

### Debt outputs share one perimeter but parity leaves the cluster

DebtStatic now uses a left-semi party gate, avoiding row multiplication while ensuring `PartyId=CDI` is resolvable. DebtDetail and DebtFinancial are subsequently left-semi joined to the distinct DebtStatic LoanId set, so a tolerated party-gate drop is inherited consistently instead of contradicting the parity assertion.

**Confirmed:** the write cell calls `.collect()` on all three distinct LoanId DataFrames and retains all three Python sets simultaneously. Expected production cardinality is about 1.04 million IDs per set, and these identifiers carry substantial Python object overhead beyond their encoded string size. The check can exhaust or heavily pressure the driver immediately before three regulatory overwrites even though the comparison is naturally distributable.

Keep the validation in Spark: build the three distinct ID DataFrames, reject null IDs, and use left-anti joins or `exceptAll`/symmetric differences with `limit(1).count()` to prove equality. That preserves the invariant without transferring the complete keyspace to the driver.

### Parent roll-up and CDI fan-out preserve two distinct grains

The balance rewrite aggregates financial values once per `(PortfolioId, uc_id)` and builds a distinct mapping from each parent to its individual CDIs. The parent totals are then repeated across those member rows, while initialization balances remain joined at parent grain. Settlement ownership is resolved directly from the settling CDI through `party_tax`, avoiding the previous dependency on a primary-owner-only exposure map for guarantors and co-owners.

The overwrite controls validate final non-emptiness, one row per `(portfolio_code, CDI)`, parent-total reconciliation after fan-out, and a 25% day-over-day row floor. These checks protect balance coverage and grain, but they do not establish settlement completeness because settlement amounts are nullable enrichments and do not affect any of those assertions.

### Participation validation does not enforce its stated contract

**Confirmed:** `_part_check` calculates `neg_cnt` and prints it, but the loop asserts only null count and maximum-value conditions. If one row has a negative percentage and another row in the same source has a value above one, every assertion passes and the negative amount is included in `SUM((ParticipationPercentage / 100.0) * SettlementAmount)`. Add `assert r["neg_cnt"] == 0` before calculating settlements.

**Confirmed:** if `dsettlementexposure` has no qualifying rows for the date, `_part_check` is empty and the loop performs no assertions. The settlement aggregate is then empty, every delivered row receives a null `bal_over_settlement`, and the balance/fan-out guards still pass. Require a non-empty settlement perimeter and, if source presence is contractual, compare the observed source set and counts against expected sources or a previous successful run.

**Confirmed:** `max_pp > 1` is not a valid invariant of a 0–100 percentage domain. A legitimate source/date where every participation value is between 0% and 1% is rejected as “pre-unification,” even though all values satisfy the declared scale. Detect the upstream schema/version explicitly or validate against a source-level contract that distinguishes fractions from percentages without inferring the unit from one day’s maximum.

### Regulatory behavior remains unexecuted in the available review artifacts

**Confirmed from the repository and commit context:** the notebooks have empty outputs and zero execution counts, and no automated tests exercise the new run-date identity rule, settlement-source switch, percentage scaling, fan-out totals, or Debt driver behavior. Local validation passed `git diff --check`, JSON parsing, and Python AST parsing for all five modified notebooks, but that does not analyze Spark SQL or source data.

Before release, run the notebooks in QA for a representative date and compare row counts, distinct keys, tax/parent mappings, settlement totals, and Debt cross-table LoanId parity against the documented on-prem controls. Include an explicit negative/empty participation test and execute Debt at production-scale cardinality after replacing the driver collections. Review context was available through the branch’s prior-review commit descriptions; the CodeCommit remote exposes no GitHub PR comment stream through the available tooling.

</details>

<details>
<summary>File map</summary>

- `.kiro/steering/platform-migration-checklist.md` — records CDI and tax/CDI fallback as accepted cross-platform identity contracts.
- `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` — separates canonical party selection from run-date tax and parent resolution.
- `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` — gates all three Debt outputs to one LoanId perimeter and adds parity validation.
- `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` — applies the CDI party gate and non-null PartyId invariant to banking portfolios.
- `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` — narrows delivery to loan-level settlements and enforces non-null tax output.
- `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` — parameterizes catalogs, adds parent/CDI fan-out, changes settlement sourcing, and adds overwrite guards.

Full diff: `git diff origin/DNA26-1716_platforms HEAD`

</details>
