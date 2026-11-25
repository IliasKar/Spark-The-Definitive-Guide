# Party-identity rule, party gating, and parent-grain fan-out for the IT delivery tables

Sixteen commits rework the identity contract of the `it.dtransfer_*` deliverables and `it.portfolio_cdi_bal`. `PartyId` stops being NULL / a source-system `PartyKey` and becomes the CDI; `ParentPartyId` becomes `COALESCE(TaxRegistrationNumber, CDI)`. Tax-number resolution is decoupled from party record dedup into a separate `party_tax` lookup so the tax tie-break no longer drags names and birth dates to a lower-priority source. The `LEFT JOIN party` in DebtStatic/DebtAB becomes a `LEFT SEMI` gate that mirrors on-prem's INNER JOIN, DebtDetail/DebtFinancial are gated to the DebtStatic `LoanId` set so all three tables share one loan perimeter, VatSettlement narrows to loan-level settlements with an INNER tax join, and `portfolio_cdi_bal` is restructured to reproduce the on-prem parent-grain aggregation plus fan-out to member CDIs, sourcing settlements from `dsettlementexposure` with a participation-scale contract check. Hardcoded `p-cdh` catalogs in `portfolio_cdi_bal` are replaced with `{env}`.

Watch for: the new party gate can empty three regulatory tables with nothing but a printed warning (confirmed); `party_tax` has no total ordering and is re-evaluated per query, so `uc_id` can differ between the queries that must agree (confirmed); an empty or stale `primary.party` snapshot silently collapses `uc_id` to `CDI` in `portfolio_cdi_bal` and every pre-write guard still passes (confirmed); `dtransfer_Customer` resolves parties from the run-date snapshot while Debt/DebtAB gate on all-history party, so delivered `PartyId` values can have no matching Customer row (confirmed); the LoanId-parity assert collects ~1M keys per table into three driver-side Python sets (confirmed).

**Verdict**: NEEDS_CHANGES

## High-level view

The identity change is the centre of the diff and it is a consumer-visible contract change, not an internal refactor. `PartyId` moves from an integer domain to an alphanumeric string (`GLC0004267942`), `ParentPartyId` from a numeric grouping key to a tax number with a CDI fallback, and `portfolio_cdi_bal` gains a `uc_id` column while `bal_over_settlement` and `initial_assign_bal_EUR` swap positions. IT has signed off on the string domain and the steering doc records it, but every consuming DDL and any positional reader has to move in step.

Tax resolution is now a standalone `party_tax` lookup in `dtransfer_Customer` and `portfolio_cdi_bal`, keyed by CDI, preferring a non-blank tax number, then source priority, then latest date. Two problems ride along. The ordering is not a total order, so when a CDI has several same-source same-date rows carrying different tax numbers the winner is arbitrary — and because `party_tax` is an uncached lazy view, `portfolio_cdi_bal` evaluates it four separate times (base, `cdi_map`, the init-balance subquery, settlements) and those evaluations are not guaranteed to agree. The three notebooks also disagree on scope: Customer and `portfolio_cdi_bal` read the run-date snapshot, VatSettlement reads all history, and the VatSettlement comment still claims it matches Customer.

The party gate is the riskiest behavioural change. Turning a LEFT join into a LEFT SEMI gate makes party coverage a hard dependency of row survival for DebtStatic, DebtDetail, DebtFinancial and DebtAB. The instrumentation added for it prints a drop percentage and a warning above 1%, then writes regardless. There is no empty-result check and no row-count floor, so a bad `primary.party` load overwrites four regulatory tables with zero rows and the job reports success. `portfolio_cdi_bal` got exactly these guards in the same branch; the Debt notebooks did not.

Debt also uses a different party scope from Customer — all-history versus run-date snapshot — so a CDI can survive the DebtStatic gate and still be absent from the delivered Customer table. The new cross-table assert checks `LoanId` parity between the three Debt tables but nothing checks `PartyId` against the Customer deliverable, which is the join IT actually performs.

The verification machinery added to `dtransfer_Debt` is expensive and partly tautological. Nothing is cached, so the DebtStatic DAG is recomputed roughly ten times across counts, asserts, the two semi-join gates and three writes. The LoanId-parity assert pulls three ~1M-element sets into the driver. The PartyId-null asserts in DebtStatic and DebtAB cannot fail: a semi join on `CDI = pty_CDI` already excludes NULL CDI.

`portfolio_cdi_bal` now reproduces the on-prem grain correctly — aggregate once per `(PortfolioId, uc_id)`, fan out to each member CDI — and the settlement reparenting through `party_tax` rather than `cdi_map` genuinely fixes dropped guarantor settlements. The pre-write guards are the right shape and fail closed by raising. Their blind spots are the ones that matter most in practice: nothing asserts `party_tax` resolved anything, nothing bounds how many CDIs a single `uc_id` may absorb, the reconciliation tolerance is an absolute €1 on a multi-billion total, the participation-scale check is max-based so one outlier defeats it, and the Step 3 zero-row path still exits SUCCESS via `dbutils.notebook.exit`.

Catalog parameterization is complete in `portfolio_cdi_bal` code but the notebook's own documentation still names `d-cdh` twice, and `dtransfer_AllAccounts` still hardcodes `p-cdh` in executable code (outside this diff, but it contradicts the branch's stated goal).

<details>
<summary>Issues (18)</summary>

1. **Empty-delivery risk in Debt/DebtAB** — the party gate's drop warning is print-only with no empty check and no row-count floor; a bad `primary.party` load overwrites four regulatory tables with zero rows and the job succeeds. Add the raise-on-empty and floor guards used in `portfolio_cdi_bal`.
2. **Party scope divergence Customer vs Debt** — Customer builds from the run-date party snapshot, Debt/DebtAB gate on all-history party, so delivered `PartyId` values can have no Customer row. Pick one scope or assert `DebtStatic.PartyId ⊆ Customer.PartyId`.
3. **VatSettlement party scope** — still all-history while Customer and `portfolio_cdi_bal` moved to run-date scope, so the same CDI can carry different tax numbers across deliverables; the "all-history dedup like dtransfer_Customer" comment is now false.
4. **`party_tax` has no total ordering** — same-source same-date rows with different tax numbers resolve arbitrarily. Add a deterministic final tie-break (e.g. `TaxRegistrationNumber`).
5. **`party_tax` re-evaluated per query** — the uncached temp view is evaluated four times in `portfolio_cdi_bal`; combined with (4) the `base`/`cdi_map` inner join can lose parents. Materialize or cache it.
6. **No guard that `party_tax` resolved anything** — an empty or stale party snapshot collapses every `uc_id` to its CDI, changing the output grain, and all four pre-write guards still pass. Assert non-empty and a plausible distinct-`uc_id`/distinct-CDI ratio.
7. **Tax number used as a grouping key without sanity checks** — a shared placeholder value merges unrelated customers into one `uc_id` (balances summed and fanned out) and one `ParentPartyId`. Bound CDIs per `uc_id` or validate the tax format.
8. **Driver-side LoanId set collects** — the parity assert pulls three ~1M-element sets into the driver. Replace with Spark-side anti-join counts.
9. **No caching in `dtransfer_Debt`** — the DebtStatic DAG is recomputed roughly ten times. Cache it, or gate on the cheaper `debt_static_gated` rather than the fully enriched `debt_static`.
10. **Tautological PartyId asserts** — the NULL-PartyId checks in DebtStatic and DebtAB cannot fail after a semi join on CDI, and each costs a full pipeline pass.
11. **Parity assert breaks on NULL LoanId** — a NULL `ExposureDerivativeID` enters the Python set but never matches a semi join, failing all three writes.
12. **Absolute reconciliation tolerance** — `recon_diff < 1.0` EUR on a multi-billion total risks a spurious hard failure from float summation order; use a relative tolerance. The guard also covers only `Total_balance_EUR`.
13. **Step 3 still exits SUCCESS** — the zero-row early exit uses `dbutils.notebook.exit` while the new Step 7 guards raise, so the most likely failure mode is reported as a clean run.
14. **Participation-scale guard is max-based** — a single value >1 in a still-fractional source passes the check and every amount is then divided by 100. Check the distribution, not the maximum.
15. **Unmatched settlements dropped silently** — settlements whose parent is absent from the output are discarded with no row or amount logging, the same class of bug review #11 fixed.
16. **VatSettlement drops are invisible** — the INNER party join plus non-null tax filter remove rows with no instrumentation, unlike the Debt gates.
17. **Stale `d-cdh` references** — `portfolio_cdi_bal` markdown still names `d-cdh.reference.initializationexposureinfo` twice after the code was parameterized.
18. **`dtransfer_AllAccounts` hardcodes `p-cdh`** — pre-existing and outside this diff, but it defeats the branch's "no hardcoded prod catalogs anywhere" objective and sits next to an available `env`.

</details>

<details>
<summary>Details</summary>

### PartyId = CDI and the string-domain contract

`dtransfer_Customer`, `dtransfer_Debt` and `dtransfer_DebtAB` previously emitted either `lit(None)` or the source-system `PartyKey` for `PartyId`. All three now emit the CDI, taken from the row's own `CDI`/`exp_CDI` column rather than from a party join, which removes the join entirely in the Debt notebooks:

```python
debt_static_gated.withColumn("PartyId", col("exp_CDI"))   # was: .join(party, ... , "left") -> PartyKey
```

The steering doc records the rationale (source-system `PartyKey` collided across systems and mis-joined downstream) and IT's sign-off on the alphanumeric domain. Every consuming int column has to be widened to string, tracked separately.

`ParentPartyId` follows `COALESCE(party_tax.tax, CDI)`, which guarantees non-null at the cost of mixing two value domains in one column: a 9-digit tax number for most rows and a CDI string for those without one. Consumers that group on `ParentPartyId` will see single-member parents for the fallback rows.

### Tax resolution decoupled from record provenance

The core of commit `6fdb4c9`. `dtransfer_Customer` keeps the record dedup (names, `BirthDate`, `PartyType`, `PartyKey`) on source priority, and resolves the tax number through a separate CDI-keyed lookup that prefers a non-blank tax first:

```python
party_tax = (
    party_all.select("CDI", "SourceSystem", "BusinessDate",
        when(trim(col("TaxRegistrationNumber")) != "", trim(col("TaxRegistrationNumber"))).alias("tax"))
    .withColumn("_tax_rn", row_number().over(Window.partitionBy("CDI").orderBy(
        when(col("tax").isNotNull(), 0).otherwise(1),
        when(col("SourceSystem") == "profits", 1).when(col("SourceSystem") == "qca", 2).otherwise(3),
        col("BusinessDate").desc())))
    .filter(col("_tax_rn") == 1).select("CDI", "tax")
)
```

Blank and whitespace-only tax numbers normalise to NULL so they fall back to CDI instead of collapsing every blank-tax customer into one empty parent key; `portfolio_cdi_bal` carries the SQL equivalent with `NULLIF(TRIM(...), '')`.

Two issues ride on this lookup. First, the ORDER BY is not a total order. A CDI with two `profits` rows on the same `BusinessDate` carrying different non-blank tax numbers has an arbitrary winner, and `row_number` gives no stability guarantee across evaluations. Second, in `portfolio_cdi_bal` `party_tax` is registered as a temp view and never cached, so it is evaluated independently in `base`, `cdi_map`, the init-balance subquery and `settlements`. If two evaluations disagree, the `base ⋈ cdi_map` inner join on `(PortfolioId, uc_id)` loses the affected parent. Guard 2 catches that as a reconciliation failure rather than shipping it, so the failure mode is a non-deterministic job failure rather than bad data — but it is avoidable with a final tie-break column and a `cache()`.

Scope is where the three notebooks diverge. Customer and `portfolio_cdi_bal` read the run-date snapshot (the Customer comment notes all-history adds only ~13 CDIs). `dtransfer_VatSettlement` reads `primary.party` with no `BusinessDate` filter at all, while its comment claims "all-history dedup like dtransfer_Customer" and "Kept IN SYNC with dtransfer_Customer's party dedup ordering". The ordering is in sync; the scope is not. For a CDI whose run-date row has no tax number but whose historical row does, Customer emits `CustomerVat = NULL` and `ParentPartyId = CDI` while VatSettlement emits the historical tax number and keeps the row.

### Party gate: LEFT SEMI with a printed warning

DebtStatic and DebtAB replace `LEFT JOIN party` with `LEFT SEMI`, which matches on-prem `DebtSp`'s INNER JOIN on `DWH.prt.Party` and, as the comment notes, cannot duplicate rows if the party dedup changes. The gate is instrumented:

```python
_drop_pct = (_dropped / _base_cnt) if _base_cnt else 0.0
print(f"DebtStatic party gate: {_base_cnt:,} -> {_gated_cnt:,} ...")
if _drop_pct > 0.01:
    print(f"WARNING: party gate dropped {_drop_pct:.2%} of DebtStatic rows ...")
```

The warning is a print. Nothing downstream of it stops the write. Row survival in four regulatory tables is now a function of `primary.party` coverage, and the failure path is unguarded end to end:

```
primary.party empty/partial
   -> gate drops 100% of rows
   -> "WARNING: party gate dropped 100.00%" printed to driver log
   -> LoanId-parity assert passes (three empty sets are equal)
   -> PartyId-null assert passes (no rows, so no NULL rows)
   -> DebtStatic / DebtDetail / DebtFinancial / DebtAB overwritten with 0 rows
   -> task reports SUCCESS
```

Every assert added in this branch passes on empty input. `portfolio_cdi_bal` received a raise-on-empty guard and a 25% row-count floor in the same branch; the Debt notebooks need the same treatment, and the >1% gate drop should arguably raise rather than print given the regulatory consumer.

The gate also introduces a referential gap against the Customer deliverable. `dtransfer_Debt` and `dtransfer_DebtAB` build `party` from `primary.party` with no `BusinessDate` filter:

```python
party_all = spark.table(f"`{env}-cdh`.primary.party")      # dtransfer_Debt, dtransfer_DebtAB
party_all = spark.table(...).filter(col("BusinessDate") == run_date_lit)   # dtransfer_Customer
```

All-history party is the broader set (the steering doc puts it at ~805K versus ~686K for a single snapshot). So a CDI can pass the DebtStatic gate on the strength of a historical party row and still be absent from `dtransfer_Customer`, whose perimeter is `party(run-date) ⋈ exposure_perimeter`. IT joining `DebtStatic.PartyId` to `Customer.PartyId` gets orphans. The new parity assert covers `LoanId` across the three Debt tables but nothing covers `PartyId` against Customer.

### Cross-table LoanId parity: correct intent, expensive implementation

DebtDetail and DebtFinancial are gated to the DebtStatic loan set so the party gate is applied once and all three tables share one perimeter by construction:

```python
.join(debt_static.select(col("LoanId").alias("_ds_loanid")).distinct(),
      dexposure["ExposureDerivativeID"] == col("_ds_loanid"), "left_semi")
```

That is the right fix for review #6. The verification on top of it is where the cost sits:

```python
_ls_static = {r[0] for r in debt_static.select("LoanId").distinct().collect()}
_ls_detail = {r[0] for r in debt_detail.select("LoanId").distinct().collect()}
_ls_fin    = {r[0] for r in debt_financial.select("LoanId").distinct().collect()}
assert _ls_static == _ls_detail == _ls_fin, (...)
```

At the expected ~1.04M loans and 64-character derivative IDs, that is roughly 130MB of Python strings per set plus set overhead, three times, on the driver. The same check expressed as two anti-join counts stays on the cluster.

Nothing in the notebook is cached, and `debt_static` is referenced by `debt_static.count()`, the PartyId assert, both semi-join gates, the parity collect, and the write. Each `count()`/`collect()`/`write` on `debt_detail` and `debt_financial` re-expands the embedded `debt_static` subtree, putting the DebtStatic DAG — a window over `primary.exposure ⋈ dexposure ⋈ portfolio` plus five enrichment joins and a `distinct()` — at roughly ten executions per run. Gating on `debt_static_gated.select("LoanId")` instead would be equivalent (the enrichment joins are all LEFT and cannot drop loans) and much cheaper, and a `cache()` on the gated set would remove the rest.

Two sharp edges in the assert itself. If any `ExposureDerivativeID` is NULL it enters `_ls_static` as `None` but can never match the semi join, so `_ls_static != _ls_detail` and all three writes fail. And the assert is placed before the writes, which is correct for fail-closed behaviour, but it means a detail/financial drift blocks the DebtStatic delivery too.

The PartyId invariants added alongside prove nothing:

```python
_bad_partyid = debt_static.filter(col("PartyId").isNull()).limit(1).count()
assert _bad_partyid == 0, "DebtStatic has NULL PartyId rows (PartyId=CDI invariant violated)"
```

`PartyId` is `exp_CDI`, and `exp_CDI` survived a semi join on `exp_CDI = pty_CDI`, which NULL cannot satisfy. The check is unfalsifiable and costs a full pass over the pipeline. DebtAB has the identical pattern.

### portfolio_cdi_bal: parent grain plus fan-out

The structural fix. On-prem `CalcCustomerBalances` aggregates at parent grain (`ParentPartyId` → `uc_id`) and the final INSERT explodes each parent into its member CDIs. The cloud version previously grouped by `COALESCE(PartyTaxRegistrationNumber, CDI)` and emitted that as `CDI`, conflating the two grains. It now computes `base` at `(PortfolioId, uc_id)`, builds a `cdi_map` of `(PortfolioId, uc_id, CDI)`, and joins them:

```
dexposure ⋈ portfolio ──┬─> base     (PortfolioId, uc_id)            : SUM(converttoeur(...))
   (+ party_tax)        └─> cdi_map  (PortfolioId, uc_id, CDI) DISTINCT
                                   │
                base ⋈ cdi_map on (PortfolioId, uc_id)
                                   │
                        one row per (PortfolioId, CDI), uc_id carried
                                   │
                  ⋈ settlements on (PortfolioId, uc_id)  [LEFT]
```

`WHERE e.CDI IS NOT NULL` on both sides keeps a NULL parent bucket out of the output, and the `cdi_map` uniqueness assert pins each `(PortfolioId, CDI)` to exactly one `uc_id`. The tax number is deliberately read from `primary.party` rather than `dexposure.PartyTaxRegistrationNumber`, which is NULL for ~3,063 CDIs whose party row has one.

What the uniqueness assert cannot catch is over-merging in the other direction: it verifies that one CDI maps to one parent, not that one parent maps to a plausible number of CDIs. A placeholder tax number shared across many parties produces a single `uc_id` that sums unrelated customers' balances and fans the total out to every member CDI. Guard 2 reconciles cleanly in that scenario because the aggregate is internally consistent. A bound on CDIs per `uc_id`, or a format check on the tax number before it becomes a key, would close this.

The silent-degradation path matters more. `party_tax` is scoped to `BusinessDate = '{datadate}'` on `primary.party`, where `datadate` comes from `primaryactive.exposure`. If those two zones are not loaded in lockstep, `party_tax` returns nothing, every `COALESCE(pt.TaxRegistrationNumber, e.CDI)` degrades to the CDI, and the output silently changes grain from ~322K parents to ~385K single-member parents with no parent roll-up at all. Guard 1 passes (non-empty), Guard 2 passes (base total equals fanned total under a 1:1 map), Guard 2b passes (one row per CDI), Guard 3 passes (the row count rises slightly). An assertion that `party_tax` is non-empty and that `distinct(uc_id) / distinct(CDI)` is near the verified ~0.84 would catch it.

### portfolio_cdi_bal guards and failure semantics

The guards raise rather than exit, for the reason the comment states: `dbutils.notebook.exit()` terminates as SUCCESS and lets the orchestration treat a rejected delivery as a good refresh. Three gaps remain.

The reconciliation tolerance is absolute:

```python
recon_diff = abs(float(parent_total) - float(fanned_total))
assert recon_diff < 1.0, (...)
```

Both sides are double sums over hundreds of thousands of values, computed through different aggregation paths and partition orders, on a total in the tens of billions. Float summation error at that magnitude is not reliably below €1, so this can fail a good delivery non-deterministically. A relative tolerance keyed to the total is the safer formulation. The guard also only reconciles `Total_balance_EUR` — `on_balance_EUR`, `off_balance_EUR`, `initial_assign_bal_EUR` and `bal_over_settlement` are unchecked.

Guard 3 classifies read failures by substring:

```python
except AnalysisException as e:
    msg = str(e).upper()
    if ("TABLE_OR_VIEW_NOT_FOUND" in msg or "NOT FOUND" in msg
            or "CANNOT BE FOUND" in msg or "DOES NOT EXIST" in msg):
        prev_count = None  # first run, no previous data to compare
    else:
        raise
```

The narrowing is a real improvement over a bare `except`, but bare `NOT FOUND` also matches `SCHEMA_NOT_FOUND` and `CATALOG_NOT_FOUND`, which are configuration errors rather than first runs, and the floor check is then skipped. Matching on the UC error class alone would be tighter.

Step 3 was left on the old semantics, which is now inconsistent with the reasoning the branch itself documents:

```python
if base_count == 0:
    dbutils.notebook.exit("No data retrieved from base calculation. Keeping previous data. Exiting.")
```

Empty `base` is the most likely real failure (upstream `dexposure` missing for `datadate`), and it is the one path that still reports SUCCESS.

### Settlements: reparenting and the scale contract

Settlements move from `primaryactive.settlementexposure` to `derivativeactive.dsettlementexposure` because the primary table leaves `ParticipationPercentage` NULL for profits-sourced (QC-migrated) settlements. The parent is resolved from `party_tax` rather than through `cdi_map`, which is the actual fix for review #11: a guarantor or co-owner who settles may not appear in `dexposure` at all, and the old `cdi_map` path dropped those settlements.

The notebook depends on an undeployed upstream change that unifies participation to a 0-100 scale, and refuses to run until it lands:

```python
assert (r["max_pp"] is None) or (float(r["max_pp"]) > 1.0), (... "looks like the 0-1 scale ...")
```

Gating on the maximum is the weak point. One bad row above 1.0 in a source that is otherwise still fractional satisfies the check, and every amount in that source is then divided by 100 — a 100x understatement of `bal_over_settlement`, delivered silently. A distributional test (share of rows ≤ 1, or the median) would hold where the maximum does not.

Settlements that resolve to a parent absent from the output are dropped by the LEFT join in Step 6. That matches on-prem's `MERGE ... WHEN MATCHED`, but neither the row count nor the dropped amount is logged, which is the same invisibility that let the original `cdi_map` bug survive. A count and a sum of unmatched settlement amounts would make a future recurrence obvious.

One consumer note: `bal_over_settlement` repeats across every CDI of a multi-CDI parent, exactly like the balance columns. Any consumer summing the column across rows double-counts. That is the on-prem behaviour being reproduced, not a defect, but it is worth stating explicitly in the IT-facing notes alongside the new `uc_id` column and the `bal_over_settlement` / `initial_assign_bal_EUR` reordering, both of which break positional readers.

### VatSettlement: loan-level scope and an INNER tax join

The table narrows to loan-level settlements, with `AccountNumber = ExposureKey` aliased at source so the `AgreementKey` fallback cannot creep back in, and the markdown and code comments both document why agreement-level rows (`CASE_ACC_SYSTEM=19`, dash-composite keys) are excluded and that they will arrive later as a separate `AgreementNumber` column. The party join changes from LEFT to INNER plus an explicit non-null filter, mirroring on-prem's INNER JOIN on `PRC.ref.partyTaxNumber`:

```python
.join(party, on="CDI", how="inner")
.filter(col("taxNumber").isNotNull())
```

Both are needed: the INNER join only proves the CDI exists in party, while the filter catches a dedup winner that carries no tax number. Together they silently discard settlements, and unlike the Debt gates there is no count or percentage printed. Post-QC-decommission CDI linkage loss is called out in the comment as a known cause, which is exactly the situation where a drop metric would be worth having.

### Environment parameterization

`portfolio_cdi_bal` had `p-cdh` hardcoded in seven places across five queries plus `TARGET_CATALOG`; all now derive from `aws_utils.get_env_prefix()`. Two leftovers: the notebook's own markdown still documents `d-cdh.reference.initializationexposureinfo` in the design-decisions list and the mapping table, and `platforms/dtransfer_AllAccounts` still calls `` `p-cdh`.primary.get_all_exposures `` on the line above an f-string that uses `{env}`. The latter is pre-existing and outside this diff, but it means the branch's "no hardcoded prod catalog values anywhere" objective is not yet met at the repo level — a QA run of that notebook reads production.

</details>

<details>
<summary>Files changed</summary>

- `.kiro/steering/platform-migration-checklist.md` — PartyId/ParentPartyId rows rewritten to the agreed string-domain rule, status moved to Accepted (DNA26-1716)
- `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` — `party_tax` lookup, `BusinessDate` tie-break, `PartyId = CDI`, `ParentPartyId = COALESCE(tax, CDI)`, `CustomerVat` from `party_tax` (both the customer and representative builds)
- `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` — LEFT SEMI party gate with drop warning, `PartyId = exp_CDI`, DebtDetail/DebtFinancial gated to the DebtStatic LoanId set, cross-table LoanId-parity assert, PartyId-null assert, notebook JSON reformatted
- `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` — same party gate, drop warning and `PartyId = CDI`; `PartyKey` no longer selected from party
- `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` — loan-level scope documented, `AccountNumber` aliased at source, tax-preferring party dedup, INNER party join plus non-null tax filter and assert
- `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` — `{env}` parameterization, `party_tax` lookup, parent-grain `base` plus `cdi_map` fan-out with `uc_id` column, settlements from `dsettlementexposure` with per-source participation contract checks, column reorder, pre-write guards

Full diff: `git -C /home/karalis/cepal/gr.cepal.dna.de.delivery diff origin/DNA26-1716_platforms..HEAD`

</details>
