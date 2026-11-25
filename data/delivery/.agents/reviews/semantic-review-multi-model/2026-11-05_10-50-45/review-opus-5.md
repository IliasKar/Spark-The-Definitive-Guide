# Party-identity rule, settlement re-sourcing and pre-write guards for the PLTFRM dtransfer delivery

Twelve commits on `DNA26-1716_platforms` close out a batch of review findings across four `dtransfer` delivery notebooks and `portfolio_cdi_bal`. The substantive changes are: a cross-platform party-identity rule (`PartyId` = CDI, `ParentPartyId` = `COALESCE(TaxRegistrationNumber, CDI)`) replacing the previously NULL/source-key columns; a party existence gate on `DebtStatic`/`DebtAB` implemented as a LEFT SEMI join instead of an attribute join; `portfolio_cdi_bal` reshaped to on-prem's parent-grain → CDI fan-out with a new `uc_id` column; settlements in `portfolio_cdi_bal` re-sourced from `derivativeactive.dsettlementexposure` with a `/100` rescale; `VatSettlement` narrowed to loan-level settlements with an INNER party join; and a set of pre-write guards and assertions.

Watch for: the `/100` rescale of `ParticipationPercentage` is wrong for profits settlements against prod data as it stands today — **confirmed** by querying the perimeter, profits rows carry participation on a 0–1 scale (all 14,717 rows exactly `1.0000`) while qca rows are 0–100, so the uniform divide understates profits settlement by 100x (€1.38M delivered vs €138.2M actual, ~88% of the perimeter settlement total missing). The notebook is correct only *after* the upstream `dsettlementexposure` unification lands, and nothing in the code detects the wrong scale. Second, the three notebooks that resolve a tax number for a CDI now do it three different ways (**confirmed**) despite comments claiming they are in sync. Third, the new party-gate warning explicitly says it "does not block the run" while the new LoanId parity assert turns the same event into a hard abort of all three Debt writes (**confirmed**).

**Verdict**: NEEDS_CHANGES

## High-level view

The party-identity rule is the backbone of the change. `PartyId` moves from NULL (Customer) or source-system `PartyKey` (Debt, DebtAB) to the CDI, and `ParentPartyId` from NULL to `COALESCE(TaxRegistrationNumber, CDI)`. Both are alphanumeric strings where on-prem delivered ints, and the steering doc records IT sign-off plus a note that consuming int columns "must be widened (DDL tracked separately)". That DDL is a hard release dependency with no gate in this repo: ship the notebooks before the target columns are widened and the upload fails on type, not on content.

Settlement sourcing in `portfolio_cdi_bal` is the blocking issue. Moving from `primaryactive.settlementexposure` (NULL `ParticipationPercentage` for profits) to `derivativeactive.dsettlementexposure` is the right call, but the accompanying `/100` assumes a unified 0–100 scale that is not in prod yet. Today profits is a fraction and qca is a percent; the code divides both. There is no range assertion, so a scale regression — or a partially-deployed fix — silently produces 100x-wrong regulatory amounts rather than failing.

Three notebooks now resolve "the tax number for this CDI" and all three disagree. `dtransfer_Customer` ranks party rows filtered to `run_date` with a non-null-tax tie-break; `dtransfer_VatSettlement` applies the same ordering over *all history*; `portfolio_cdi_bal.party_tax` uses all history *without* the tax tie-break, while its comment claims "the same source priority as dtransfer_Customer" and its design note claims `uc_id` therefore "agrees with dtransfer_Customer.ParentPartyId". 401,061 CDIs have both null-tax and non-null-tax party rows, so the divergence is not theoretical.

`portfolio_cdi_bal` gains a real grain change: balances aggregate at parent (`uc_id`) grain and fan out to each member CDI via `cdi_map`, matching on-prem's final INSERT. The fan-out is guarded by a balance reconciliation, but `base` and `cdi_map` are built from the same query with one difference — `cdi_map` filters `CDI IS NOT NULL` and `base` does not. There are zero NULL-CDI exposures in the perimeter today, so the guard passes; the first one that appears with a non-zero balance makes the reconciliation abort the run instead of warning.

The guard posture is inconsistent in three directions. Several new assertions cannot fail by construction (NULL `PartyId` after a semi-join on that same column, NULL `taxNumber` after a `isNotNull` filter, one `uc_id` per CDI when `uc_id` is a function of CDI), which buys no safety while reading as coverage. The party gate warns at a 1% threshold but the downstream parity assert aborts on a single loan. And `portfolio_cdi_bal` got empty-result, reconciliation and 25%-shrink guards while `Debt`, `DebtAB` and `VatSettlement` — same regulatory blast radius, and `VatSettlement` just got a deliberate perimeter reduction — still overwrite unconditionally with no row-count floor.

`VatSettlement` narrows twice in this diff: loan-level only (agreement-level `CASE_ACC_SYSTEM=19` excluded, tracked as a follow-up column) and LEFT → INNER party join plus a non-null tax filter to reproduce on-prem's no-null-tax contract. Both are justified in the notebook, but the row count moves and nothing records or enforces the new expected number.

Verification remains entirely manual and prod-bound. There is no test harness in the repo, the hardcoded expected count in `dtransfer_Debt` was replaced by a pointer to the checklist rather than by a machine check, and `portfolio_cdi_bal` hardcodes `p-cdh`/`p-cdh-delivery` (extended by the new `party_tax` and `dsettlementexposure` reads) so it cannot be exercised in dev or QA without editing the notebook. On the plus side, the flag-type swap in the settlement query is a genuine fix: `primaryactive.portfolio.processSettlementFlag` is `boolean` and `dsettlementexposure.IsActiveFlag` is `int`, so the base-branch predicates were crossed and would have failed analysis.

<details>
<summary>Issues (17)</summary>

1. **ParticipationPercentage 100x understatement** — profits rows in the cdi_bal perimeter are on a 0–1 scale today (all `1.0000`), so `/100` delivers €1.38M instead of €138.2M. Do not run or release until the upstream `dsettlementexposure` unification is deployed and verified.
2. **No scale assertion on ParticipationPercentage** — add a pre-aggregate check (e.g. per `SourceSystem`, `MAX(pp) > 1` and `MAX(pp) <= 100`) so a missing or regressed upstream fix fails the run instead of silently rescaling.
3. **NULL and negative participation unguarded** — a NULL makes `SUM` skip the row, silently contributing 0; qca currently carries `-0.0100`. Assert non-null and decide explicitly whether negatives are valid.
4. **`party_tax` dedup diverges from Customer/VatSettlement** — it lacks the non-null-tax tie-break, so `uc_id` can be a CDI where `dtransfer_Customer.ParentPartyId` is a tax number. Align the ordering or drop the "agrees with dtransfer_Customer" claim.
5. **Customer party is single-date, VatSettlement is all-history** — same ordering over different input sets still resolves different winners. Pick one scope for the shared party-identity resolution.
6. **Customer tie-break changes attribute provenance** — ranking non-null tax first can switch the winning party row to a lower-priority source, moving `LegalNameGR`, `BirthDate`, `cust_id` and the commercial name with it. Resolve the tax number separately from the attribute row, or measure the attribute churn before shipping.
7. **Party gate warns, parity assert aborts** — the gate tolerates up to 1% dropped rows as a print-only warning while the LoanId parity assert hard-fails all three Debt writes on a single loan-level divergence. Gate `debt_detail`/`debt_financial` with the same semi-join, or make the assert a threshold.
8. **Assertions that cannot fail** — NULL `PartyId` post-semi-join (Debt, DebtAB), NULL `taxNumber` after the `isNotNull` filter (VatSettlement), and the `cdi_map` one-`uc_id`-per-CDI check are tautological. Move them to the pre-gate frames or delete them.
9. **`base` vs `cdi_map` NULL-CDI asymmetry** — `cdi_map` filters `CDI IS NOT NULL`, `base` does not, so one NULL-CDI exposure with a balance fails the reconciliation assert and aborts the run. Apply the same filter to `base`.
10. **Guard 3 fails open** — `except Exception: prev_count = None` disables the row-drop guard on any read failure (permissions, metastore error), not just a missing table. Catch only table-not-found.
11. **Row inflation unguarded** — the reconciliation rolls back with `MAX` per `(portfolio, uc_id)`, so duplicated CDI rows net to zero diff, and only a >25% shrink is caught. Add a distinct-`(portfolio_code, CDI)` = row-count check.
12. **Driver-side LoanId sets** — three `.distinct().collect()` of ~1.05M keys into Python sets, on an uncached DAG that is also recomputed for each `count()`. Replace with Spark `LEFT ANTI` counts and cache `debt_static`.
13. **No pre-write guards on Debt/DebtAB/VatSettlement** — unconditional `overwrite` on regulatory tables with no empty-result or row-count floor, unlike `portfolio_cdi_bal`. Extend the same guards.
14. **VatSettlement perimeter reduction unrecorded** — loan-level-only plus the INNER party join moves the row count off the documented ~76K with no new expected value and no guard. Record the new count and add a floor.
15. **String PartyId/ParentPartyId needs target DDL widened** — a cross-team dependency with no gate in this repo. Confirm the int→string widening is deployed before these notebooks run in prod.
16. **Prod catalogs hardcoded in `portfolio_cdi_bal`** — new `party_tax` and `dsettlementexposure` reads extend the `p-cdh` hardcoding, so the notebook cannot be tested in dev/QA. Switch to `aws_utils.get_env_prefix()` like the dtransfer notebooks.
17. **Guard exits look like successful runs** — Guards 1 and 3 use `dbutils.notebook.exit`, which finishes green. Confirm the workflow surfaces the exit message as an alert, otherwise a skipped delivery goes unnoticed.

</details>

<details>
<summary>Details</summary>

### ParticipationPercentage: two scales in prod, one divisor in the code

The notebook states the assumption plainly:

```
# Scaling: dsettlementexposure.ParticipationPercentage is on a single 0-100
# percent scale for BOTH qca and profits (unified in dsettlementexposure), so divide by
# 100 uniformly. No COALESCE default is needed - the column is fully populated.
```

Measured against prod at the current reference date, restricted to the exact cdi_bal perimeter (`unit_code_profits IS NOT NULL`, `PortfolioId < 600`, `processSettlementFlag = true`, `IsActiveFlag = 1`, `CDI IS NOT NULL`):

| SourceSystem | rows | min pp | max pp | `SUM(pp/100 * amt)` | `SUM(pp * amt)` |
|---|---|---|---|---|---|
| profits | 14,717 | 1.0000 | 1.0000 | 1,382,045 | 138,204,537 |
| qca | 4,692 | 0.0000 | 100.0000 | 16,264,305 | 1,626,430,509 |

profits is a fraction, qca is a percent. The uniform `/100` is right for qca and 100x low for profits, so `bal_over_settlement` would be delivered as ~€17.6M against an actual ~€154.5M. The dependency on the upstream unification fix is documented; what is missing is that the notebook neither detects nor survives the pre-fix state. A per-source range check before the aggregation (for each `SourceSystem` in the perimeter, `MAX(pp) > 1 AND MAX(pp) <= 100`) turns a silent 100x error into a failed run.

Across the whole table (not just the perimeter) the same query shows profits `max = 1.0000` over 31,992 active rows and qca spanning `-0.0100` to `100.0000`. The negative value matters independently of the scale fix: a negative participation produces a negative settlement amount that no guard rejects. And while `null_pp` is 0 today, the "no COALESCE needed" reasoning is load-bearing — under `SUM`, a NULL participation drops that settlement's contribution to zero rather than surfacing anything.

### Three resolutions of the same tax number

The branch set out to unify party dedup (commit `9d5e49e`, "unify party dedup tie-break across Customer and VatSettlement") and left three variants:

```
dtransfer_Customer      party filtered to BusinessDate = run_date
                        order: [tax non-null, src priority, BusinessDate desc]

dtransfer_VatSettlement party = all history
                        order: [tax non-null, src priority, BusinessDate desc]

portfolio_cdi_bal       party = all history
  (party_tax)           order: [src priority, BusinessDate desc]
```

`party_tax` was added *after* the unification commit and still carries the pre-unification ordering, under a comment that says "deduped by CDI with the same source priority as dtransfer_Customer" and a design note that says the parent key therefore "agrees with `dtransfer_Customer.ParentPartyId`". For a CDI whose profits row has no tax number but whose qca row does, `dtransfer_Customer` emits the tax number as `ParentPartyId` while `portfolio_cdi_bal` emits the CDI as `uc_id` — two delivered tables keyed on what is documented as the same parent identifier. 401,061 CDIs in `primary.party` have both null-tax and non-null-tax rows, which bounds the exposure; the exact divergence count timed out at the 60s query limit and should be measured.

The Customer/VatSettlement pair is also only half-aligned: identical ordering, but Customer ranks within a single `BusinessDate` while VatSettlement ranks across all history. A CDI whose current-date row lacks a tax number and whose older row has one resolves differently in the two notebooks. In Customer the `BusinessDate desc()` tie-break is inert for the same reason — there is only one date in the frame.

Separately, the Customer tie-break has reach beyond the parent key. The winning party row supplies `LegalNameGR`/`LastNameGR`/`FirstNameGR`, `PrimaryCommercialGR`, `BirthDate` and `PartyKey`→`cust_id`. Ranking non-null tax ahead of source priority can hand all of those to a non-profits row for CDIs where profits lacks a tax number, which is a change in attribute provenance that the commit message frames purely as a dedup alignment.

### The party gate warns; the parity assert aborts

`DebtStatic` and `DebtAB` replace a LEFT attribute join on party with a LEFT SEMI gate — the right shape, since it cannot inflate rows if party dedup changes. The drop is instrumented and explicitly non-blocking:

```python
if _drop_pct > 0.01:
    print(f"WARNING: party gate dropped {_drop_pct:.2%} of DebtStatic rows "
          f"(> 1% threshold) - investigate upstream party/CDI coverage.")
```

Three cells later, strict set equality across the three Debt tables:

```python
assert _ls_static == _ls_detail == _ls_fin, (...)
```

`debt_detail` and `debt_financial` are built from `dexposure` with no party gate, so the sets can only agree while the gate drops nothing at loan level. The moment the gate removes every relationship row for one loan — the case the 1% threshold is there to tolerate — the assert fires and no table is written. Fail-closed on a regulatory table is defensible, but the two guards encode opposite policies for the same upstream condition, and the operational result is that a CDI-coverage blip blocks the entire Debt delivery with no override. Gating `debt_detail`/`debt_financial` with the same semi-join would make parity hold by construction and leave the warning as the single policy.

Today both hold: 0 rows dropped by the gate in practice, and an anti-join against prod shows 0 of 1,045,917 `dexposure` loans in the non-banking perimeter lacking a `primary.exposure` relationship row, so `static ⊆ detail` is tight.

The parity check itself pulls the keys to the driver:

```python
_ls_static = {r[0] for r in debt_static.select("LoanId").distinct().collect()}
_ls_detail = {r[0] for r in debt_detail.select("LoanId").distinct().collect()}
_ls_fin = {r[0] for r in debt_financial.select("LoanId").distinct().collect()}
```

Three Python sets of ~1.05M keys each, on frames that are never cached — `debt_static` alone is recomputed for the gate counts, the row-count print, the NULL-`PartyId` assert, this collect and the write. Two `LEFT ANTI` counts plus a distinct count give the same answer in Spark.

### Assertions that cannot fail

Four of the new guards are tautological:

- `debt_static.filter(col("PartyId").isNull())` — `PartyId` is `exp_CDI`, and the preceding semi-join on `exp_CDI = pty_CDI` cannot match a NULL. Same for `DebtAB`.
- `vatsettlement_df.filter(col("taxNumber").isNull())` — directly preceded by `.filter(col("taxNumber").isNotNull())`.
- `cdi_map` "a CDI must map to exactly one `uc_id`" — `uc_id` is `COALESCE(pt.TaxRegistrationNumber, e.CDI)` with `party_tax` already deduped to one row per CDI, so `uc_id` is a function of `CDI` and `COUNT(DISTINCT uc_id) > 1` is unreachable.

Each costs a full recomputation of its frame and reads as validation coverage that is not there. The checks have value one step earlier — on `debt_static_base` (pre-gate NULL CDI), on the party frame (CDIs with no resolvable tax), and on a `cdi_map` variant built before the dedup.

### `base` vs `cdi_map`: one filter apart

Both views are the same `portfolio ⋈ dexposure ⋈ party_tax` query at parent grain. `cdi_map` adds `WHERE e.CDI IS NOT NULL`; `base` has no such filter. A NULL-CDI exposure therefore lands in `base` under `uc_id IS NULL` (the `party_tax` join also misses, so `COALESCE` yields NULL) with its balance counted, and has no `cdi_map` row, so the inner join in Step 4 drops it. The new reconciliation then compares a `parent_total` that includes the orphan group against a `fanned_total` that does not, and `assert recon_diff < 1.0` aborts the run.

The perimeter has 0 NULL-CDI rows at the current reference date, so this is latent rather than active — but it converts a single upstream data-quality blip into a hard failure of the whole notebook, which is the opposite of what Guards 1 and 3 do for comparable conditions (clean `dbutils.notebook.exit`, previous data retained).

### Guard coverage: shrink yes, inflation no, read errors open

Guard 3 reads the currently delivered table to compare row counts:

```python
try:
    prev_count = spark.table(target_fqn).count()
except Exception:
    prev_count = None
```

The comment scopes this to "treat a missing table as first run", but the bare `except` also swallows permission errors, metastore timeouts and malformed-table errors, each of which disables the guard and lets the overwrite proceed. Narrowing to the table-not-found condition keeps the first-run path while failing loudly on everything else.

The guard set is also asymmetric by direction. Guard 2 rolls the fanned output back with `MAX(Total_balance_EUR)` per `(portfolio_code, uc_id)`, so duplicate CDI rows inside a parent — the classic fan-out regression — leave the reconciliation total unchanged. Guard 3 only triggers below 75% of the previous count. A fan-out bug that doubles rows passes both. A distinct-`(portfolio_code, CDI)` versus total-row-count assertion closes that, and is the natural companion to the `cdi_map` uniqueness check that cannot currently fail.

Guard 1 and Guard 3 use `dbutils.notebook.exit`, which ends the notebook with a success status. In a job context that means a silent skip unless the orchestration inspects the exit value — worth confirming the Databricks workflow treats the exit string as an alert rather than a green run.

### VatSettlement: two deliberate narrowings, no new floor

The scope reduction to loan-level settlements and the LEFT → INNER party join (plus `taxNumber IS NOT NULL`) are both argued in the notebook against on-prem's `prep_VatSettlement` behaviour, and the agreement-level follow-up is documented well enough that nobody will reintroduce the `AgreementKey` fallback by accident. What is missing is the consequence: the delivered row count moves away from the ~76K in the migration checklist, no new expected value is recorded, and the write stays an unconditional `overwrite` with no empty-result or floor check — on the same catalog that `portfolio_cdi_bal` now protects with three guards. `Debt` and `DebtAB` share that gap: one bad upstream day empties or halves any of these three tables and the delivery replaces good regulatory data without complaint.

### Verification posture and diff hygiene

There is no test harness in the repo; correctness rests on notebook prints, the new asserts, and manual comparison against on-prem via the MSSQL MCP. The hardcoded `1,608,266` expectation in `dtransfer_Debt` was replaced with "see the migration verification checklist for the current expected count" — defensible as a comment, but it removes the only number a reader could check against, and the checklist's `dtransfer_Debt` row still reads "Validated (pending upstream fixes)" with pre-gate figures. `portfolio_cdi_bal` remains pinned to `p-cdh`/`p-cdh-delivery` (the new `party_tax` and `dsettlementexposure` reads extend that), so nothing in it can be validated outside prod.

Both `dtransfer_Debt.ipynb` and `dtransfer_DebtAB.ipynb` were reformatted whole-file (one-line JSON objects expanded, trailing newline dropped, BOM removed from DebtAB), which inflates the diff around the real edits. One `source` element in `dtransfer_Debt` also now lacks its trailing `\n` and relies on the next element being `"\n"`; all five notebooks parse and every code cell compiles, but hand-edited notebook JSON with implicit line joins is a trap for the next edit.

</details>

<details>
<summary>File map</summary>

- `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` — `PartyId` = CDI and `ParentPartyId` = `COALESCE(TaxRegistrationNumber, CDI)` in both the party and representative branches; party dedup tie-break now ranks non-null tax first.
- `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` — party LEFT join replaced by a LEFT SEMI gate with drop instrumentation; `PartyId` = exposure CDI; NULL-`PartyId` assert; cross-table LoanId parity assert before the three writes; whole-file JSON reformat.
- `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` — same gate/`PartyId`/assert treatment; whole-file JSON reformat and BOM removal.
- `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` — loan-level-only scope documented, INNER party join plus non-null tax filter, party dedup aligned with Customer, NOT NULL assert.
- `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` — `uc_id` parent grain with CDI fan-out via `cdi_map`, `party_tax` lookup, settlements re-sourced from `derivativeactive.dsettlementexposure` with `/100` rescale and flag-literal fixes, output column reorder, three pre-write guards.
- `.kiro/steering/platform-migration-checklist.md` — Known Differences updated for the CDI/`ParentPartyId` rule and IT sign-off.

Full diff: `git -C /home/karalis/cepal/gr.cepal.dna.de.delivery diff origin/DNA26-1716_platforms..HEAD`

</details>
