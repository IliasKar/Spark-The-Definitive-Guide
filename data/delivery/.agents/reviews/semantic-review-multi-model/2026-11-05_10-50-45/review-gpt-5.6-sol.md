# Parent-grain identity and derivative settlement delivery

## Summary

This branch replaces source-local party IDs with CDI across Customer, Debt, and DebtAB, introduces `ParentPartyId`/`uc_id` as a tax-number-or-CDI parent key, and reshapes `portfolio_cdi_bal` to fan parent balances back out to individual CDIs. Settlement allocation moves to `derivativeactive.dsettlementexposure` under the planned unified 0–100 participation contract, while new party gates and pre-write checks attempt to protect the regulatory outputs. The branch also responds to earlier review feedback with identity assertions, coverage logging, and row/balance reconciliation. Several safeguards miss material data states or fail open, so this version should not ship yet.

Watch for: **confirmed** blank tax values bypass the CDI fallback and can collapse unrelated customers under an empty parent key; **confirmed** party/tax selection differs across outputs and changes non-tax Customer attributes; **confirmed** settlement calculation assumes an upstream scale not yet present in production; **confirmed** pre-write guard failures can leave a stale table while the task reports success; **likely** the Debt parity check can heavily pressure the driver; **confirmed** party-gate breaches only print warnings before overwrite; **confirmed** end-to-end regulatory reconciliation has not been demonstrated for this diff.

**Verdict**: NEEDS_CHANGES

## High-level view

**Confirmed:** `COALESCE` and `isNotNull` treat empty or whitespace-only tax numbers as valid. Customer can therefore publish an empty `ParentPartyId`, VatSettlement can publish an empty `taxNumber`, and `portfolio_cdi_bal` can aggregate unrelated CDIs into one empty `uc_id` per portfolio.

**Confirmed:** the three consumers do not share one CDI-to-tax policy. Customer uses current-date, tax-present-first ranking; VatSettlement uses all history with tax-present-first ranking; `portfolio_cdi_bal` uses all history with source-first ranking. Customer also selects the entire lower-priority party row to obtain tax, changing fields unrelated to parent identity.

**Confirmed:** the settlement formula is valid only after the separately owned producer change reaches production. Current production data is still mixed-scale and contains null/out-of-range participation values, while this branch divides every source by 100 without checking the contract.

**Confirmed:** the overwrite protections do not fail the job for empty or sharply reduced output and can disable the previous-row comparison on any target-read exception. The prior table can remain stale while orchestration and downstream dependencies see a successful refresh.

**Likely:** collecting approximately one million IDs from each Debt output into Python sets creates avoidable driver-memory and serialization pressure. **Confirmed:** party-gate threshold breaches also continue to overwrite, allowing relationship-level loss even when LoanId-set parity passes.

**Confirmed gap:** no date-scoped execution of the changed notebooks or output-level key, distribution, and financial reconciliation accompanies the diff.

<details>
<summary>Issues (7)</summary>

1. **Blank parent and tax identifiers** (**confirmed**) — empty tax strings bypass the CDI fallback and can merge unrelated parties into one `uc_id`; normalize blank values to null and assert all delivered identifiers are nonblank.
2. **Divergent party resolution and Customer row drift** (**confirmed**) — the three notebooks use different date/ranking policies, while Customer changes the whole canonical row merely to obtain tax; preserve canonical source-first selection and reuse one normalized CDI-to-tax mapping everywhere.
3. **Undeployed participation scale contract** (**confirmed**) — production remains mixed 0–1/0–100 while the new formula always divides by 100; block deployment on the producer rollout and fail on null or out-of-range participation.
4. **Fail-open overwrite guards** (**confirmed**) — rejected output exits successfully and any target-read exception disables the prior-row check; raise task-failing errors and suppress only a genuine table-not-found condition.
5. **Driver-side LoanId parity** (**likely**) — three million-row ID sets are collected and retained on the driver; compare sets with distributed anti-joins and collect only bounded diagnostics.
6. **Non-blocking party-gate thresholds** (**confirmed**) — DebtStatic and DebtAB overwrite after a greater-than-1% party drop, and LoanId parity cannot detect relationship-only loss; make the threshold or a monitored relationship-distribution check block delivery.
7. **Missing date-scoped regulatory verification** (**confirmed**) — syntax checks do not establish output correctness; execute against a non-production target and compare keys, distributions, and totals with the accepted on-prem baselines.

</details>

<details>
<summary>Details</summary>

### Empty tax strings defeat the parent fallback

**Confidence: confirmed.** The new parent expressions use `coalesce(TaxRegistrationNumber, CDI)` in Customer (`platforms/dtransfer_Customer/dtransfer_Customer.ipynb:217,313`) and throughout `portfolio_cdi_bal` (`portfolio_cdi_bal/portfolio_cdi_bal.ipynb:238,257,269,400,411,480`). VatSettlement accepts `taxNumber` using only `isNotNull` (`platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb:185`). Empty and whitespace-only strings are non-null, so the CDI fallback and tax rejection never run. Read-only production checks found 44,042 blank tax winners under the new Customer ranking, including 9,188 CDIs in the current exposure perimeter; source-first selection still produced blank tax for 6,128 exposed CDIs.

In `portfolio_cdi_bal`, blank winners within a portfolio receive the same `uc_id`; their balances are summed into one parent and repeated onto every member CDI. The new uniqueness and balance-reconciliation assertions accept this because each CDI still maps to one parent and the combined total remains conserved. VatSettlement likewise passes blank tax through its new contract check.

Normalize with `NULLIF(TRIM(TaxRegistrationNumber), '')` before ranking and use the normalized field in every fallback and filter. Assert that `ParentPartyId`, `uc_id`, and delivered `taxNumber` are nonblank, and reject any empty parent associated with multiple CDIs.

### Tax enrichment changes the canonical Customer row and disagrees across notebooks

**Confidence: confirmed.** Customer ranks tax presence before source priority (`platforms/dtransfer_Customer/dtransfer_Customer.ipynb:92-96`), while `portfolio_cdi_bal.party_tax` ranks profits before QCA without tax presence (`portfolio_cdi_bal/portfolio_cdi_bal.ipynb:222-228`). VatSettlement uses tax-first ranking over all history (`platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb:150-161`), whereas Customer scans only `run_date`. A production comparison found 3,063 exposed CDIs with different parent keys under tax-first versus source-first ranking. Across the current-date party population, tax-first ranking changed the winning source for 34,094 CDIs and the selected tax for 34,095.

Customer ranks the whole party record, so obtaining tax from a lower-priority row also replaces `PartyKey`, names, party type, birth date, and the representative-join key. The notebooks' “in sync” comments therefore do not describe runtime behavior.

Keep the canonical Customer row under the established source-first/latest policy and derive normalized tax in a dedicated CDI-to-tax lookup. Reuse that lookup—or a persisted shared mapping—in Customer, VatSettlement, and `portfolio_cdi_bal`, with one documented date scope and deterministic tie-breakers.

### Settlement allocation is coupled to an undeployed scale contract

**Confidence: confirmed.** The new query computes `(ParticipationPercentage / 100.0) * SettlementAmount` (`portfolio_cdi_bal/portfolio_cdi_bal.ipynb:480-482`), matching the future 0–100 contract. Production did not meet it on 2026-11-03: profits had 39,726 rows with a maximum participation of `1.0000`, QCA reached `100.0000`, three profits rows were null, and QCA included `-0.0100`. Against that snapshot, the notebook understates profits allocations by a factor of 100 and silently omits null participation from `SUM`.

Make the producer rollout a hard deployment dependency, including rerun sequencing for the date consumed here. Before aggregation, fail on null or out-of-range values and record per-source min/max/distribution so a mixed-scale snapshot cannot be delivered.

### Overwrite guards preserve stale data but report success

**Confidence: confirmed.** Both new rejection paths use `dbutils.notebook.exit(...)` (`portfolio_cdi_bal/portfolio_cdi_bal.ipynb:603-606,637-641`). Databricks treats this as successful notebook termination, so an empty final result or a greater-than-25% row-count drop leaves the old table while the task and dependent workflow can remain green.

The prior-row check catches every exception from `spark.table(target_fqn).count()` and treats it as a first run (`portfolio_cdi_bal/portfolio_cdi_bal.ipynb:632-635`). A transient read failure, catalog problem, or unexpected runtime error therefore disables the regression check instead of failing closed.

Raise an exception for rejected deliveries and include old/new dates and counts. Suppress only the specific table-not-found condition for a genuine first run; every other target-read failure must stop the write.

### LoanId parity is implemented as a driver-side million-row operation

**Confidence: likely.** DebtStatic, DebtDetail, and DebtFinancial each contain roughly 1.04 million distinct loans, but the new check collects all three sets into Python and retains them simultaneously (`platforms/dtransfer_Debt/dtransfer_Debt.ipynb:827-835`). This adds serialization, driver heap, and repeated full-plan evaluation immediately before three large overwrites. Its error text compares only Static and Detail, so a Financial-only mismatch can fail with zero reported differences.

Use bidirectional `left_anti` joins or `exceptAll`, reducing only mismatch counts and a bounded sample to the driver. Compare Detail and Financial against one canonical set and report both relationships.

### Party-gate thresholds do not protect relationship coverage

**Confidence: confirmed.** DebtStatic and DebtAB print a warning above a 1% party-gate drop and still overwrite (`platforms/dtransfer_Debt/dtransfer_Debt.ipynb:395-397`; `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb:174-176`). DebtStatic's LoanId assertion cannot detect missing guarantor or co-owner rows when a loan retains another valid relationship; DebtAB has no equivalent relationship-count guard.

Fail above the accepted threshold or publish the metric to a quality gate that blocks overwrite. Validate relationship-type distributions as well as distinct LoanIds so a stable loan perimeter cannot mask customer-relationship loss.

### No date-scoped regulatory reconciliation accompanies the notebook changes

**Confidence: confirmed.** `git diff --check` passed, all five changed notebooks are valid JSON, and all 29 Python code cells compile. Read-only production queries confirmed the blank-tax, ranking, and mixed-scale findings. Not tested: fixed-date execution against a non-production target; uniqueness of `(portfolio_code, CDI)` and nonblank `uc_id`; Debt/DebtAB relationship distributions after gating; settlement totals by source before and after participation; and on-prem versus cloud row counts and financial totals.

Run the changed notebooks for one pinned business date and apply the repository's regulatory verification levels before approval. Include parent/CDI cardinality, blank/null key counts, source-level participation distributions, relationship-type counts, and settlement/balance totals.

</details>

<details>
<summary>File map</summary>

- `.kiro/steering/platform-migration-checklist.md` — records the accepted CDI and tax/CDI parent-key substitutions.
- `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` — publishes CDI as PartyId and derives ParentPartyId while changing party-row ranking.
- `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` — gates DebtStatic through party coverage, publishes CDI as PartyId, and adds cross-table loan-set validation.
- `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` — applies the CDI party gate and PartyId substitution to banking debt relationships.
- `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` — narrows delivery to loan-level settlements and requires a resolved tax value.
- `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` — introduces parent-grain aggregation/fan-out, derivative settlement participation, and pre-overwrite guards.

Full diff: `git diff origin/DNA26-1716_platforms..HEAD`

</details>
