# Party-identity rule across dtransfer tables, plus uc_id/CDI fan-out in portfolio_cdi_bal

Five commits (`4ecf465..5fdbe10`) land the agreed cross-platform party-identity rule and rework two delivery notebooks. `dtransfer_Customer`, `dtransfer_Debt` and `dtransfer_DebtAB` now emit `PartyId = CDI` and `ParentPartyId = COALESCE(TaxRegistrationNumber, CDI)` instead of NULLs/`PartyKey`, and the party lookup becomes a `left_semi` gate so a row is delivered only when its CDI resolves in `primary.party`. `dtransfer_VatSettlement` widens its perimeter to agreement-level settlements (`AccountNumber = COALESCE(ExposureKey, AgreementKey)`), switches the party join to INNER plus a non-null tax filter, and documents the remaining 35% row gap against on-prem. `portfolio_cdi_bal` splits what was a single `CDI` key into `uc_id` (parent) and `CDI` (member) and reproduces the on-prem fan-out: balances aggregated once per `(PortfolioId, uc_id)` and repeated across each member CDI.

Watch for: the reworked settlement block in `portfolio_cdi_bal` cannot execute as written (two int/boolean type errors, confirmed against the live schemas), and once those are fixed it produces amounts ~100x on-prem and populates `bal_over_settlement` on 1,586 rows versus 18,493 on-prem (confirmed by reproducing the logic in prod). `VatSettlement.AccountNumber` now emits dash-composite agreement keys (`CSR7009902691-0000000002649116`) for 8,147 rows; the on-prem target contains no such value (confirmed). The parent key `uc_id` is read from `dexposure.PartyTaxRegistrationNumber`, which disagrees with `dtransfer_Customer.ParentPartyId` for 3,063 CDIs (confirmed). The fan-out grain itself checks out: 374,076 cloud `(portfolio, uc_id)` groups against 374,135 on-prem, and €50.57B fanned balance against €51.05B.

**Verdict**: NEEDS_CHANGES

## High-level view

The `left_semi` gate cannot duplicate rows the way an INNER join on a re-deduplicated party table could, and it guarantees the non-null `PartyId` the on-prem targets declare. On current data it filters almost nothing (0 unresolvable CDIs, 45 null-CDI exposure rows at 2026-11-02), so published row counts barely move, but nothing logs how many rows it removes — a regression in party coverage would silently shrink a regulatory table.

The identity rule changes the value domain of columns the on-prem targets define as `int NOT NULL` (`dtransfer.Customer.PartyId`, `DebtStatic.PartyId`, `DebtAB.PartyId`). 89% of cloud CDIs are not int-castable, so the IT sign-off recorded in the notebook comments has to be matched by a type change on the consuming schema. The tracked steering checklist still documents `PartyId = source-system PartyKey` and hardcoded expected row counts, while the Debt notebook now points at that checklist for "the current expected count" — a circular reference until the checklist is updated.

The `portfolio_cdi_bal` fan-out holds up against on-prem at the grain and the money level. The settlement column does not, and it fails in three independent ways: the statement cannot be analysed because of two int/boolean comparisons in the WHERE clause it reworks; a NULL `ParticipationPercentage` on 46% of active settlements NULLs the whole product and leaves 1,586 populated rows against 18,493 on-prem; and the surviving amounts sit at 100.8x on-prem, a percent-versus-fraction mismatch.

The parent key `uc_id` is sourced from `dexposure.PartyTaxRegistrationNumber` rather than from the party table. For 3,063 CDIs that column is NULL while `primary.party.TaxRegistrationNumber` is populated, so `portfolio_cdi_bal` treats them as their own parent while `dtransfer_Customer` publishes the tax number as `ParentPartyId` — the two delivered keys cannot be joined, and those customers do not roll up with their siblings. The same choice drives the settlement mapping: because the CDI→uc_id map is built from exposures, 2,492 settlement rows (€129M) whose CDI has no exposure row cannot reach their parent at all, where on-prem maps through `DWH.prt.Party` and still finds it.

`VatSettlement` gains 8,147 agreement-level rows, but `AgreementKey` is a dash-composite while the on-prem target holds 11-19 character account numbers with no dashes, so the new rows land outside the key domain IT consumes. The new "prefer a row with a tax number" dedup tie-break is applied only in this notebook, so the same CDI can get a tax number in `dtransfer_vatsettlement` and a NULL `CustomerVat` in `dtransfer_customer`. The documented 35% row gap decomposes into two distinct causes worth recording in the notebook: 10% fewer active settlements and 27% fewer accounts per settlement (1.49 against 2.04).

<details>
<summary>Issues (17)</summary>

1. **Settlement amount scale** (confirmed) — cloud `bal_over_settlement` averages €1,037,833 against on-prem €10,294 (100.8x). `ParticipationPercentage` ranges 0-100 in `settlementexposure`; divide by 100 (or confirm the on-prem factor) before summing.
2. **Settlement coverage collapse** (confirmed) — 46% of active settlements have NULL `ParticipationPercentage`, NULLing the product and leaving 1,586 populated rows against 18,493 on-prem. Decide on a default (`COALESCE(..., 100)`) or fix upstream.
3. **Settlements statement cannot run** (confirmed) — `se.IsActiveFlag = true` (INT vs BOOLEAN) and `pf.processSettlementFlag = 1` (BOOLEAN vs INT) both raise `DATATYPE_MISMATCH`. Use `= 1` and `= true` respectively.
4. **Settlement parent mapping drops rows** (confirmed) — the INNER join to `cdi_map` drops 2,492 settlement rows (€129M, 7.6%) whose CDI has no exposure; ~€48M of that belongs to parents present in `base`. Map CDI→uc_id from the party table instead.
5. **uc_id provenance mismatch** (confirmed) — `dexposure.PartyTaxRegistrationNumber` is NULL for 3,063 CDIs that do have a tax number in `primary.party`, so `uc_id` disagrees with `dtransfer_Customer.ParentPartyId` and those parents under-roll-up. Derive `uc_id` from party, or coalesce through it.
6. **Output column order** (confirmed) — the final select ends `initial_assign_bal_EUR, bal_over_settlement`; the on-prem table ends `bal_over_settlement, initial_assign_bal_EUR`. Two decimal columns would swap under positional ingestion.
7. **Null-CDI balance leakage** (confirmed, latent) — `base` includes exposures with NULL CDI while `cdi_map` excludes them, so the inner fan-out join silently drops those balances. Add a reconciliation of parent-grain total against fanned total.
8. **No uniqueness guard on the new grain** (likely) — `uc_id` is computed per exposure row, so one CDI can appear under two parents and duplicate `(portfolio_code, CDI)`. Zero occurrences on 2026-10-30; add an assertion like the one `VatSettlement` now carries.
9. **Delivered shape change** (confirmed) — `portfolio_cdi_bal` gains a column and repeats balances across member CDIs, so row-level sums rise ~3%. Needs explicit consumer notification before the overwrite ships.
10. **VatSettlement account-number domain** (confirmed) — 8,147 new rows emit dash-composite `AgreementKey` values; the on-prem target has none (max length 19, no dashes), and loan-level keys are bare numeric in cloud versus 3-char-prefixed on-prem. Agree the expected format with IT.
11. **Dedup tie-break divergence** (likely) — the "prefer non-null tax" ordering is applied only in `VatSettlement`, so the same CDI can yield a tax number there and a NULL `CustomerVat` in `dtransfer_Customer`. Apply one ordering in both or document the split.
12. **Perimeter gap not decomposed** (confirmed) — record in the notebook that the 35% gap is 10% fewer active settlements plus 27% fewer accounts per settlement, so the investigation targets missing settlement↔account links.
13. **Broken markdown table** (confirmed) — the perimeter-gap blockquote sits inside the mapping table; rows after it stop rendering as table rows. Move it above or below.
14. **Party gate is silent** (confirmed) — neither Debt notebook logs how many rows the `left_semi` removes. Print pre/post counts so a coverage regression is visible.
15. **PartyId type contract** (confirmed) — on-prem `Customer.PartyId`, `DebtStatic.PartyId` and `DebtAB.PartyId` are `int NOT NULL`; 89% of CDIs are not int-castable. The sign-off needs a matching DDL change on the consuming side, captured somewhere other than a notebook comment.
16. **Stale steering checklist** (confirmed) — `.kiro/steering/platform-migration-checklist.md` still lists `PartyId = PartyKey` and hardcoded expected counts that the Debt notebook now defers to. Update it in this change.
17. **Reference-date lag** (confirmed, pre-existing) — `portfolio_cdi_bal` derives the date from `primaryactive.exposure` (2026-11-02) but reads `derivative.dexposure` (max 2026-10-30), so the run exits at the safety check and the new grain never reaches the target. Pin the date to `dexposure` or pass it explicitly.

</details>

<details>
<summary>Details</summary>

### The uc_id/CDI fan-out reproduces the on-prem grain

The design matches the SP: on-prem aggregates at `ParentPartyId`, then joins `Party` + `PartyReferenceCodes` in the final INSERT to explode each parent into its member CDIs. Reproducing the new cloud logic against prod for 2026-10-30:

```
                            cloud       on-prem      delta
(PortfolioId, uc_id) groups  374,076    374,135     -0.02%
output rows (pf, CDI)        385,324    390,430      -1.3%
distinct uc_id               322,432    322,498     -0.02%
SUM(total_balance_EUR)       €50.57B    €51.05B      -0.9%
```

The parent-grain sum is €47.45B, so on-prem's €51.05B also double-counts across the fan-out — the two numbers agree because both repeat the same way, which is the point of the change.

Two structural gaps remain around the join. `base` aggregates every exposure row, including those with NULL CDI, whose `uc_id` can itself be NULL; `cdi_map` filters `CDI IS NOT NULL`, and the inner join then drops those groups without a trace. Zero such rows exist on 2026-10-30, so this is latent rather than active, but nothing reconciles the parent-grain total against the fanned total to catch it. Separately, `uc_id` is recomputed per exposure row, so a CDI whose `PartyTaxRegistrationNumber` is populated on some exposures and NULL on others would appear under two parents and duplicate `(portfolio_code, CDI)` with two different balance sets. Verified clean today (385,324 map rows for 385,324 distinct pairs), and `VatSettlement` in this same diff grew an assertion for its own contract — this notebook should get the equivalent.

The `uc_id` insertion point matches the on-prem column order. The tail does not: the on-prem table is `..., off_balance_EUR, bal_over_settlement, initial_assign_bal_EUR`, while the notebook writes `..., off_balance_EUR, initial_assign_bal_EUR, bal_over_settlement`. Both are decimals, so a positional load would swap them silently.

### bal_over_settlement is unreachable, then mis-scaled

The settlement statement is reworked to aggregate at parent grain, but it fails analysis before producing anything. `primaryactive.settlementexposure.IsActiveFlag` is `INT` and is compared to `true`; `primaryactive.portfolio.processSettlementFlag` is `BOOLEAN` and is compared to `1`. Both raise `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` (verified against the live schemas on the SQL warehouse; Spark applies the same coercion rules). The two comparisons are pre-existing lines, but they sit in the statement this commit rewrites, so the new parent-grain logic has never run.

With both comparisons corrected, reproducing the block for 2026-10-30 gives 9,204 parent groups, of which 7,699 carry a NULL amount: `ParticipationPercentage * SettlementAmount` is NULL whenever the percentage is NULL, which is the case for 23,170 of 49,900 active settlements (`SettlementAmount` is never NULL). After the fan-out that leaves 1,586 rows with a settlement amount against 18,493 on-prem.

The 1,505 groups that do produce a number are out by two orders of magnitude:

```
                  cloud          on-prem
groups/uc_ids     1,505          17,633
sum               €1.56B         €190.4M
avg               €1,037,833     €10,294
max               €16,248,000    €770,000
```

The average ratio is 100.8x and `ParticipationPercentage` ranges from -0.01 to 100 in `settlementexposure`, so the cloud column is a percentage where the on-prem expression consumed a fraction. A `/100` (and a decision on NULL percentages) is needed before this column can be delivered.

The mapping itself also loses settlements. Because `cdi_map` is built from `dexposure`, a settlement whose CDI holds no exposure in that portfolio cannot be resolved to a parent: 2,492 of 20,203 in-perimeter settlement rows, carrying €129.1M of €1.69B. On-prem resolves the parent through `DWH.prt.Party`, so it still credits the parent when the settling party itself has no loans; roughly 426 rows and €48.1M of the dropped amount belong to parents that are present in `base` (approximate — a party-based map is not unique per CDI without the standard dedup, which inflates that probe). Building the CDI→uc_id map from the party table fixes this and issue 5 together.

### PartyId = CDI and the left_semi party gate

The gate's filtering effect on current data is negligible: at 2026-11-02, the non-banking perimeter has 3,238,600 exposure rows with 45 null CDIs and zero unresolvable CDIs; the core perimeter has 505,725 rows with neither. So the published counts in the checklist stay valid, and the Debt notebook's new "see the checklist rather than a hardcoded number" comment points at a document that was not updated in this change and still records the old `PartyId = PartyKey` mapping. Since the gate is the only thing standing between a party-coverage regression and a shrunken regulatory table, it should log the number of rows it drops.

The value domain is the open contract question. `dtransfer.Customer.PartyId`, `DebtStatic.PartyId` and `DebtAB.PartyId` are all `int NOT NULL` on-prem; in cloud, 2,041,720 of 2,293,224 party rows (89%) have a CDI that is not int-castable, up to 23 characters, and 60,319 rows have a non-int-castable `TaxRegistrationNumber`. The sign-off is recorded in notebook comments only; the consuming schema has to change with it. `PRC.operation.portfolio_cdi_bal` is the counter-example that works in the sign-off's favour: `uc_id` is already `nvarchar(30)` and `CDI` `nvarchar(40)`, and the cloud values (tax ≤18, CDI ≤23 characters) fit.

### VatSettlement: wider perimeter, mixed key formats

Including `CASE_ACC_SYSTEM = 19` settlements adds 8,147 rows at 2026-10-30 (8,160 at 2026-11-02), but it puts two key shapes in one column. `ExposureKey` in `settlementexposure` is bare numeric (41,807 of 41,809 at 2026-11-02), while every `AgreementKey` is a dash-composite — `CSR7009902691-0000000002649116`, `GLB1809939191-0000000004165600`. The on-prem target holds 71,509 distinct account numbers, 11-19 characters, zero containing a dash, with 45,546 carrying a 3-char portfolio prefix (`GLB989007135311800`). So the agreement-level rows are outside the domain IT matches on, and the loan-level rows are bare where on-prem is prefixed — the same expression this commit rewrites. `AccountNumber` is `nvarchar(100)`, so nothing truncates; the mismatch is silent.

The INNER join's rationale checks out — on-prem `taxNumber` is `nvarchar(100) NOT NULL`. The dedup change underneath it is the part to watch: ranking non-null `TaxRegistrationNumber` ahead of source-system priority is done only in this notebook, so for a CDI whose profits row has no tax number but another source does, this table publishes a tax number while `dtransfer_Customer.CustomerVat` publishes NULL — and the winning row can come from a lower-trust source than the one `dtransfer_Customer` picks for the same customer. 401,036 CDIs have both null and non-null tax rows, the upper bound on the divergence; the exact count of CDIs where the tie-break changes the winner did not complete inside the query timeout.

The documented gap is honest about not being parity, and the numbers in it hold up (≈49.7K deliverable rows from 49,900 active less 242 null-CDI). It is worth recording the decomposition against on-prem's 76,455: cloud has 33,487 distinct active settlements against 37,406 on-prem codes (-10.5%), and 1.49 accounts per settlement against 2.04 (-27%). Even delivering every active row, cloud reaches 65% of on-prem — so the investigation belongs on missing settlement↔account links, not on the active-status filter.

One rendering nit in the same cell: the perimeter-gap blockquote is inserted between rows of the mapping table, which terminates the table early and leaves the remaining three mappings rendering as loose text.

### Verification posture

Every number here came from re-running the notebooks' logic by hand against prod. `VatSettlement`'s new null-`taxNumber` assertion is the only programmatic contract check in the diff. Not tested: `(portfolio_code, CDI)` uniqueness on the new grain, parent-grain versus fanned-out balance reconciliation, the count of rows the party gate removes, and whether `uc_id` agrees with `dtransfer_Customer.ParentPartyId` across the two deliverables.

`portfolio_cdi_bal` also cannot currently produce the new grain: the reference date comes from `primaryactive.exposure` (2026-11-02) while `derivative.dexposure` stops at 2026-10-30, so `base` is empty and the notebook exits at its safety check. That is a pre-existing date derivation, but its effect is that the delivered table keeps the old shape (no `uc_id`) while reporting a clean run.

</details>

<details>
<summary>Files changed</summary>

- `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` — `PartyId = CDI`, `ParentPartyId = COALESCE(TaxRegistrationNumber, CDI)` in both the customer and representatives branches.
- `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` — party LEFT JOIN replaced by `left_semi` gate; `PartyId` from the exposure CDI; mapping doc and row-count comment updated.
- `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` — same gate and `PartyId` change; party frame reduced to the join key.
- `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` — agreement-level settlements included via `COALESCE(ExposureKey, AgreementKey)`; party join INNER plus non-null tax filter, assertion, tax-first dedup tie-break; perimeter gap documented.
- `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` — `uc_id`/`CDI` split, `cdi_map` fan-out, init-balance and settlement joins rekeyed to `uc_id`.

Full diff: `git -C /home/karalis/cepal/gr.cepal.dna.de.delivery diff 4ecf465..5fdbe10`

</details>
