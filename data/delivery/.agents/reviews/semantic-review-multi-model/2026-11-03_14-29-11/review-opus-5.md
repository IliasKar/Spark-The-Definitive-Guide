# Cross-platform party-identity rule for dtransfer deliverables

Four dtransfer notebooks plus `portfolio_cdi_bal` adopt a single party-identity convention: `PartyId` is now the `CDI`, `ParentPartyId` is `COALESCE(TaxRegistrationNumber, CDI)`, and the party lookups that previously supplied `PartyKey` become INNER joins so the identity column can never be null. `portfolio_cdi_bal` gains a `uc_id` column as the cloud substitute for on-prem's integer `ParentPartyId`. Alongside this, `dtransfer_VatSettlement` widens its perimeter to agreement-level settlements via `COALESCE(ExposureKey, AgreementKey)` and switches its party join to INNER to mirror on-prem's INNER JOIN on the tax table.

Reviewed change set: commits `0837d63` and `edc9b64` (unpushed, vs `origin/DNA26-1716_platforms`) plus the uncommitted `dtransfer_VatSettlement` edit in the working tree. Verification queries below were run against on-prem `PLTFRM`/`PRC` and `p-cdh` at `BusinessDate = 2026-10-30`.

Watch for: the new `uc_id` in `portfolio_cdi_bal` is byte-identical to the existing `CDI` column, and on-prem those two columns hold different values at a different grain (confirmed). The VatSettlement INNER join on CDI does not deliver the promised non-null `taxNumber` — the target column is `NOT NULL` on-prem and 8 settlement CDIs still resolve to a null tax number (confirmed). `PartyId` moves from numeric strings to alphanumeric CDIs like `GLC0004267942`, while every on-prem `PartyId` column is `int NOT NULL` (confirmed). VatSettlement's new `AccountNumber` mixes bare loan keys with prefixed agreement keys (confirmed).

**Verdict**: NEEDS_CHANGES

## High-level view

The identity substitution itself holds up under measurement. On on-prem `dtransfer.Customer`, `ParentPartyId` yields 639,113 distinct groups and `COALESCE(CustomerVat, CDI)` yields 639,103, with 639,113 distinct pairs of the two — the substitute never splits a parent group and merges only 10 of them. The parent key also agrees between its two cloud sources (`party.TaxRegistrationNumber` and `dexposure.PartyTaxRegistrationNumber`) for all 585,481 dexposure CDIs, so Customer and `portfolio_cdi_bal` will join on it.

The value domain is where this needs sign-off. `DebtStatic.PartyId` currently holds numeric strings (1,621,642 of 1,621,689 rows match `^[0-9]+$`); after the change it holds CDIs, which are 10-13 character portfolio-prefixed strings. On-prem `Customer.PartyId`, `Representatives.PartyId`, `DebtStatic.PartyId` and `DebtAB.PartyId` are all `int NOT NULL`, and `Customer.ParentPartyId` is `int`. The delivery Delta tables already type these as STRING so nothing breaks on write, but the team obtained an explicit IT confirmation for a string `uc_id` in `portfolio_cdi_bal` and no equivalent is documented for these four columns.

In `portfolio_cdi_bal` the new column does not do what the docs say. The `base` CTE aliases `COALESCE(e.PartyTaxRegistrationNumber, e.CDI)` as `CDI`, so `b.CDI AS uc_id` and `b.CDI AS CDI` emit the identical value. On-prem the two are distinct: `uc_id` is the numeric parent id and `CDI` is the real portfolio-scoped CDI, at one row per (portfolio, CDI) — 390,430 rows, 390,430 distinct CDIs, 322,498 distinct `uc_id`s. The cloud notebook aggregates at parent grain, so it has no real CDI to emit and the new column adds a duplicate instead of the missing key.

The VatSettlement rewrite achieves less than its comments claim. The INNER join on CDI only requires the CDI to exist in party; it does not require a tax number, and 8 of 23,402 settlement CDIs resolve to a dedup winner whose `TaxRegistrationNumber` is null even though another party row for the same CDI has one. On-prem `VatSettlement.taxNumber` is `nvarchar(200) NOT NULL` with zero nulls across 76,455 rows. Separately, the agreement-level expansion lifts the perimeter from roughly 41.7K to 49.7K rows against an on-prem 76,455, so a 35% coverage gap survives a change whose markdown now reads as on-prem parity.

The party joins in `dtransfer_Debt` and `dtransfer_DebtAB` are no longer joins in any meaningful sense. `PartyId` is taken from the party side of an equality on CDI, so it always equals the exposure's own CDI and the operation is purely a row filter. In DebtStatic that filter is applied to one of three sibling tables: DebtDetail and DebtFinancial are built straight from `dexposure` with no party constraint, while on-prem guarantees all three carry the same 1,048,759 LoanIds with zero orphans. On 2026-10-30 the filter removes exactly 47 rows and costs no loan its DebtStatic representation, so the invariant survives today without anything enforcing it.

<details>
<summary>Issues (10)</summary>

1. **PartyId value domain** — `PartyId` becomes an alphanumeric CDI (`GLC0004267942`) where every on-prem `PartyId` column is `int NOT NULL` and current cloud values are numeric strings. Get the same explicit IT confirmation that was obtained for string `uc_id`, covering `Customer`, `Representatives`, `DebtStatic` and `DebtAB`.
2. **ParentPartyId type** — on-prem `Customer.ParentPartyId`/`Representatives.ParentPartyId` are `int`; the new value is a tax number with leading zeros (`094014760`) or a CDI fallback. Confirm consumers read it as a string, since an int cast both fails on CDIs and silently drops leading zeros.
3. **VatSettlement taxNumber can still be null** — the INNER join on CDI does not imply a tax number exists; 8 of 23,402 settlement CDIs resolve to a null `TaxRegistrationNumber`. Add `taxNumber IS NOT NULL`, or prefer non-null tax rows in the party dedup ordering, to match the `NOT NULL` target.
4. **VatSettlement AccountNumber mixes key formats** — 41,753 loan-level values are bare numbers while all 8,147 agreement-level values are portfolio-prefixed. Confirm with IT which format each row class should carry before shipping.
5. **VatSettlement coverage gap** — post-change perimeter is ~49.7K rows versus 76,455 on-prem (37,406 distinct SettlementCodes). The markdown now claims on-prem parity; investigate the residual gap so verification does not accept a 35% shortfall as success.
6. **uc_id duplicates CDI** — `b.CDI AS uc_id` and `b.CDI AS CDI` emit the same coalesced parent key, where on-prem holds the parent id and the real CDI in those two columns. Either group `base` by (PortfolioId, CDI) and carry the parent key as `uc_id`, or document that cloud `CDI` is not a CDI.
7. **uc_id doc inaccuracies** — the added bullet labels on-prem `uc_id` as int (the column is `nvarchar(60)`; the int is the source `ParentPartyId`), and the 362,267 vs 361,959 parity check measures a grouping count, not the table's 390,430-row CDI grain. Correct both claims.
8. **DebtStatic filter not applied to siblings** — the INNER party join filters DebtStatic only, while on-prem guarantees identical LoanId sets across DebtStatic/DebtDetail/DebtFinancial. Express it as `left_semi` for clarity and either apply the same constraint to Detail/Financial or assert the invariant.
9. **Stale verified-count comment** — the DebtStatic base SQL still says "verified: gives 1,608,266", a number measured before the perimeter-changing INNER join. Update it so the verification step compares against the right baseline.
10. **VatSettlement change uncommitted** — the Customer/Debt/DebtAB/portfolio_cdi_bal edits are committed, the VatSettlement edit is still in the working tree and would be left out of a push.

</details>

<details>
<summary>Details</summary>

### The substitution rule measured against on-prem

Within the on-prem Customer perimeter:

```
dist ParentPartyId            : 639,113
dist COALESCE(CustomerVat,CDI): 639,103
dist (ParentPartyId, substitute) pairs: 639,113
```

The pair count equalling the parent count means the mapping is a function from parent to substitute — the substitute never splits an on-prem parent group, and the 10-group shortfall is 10 parent pairs collapsing onto a shared substitute. That is a stronger result than the 0.085% group-count comparison recorded in the `portfolio_cdi_bal` docs, which compared cardinalities without checking that the partitions align.

### PartyId leaves the numeric domain

CDI in `primary.party` is a portfolio-prefixed alphanumeric key — `GLC0004267942`, `HRB0015578133`, `DMR3036928` — and the same real-world customer carries a different CDI per portfolio (`HRB0015578133` and `CEL0015578133` both resolve to tax number `094057784`). That portfolio scoping matches on-prem, where Customer has 806,161 distinct `PartyId` against 805,937 distinct CDI, so the grain is not the problem.

The type is. Every on-prem target column is `int`:

```
PLTFRM.dtransfer.Customer.PartyId          int NOT NULL
PLTFRM.dtransfer.Customer.ParentPartyId    int NULL
PLTFRM.dtransfer.Representatives.PartyId   int NOT NULL
PLTFRM.dtransfer.DebtStatic.PartyId        int NOT NULL
PLTFRM.dtransfer.DebtAB.PartyId            int NOT NULL
```

and the values today are consistent with that: `dtransfer_debtstatic` holds 1,621,642 numeric `PartyId` values out of 1,621,689, `dtransfer_debtab` 492,839 out of 492,839. The delivery tables are already STRING-typed so the Delta writes will not fail, and the knowledge base records a previously accepted *value* difference (DWH id vs PartyKey) — both sides numeric. This change makes it a domain difference, and `DebtStatic`/`DebtAB` have no `CDI` column at all, so the CDI value lands in the column a consumer is most likely to cast.

### uc_id is the column it was meant to replace

The `base` CTE in `portfolio_cdi_bal` already renames the parent key:

```sql
COALESCE(e.PartyTaxRegistrationNumber, e.CDI) AS CDI
...
GROUP BY pf.PortfolioId, COALESCE(e.PartyTaxRegistrationNumber, e.CDI)
```

so the two added lines, `b.CDI AS uc_id` and the pre-existing `b.CDI AS CDI`, emit the same string. On-prem they do not:

```
portfolio_code | uc_id    | CDI
52             | 2872240  | CEL0005668634
13             | 26434    | MRC0007201268
69             | 23753461 | DMR3036928
```

and the grain differs too. On-prem `[PRC].[operation].[portfolio_cdi_bal]` at 2026-10-30 has 390,430 rows, 390,430 distinct `(portfolio_code, CDI)` and 390,430 distinct CDI — one row per portfolio-CDI, with `uc_id` as a rollup attribute spanning 322,498 distinct values. The cloud notebook aggregates at parent grain, which is why there is no CDI left to emit and why adding `uc_id` as a second copy of the grouping key cannot close the gap. Grouping `base` by `(PortfolioId, CDI)` and carrying `COALESCE(PartyTaxRegistrationNumber, CDI)` as a non-grouping attribute would reproduce both the grain and the two distinct columns.

Column ordering is not a problem: on-prem `uc_id` is column 3 and `CDI` column 4, and inserting `uc_id` immediately before `CDI` preserves that order for positional consumers.

### VatSettlement: the INNER join does not enforce what the comment promises

The added comment states the INNER join mirrors on-prem's INNER JOIN on `PRC.ref.partyTaxNumber` so that `taxNumber` is never null. On-prem does hold that line — `VatSettlement.taxNumber` is `nvarchar(200) NOT NULL` with 0 nulls across 76,455 rows. The cloud join requires only that the settlement's CDI exists in `primary.party`:

```python
.join(party, on="CDI", how="inner")   # party = dedup winner per CDI, tax may be NULL
```

Every one of the 23,402 settlement CDIs has at least one party row with a non-null `TaxRegistrationNumber`, but the dedup winner — chosen by source priority then `BusinessDate DESC`, not by tax-number presence — is null for 8 of them. Those rows will be emitted with a null `taxNumber` into a column the contract says cannot be null. Either filter the result or push tax-number presence into the window ordering.

### VatSettlement: two key spaces in one column

The agreement-level expansion is well motivated — 8,147 of 49,900 active settlements carry no `ExposureKey` — but the two branches do not share a key format:

```
active settlements          : 49,900
has ExposureKey             : 41,753   (prefixed: 2)
agreement-only              :  8,147   (prefixed: 8,147)
```

Every agreement-level value is portfolio-prefixed; essentially every loan-level value is bare. On-prem's `AccountNumber` is 45,546 prefixed against 30,909 bare out of 76,455, with prefixes like `PRA`/`CEL`/`GLA`/`MRC`/`ORB` at lengths 17-19 and bare values at 14-16. The pre-existing bare-`ExposureKey` question is not this change's doing, but routing a second, consistently-prefixed key space into the same column makes the format of `AccountNumber` depend on which branch produced the row, and IT resolves that column against account identifiers.

Duplicate keys are not a new risk: on-prem already carries 76,455 rows over 72,474 distinct `(AccountNumber, SettlementCode)` pairs.

### VatSettlement coverage after the change

Row-count arithmetic for the new perimeter: 49,900 active settlements, less the 242 null-CDI rows the INNER join drops, less portfolio and party join misses, lands near 49.7K against on-prem's 76,455 — up from roughly 41.7K before the change but still about 35% short, with on-prem spanning 37,406 distinct `SettlementCode`s. The notebook markdown now presents the cloud logic as mirroring on-prem including the INNER join rationale, which reads as parity. Whoever runs the verification pass should treat 49.7K as a known gap rather than a pass.

### DebtStatic's party join is a filter, and only DebtStatic gets it

After this change both `dtransfer_Debt` and `dtransfer_DebtAB` select the identity column from the party side of an equality on CDI:

```python
.join(party.select(col("CDI").alias("pty_CDI"), col("CDI").alias("PartyId")),
      col("exp_CDI") == col("pty_CDI"), "inner")
```

`PartyId` is therefore always equal to `exp_CDI`, and the join contributes nothing but row elimination. `left_semi` with `col("exp_CDI").alias("PartyId")` would say that outright and remove the chance of a future duplicate-row regression if the party dedup ever stops being one-row-per-CDI.

The asymmetry matters more than the idiom. DebtStatic now requires a party-resolvable CDI; DebtDetail and DebtFinancial are built from `dexposure` joined to portfolio with no party constraint at all. On-prem keeps the three tables in lockstep:

```
DebtStatic distinct LoanId    : 1,048,759
DebtDetail distinct LoanId    : 1,048,759
DebtFinancial distinct LoanId : 1,048,759
DebtDetail LoanIds absent from DebtStatic: 0
DebtStatic rows with NULL PartyId       : 0
```

Measured on prod for 2026-10-30, the filter is nearly inert: 47 of 3,741,503 exposure rows have a null CDI, zero exposure CDIs are missing from the all-history party dedup, and zero of the 1,047,794 non-banking `dexposure` loans lose every one of their DebtStatic rows. The 47 dropped rows line up exactly with the 47 null `PartyId` rows in today's `dtransfer_debtstatic`. So the invariant holds now by data accident, not by construction, and nothing in the notebook would surface it breaking.

```
exposure (run_date)  ──┬── INNER party-on-CDI ──> DebtStatic   (perimeter filtered)
                       │
dexposure (run_date) ──┼───────── no party join ──> DebtDetail
                       └───────── no party join ──> DebtFinancial
```

The base SQL above the join still carries `# Use SQL approach (verified: gives 1,608,266 matching on-prem)`. That figure was measured with the downstream party join as LEFT; the current table holds 1,621,689 rows and the new INNER join subtracts 47. Small, but the comment is what the next verification run will anchor on.

### PartyIdQualco

Both Customer and Representatives join `party_qca`, which carries `PartyIdQualco`, then override it in the select with `lit(None).cast("string")`. The null literal predates this change, but the commit's stated purpose is applying the party-identity rule, and this is the third identity column with a source already present in the DataFrame. Worth confirming it was deliberately left out of scope rather than missed.

### Not tested

No automated checks accompany any of these notebooks — the repository pattern is manual Databricks runs plus on-prem comparison queries, and that is what this change will rely on. Specifically unverified by this diff: that the delivery `uc_id` and `CDI` columns differ, that `taxNumber` is non-null after the write, that DebtStatic/DebtDetail/DebtFinancial share a LoanId set, and the post-change row counts for DebtStatic, DebtAB and VatSettlement. The first three are cheap post-write assertions and would turn the invariants above into something enforced rather than assumed.

</details>

<details>
<summary>Files changed</summary>

- `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` — `PartyId` filled from CDI and `ParentPartyId` from `COALESCE(TaxRegistrationNumber, CDI)` in both the customer and representatives selects.
- `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` — DebtStatic party lookup switched to INNER and `PartyId` sourced from CDI; mapping table note updated.
- `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` — same party change for the core-portfolio table.
- `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` — new `uc_id` column in both result SELECTs plus design-decision and mapping documentation.
- `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` *(uncommitted)* — `AccountNumber` from `COALESCE(ExposureKey, AgreementKey)`, party join switched to INNER, mapping table rewritten.

Full diff: `git -C /home/karalis/cepal/gr.cepal.dna.de.delivery diff origin/DNA26-1716_platforms...HEAD` and `git -C /home/karalis/cepal/gr.cepal.dna.de.delivery diff`.

</details>
