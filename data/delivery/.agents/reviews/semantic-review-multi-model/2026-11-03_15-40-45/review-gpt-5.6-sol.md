# CDI-based party identity and parent-level balance fan-out

This range publishes CDI as `PartyId` and `COALESCE(TaxRegistrationNumber, CDI)` as the parent key. Debt deliveries now require a resolvable party, while `portfolio_cdi_bal` separates parent aggregation from individual-CDI fan-out. VatSettlement adds agreement-level settlements and enforces non-null tax numbers. Two overwrite paths still lack adequate completeness protection.

Watch for: **confirmed** VatSettlement writes a documented ~35% undercount as a full replacement; **confirmed** the balance notebook can pass its safety check and then overwrite with an empty result if the new CDI map is empty.

**Verdict**: NEEDS_CHANGES

## High-level view

Customer, Representatives, DebtStatic, and DebtAB now share one identifier contract: CDI is the portable party key, and the debt notebooks use party data only as an existence gate. The semi-join avoids source-specific `PartyKey` values and cannot multiply debt rows from duplicate party history.

`portfolio_cdi_bal` aggregates balances, initialization overrides, and settlements by `(PortfolioId, uc_id)`, then uses `cdi_map` to repeat parent totals for each member CDI. A production-source check for 2026-10-30 found 385,324 map rows and the same number of distinct `(PortfolioId, CDI)` pairs.

VatSettlement retains rows with only `AgreementKey`, prefers party records carrying a tax number, and rejects unresolved tax numbers. Its documentation nevertheless records ~49.7K cloud rows against 76,455 on-prem while the notebook still overwrites the target unconditionally.

The range adds count logging but no executable parity checks for identifier consistency, debt-row retention, fan-out cardinality, or financial totals. JSON/Python syntax passes; behavioral validation remains manual.

<details>
<summary>Issues (3)</summary>

1. **Confirmed — incomplete VatSettlement overwrite** — Cloud coverage remains ~35% below on-prem, but the notebook overwrites the full target; resolve the missing classes or require explicit acceptance and approved coverage thresholds.
2. **Confirmed — fan-out bypasses the empty-data safeguard** — A non-empty `base` can pass the guard even when `cdi_map` makes the final result empty; validate final cardinality before overwrite.
3. **Confirmed — no behavioral regression gate** — Identifier consistency, debt retention, fan-out uniqueness, financial totals, and settlement coverage have no executable checks; add notebook assertions or a repeatable validation suite.

</details>

<details><summary>Details</summary>

## Unified CDI identity across customer and debt deliveries

Customer and Representatives emit the same value in `PartyId` and `CDI`, with the agreed tax-number-or-CDI fallback for `ParentPartyId`. DebtStatic and DebtAB derive `PartyId` from exposure CDI after a left-semi party lookup, so party history controls eligibility without contributing its source-specific identifier.

```text
primary.party CDI ──existence gate──> primary.exposure CDI
                                          │
                                          └── PartyId = CDI

Customer.ParentPartyId = COALESCE(TaxRegistrationNumber, CDI)
```

This contract lets downstream joins use CDI across source systems while preventing duplicate party records from fanning out debt relationships.

## Parent aggregation and individual-CDI fan-out

The balance flow now separates the on-prem `uc_id` parent grain from individual CDI membership:

```text
dexposure ──COALESCE(tax, CDI)──> base (PortfolioId, uc_id)
     │
     └──distinct members────────> cdi_map (PortfolioId, uc_id, CDI)
                                      │
base + parent-level init/settlement ──┴──> one output row per member CDI
```

For 2026-10-30, all 385,324 `cdi_map` rows were distinct by `(PortfolioId, CDI)`, with no CDI assigned to multiple parents inside a portfolio. Membership still comes from `dexposure` rather than the complete party master, consistent with the smaller cloud fan-out documented in the notebook.

**Confirmed — the empty-result guard does not cover the new fan-out stage.** Only `base_count == 0` is checked at `portfolio_cdi_bal/portfolio_cdi_bal.ipynb:301`; `cdi_map` excludes null CDIs at line 253 and is inner-joined to `base` at line 363. If CDI resolution fails while tax data keeps `base` populated, the check passes, the final result can be empty, and overwrite removes the previous good table. Validate `cdi_map` and `portfolio_cdi_bal_final` before writing, with a non-zero requirement plus a fan-out or date-over-date threshold.

## VatSettlement expands the key perimeter without reaching parity

`COALESCE(ExposureKey, AgreementKey)` adds agreement-level settlements. Tax-first party ranking plus the inner join/filter enforces the target’s non-null tax contract, but the post-build assertion only rechecks rows after nulls were removed; it does not detect join loss.

**Confirmed — the notebook knowingly overwrites a materially incomplete settlement perimeter.** `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb:77` records ~49.7K cloud rows versus 76,455 on-prem, roughly 35% fewer, while line 220 overwrites the full delivery table. Resolve the missing settlement classes, or obtain explicit business acceptance and enforce approved row-count and distinct-settlement thresholds before overwrite.

## Behavioral validation remains manual

**Confirmed — no regression test or executable parity gate covers the changed semantics.** The only added assertion checks for null tax numbers after filtering, leaving `PartyId == CDI`, DebtStatic/DebtAB retention, `(PortfolioId, CDI)` uniqueness, parent-level totals, and VatSettlement coverage unchecked. Add notebook assertions or a repeatable validation suite for these invariants before delivery writes.

This review parsed all five notebooks as JSON, compiled every Python code cell, and checked the 2026-10-30 fan-out keys against production source data. It did not execute the branch notebooks end to end or write target tables.

</details>

<details>
<summary>File map</summary>

- `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` — publishes CDI-based customer and representative identifiers.
- `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` — gates DebtStatic on party existence and emits CDI as `PartyId`.
- `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` — applies the same contract to core portfolios.
- `platforms/dtransfer_VatSettlement/dtransfer_VatSettlement.ipynb` — includes agreement-level settlements and filters unresolved tax numbers.
- `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` — adds parent-level aggregation and individual-CDI fan-out.

Full diff: `git diff 4ecf465..5fdbe10`

</details>
