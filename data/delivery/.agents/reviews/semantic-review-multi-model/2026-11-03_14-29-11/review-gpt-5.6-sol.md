# Hoist collection invoicing export with unified delivery identifiers

## Summary

This change migrates the Hoist collection-invoicing report to a Databricks notebook, resolves collections through previous-business-day exposure and assignment snapshots, and writes the formatted result to a delivery Volume. It also establishes CDI as the cloud `PartyId`, adds the agreed `COALESCE(TaxRegistrationNumber, CDI)` parent identifier, exposes that parent key as `uc_id`, and deliberately leaves real-estate `OwnerPartyKey` and loan identifiers unchanged. The identity changes are consistent across the touched delivery tables, but the new report is not reproducible for historical dates because part of its state is selected from the latest available snapshot rather than the requested reporting timeline. The export also creates an unexpected duplicated filename prefix and accumulates another timestamped artifact on every rerun.

Watch for: **confirmed** future-state leakage into historical Hoist reports; **confirmed** duplicated export naming; **confirmed** multiple artifacts accumulating for the same reporting date; **confirmed** absence of an execution-level regression check for these regulatory outputs.

**Verdict**: NEEDS_CHANGES

## High-level view

The customer-facing delivery contract now uses CDI wherever `PartyId` represents a person or organization, while `ParentPartyId` and `portfolio_cdi_bal.uc_id` use the approved tax-number-first grouping key. Debt and DebtAB resolve the same CDI through the all-history party perimeter, and the intentionally different real-estate owner key and loan IDs are untouched.

The Hoist report builds its previous-month collection perimeter, maps each payment to the previous business day, and uses that date to obtain exposure and collection-assignment attributes. That temporal model is broken for settlement debt: the notebook reads the globally latest active settlement snapshot, so rerunning an old `business_date` can inject data that did not exist at that date. The portfolio-ID mapping has the same unbounded-latest behavior, although its names are less likely to change than settlement balances.

The report writes through the shared `export_data` helper into a date-specific Volume folder. Passing `output_file_prefix` as both the helper's prefix and report name produces `hoistinvoicing_hoistinvoicing_...`, and the timestamped filename plus lack of archive/cleanup leaves multiple valid-looking files after a rerun.

All changed notebooks parse as JSON and their Python cells compile. No execution-level evidence covers Hoist parity, historical replay, export naming, or post-change identity-table counts.

<details>
<summary>Issues (5)</summary>

1. **Historical settlement state — confirmed** — Hoist always reads the latest active settlement snapshot, so a backfill can include balances and statuses from after its reporting period. Join historical settlement data at `PreviousBusinessDay`, or use a latest-`<= run_date` rule with explicit business acceptance.
2. **Unbounded portfolio mapping — confirmed** — Portfolio IDs are resolved from the globally latest snapshot, allowing a later rename to alter an older run's perimeter. Resolve the map at or before `run_date`, or enforce and document immutability.
3. **Duplicated export name — confirmed** — The same `output_file_prefix` is supplied as both prefix and report name, producing `hoistinvoicing_hoistinvoicing_...`. Pass the intended logical report name and verify the consumer's filename contract.
4. **Rerun artifact accumulation — confirmed** — Timestamped exports are added to the same date folder without archiving or replacing prior files. Archive old artifacts or publish one deterministic final file per report date.
5. **No runtime parity verification — confirmed** — The diff has no Databricks execution result or regression test for report grain, financial totals, historical replay, output delivery, or changed identity counts. Run and retain the migration-checklist comparisons before release.

</details>

<details>
<summary>Details</summary>

### CDI-based identity stays aligned across customer and debt outputs

`dtransfer_Customer` now emits CDI in both `PartyId` and `CDI`, and computes `ParentPartyId` with the approved tax-registration-number fallback. `dtransfer_Debt` and `dtransfer_DebtAB` select the same CDI as `PartyId`, so their relationship rows can join directly to Customer without translating source-system `PartyKey` values. Their party joins also change from left to inner; a production check for 2026-06-30 found 104 exposure rows without a party match, all caused by null CDI rather than a missing non-null identifier, which is consistent with the stated requirement to exclude relationships that cannot identify a customer.

The scope boundaries requested for the migration are preserved: `dtransfer_Res.PartyId` remains `realestateinfo.OwnerPartyKey`, and the Hoist/Debt loan identifiers are not replaced. `portfolio_cdi_bal.uc_id` is sourced from `base.CDI`, whose upstream expression is already `COALESCE(PartyTaxRegistrationNumber, CDI)`, so the newly exposed column carries the intended parent grouping key rather than the raw child CDI.

### Settlement debt escapes the report's as-of timeline

**Confirmed:** `HoistInvoicing/hoist_collection_invoicing.ipynb:336-344` computes `dse_bd` as the unrestricted maximum `BusinessDate` in `derivativeactive.dsettlementexposure`. The selected `business_date`, previous-month payment window, and each payment's `PreviousBusinessDay` do not constrain that snapshot. The same notebook otherwise pins exposure and assignment records to the payment's previous business day, making settlement debt the one financial field sourced from a potentially future state.

```text
business_date -> previous month -> collection -> PreviousBusinessDay
                                             |-> dexposure @ PBD
                                             |-> assignment @ PBD
                                             `-> settlement @ MAX(all dates)  <-- future-state leak
```

This is observable rather than theoretical: the production settlement maximum was 2026-10-30 during review. A backfill such as `business_date=2026-06-30` would therefore use October settlement balances and active flags in a May report. Re-running that same backfill after another settlement load can change the result without any source data in the requested reporting period changing. Read the historical derivative view at `PreviousBusinessDay` where available, or define a documented latest-`<= run_date` fallback and obtain explicit business acceptance for the remaining approximation.

**Confirmed:** the portfolio map at `HoistInvoicing/hoist_collection_invoicing.ipynb:157-159` also uses the unrestricted latest portfolio snapshot. Resolve the fixed IDs from a snapshot at or before `run_date`, or assert that the ID-to-name mapping is immutable; otherwise the historical perimeter can also change after a portfolio rename.

### Export naming and reruns leave ambiguous delivery artifacts

**Confirmed:** `HoistInvoicing/hoist_collection_invoicing.ipynb:592` calls `export_data(output_df, output_file_prefix, run_date, output_dir, output_file_prefix)`. The helper builds filenames as `{output_file_prefix}_{name}_{date}`, so this configuration emits `hoistinvoicing_hoistinvoicing_<date>_<timestamp>.xlsx` rather than a single-prefix report name. Pass a distinct logical report name, or adjust the configured prefix to the exact naming contract expected by the consumer.

**Confirmed:** the output directory is created at lines 587-588, but no existing artifacts are archived or removed before the timestamped export. Every retry for the same `run_date` therefore leaves another current-looking workbook or CSV in the same date folder. Add the repository's archive/cleanup behavior or write to a deterministic final name so downstream delivery cannot pick up both the failed/replaced run and its retry.

### Runtime and parity coverage are still missing

**Confirmed:** the diff contains no automated tests or checked-in validation result for either the new report or the identifier migration. JSON parsing and compilation cover notebook integrity only; they cannot exercise Spark column resolution, Unity Catalog permissions, Volume writes, or the joins that establish report grain. Before shipping, run the migration checklist against an on-prem month, including row counts, `(ExposureKey, PayDate)` uniqueness, payment and debt totals, null distributions, and representative record comparisons; rerun the same `business_date` twice to verify deterministic source selection and single-artifact delivery. The identity outputs also need before/after row counts and uniqueness checks for `PartyId`, `ParentPartyId`, and `uc_id`.

Not tested: end-to-end Databricks execution; historical Hoist replay; parity with the on-prem workbook; retry/export cleanup; downstream consumption of the renamed identifiers.

</details>

<details>
<summary>File map</summary>

- `HoistInvoicing/config/live/hoist_collection_invoicing.json` — defines Hoist output and email metadata.
- `HoistInvoicing/hoist_collection_invoicing.ipynb` — builds the previous-month invoicing dataset and exports it to a Volume.
- `platforms/dtransfer_Customer/dtransfer_Customer.ipynb` — emits CDI as `PartyId` and the tax-number/CDI parent key.
- `platforms/dtransfer_Debt/dtransfer_Debt.ipynb` — aligns DebtStatic `PartyId` to CDI and requires a resolvable party.
- `platforms/dtransfer_DebtAB/dtransfer_DebtAB.ipynb` — applies the same identity rule to banking exposures.
- `portfolio_cdi_bal/portfolio_cdi_bal.ipynb` — adds the parent grouping key as `uc_id`.

Full diff: `git diff --find-renames --find-copies origin/master...HEAD`

</details>
