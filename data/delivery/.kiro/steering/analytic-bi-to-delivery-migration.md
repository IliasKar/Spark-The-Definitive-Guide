---
inclusion: manual
---

# Analytic/BI → Delivery (RCG) Migration — Guide

How to convert an on-prem MSSQL stored procedure in `dwhcore` **`[Analytic].[bi]`** schema into a Databricks delivery notebook that produces a table delivered to an **external company** (RCG-style deliveries).

Reference implementation: `RCG/CollateralsREAll/CollateralsREAll.ipynb` (migrates `[bi].[sp_CollateralsRE]` → `[Analytic].[bi].[CollateralsREAll]`).

> This is distinct from the `platform-migration-checklist.md` (PLTFRM `dtransfer` → regulatory platforms, under `platforms/`). That flow uses `primary.*`/`derivative.*` and `business_date`. **This** flow targets external-company deliveries, uses the `primaryactive.*`/`derivativeactive.*` curated-active tables, the `RCG/` directory, and a `run_date` widget.

---

## 1. Overview

| Aspect | Value |
|---|---|
| On-prem source | `dwhcore` → `[Analytic].[bi]` schema (tables built by `[bi].sp_*` procedures) |
| On-prem golden layer | `dwhcore\DWH` (read by the SP; **not** directly queryable from cloud) |
| Cloud source | `` `{env}-cdh`.primaryactive.* `` , `` `{env}-cdh`.derivativeactive.* `` (and `primary.*`/`derivative.*` where needed) |
| Cloud target | `` `{env}-cdh-delivery`.it.{TableName} `` (schema `it`, table name preserves on-prem casing) |
| Repo | `gr.cepal.dna.de.delivery` |
| Directory | `RCG/{TableName}/{TableName}.ipynb` (one folder + notebook per table) |
| Write mode | `overwrite` + `overwriteSchema` (full daily refresh) |
| Parameter | `run_date` widget (empty = latest available `BusinessDate`) |

**Criticality:** the output is delivered daily to an external company that processes it on their side. **Preserve the on-prem schema and values as closely as possible.** Any deviation must be documented in a "Known differences" cell so the consumer is not surprised and does not waste effort.

---

## 2. Prerequisites & Access

- **On-prem read access** via the MCP MSSQL server (ITStaging database on dwhcore), tool `mcp_mssql_itstaging_query`.
  - You can query the SP **output table** directly: `SELECT ... FROM [Analytic].[bi].[{Table}]`.
  - The underlying `DWH.*` tables are **not** queryable — infer joins from the SP text and validate against the output table.
  - The MSSQL tool caps results at **500 rows** — use aggregates (`COUNT`, `GROUP BY`) for comparisons, not full dumps.
- **Cloud query access** via the Databricks MCP tools:
  - `mcp_databricks_execute_sql_query` (prod `p-cdh`), `mcp_databricks_dev_execute_sql_query` (dev `d-cdh`).
  - `mcp_databricks_describe_uc_table`, `mcp_databricks_*_run_notebook`.
- **Dev cluster id** for running notebooks: read from `.databricks/.databricks.env` (`DATABRICKS_CLUSTER_ID`).

---

## 3. Analysis Workflow (do this before writing any code)

1. **Read the SP** and identify:
   - The **grain** (what one output row represents — e.g. collateral × property × ownership × agreement).
   - Every source table and **join type** (INNER vs LEFT matters — an INNER join is a filter).
   - All `WHERE` predicates, `CurrentRecord=1 / StatusRecord=1` SCD filters, and any function calls (e.g. `bi.convertToEuro`, `bi.getCLSDesc`).
   - The exact **output column list** and their derivations.
2. **Profile the on-prem output table** for the baseline:
   ```sql
   SELECT COUNT(*) total_rows,
          COUNT(DISTINCT {key}) dist_key,
          MAX(ReferenceDate) max_ref
   FROM [Analytic].[bi].[{Table}];
   ```
   Capture distributions by categorical columns (status, portfolio, relationship type) and distinct counts of every key column.
3. **Map each on-prem column to a cloud source** (see §5). Prefer the **curated `primaryactive.*`** tables — they usually carry the SP's business logic already applied, and often preserve Greek status text that `derivative.*` drops.
4. **Verify each mapping with a real record**: pick a specific key from the on-prem output, find the same record in cloud, and confirm the values line up field-by-field before committing to the mapping.
5. **Decide the strategy for gaps** (reconstruct, accept, or escalate) — see §6.

---

## 4. Notebook Structure

Path: `RCG/{TableName}/{TableName}.ipynb`. Six cells (markdown mapping → params → load → transform → known-differences → write). Follow the reference notebook.

### Cell 1 (markdown) — Table mapping
A table of `On-prem column | Cloud source` plus a one-line statement of the grain and the primary source table/filter. This is documentation the reviewer and the external consumer rely on.

### Cell 2 (code) — Parameters & setup
```python
import sys
from datetime import datetime

dbutils.widgets.removeAll()
dbutils.widgets.text("run_date", "", "Run Date (YYYY-MM-DD, empty=latest)")

sys.path.append("/Workspace/Shared")
import aws_utils

from pyspark.sql import functions as F, Window

env = aws_utils.get_env_prefix()
src_catalog = f"`{env}-cdh`"

run_date = dbutils.widgets.get("run_date").strip()
if not run_date:
    run_date = str(
        spark.table(f"{src_catalog}.primaryactive.{driver_table}")
        .agg({"BusinessDate": "max"})
        .collect()[0][0]
    )

try:
    datetime.strptime(run_date, "%Y-%m-%d")
except ValueError:
    raise ValueError(f"run_date '{run_date}' must be YYYY-MM-DD")

run_date_lit = F.lit(run_date).cast("date")

TARGET_TABLE = f"{env}-cdh-delivery.it.{TableName}"
sql_target = ".".join(f"`{p.strip('`')}`" for p in TARGET_TABLE.split("."))

print(f"env={env}  run_date={run_date}  target={TARGET_TABLE}")
```
- Uses **`run_date`** (empty ⇒ latest `BusinessDate` from the driver table). This lets the job run daily (empty) or backfill a specific reference date.
- `sql_target` converts `a.b.c` into `` `a`.`b`.`c` `` so `saveAsTable` handles the hyphenated catalog `{env}-cdh-delivery`.

### Cell 3 (code) — Load sources
One deduplicated DataFrame per source, each filtered by `BusinessDate == run_date_lit`. Alias join keys distinctly (e.g. `pf_thesis_id`, `reb_key`, `op_thesis_key`) to avoid column-ambiguity and to make the join conditions explicit.

### Cell 4 (code) — Transform
Chain the joins onto the driver DataFrame, build the final `select` with **explicit casts and on-prem column aliases**, and finish with `.distinct()`. Guard the write:
```python
row_count = result.count()
assert row_count > 0, f"0 rows for run_date={run_date} -- aborting write"
print(f"Records to write: {row_count:,}")
```

### Cell 5 (markdown) — Known differences vs on-prem
Enumerate every reconstructed/accepted/deviating field so the external consumer knows what to expect (see §6). This cell ships **with** the notebook.

### Cell 6 (code) — Write
```python
(
    result.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(sql_target)
)
print(f"Written {row_count:,} rows to {TARGET_TABLE}  (ReferenceDate={run_date})")
```

---

## 5. On-Prem → Cloud Mapping (Analytic/BI context)

The BI SPs read `DWH.*`. The curated-active cloud layer already unifies source systems and applies status logic, so map to it directly.

| On-prem `DWH` table / concept | Cloud source | Notes |
|---|---|---|
| `DWH.ast.Collateral` / `CollateralRE` | `` `{env}-cdh`.primaryactive.collateral `` | Real Estate = `Category = 'Επί ακινήτων'`. Carries Greek status text, ownership, RE keys, applied date |
| `DWH.ast.Property` / `PropertyOwnership` property asset | `` primaryactive.realestatebuilding `` | `RealEstateCepalKey` = on-prem prefixed property key (e.g. `CH230780`); join via `RealEstateKey` |
| `DWH.prt.Party` (+ `PartyReferenceCodes` for CDI) | `` derivativeactive.dparty `` | Has `CDI` and per-source keys `PartyKeyThesis/Profits/QCCepal/Arotron/...`. Already deduped one CDI per party key |
| `DWH.ast.AssetXPartyRltp` type 2018 (Agreement-Main Beneficiary) | `` primaryactive.exposure `` where `PartyRelationshipType='Primary Owner'` | Use to derive the agreement's primary CDI |
| `DWH.ref.Portfolio` (numeric `PortfolioId`) | `` primaryactive.portfolio `` | `PortfolioId` matches the on-prem DWH id; join curated `PortfolioKey` to `portfolio_id_thesis`. `PortfolioName` also matches on-prem directly |
| `DWH.adm.SGKCurrency` + `bi.convertToEuro(amount, ccy, date)` | `` {env}-cdh.derivative.converttoeur(currency_code, amount, rate_date) `` | UDF signature: `(STRING, DECIMAL(19,2), DATE)`. EUR passes through; non-EUR needs an FX rate on `rate_date` (see §7) |
| `DWH.adm.SourceExtraction.ReferenceDate` | `run_date` | `ReferenceDate = run_date`; `ETLLoadingDate = current_date()` |
| `DWH.cls.Classification` numeric ids (status) | Reconstruct from curated status **text** | Curated keeps text, not the DWH `classificationid`. Reconstruct via a small static map (see §6) |

### Relationship-type mapping (same as PLTFRM flow)
| On-prem Classification.Description | Cloud `PartyRelationshipType` |
|---|---|
| loan-Main Beneficiary Rltp | Primary Owner |
| Loan-Guarantor Rltp | Guarantor |
| Loan-Other Beneficiaries Rltp | Co-Owner |

### Key format note
Curated `collateral.CollateralKey` is the **bare Thesis id** (e.g. `424956`); `CollateralOldKey`/`CollateralCode` carry the **prefixed** form (e.g. `GLBCH424956`). Match on-prem `ThesisCollateralID` ↔ `CollateralKey` and on-prem `CollateralKey` ↔ `CollateralOldKey`.

---

## 6. Handling Gaps (preserve first, then reconstruct, then accept/escalate)

Priority order: (1) find an exact cloud source; (2) reconstruct deterministically; (3) accept with documentation; (4) escalate to IT/business.

- **Numeric classification ids** (e.g. `CollateralStatusId` 11157/11212, `OwnershipStatusId` 11006/11009): curated has only the text. Reconstruct with a fixed `when()` map. Any value outside the map → `NULL`. Two status fields can be independent (e.g. RE status `Ρευστοποιημένη` = inactive while collateral-level status is still `Ενεργή` = 11157) — source them separately, do not conflate.
- **DWH internal numeric ids without a cloud equivalent** (e.g. `Collateral.CollateralId`): populate from the stable cloud key (e.g. `CollateralKey` = ThesisCollateralID) if the consumer agrees, and document it.
- **INNER joins in the SP are filters** — replicate them as INNER joins in Spark. Example: the type-2018 main-beneficiary join drops collaterals whose agreement has no Primary Owner (including "moved to new portfolio" statuses like `Πώληση σε νέο χαρτ.`). Getting this right is what makes the row count match.
- **Fallback strings**: reproduce on-prem `COALESCE` fallbacks verbatim (e.g. `NoCDI - PropertyOwnerId = <id>`).
- **Preserve Greek status text** where the consumer expects it — pull from `primaryactive.*` (the `derivative.*` layer often stores only translated `Active/Inactive`).

Every one of the above belongs in the **Known differences** markdown cell.

---

## 7. Currency Conversion (`converttoeur`)

- UDF: `` `{env}-cdh`.derivative.converttoeur(currency_code STRING, amount DECIMAL(19,2), rate_date DATE) ``.
- Build the call with the env-aware catalog (never hardcode `p-cdh`):
  ```python
  F.expr(
      f"{src_catalog}.derivative.converttoeur(CollateralCurrency, "
      f"CAST(CollateralAmount AS decimal(19,2)), CAST('{run_date}' AS date))"
  ).cast("decimal(19,2)").alias("CollateralAmountEuro")
  ```
- **EUR** passes through unchanged. **Non-EUR** requires a rate for the **exact `rate_date`** in `reference.exchangerates`.
- **Gotcha (observed in dev):** the function does an **exact-date match** and returns **NULL** when no rate exists for that date (weekends/holidays/feed gaps). Verify against a date that has FX data before concluding the logic is wrong. If the consumer needs non-EUR conversion on every run, consider a most-recent-rate-on-or-before fallback (escalate/agree first).

---

## 8. PySpark Gotchas (critical)

- **Date filtering:** always compare with `run_date_lit = F.lit(run_date).cast("date")`. A bare string compare against a date column silently returns 0 rows.
- **Join fan-out:** before joining a lookup, confirm it is 1:1 on the join key or `dropDuplicates([key])` it. A lookup table can have many rows per key even when the *attribute* you need is constant (e.g. `realestatebuilding` had multiple rows per `RealEstateKey` but a single `RealEstateCepalKey`). Un-deduped lookups inflate the row count — this was the entire cause of a false "+15%" gap that vanished once dedup was applied.
- **No `regexp_extract` in `.ipynb` JSON:** backslash patterns get double-escaped in the notebook JSON. Use `substring(col, start, len)` instead.
- **Explicit casts + aliases** on every output column so the delivered schema exactly matches on-prem (including original spellings/typos, e.g. `IntialLegalOrder`).
- **`.distinct()`** on the final frame when joins can introduce duplicates.

---

## 9. Validation (multi-level, against on-prem)

Because the table is delivered externally, validate thoroughly. On-prem may be one business day behind the cloud snapshot — compare structure/ratios exactly and absolute counts approximately.

1. **Schema** — column set, order, types, and exact names match on-prem (query `information_schema.columns` on the output table).
2. **Row count & distinct keys** — total rows and distinct counts of every key column within tolerance.
3. **Distribution** — counts by status / portfolio / relationship type match the on-prem shape.
4. **Column completeness** — NULL counts of reconstructed/joined columns (e.g. 0 NULL `PortfolioId`, 0 NULL `primaryCDI` after an INNER join, count of `NoCDI` fallbacks).
5. **Record-level spot-checks** — pick specific keys and compare field-by-field on-prem vs cloud, covering EUR/non-EUR, active/inactive, multi-ownership, and fallback rows.

Investigate any gap by decomposing it (e.g. split by status) rather than accepting a headline percentage.

---

## 10. Running the Notebook on Dev

1. Find a `run_date` that exists in **all** dev source tables (they may cap earlier than `portfolio`):
   ```sql
   SELECT 'collateral' t, MAX(BusinessDate) FROM `d-cdh`.primaryactive.collateral
   UNION ALL SELECT 'exposure', MAX(BusinessDate) FROM `d-cdh`.primaryactive.exposure
   -- ... one row per source table
   ```
2. Confirm any UDF exists in dev (e.g. `d-cdh.derivative.converttoeur`) via `information_schema.routines`.
3. Run it with the dev cluster id from `.databricks/.databricks.env`:
   - Tool: `mcp_databricks_dev_run_notebook`, `is_local_file=true`, `parameters={"run_date": "YYYY-MM-DD"}`, `cluster_id=<from env>`.
4. Validate the output table `` `d-cdh-delivery`.it.{tablename} `` per §9.
   - Note: the local Databricks extension may write execution outputs back into the `.ipynb` after a run — do **not** commit that noise; keep the committed notebook output-free.

---

## 11. Git & Deployment

- Branch from a **fresh `master`** (`git checkout master && git checkout -b {TICKET}_{table}_RCG`). If the branch was cut from a stale local master, `git rebase origin/master` after `git fetch origin master`, then `git push --force-with-lease`.
- Commit **only the notebook** (stage the specific file; leave local settings like `.kiro/settings/mcp.json`, `.venv/`, report artifacts untracked and uncommitted).
- Repo uses **AWS CodeCommit**. `git push` works; management APIs (`CreatePullRequest`, `GetBranch`) may be **blocked by an org SCP** for the CLI user — create the PR in the AWS Console instead. Target **`development`** (then `development` → `master` per repo flow).
- After merge, wire the Databricks workflow job: `run_date` empty for the daily run, a specific date for backfills; git source on `master`.

---

## 12. Reference Implementation: CollateralsREAll

- SP `[bi].[sp_CollateralsRE]` → `RCG/CollateralsREAll/CollateralsREAll.ipynb` → `` `{env}-cdh-delivery`.it.CollateralsREAll ``.
- Grain: RE collateral × property/ownership × agreement main-beneficiary.
- Sources: `primaryactive.collateral` (RE), `primaryactive.portfolio`, `primaryactive.realestatebuilding`, `derivativeactive.dparty`, `primaryactive.exposure` (Primary Owner).
- Verified parity (prod 2026-09-01): **349,242 vs 349,232** on-prem (~0.003%); dev 2026-09-03 run: 349,193 rows, schema and spot-checks all matched.
- Documented gaps: reconstructed `CollateralStatusId`/`OwnershipStatusId`, `CollateralId = ThesisCollateralID`, `converttoeur` FX-availability behaviour, INNER Primary-Owner join, `NoCDI` fallback.
- Test report kept alongside the notebook: `RCG/CollateralsREAll/CollateralsREAll_TestResults.md`.
