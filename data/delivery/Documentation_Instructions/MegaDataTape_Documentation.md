# MegaDataTape Pipeline Documentation

---

## Overview

The MegaDataTape pipeline loads data from CDH source tables into delivery delta tables, then exports portfolio-specific Excel reports and sends email notifications.

**Flow:** LoadData (parallel) → Export (sequential per portfolio) → Email (per portfolio)

---

## Folder Structure

```
MegaDataTape/
  LoadData/                          # 13 notebooks loading delta tables
    Borrower.py
    BorrowerMapping.py
    CetusContactHistory.py
    Collateral.py
    CollateralRealEstate.py
    Collections.py
    ContactHistory.py
    ExternalLienDB.py
    Installments.py
    Loan.py                          # depends on Collections
    LoanSales.py
    ReAssetAppraisals.py
    RestructureMapping.py
  Export/                            # 21 portfolio export notebooks + scheduler
    Report_Automation_Scheduler.py   # Orchestrator: batch or single portfolio mode
    artemis.py
    cetus.py
    cosmosretail.py
    cosmoswholesale.py
    domus.py
    euclid.py
    eucliddv01.py
    gaiai.py
    gaiaii.py
    galaxyii.py
    galaxyiv.py
    hermesa.py
    hermesb.py
    hermesc.py
    highbalance.py
    imola.py
    monza.py
    orionx.py
    rhodium.py
    solarab.py
    solarpb.py
  MasterJob/
    master_orchestrator.py           # Chains LoadData → Export by job name
    check_run_eligibility.py         # Gate: runs only on Nth business day
  config/
    live/                            # 14 active portfolio configs
    deprecated/                      # 7 inactive portfolio configs (artemis, galaxy, hermes, orion)
```

---

## Databricks Jobs (DatabricksWorkflows/)

| YAML File | Job Name | Description |
|-----------|----------|-------------|
| MegaDataTape_LoadData.yaml | MegaDataTape_LoadData | Runs all 13 LoadData notebooks in parallel (Loan depends on Collections) |
| MegaDataTape_Export.yaml | MegaDataTape_Export | Runs Report_Automation_Scheduler which exports all/selected portfolios |
| MegaDataTape_Master.yaml | MegaDataTape_Master | DAG: check_run_eligibility → LoadData job → Export job (requires job IDs) |
| MegaDataTape_Master_Notebook.yaml | MegaDataTape_Master_Notebook | Notebook orchestrator: calls LoadData and Export jobs by name (no job IDs needed) |
| Email.yaml | Email_report_job | Sends email notifications (from reportscatalog repo) |

---

## Parameters

### MegaDataTape_LoadData
| Parameter | Default | Description |
|-----------|---------|-------------|
| run_date | "" | YYYY-MM-DD (last business day of month). Empty = auto previous month EOM |
| portfolio | "" | Comma-separated, ILIKE matching. Empty = all portfolios |

### MegaDataTape_Export
| Parameter | Default | Description |
|-----------|---------|-------------|
| run_date | "" | Same as LoadData |
| portfolio | "" | Same as LoadData. Matches against JSON config filenames |
| config_folder | "live" | "live", "deprecated", or "all" |
| skip_email | "false" | "true" to skip email notifications |

### MegaDataTape_Master / MegaDataTape_Master_Notebook
| Parameter | Default | Description |
|-----------|---------|-------------|
| run_date | "" | Passed to LoadData and Export |
| portfolio | "" | Passed to LoadData and Export |
| skip_email | "false" | Passed to Export |
| trigger_on_business_day | "5" | Run only on 5th business day. Empty = bypass (adhoc) |

---

## Delta Tables

Schema: `<env>-cdh-delivery`.`mega-datatape`

All tables are partitioned by `ReportingDate` (renamed from `BusinessDate`).

| Table | LoadData Notebook | Notes |
|-------|-------------------|-------|
| borrower | Borrower.py | |
| borrowermapping | BorrowerMapping.py | |
| cetuscontacthistory | CetusContactHistory.py | Cetus-specific WHL source |
| collateral | Collateral.py | |
| collateralrealestate | CollateralRealEstate.py | |
| collections | Collections.py | Uses SecondBusinessdate for ReportingDate |
| contacthistory | ContactHistory.py | |
| externalliendb | ExternalLienDB.py | |
| installments | Installments.py | |
| loan | Loan.py | Depends on collections table |
| loansales | LoanSales.py | |
| reassetappraisals | ReAssetAppraisals.py | |
| restructuremapping | RestructureMapping.py | |

---

## LoadData Dependencies

All notebooks run in parallel except:
- **Loan** depends on **Collections** (reads from `mega-datatape.collections` table)

---

## Export Flow (Report_Automation_Scheduler.py)

Two modes:

**Batch mode** (default, no `report_args`):
1. Reads JSON configs from `config/<config_folder>/`
2. Filters by `portfolio` parameter (ILIKE matching)
3. For each portfolio: runs export notebook → sends email (unless `skip_email=true`)
4. Summary at end with pass/fail counts

**Single portfolio mode** (when `report_args` is provided):
1. Parses `report_args` JSON directly
2. Runs the single portfolio export
3. No email in this mode

---

## Collections Special Logic

The Collections notebook calculates `SecondBusinessdate` (2nd business day of the run_date's month) and uses it as the `ReportingDate` value. This is different from other notebooks which use `run_date` directly.

---

## Deployment Steps

1. **ALTER TABLE** — rename `BusinessDate` to `ReportingDate` on all mega-datatape tables:
```sql
ALTER TABLE <table> SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5');
ALTER TABLE <table> RENAME COLUMN BusinessDate TO ReportingDate;
```

2. **SSM Parameters** — ensure exist:
   - `/cfn/bucket_prefix`
   - `/reports/exports/s3_main_path`

3. **Create jobs** (in order):
   - MegaDataTape_LoadData
   - MegaDataTape_Export
   - Email_report_job
   - MegaDataTape_Master (requires LoadData and Export job IDs)
   - OR MegaDataTape_Master_Notebook (no job IDs needed)

4. **Update git_branch** in YAMLs from `DT-560` to target branch

5. **Enable schedule** — set `pause_status: UNPAUSED`

6. **Test** — run adhoc: `trigger_on_business_day=""`, `skip_email="true"`, `portfolio="cetus"`

---

## Adhoc Scenarios

| Scenario | Parameters |
|----------|-----------|
| Run everything for current month | All defaults |
| Run for specific date | `run_date="2026-02-27"` |
| Run only Cosmos portfolios | `portfolio="cosmos"` |
| Run without emails (testing) | `skip_email="true"` |
| Run on any day (bypass schedule) | `trigger_on_business_day=""` |
| Re-run failed exports only | Use Export job directly with `portfolio="cetus,domus"` |
| Run deprecated portfolios | Use Export job with `config_folder="deprecated"` |
