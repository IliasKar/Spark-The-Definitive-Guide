# Report Orchestration Framework

## Overview

A centralized orchestrator that manages all report executions. A single master job (`Report_Orchestrator`) runs daily at 07:00, reads a Unity Catalog registry table to determine which reports are eligible, and triggers them in parallel. Each report uses the `for_each_task` pattern for native per-portfolio retry on repair.

## Architecture

```
Report_Orchestrator (daily 07:00 MON-FRI)
│
├─ Reads: report_catalog.report_registry
├─ Checks: frequency (daily/monthly), business day number
│
├─ No external dep → triggered immediately at 07:00. eg.:
│   ├─ Backstop (monthly, 8th bday)
│   ├─ MegaDataTape_Master (monthly, 5th bday)
│   └─ HoistDebtJson (monthly, 3rd bday)
│
└─ Has external dep (submit_s2l2l_zone_databricks_job) → waits until 12:00:
    ├─ DetailedPortfolio (daily)
    └─ DialerManualCampaign (daily)

Each report job uses for_each_task:
├─ load_configs → reads config/live/*.json
└─ for_each_task → runs notebook per portfolio
     └─ On repair: only failed portfolios rerun
```

## Components

### Report_Orchestrator (master job)

| File | Purpose |
|------|---------|
| `Report_Orchestrator/orchestrator.py` | Main logic: reads registry, checks eligibility, polls for external deps, triggers jobs in parallel, updates run status |
| `Report_Orchestrator/create_report_registry_table.sql` | DDL + seed data for the registry table |
| `DatabricksWorkflows/Report_Orchestrator.yaml` | Job definition (params: `run_date`, `skip_email`, `frequency_filter`) |

### report_registry table

Unity Catalog table (`{env}-cdh-delivery.report_catalog.report_registry`) that defines:

| Column | Purpose |
|--------|---------|
| `report_name` | Unique identifier |
| `job_name` | Databricks job to trigger |
| `frequency` | `daily` or `monthly` |
| `run_days` | Comma-separated weekdays on which the job runs, e.g. `Monday,Tuesday,Thursday,Friday`. **Note:** This field applies only to jobs scheduled to run daily, i.e. jobs where `frequency = 'daily'.|
| `trigger_business_day` | For monthly: Nth business day to run |
| `depends_on_workflow` | External workflow that must complete today before this report runs |
| `depends_on_report` | Another report that must complete first |
| `is_active` | Enable/disable without code changes |
| `last_successful_run` | Timestamp of last success |
| `last_run_status` | SUCCESS / FAILED / SKIPPED |
| `last_run_duration_sec` | Duration tracking |

### Report Jobs

Each report follows the same pattern:

```
ReportName/
├── ReportNotebook.py          # The actual report logic
├── load_portfolio_configs.py  # Reads config/live/*.json, outputs iteration list
└── config/
    └── live/
        ├── portfolio1.json    # Per-portfolio parameters
        └── portfolio2.json
```

## How to Add a New Report

1. **Create the report notebook** (`ReportName/ReportNotebook.py`) following the Databricks `.py` notebook format:
   - First cell: `%pip install openpyxl` + `%restart_python`
   - Import from `aws_utils` (`get_env_prefix`, `get_env_fullname`)
   - Resolve output path from SSM if relative:
     ```python
     if not report_output.startswith("/Volumes"):
         export_base_path = connect_to_aws_and_access_ssm("/reports/exports/s3_main_path", ...)
         report_output = export_base_path + report_output
     ```
   - Use `export_data()` from `export_utils` for file export
   - Use `archive_existing_files()` from `helpers` before writing

2. **Create config files** (`ReportName/config/live/portfolio.json`):
   ```json
   {
     "report_args": {
       "portfolio_name": "PortfolioName",
       "output_file_prefix": "PortfolioName_ReportName_",
       "report_output": "internalreports/reportname/portfolio/"
     },
     "email_args": {
       "recipients_entra_group": "DNA_InternalReports_ReportName_Portfolio"
     }
   }
   ```

3. **Create `load_portfolio_configs.py`** — copy from an existing report and change the `report_name` parameter and path split string.

4. **Create the YAML** (`DatabricksWorkflows/ReportName.yaml`):
   ```yaml
   resources:
     jobs:
       ReportName:
         name: ReportName
         tags:
           category: "report"
         max_concurrent_runs: 5
         parameters:
           - name: run_date
             default: ""
           - name: portfolio
             default: ""
           - name: config_folder
             default: "live"
           - name: skip_email
             default: "false"
         git_source:
           git_url: https://git-codecommit.eu-central-1.amazonaws.com/v1/repos/gr.cepal.dna.de.delivery
           git_provider: awsCodeCommit
           git_branch: master
         tasks:
           - task_key: load_configs
             max_retries: 0
             notebook_task:
               notebook_path: ReportName/load_portfolio_configs
               base_parameters:
                 run_date: "{{job.parameters.run_date}}"
                 portfolio: "{{job.parameters.portfolio}}"
                 config_folder: "{{job.parameters.config_folder}}"
               source: GIT
           - task_key: run_reports
             depends_on:
               - task_key: load_configs
             for_each_task:
               inputs: "{{tasks.load_configs.values.portfolio_configs}}"
               task:
                 task_key: portfolio_report
                 max_retries: 0
                 notebook_task:
                   notebook_path: ReportName/ReportNotebook
                   base_parameters:
                     report_args: "{{input.report_args}}"
                     report_name: "{{input.report_name}}"
                     run_date: "{{input.run_date}}"
                     env_prefix: "{{input.env_prefix}}"
                   source: GIT
         queue:
           enabled: true
         run_as:
           service_principal_name: 00f3ea27-8236-4f6a-8f9c-177668555720
         permissions:
           - group_name: Cepal-DSE-Users
             permission_level: CAN_MANAGE_RUN
         performance_target: PERFORMANCE_OPTIMIZED
   ```

5. **Register in the table** — Insert a row:
   ```sql
   INSERT INTO `{env}-cdh-delivery`.report_catalog.report_registry
   (report_name, job_name, frequency, run_days, trigger_business_day, is_active, execution_order, depends_on_workflow)
   VALUES ('ReportName', 'ReportName', 'daily','Monday,Tuesday,Wednesday,Thursday,Friday,Saturday', NULL, TRUE, 10, 'submit_s2l2l_zone_databricks_job');
   ```

6. **Deploy the YAML** via Databricks Asset Bundles.

## Repair Behavior

- **Orchestrator level**: Re-running the orchestrator re-triggers all eligible reports (no per-report repair at this level)
- **Report level**: Each report uses `for_each_task`. If one portfolio fails, repairing the report job only re-executes the failed portfolio(s)
- **Adhoc runs**: For portfolio-specific or parameter-specific runs, go directly to the individual report job

## Parameters

### Report_Orchestrator
| Parameter | Purpose |
|-----------|---------|
| `run_date` | Override date (YYYY-MM-DD). Empty = today |
| `skip_email` | Skip email notifications for all reports |
| `frequency_filter` | Run only `daily`, `monthly`, or `all` (default) |

### Individual Report Jobs
| Parameter | Purpose |
|-----------|---------|
| `run_date` | Report date |
| `portfolio` | Comma-separated filter (empty = all) |
| `config_folder` | `live`, `deprecated`, or `all` |
| `skip_email` | Skip email notifications |
| `portfolio_exact_match` | (MegaDataTape only) Use exact match instead of ILIKE |
