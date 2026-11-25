# HoistInvoicing — "File Available" Email Trigger Recipe

Investigation date: 2026-10-23
Mode: READ-ONLY (no files modified)
Repos: `/home/karalis/cepal/gr.cepal.dna.de.delivery` (branch `DNA26-1716_platforms`), `gr.cepal.dna.de.redis`

---

## A. Summary answer — which mechanism, and why

**Use `send_report_email()` from `01-utils/helpers.py`.** It is the only sanctioned,
shipped email-trigger path in the repo, it is the one that forwards
`recipients_entra_group`, and the General Implementation Guide names it as the
standard email integration for reports. A self-contained notebook CAN call it
directly — it is a plain Python function with no dependency on the MegaDataTape
scheduler. The only thing the scheduler does that a standalone notebook must now
do itself is build the four constants (`EMAIL_JOB_NAME`, `LOG_TABLE`,
`BODY_TEMPLATE`, `unc_base_path`) and the small `report_args`/`email_args` dicts.

`send_report_email` does NOT send the email itself. It looks up a Databricks job
named **`Email_report_job`** by name via the Databricks SDK and triggers it
asynchronously with `jobs.run_now(...)`, passing `recipients_entra_group` (and the
other fields) as `notebook_params`. **The Entra-group → email-address resolution
happens inside that `Email_report_job`, which lives in a *different* repo
(`reportscatalog`) that is NOT checked out here.** So the exact resolution method
(Graph API vs. lookup table vs. distribution list) is **UNVERIFIABLE from the
repos available**.

Important disambiguation: the simple notebook `01-utils/email_module/email_task.py`
is **NOT** the current Entra-aware job. It only reads `recipients_to`/`recipients_cc`
and fails hard if `recipients_to` is empty. It is a legacy/sample SMTP sender. The
real `Email_report_job` is the reportscatalog one.

Answer to the sub-questions:
- Q1 → mechanism (a): the Databricks job `Email_report_job` invoked via
  `send_report_email`. It is NOT the `email_task.py` in this repo, and it is NOT a
  Redis-driven process. The Redis `reports` records are consumed by the
  *scheduler/orchestrator* flow (Report_Orchestrator), not by a standalone notebook
  calling `send_report_email`.
- Entra expansion → happens inside `Email_report_job` (reportscatalog repo, not
  present). **UNVERIFIABLE-from-repo.**

---

## B. Ready-to-adapt code snippet for HoistInvoicing

### B.1 The verbatim template that already works (MegaDataTape)

Source: `MegaDataTape/Export/run_single_portfolio.py` (final COMMAND cell). This is
the shipped, working trigger. Quoted verbatim:

```python
# Send email notification
if skip_email:
    print(f"  ⏭ Email skipped")
else:
    EMAIL_JOB_NAME = "Email_report_job"
    LOG_TABLE = f"`{env_prefix}-cdh-delivery`.report_catalog_email_logs.email_logs"
    BODY_TEMPLATE = f"/Volumes/{env_prefix}-cdh/reference/reportscatalog/email_templates/report_success_template_email.html"

    report_args = {
        "portfolio_name": portfolio_name,
        "output_file_prefix": output_file_prefix,
        "report_output": report_output,
    }
    send_report_email(report_args, email_args, last_eom_business_date, export_base_path, EMAIL_JOB_NAME, LOG_TABLE, BODY_TEMPLATE, unc_base_path)
    print(f"  ✓ Email sent: {portfolio_name}")
```

Note: **no shipped report triggers email from *inside its own* report notebook.**
Every email trigger in the repo goes through the MegaDataTape scheduler's
`run_single_portfolio.py` wrapper. HoistInvoicing will be the first self-contained
notebook to call `send_report_email` directly. That is supported (plain function),
but it means the notebook must itself resolve `unc_base_path` from SSM and read the
config JSON.

### B.2 Adapted snippet for HoistInvoicing (all values filled in)

Add this as a new final cell of `HoistInvoicing/hoist_collection_invoicing.ipynb`,
AFTER `export_data(...)` has written the file. The notebook already has `env`
(from `aws_utils.get_env_prefix()`) and `run_date` in scope.

```python
# ==================== Email notification (file available) ====================
import os, sys, json, boto3
# helpers.py lives in 01-utils of the delivery repo. Resolve its path the same
# way the guide recommends, with a fallback.
try:
    _nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _repo_root = "/Workspace" + _nb.rsplit("/HoistInvoicing/", 1)[0]
    sys.path.insert(0, os.path.join(_repo_root, "01-utils"))
    sys.path.insert(0, os.path.join(_repo_root, "aws_utils"))
except Exception:
    sys.path.append("../01-utils")
    sys.path.append("../aws_utils")

from helpers import load_configs, send_report_email
from aws_utils import get_scope_key_names, get_aws_credentials, connect_to_aws_and_access_ssm

# 1) Read the report's config (same JSON used to drive output paths)
cfg = load_configs(dbutils, "live", report_name="HoistInvoicing")[0]
report_args = cfg["report_args"]        # portfolio_name, output_file_prefix, report_output
email_args  = cfg.get("email_args", {}) # recipients_entra_group = DNA_InvestorReports_HoistInvoicing

# 2) Resolve the UNC base path from SSM (the location string shown in the email body)
_scope = "aws-databricks"
_keys = get_scope_key_names(_scope)
_key = _keys[0] if _keys else None
_secret = get_aws_credentials(_scope, _key)
_ssm = boto3.client("ssm", region_name="eu-central-1",
                    aws_access_key_id=_key, aws_secret_access_key=_secret)
UNC_BASE_PATH = connect_to_aws_and_access_ssm("/reports/exports/unc_main_path", _ssm, _secret)

# 3) Constants (identical to run_single_portfolio.py)
EMAIL_JOB_NAME = "Email_report_job"
LOG_TABLE      = f"`{env}-cdh-delivery`.report_catalog_email_logs.email_logs"
BODY_TEMPLATE  = f"/Volumes/{env}-cdh/reference/reportscatalog/email_templates/report_success_template_email.html"

# 4) Trigger the email job asynchronously
#    export_base_path is only used to build report_args; it is NOT needed for the
#    email body (which uses unc_base_path), so "" is acceptable here.
send_report_email(
    report_args=report_args,
    email_args=email_args,
    run_date=run_date,
    export_base_path="",
    email_job_name=EMAIL_JOB_NAME,
    log_table=LOG_TABLE,
    body_template=BODY_TEMPLATE,
    unc_base_path=UNC_BASE_PATH,
)
```

What `send_report_email` then sends to `Email_report_job` (from helpers.py, verbatim
logic), with HoistInvoicing values substituted:

- `recipients_entra_group` = `"DNA_InvestorReports_HoistInvoicing"`
- `email_subject` = `"hoistinvoicing Report"`  (built as `f"{output_file_prefix.rstrip('_')} Report"`; `output_file_prefix` is `"hoistinvoicing"`)
- `body_data_json` = `{"report_name": "hoistinvoicing", "run_date": "<run_date>", "report_location_info": "<UNC>\\internalreports\\hoistinvoicing\\<YYYYMMDD>"}`
- `log_table` = `` `{env}-cdh-delivery`.report_catalog_email_logs.email_logs ``
- `body_template` = `/Volumes/{env}-cdh/reference/reportscatalog/email_templates/report_success_template_email.html`
- `recipients_to` = `""`  (not in config)
- `recipients_cc` = `""`  (not in config)

> ⚠️ **Subject cosmetics.** With `output_file_prefix = "hoistinvoicing"` the subject
> becomes `"hoistinvoicing Report"`. If you want a nicer subject (e.g.
> "Hoist Collection Invoicing Report") you must either change `output_file_prefix`
> in the config (it also drives the output filename, so be careful) or extend
> `send_report_email` to accept an explicit subject. This is a cosmetic choice, not
> a blocker.

---

## C. Required parameters, template path, and log table

`send_report_email(report_args, email_args, run_date, export_base_path, email_job_name, log_table, body_template, unc_base_path="")`

| Param | Shape / value | Mandatory | Source of truth |
|---|---|---|---|
| `report_args` | dict with `portfolio_name`, `output_file_prefix`, `report_output` | yes | config JSON `report_args` |
| `email_args` | dict; here `{"recipients_entra_group": "DNA_InvestorReports_HoistInvoicing"}` | yes | config JSON `email_args` |
| `run_date` | `"YYYY-MM-DD"` string | yes | notebook `run_date` |
| `export_base_path` | string; only used to compute report_args — **not** used for email body; `""` is fine | required positional (may be empty) | — |
| `email_job_name` | `"Email_report_job"` | yes | run_single_portfolio.py:110 |
| `log_table` | `` `{env}-cdh-delivery`.report_catalog_email_logs.email_logs `` | yes | run_single_portfolio.py:111 |
| `body_template` | `/Volumes/{env}-cdh/reference/reportscatalog/email_templates/report_success_template_email.html` | yes | run_single_portfolio.py:112 |
| `unc_base_path` | SSM `/reports/exports/unc_main_path` value | optional (defaults `""`) — needed for a correct location string | load_portfolio_configs.py:53 |

Fields assembled INSIDE `send_report_email` and passed to the job as
`notebook_params` (helpers.py:226-234): `recipients_entra_group`, `email_subject`,
`body_data_json`, `log_table`, `body_template`, `recipients_to`, `recipients_cc`.

`body_data_json` keys (helpers.py:219-223): `report_name`, `run_date`,
`report_location_info`.

**Exact template path to use:**
`/Volumes/{env}-cdh/reference/reportscatalog/email_templates/report_success_template_email.html`
(same path the Redis importer writes into `template_path` — see
`redis/IMPORT data TO redis_reports.ipynb` line ~299, confirming it is the canonical
success template).

**Exact log table:** `` `{env}-cdh-delivery`.report_catalog_email_logs.email_logs ``

---

## D. Prerequisites outside the notebook

| Prerequisite | Status | Evidence / note |
|---|---|---|
| Databricks job `Email_report_job` exists in the workspace | **UNVERIFIABLE-from-repo** | Referenced only by name in `run_single_portfolio.py` / `Report_Automation_Scheduler_deprecated.py` and looked up at runtime via `w.jobs.list(name="Email_report_job")`. No YAML defines it in this repo (`grep` of `DatabricksWorkflows/*.yaml` = no match). Documentation says it comes "from reportscatalog repo" (`MegaDataTape_Documentation.md`). |
| That job can resolve Entra group → addresses | **UNVERIFIABLE-from-repo** | Resolution code is inside `Email_report_job` (reportscatalog repo, not checked out). Whether it uses Graph API, a lookup, or an IT distribution list cannot be confirmed here. The repo's own `email_task.py` does NOT do this. |
| Entra group `DNA_InvestorReports_HoistInvoicing` exists & is populated by IT | **UNVERIFIABLE-from-repo** (user states IT has created it) | Only referenced as a string in the config JSON. |
| Success template file present at the Volume path | **UNVERIFIABLE-from-repo** | `/Volumes/{env}-cdh/reference/reportscatalog/email_templates/report_success_template_email.html` is a Unity Catalog Volume object, not in git. The template content/renderer live in reportscatalog. |
| Log table `report_catalog_email_logs.email_logs` exists in `{env}-cdh-delivery` | **UNVERIFIABLE-from-repo** | UC object, not in git. The email job appends to it. |
| SSM parameter `/reports/exports/unc_main_path` exists | **VERIFIED-from-code that it is used** (its value is UNVERIFIABLE) | Read in `load_portfolio_configs.py:53`. |
| Databricks SDK `WorkspaceClient()` available + permission to trigger jobs | **VERIFIED-from-code (requirement)** | `send_report_email` does `from databricks.sdk import WorkspaceClient; w = WorkspaceClient(); w.jobs.list(...); w.jobs.run_now(...)`. The run-as principal must have CAN-manage/run on `Email_report_job`. The actual grant is a workspace setting → **UNVERIFIABLE-from-repo**. |
| A Redis `reports` record for HoistInvoicing | **NOT a prerequisite for this path** | The `send_report_email` → `Email_report_job` path reads the Entra group from the config JSON passed in `notebook_params`, not from Redis. The Redis `reports` record (created by `IMPORT data TO redis_reports.ipynb`) drives the separate **orchestrator/scheduler** flow (`Report_Orchestrator`, `report_job_id`/`delivery_job_id`/`email_job_id`). Since HoistInvoicing is self-contained and calls `send_report_email` directly, **no Redis change is required** for the email to work — UNLESS `Email_report_job` itself internally looks the report up in Redis to resolve the Entra group (it might; this is inside reportscatalog and **UNVERIFIABLE-from-repo**). Flag this to IT as the one residual risk. |

---

## E. Link vs. attachment

**It sends a LINK / location string, not an attachment.** Evidence:

- `send_report_email` builds `report_location_info` as a Windows UNC-style path
  (`unc_base_path\report_output\YYYYMMDD`) and passes it in `body_data_json`
  (helpers.py:216-223). It never reads or attaches the exported file.
- The template is named `report_success_template_email.html` and the `body_data`
  keys are `report_name` / `run_date` / `report_location_info` — i.e. the email body
  tells the recipient *where* the file is, it does not carry the file.
- The repo's reference SMTP sender (`email_task.py`) only ever does
  `message.set_content(message_body, subtype='html')` — no `add_attachment` call
  anywhere. (The real `Email_report_job` renderer is in reportscatalog and not
  visible, but the data contract — a location string, no file bytes — makes an
  attachment impossible from these inputs.)

**Consequence for HoistInvoicing (writes to a UC Volume `s3_external`):** the body's
`report_location_info` will be the UNC path
`<unc_main_path>\internalreports\hoistinvoicing\<YYYYMMDD>`, i.e. the on-prem/IT
share location, **not** the Volume path and **not** an S3 URI. This mirrors every
other report: the file is expected to land on the IT share (via the `s3_external`
Volume → S3 → on-prem sync, which is a UC/infra concern, not code here — see the
991dialerexports findings for the same `s3_external` pattern). If HoistInvoicing's
file does NOT actually reach that UNC share, the email link will point somewhere the
recipient cannot open. **Confirm the `s3_external` → IT-share delivery path with IT**
before relying on the UNC location string. (Location-string mechanics
VERIFIED-from-code; whether the file actually arrives there is
UNVERIFIABLE-from-repo.)

---

## F. Conclusions & recommendations

1. **Mechanism to implement:** call `send_report_email(...)` from `01-utils/helpers.py`
   as the last step of the notebook, after `export_data(...)`. Use the four constants
   exactly as `run_single_portfolio.py` does. (Section B.2 is ready to paste.)
2. **It is asynchronous** (`jobs.run_now`) and fire-and-forget; the notebook does not
   wait for the email to be sent and will not fail if delivery fails downstream.
   If you need the notebook to fail on a bad trigger, wrap the call and check the
   returned `run` — but matching existing behavior means leaving it fire-and-forget.
3. **Entra resolution + the `Email_report_job` itself are in the reportscatalog repo**
   (not checked out). Everything about *how* `DNA_InvestorReports_HoistInvoicing` is
   expanded to addresses is UNVERIFIABLE from the repos here. Treat the job's
   existence and its Entra capability as external prerequisites.
4. **Residual risk to raise with IT:** confirm (a) `Email_report_job` is deployed and
   the run-as principal can trigger it; (b) whether that job resolves the Entra group
   from the passed `notebook_params` alone or ALSO expects a Redis `reports` record —
   if the latter, a Redis entry for HoistInvoicing must be created via
   `redis/IMPORT data TO redis_reports.ipynb`; (c) the `s3_external` Volume for
   `internalreports/hoistinvoicing/` actually syncs to the UNC share referenced in the
   email body.
5. **Subject cosmetics:** `output_file_prefix="hoistinvoicing"` yields the subject
   `"hoistinvoicing Report"`. Decide whether that is acceptable or whether to pass a
   friendlier subject (would require a small `send_report_email` signature change).

---

## G. Key file/symbol citations

- `01-utils/helpers.py::send_report_email` (lines ~210-238) — the trigger function;
  builds `report_location_info`, `body_data`, `notebook_params`; `WorkspaceClient()`
  → `w.jobs.list(name=email_job_name)` → `w.jobs.run_now(...)`.
- `MegaDataTape/Export/run_single_portfolio.py` (lines ~106-121) — the only shipped
  caller; defines `EMAIL_JOB_NAME`, `LOG_TABLE`, `BODY_TEMPLATE`.
- `MegaDataTape/Export/load_portfolio_configs.py` (lines ~49-55) — SSM reads
  `/reports/exports/s3_main_path` (EXPORT_BASE_PATH) and `/reports/exports/unc_main_path`
  (UNC_BASE_PATH).
- `01-utils/email_module/email_task.py` — legacy SMTP sender; reads only
  `recipients_to`/`recipients_cc`; **not** the Entra-aware job; no attachment logic.
- `Documentation_Instructions/MegaDataTape_Documentation.md` — table row
  `Email.yaml | Email_report_job | Sends email notifications (from reportscatalog repo)`.
- `Documentation_Instructions/General_Implementation_Guide.md` — "Email Integration"
  section: use `send_report_email()`, job looked up by name, no hardcoded IDs.
- `redis/IMPORT data TO redis_reports.ipynb` — builds the Redis `reports` record
  (`report_job_id`/`delivery_job_id`/`email_job_id`, `recipients_entra_group`,
  `template_path` = the success template, `report_location_info` =
  `\\dwhsideapps\share\in\{env}\...`). Drives the orchestrator flow, not the direct
  `send_report_email` call.
- `redisInteraction/Update environment in json.py` (redis repo) — schema for the
  Redis `reports` JSON incl. `email_args.external.recipients_entra_group` and
  `email_job_id` (hardcoded `869638610341136` in prod remap).
- `01-utils/export_utils.py::export_data` — the export function already in the
  notebook flow; filename pattern `{output_file_prefix}_{name}_{YYYYMMDD_...}.xlsx|csv`.

### Verification limits
- Absence claims (no `Email_report_job` YAML, no Entra/Graph resolution code) are from
  repo-wide `grep` across the delivery and redis repos.
- All Unity Catalog Volume objects, the `Email_report_job` definition, the Entra group,
  the log table, and the SSM parameter *values* live outside these git repos and are
  marked UNVERIFIABLE-from-repo above.
