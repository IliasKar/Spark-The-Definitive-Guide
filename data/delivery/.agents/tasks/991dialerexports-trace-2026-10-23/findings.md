# 991DialerExports — End-to-End Delivery Trace

Investigation date: 2026-10-23
Repo: `/home/karalis/cepal/gr.cepal.dna.de.delivery`
Mode: READ-ONLY (no files modified)

---

## 1. Summary answer

**991DialerExports is a self-contained single notebook.** It does NOT use the repo's shared export/delivery/email machinery at all.

- **(a) File export** — NOT via `export_data`/`export_utils`. The notebook builds a pipe-delimited `.txt` with a raw Python `open(...).write(...)` loop and writes it to a Databricks Unity Catalog **Volume** path.
- **(b) S3 delivery** — **No explicit S3 step in this repo.** There is no `S3_delivery` call, no `boto3`, no `s3://` URI, no `dbutils.notebook.run`, and no job orchestration in git. The output path is under a Volume named `s3_external` (`/Volumes/{env}-cdh-delivery/s3_external/reports/991dialerexports/`). The name strongly implies the Volume is backed by an external S3 location, but **that backing is defined in Unity Catalog, not in this repo, so it cannot be confirmed from code.**
- **(c) Email notification** — **None.** No `send_report_email`, no `Email_report_job`, no `smtplib`, no `email_task` invocation anywhere in or around this notebook.
- **Driving job / config** — **No Databricks workflow YAML in git references 991DialerExports**, and there is **no JSON config** (no `991DialerExports/config/` folder, no `Input JSON Files/` entry). How this notebook is scheduled lives only in the Databricks workspace, not in git.

**Do not confuse with `DialerManualCampaign`** — that is a separate, config-driven, email-capable report with its own YAML (`DatabricksWorkflows/DialerManualCampaign.yaml`). It shares the word "Dialer" but shares no code or config with 991DialerExports. See §7.

---

## 2. Evidence — the notebook is self-contained

File: `/home/karalis/cepal/gr.cepal.dna.de.delivery/991DialerExports/991DialerExports.ipynb` (read in full; single file, folder contains only this notebook).

**Imports (cell 1)** — plain Python/Spark only; no project utils, no boto3, no smtplib:
```python
import logging
from py4j.java_gateway import JavaObject
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import concat_ws, lit, regexp_extract
from datetime import datetime, date
import re
import shutil
import os
```
There is no `sys.path.append(...01-utils...)`, no `import export_utils`, no `import helpers`, no `aws_utils`.

**Logic flow:**
1. Build today's dialer activity text rows from `` `p-cdh`.primary.dialer `` (SQL, OUTBOUND/`cti` only) → temp view `dialer991ExportData`.
2. Read yesterday's latest `.txt` back from the Volume and compare (idempotency guard).
3. Write today's `.txt` to the Volume **iff** today differs from yesterday; otherwise write an empty file.

---

## 3. Q1 — How is the export invoked?

**Not `export_data` / `export_utils`.** The export is a hand-rolled Python file write (final code cell):

```python
today = datetime.today().strftime("%Y%m%d")
dialerTargetPath = f"/Volumes/{env}-cdh-delivery/s3_external/reports/991dialerexports/MASS_ACTIVITIES_{today}_93.txt"

# "Yesterday" dataset is **DIFFERENT** from today dataset
if todayUnequalToYesterdayCalls:
    rows = dialer991ExportData.collect()
    with open(dialerTargetPath, "w", encoding = "cp1253", errors = "replace", newline = "\r\n") as f:
        for row in rows:
            f.write(row['ForText'] + "\n")
# "Yesterday" dataset is **EQUAL** to today dataset
else:
    with open(dialerTargetPath, "w", encoding = "cp1253", errors = "replace", newline = "\r\n") as f:
        pass
```

Notes:
- Output is a fixed-format `.txt` (not xlsx/csv), encoded `cp1253` (Greek), CRLF line endings — matching the legacy on-prem format documented in the notebook markdown (lines end in CR LF; trailing empty line).
- `dialer991ExportData.collect()` pulls rows to the driver and writes them one line at a time. This is deliberate because `export_data` cannot produce this bespoke `|||`-delimited, cp1253/CRLF format.

Confirmed by search: no `export_data`, `run_portfolio_export`, `archive_existing_files`, or `load_configs` tokens appear in the notebook.

---

## 4. Q2 — Where does the output go?

**A Databricks Unity Catalog Volume path**, constructed from an `env` literal + f-string. No SSM, no widget (the notebook declares `"widgets": {}` in metadata).

Three occurrences of the same base path:
- `991DialerExports.ipynb` `fetch_latest_business_date`:
  `volume_folder = f"/Volumes/{env}-cdh-delivery/s3_external/reports/991dialerexports/"`
- `991DialerExports.ipynb` `load_and_parse_text_file`:
  `volume_folder = f"/Volumes/{env}-cdh-delivery/s3_external/reports/991dialerexports/"`
  `text_file_path = volume_folder + f'MASS_ACTIVITIES_{latest_business_date_str}_93.txt'`
- Write target (final cell): `dialerTargetPath = f"/Volumes/{env}-cdh-delivery/s3_external/reports/991dialerexports/MASS_ACTIVITIES_{today}_93.txt"`

Environment prefix is **hard-coded** in the driver cell:
```python
env = 'p'
latest_business_date_str = fetch_latest_business_date(env = env)
```
So in its committed state this notebook always targets the **production** catalog `` `p-cdh` `` for both the source table (`` `p-cdh`.primary.dialer ``) and the delivery Volume (`p-cdh-delivery`). There is no qa/dev switch wired in.

Filename pattern: `MASS_ACTIVITIES_{yyyymmdd}_93.txt`.

**Is the Volume backed by S3?** The Volume is named `s3_external`, and the same `s3_external` Volume is used by another report (`Payments/Allocation_Reference_Values.ipynb` writes to `/Volumes/{env}-cdh-delivery/s3_external/reports/internalreports/allocation_reference/...`). This naming strongly suggests an external-location Volume mapped to an S3 bucket, meaning the write effectively lands in S3 without an explicit boto3/`S3_delivery` step. **However, the Volume's external-location/S3-bucket binding is a Unity Catalog object defined outside this git repo, so it cannot be confirmed from code.** Flagged as unverifiable here.

The notebook markdown also references the legacy on-prem destination `\\dwhsideapps\Share\in\Prod\991dialerexports`, i.e. the file is ultimately consumed by an IT share/downstream system; the cloud equivalent is this Volume.

---

## 5. Q3 — Is S3 delivery involved?

**Not explicitly, and not through the CUD `S3_delivery.ipynb` module.**

- No `S3_delivery` reference in the notebook (repo-wide grep for `S3_delivery` returns other reports — e.g. `Volume_to_S3.ipynb`, `Levy/*`, MegaDataTape export files — but **not** 991DialerExports).
- No `boto3`, no `s3://`, no SSM `/cfn/bucket_prefix`, no `aws-databricks` secret usage in the notebook.
- No `dbutils.notebook.run(...)` chaining to any delivery module.

The only S3 relationship is the implicit one via the `s3_external` Volume name (see §4), which is not code-visible in this repo. **If S3 delivery is required and the Volume is NOT actually S3-backed, then S3 delivery is effectively missing for this report** — see §8 recommendations.

---

## 6. Q4 — Is email involved?

**No.** Repo-wide search for `send_report_email`, `Email_report_job`, `smtplib`, and `email_task` shows matches only in the shared modules (`01-utils/helpers.py`, `01-utils/email_module/email_task.py`) and other reports — **never from the 991 notebook**. The notebook imports no email library, declares no recipients, and references no template. There is no `skip_email` parameter here (unlike DialerManualCampaign).

---

## 7. Q5 — Which Databricks job runs this notebook? (and the DialerManualCampaign distinction)

**No workflow YAML in git runs 991DialerExports.** Full listing of `DatabricksWorkflows/`:
```
AssignmentOfferCollectionToQualcoAutomated.yaml
Backstop.yaml
DetailedPortfolio.yaml
DialerManualCampaign.yaml
HoistDebtJson.yaml
Intraday_8702_assignments.yaml
MegaDataTape_Export.yaml
MegaDataTape_LoadData.yaml
MegaDataTape_Master.yaml
Report_Orchestrator.yaml
```
`grep -rni -e '991' -e 'DialerExport' -e 's3_external' DatabricksWorkflows/` → **no matches.** The only repo-wide references to `991DialerExports` outside the notebook are Databricks bundle **sync-snapshot** files (`.databricks/bundle/dev/...` and `.../qa/...`), which merely record that the notebook file syncs to the workspace path `991DialerExports/991DialerExports` — they are deployment artifacts, not a job definition.

**Conclusion:** the schedule/trigger for this notebook is defined only in the Databricks workspace (job UI or a bundle not committed here). This is **unverifiable from git** — stated explicitly rather than assumed.

**DialerManualCampaign (the lookalike) is a different report:**
- Job: `DatabricksWorkflows/DialerManualCampaign.yaml` — job name `DialerManualCampaign`, tag `category: report`, `git_branch: master`, run as service principal `00f3ea27-8236-4f6a-8f9c-177668555720`.
- Parameters: `run_date`, `portfolio`, `config_folder` (default `live`), **`skip_email`** (default `false`).
- Task 1 `load_configs` → `DialerManualCampaign/load_portfolio_configs`; Task 2 `run_reports` is a `for_each_task` fan-out over `portfolio_configs` running `DialerManualCampaign/DialerManualCampaignReport`.
- It is config-driven (`Input JSON Files/Dialer Manual Campaign Report/InputJson_DialerManualCampaign.json`) and email-capable (`skip_email`).

991DialerExports shares none of this: no configs, no fan-out, no email, no YAML. They are unrelated beyond the word "Dialer".

---

## 8. Q6 — Any JSON config driving 991DialerExports?

**No.**
- `ls /home/karalis/cepal/gr.cepal.dna.de.delivery/991DialerExports/config` → "No such file or directory". The folder contains only `991DialerExports.ipynb`.
- `Input JSON Files/` subfolders: `Backstop Report`, `Detailed Portfolio Report`, `Dialer Manual Campaign Report`, `Hoist Monthly Report`, `Jupiter Data Tape`, `Mercury Monthly Billing`, `Other Monthly Billing`, `Payments Internal Report`, `Payments Investor Report`. **No 991 / DialerExports entry.**

The notebook is fully parameter-free in git (`env = 'p'` hard-coded, no widgets). Unlike MegaDataTape (scheduler + per-portfolio notebooks + `config/live/*.json`), 991DialerExports takes no config input.

---

## 9. Conclusions

1. 991DialerExports is an **outlier**: a standalone notebook with bespoke file I/O, bypassing `export_utils`, the CUD `S3_delivery` module, and the email module entirely.
2. Its only output is a cp1253/CRLF `.txt` written with native Python `open()` to `/Volumes/p-cdh-delivery/s3_external/reports/991dialerexports/MASS_ACTIVITIES_{yyyymmdd}_93.txt`.
3. S3 and email, as wired for other reports, are **absent from the code**. Any S3 landing happens only implicitly if the `s3_external` Volume is an S3-backed external location — a fact that lives in Unity Catalog and is **not verifiable from this repo**.
4. The scheduling job is **not in git**; it exists only in the Databricks workspace.

## 10. Recommendations (not implemented)

- **Confirm the Volume backing in Unity Catalog.** Run `DESCRIBE VOLUME `p-cdh-delivery`.s3_external.<...>` / check the external location to verify whether `s3_external` maps to an S3 bucket. If it does, the current design delivers to S3 transparently and no `S3_delivery` call is needed. If it does not, S3 delivery is silently missing.
- **Parameterize the environment.** `env = 'p'` is hard-coded, so qa/dev runs would hit production tables and the production Volume. Consider a widget (`env_prefix`) like other notebooks use via `aws_utils.get_env_prefix()`.
- **Commit the job definition.** Add a `DatabricksWorkflows/991DialerExports.yaml` so the schedule, cluster, git source, and run-as are tracked in git like the other reports, rather than living only in the workspace.
- **If email notification is expected**, it must be added explicitly (e.g. `send_report_email` / `Email_report_job`); it currently does not exist for this report.

---

## 11. Verification notes / limits

- Claims about notebook contents are from reading the full `.ipynb`.
- Absence claims (no S3/email/config/YAML) are from repo-wide `grep` excluding `.git` and `.venv`; the only 991 hits outside the notebook are Databricks bundle sync-snapshot JSONs (deployment metadata, not job logic).
- **Unverifiable from git:** (a) whether the `s3_external` Volume is actually S3-backed; (b) the Databricks job/schedule that invokes this notebook. Both are Databricks workspace / Unity Catalog objects, not committed code.
