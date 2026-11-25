# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ═══════════════════════════════════════════════════════════════════════════════
# MainReport Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════
# Reads the report_registry table to determine which reports to run today,
# then triggers each eligible report job via the Databricks SDK.
#
# Frequency logic:
#   - "daily"   → always runs
#   - "monthly" → runs only on the Nth business day of the month
#   - "weekly"  → runs on the specified business day of the week (future)
#
# Dependency logic:
#   - If depends_on_report is set, waits for that report to complete first.
#
# Parameters:
#   - run_date: override date (YYYY-MM-DD). Empty = use validated EOM/current.
#   - portfolio: comma-separated filter passed to child jobs. Empty = all.
#   - monthly_trigger_business_day: override for monthly trigger day. Empty = use table value.
#   - skip_email: override to skip emails for all reports.
#   - frequency_filter: run only reports of this frequency (daily/monthly/all). Default = all.
# ═══════════════════════════════════════════════════════════════════════════════

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("skip_email", "false")
dbutils.widgets.text("frequency_filter", "all")  # daily, monthly, or all

import os
import sys
import json
from datetime import datetime
from pytz import timezone
from databricks.sdk import WorkspaceClient

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/Report_Orchestrator/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "aws_utils"))
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("../../aws_utils")
    sys.path.append("../../01-utils")

from aws_utils import get_env_prefix, get_env_fullname

# COMMAND ----------

env_name = get_env_fullname()
env_prefix = get_env_prefix()

now_athens = datetime.now(timezone("Europe/Athens"))
today = now_athens.strftime("%Y-%m-%d")
today_name = now_athens.strftime("%A")
today_calendar_day = now_athens.day  # day of month (1-31)

run_date = dbutils.widgets.get("run_date").strip()
skip_email_override = dbutils.widgets.get("skip_email").strip().lower() == "true"
frequency_filter = dbutils.widgets.get("frequency_filter").strip().lower() or "all"

REGISTRY_TABLE = f"`{env_prefix}-cdh-delivery`.report_catalog.report_registry"

print(f"═══ MainReport Orchestrator ═══")
print(f"today: {today} | env: {env_prefix} | frequency_filter: {frequency_filter}")
print(f"run_date override: {run_date or '(none)'}")

# COMMAND ----------

# ── Determine today's business day number in the month ──
bday_row = spark.sql(f"""
    SELECT BusinessDayOfMonth
    FROM {env_name}.reference.ref_calendar
    WHERE CAST(CalendarDate AS DATE) = CAST(getdate() AS DATE)
""").first()

today_bday_number = bday_row["BusinessDayOfMonth"] if bday_row else 0
print(f"Today is business day #{today_bday_number} of the month")

# COMMAND ----------

# ── Load active reports from registry ──
reports_df = spark.sql(f"""
    SELECT *
    FROM {REGISTRY_TABLE}
    WHERE is_active = TRUE
    ORDER BY frequency, execution_order, report_name
""")

reports = [row.asDict() for row in reports_df.collect()]
print(f"Active reports in registry: {len(reports)}")

# COMMAND ----------

# ── Registry write helpers (concurrency-safe) ──
# All report runs execute in a ThreadPoolExecutor (same process). Delta does file-level
# conflict detection, so concurrent in-place UPDATEs on this small table can collide
# (ConcurrentModificationException) and silently lose a status write. We serialize every
# registry UPDATE with an in-process lock: the write is milliseconds, reports run outside
# the lock, so parallelism is unaffected while writes never collide.
import threading

_registry_lock = threading.Lock()


# All registry timestamps are stored in Athens local time, matching the repo-wide
# pattern (from_utc_timestamp(current_timestamp(), 'Europe/Athens') used across the
# delivery notebooks for IngestionTimestamp). Spark current_timestamp() is UTC, so we
# convert once and reuse the same SQL everywhere. A single consistent clock is essential:
# the view compares updated_at >= triggered_at, so both MUST be in the same timezone.
ATHENS_NOW_SQL = "from_utc_timestamp(current_timestamp(), 'Europe/Athens')"


def mark_triggered_all(all_reports):
    """
    STEP 1 (batch, single UPDATE): stamp triggered_at on every active report for this
    cycle. Paired with updated_at in v_reports_today: todays_status = last_run_status
    when updated_at >= triggered_at, else 'pending'. Written once, before the thread
    pool, so there is no concurrency here.

    triggered_at uses the SAME clock as updated_at (Athens local), so the view's
    updated_at >= triggered_at comparison is apples-to-apples.
    """
    names = [r["report_name"] for r in all_reports]
    if not names:
        return
    in_list = ", ".join("'" + n.replace("'", "''") + "'" for n in names)
    spark.sql(f"""
        UPDATE {REGISTRY_TABLE}
        SET triggered_at = {ATHENS_NOW_SQL}
        WHERE report_name IN ({in_list})
    """)
    print(f"[registry] triggered_at stamped on {len(names)} report(s) for this cycle")


def update_report_status(report_name, status, duration_sec=0):
    """
    Write a terminal status for one report (SUCCESS / FAILED / SKIPPED), concurrency-safe
    via the in-process lock. Sets updated_at = now (Athens) so updated_at >= triggered_at
    marks the report as 'completed this cycle'. last_successful_run advances only on
    SUCCESS. All timestamps are Athens local, matching triggered_at and the repo pattern.
    """
    with _registry_lock:
        spark.sql(f"""
            UPDATE {REGISTRY_TABLE}
            SET last_successful_run = CASE WHEN '{status}' = 'SUCCESS'
                                           THEN {ATHENS_NOW_SQL}
                                           ELSE last_successful_run END,
                last_run_status = '{status}',
                last_run_duration_sec = {duration_sec},
                updated_at = {ATHENS_NOW_SQL}
            WHERE report_name = '{report_name}'
        """)

# COMMAND ----------

# ── EOM schedule + area-gate helpers ──
# EOM reports run on the calendar day immediately after the month's final business
# date. This includes Saturdays: for example, business_date 2026-10-30 is processed by
# the upstream workflow on 2026-10-31, and the EOM report runs after that workflow.
eom_trigger_row = spark.sql(f"""
    SELECT
        CAST(today_cal.CalendarDate AS DATE) AS trigger_date,
        today_cal.PreviousBusinessDay AS previous_business_date,
        DATE_ADD(today_cal.PreviousBusinessDay, 1) AS expected_trigger_date,
        previous_cal.CurrentMonthLastWorkingDate AS month_last_working_date
    FROM {env_name}.reference.ref_calendar today_cal
    LEFT JOIN {env_name}.reference.ref_calendar previous_cal
        ON CAST(previous_cal.CalendarDate AS DATE) = today_cal.PreviousBusinessDay
    WHERE CAST(today_cal.CalendarDate AS DATE) = CAST('{today}' AS DATE)
""").first()

trigger_date = eom_trigger_row["trigger_date"] if eom_trigger_row else None
expected_eom_business_date = (
    eom_trigger_row["previous_business_date"] if eom_trigger_row else None
)
expected_eom_trigger_date = (
    eom_trigger_row["expected_trigger_date"] if eom_trigger_row else None
)
expected_eom_month_last_working_date = (
    eom_trigger_row["month_last_working_date"] if eom_trigger_row else None
)


def check_eom_schedule_gate():
    """Return whether today is the calendar day immediately after an EOM business date."""
    if eom_trigger_row is None:
        return False, f"monthly Eom → no ref_calendar record for trigger date {today}"

    if expected_eom_business_date is None:
        return False, f"monthly Eom → no previous business date found for trigger date {today}"

    if expected_eom_month_last_working_date is None:
        return False, (f"monthly Eom → no month-end calendar record for previous business date "
                       f"{expected_eom_business_date}")

    if expected_eom_business_date != expected_eom_month_last_working_date:
        return False, (f"monthly Eom → previous business date {expected_eom_business_date} "
                       f"is not month-end {expected_eom_month_last_working_date}")

    if trigger_date != expected_eom_trigger_date:
        return False, (f"monthly Eom → trigger date {trigger_date} is not the calendar day "
                       f"immediately after EOM business date {expected_eom_business_date} "
                       f"(expected {expected_eom_trigger_date})")

    return True, (f"monthly Eom → trigger date {trigger_date} is immediately after "
                  f"EOM business date {expected_eom_business_date}")


# EOM reports are also gated on pipeline area run state via run_after_area.
# v_area_latest_status is queried fresh on every call because depends_on_workflow may
# advance it. Besides confirming that the area's latest date is a month-end date, the
# gate binds it to the EOM business date expected for this orchestrator run; an old EOM
# from a prior month must not satisfy the gate.
def check_eom_area_gate(run_after_area):
    """
    Return (ok: bool, reason: str) for today's EOM schedule and area-close state.
    Reads v_area_latest_status + ref_calendar fresh on every area check.
    """
    schedule_ok, schedule_reason = check_eom_schedule_gate()
    if not schedule_ok:
        return False, schedule_reason

    if not run_after_area:
        return False, "monthly Eom → run_after_area not set"

    row = spark.sql(f"""
        SELECT
            v.business_date,
            c.CurrentMonthLastWorkingDate AS month_last_working_date
        FROM `{env_prefix}-cdh`.pipeline.v_area_latest_status v
        LEFT JOIN {env_name}.reference.ref_calendar c
            ON CAST(c.CalendarDate AS DATE) = v.business_date
        WHERE v.pipeline_area = '{run_after_area}'
    """).first()

    if row is None or row["business_date"] is None:
        return False, f"monthly Eom → no area_runs record for area '{run_after_area}'"

    area_date = row["business_date"]
    month_last_working_date = row["month_last_working_date"]
    if month_last_working_date is None:
        return False, (f"monthly Eom → no ref_calendar record for area '{run_after_area}' "
                       f"business_date {area_date}")

    if area_date != expected_eom_business_date:
        return False, (f"monthly Eom → area '{run_after_area}' max business_date "
                       f"{area_date} != expected EOM business date {expected_eom_business_date}")

    if area_date != month_last_working_date:
        return False, (f"monthly Eom → area '{run_after_area}' max business_date "
                       f"{area_date} != its month's last working date {month_last_working_date}")

    return True, (f"monthly Eom → area '{run_after_area}' max business_date {area_date} "
                  f"matches expected EOM business date {expected_eom_business_date}")

# COMMAND ----------

def is_eligible(report):

    """Determine if a report should run today based on its frequency"""

    freq = report["frequency"].lower()

    if frequency_filter != "all" and freq != frequency_filter:
        return False, f"filtered out (frequency_filter={frequency_filter})"

    if freq == "daily":
        run_days = report.get("run_days") # Can be None or a non-empty string of weekdays ex. "Monday,Tuesday,Thursday,Friday"
        current_day = today_name.lower()

        if not run_days:
            return True, "daily → always eligible"
        else:
            configured_days = {day.strip().lower() for day in run_days.split(",") if day.strip()}

            if current_day in configured_days:
                return True, (f"daily → eligible today ({today_name}); "f"configured days: {run_days}")
            
            else:
                return False, (f"daily → not scheduled for today ({today_name}); "f"configured days: {run_days}")

    elif freq == "monthly":
        # EOM reports need two independent gates:
        #   1. Date gate: today must be the calendar day immediately after the month's
        #      final business date. This allows Saturday execution for a Friday EOM and
        #      prevents a duplicate execution on the following Monday.
        #   2. Area gate: run_after_area must have completed that exact EOM date.
        #
        # If depends_on_workflow is set, only the area gate is deferred because that
        # workflow advances v_area_latest_status. The date gate must never be deferred.
        run_days = report.get("run_days")
        if run_days and run_days.strip().lower() == "eom":
            schedule_ok, schedule_reason = check_eom_schedule_gate()
            if not schedule_ok:
                return False, schedule_reason

            if report.get("depends_on_workflow"):
                return True, (f"monthly Eom → eligible-pending for business date "
                              f"{expected_eom_business_date}; area gate deferred until "
                              f"workflow '{report['depends_on_workflow']}' completes")
            return check_eom_area_gate(report.get("run_after_area"))

        # Calendar-day trigger takes precedence over business-day trigger.
        # When trigger_calendar_day is set, the report runs on that day of the month.
        trigger_cal_day = report.get("trigger_calendar_day")
        if trigger_cal_day is not None:
            if today_calendar_day == trigger_cal_day:
                return True, f"monthly → today is calendar day #{trigger_cal_day}"
            else:
                return False, f"monthly → today is calendar day #{today_calendar_day}, needs #{trigger_cal_day}"

        trigger_day = report.get("trigger_business_day")
        if trigger_day is None:
            return True, "monthly with no trigger_business_day → always eligible"
        if today_bday_number == trigger_day:
            return True, f"monthly → today is business day #{trigger_day}"
        else:
            return False, f"monthly → today is bday #{today_bday_number}, needs #{trigger_day}"

    elif freq == "weekly":
        # Future: implement weekly logic
        return True, "weekly → not yet implemented, running"

    return False, f"unknown frequency: {freq}"

# COMMAND ----------

# ── STEP 1: stamp triggered_at on ALL active reports (batch, before any run) ──
mark_triggered_all(reports)

# ── Build execution plan ──
eligible = []
skipped = []

for report in reports:
    ok, reason = is_eligible(report)
    if ok:
        eligible.append(report)
        print(f"  ✓ {report['report_name']} ({reason})")
    else:
        skipped.append((report["report_name"], reason))
        print(f"  ⏭ {report['report_name']} ({reason})")

print(f"\nEligible: {len(eligible)} | Skipped: {len(skipped)}")

# Result accumulators (also used by the execution threads below)
completed = {}  # report_name → "SUCCESS" / "FAILED" / "SKIPPED"
results = []    # (report_name, status, duration_sec, error)

# ── Non-eligible active reports are SKIPPED for this cycle (tracked, not silent) ──
# This keeps every active report's todays_status meaningful every day: a report the
# downstream IT flow waits on shows SKIPPED (don't expect fresh data) instead of a
# misleading 'pending' that never resolves.
for report_name, reason in skipped:
    update_report_status(report_name, "SKIPPED", 0)
    completed[report_name] = "SKIPPED"
    results.append((report_name, "SKIPPED", 0, reason))

# COMMAND ----------

# ── Execute reports respecting dependencies ──
w = WorkspaceClient()

# ── External workflow dependency check ──
import time

POLL_INTERVAL_SEC = 1800  # check every 30 minutes
POLL_START_HOUR = 12  # start checking at 12:00
POLL_END_HOUR = 24  # stop checking at 24:00 (midnight)


def wait_for_workflow_dependency(workflow_name):
    """
    Wait/poll until the external workflow has a successful run that completed today.
    Starts checking at 12:00, polls every 30 minutes until 24:00.
    Returns True if dependency satisfied, False if timed out.
    """
    print(f"\n⏳ Waiting for external workflow '{workflow_name}' to complete...")
    job = next(w.jobs.list(name=workflow_name), None)
    if job is None:
        raise Exception(f"External workflow '{workflow_name}' not found in workspace")

    today_start = datetime.strptime(today, "%Y-%m-%d").replace(tzinfo=timezone("Europe/Athens"))
    today_start_ms = int(today_start.timestamp() * 1000)

    # Wait until 12:00 before starting to poll
    now = datetime.now(timezone("Europe/Athens"))
    poll_start = now.replace(hour=POLL_START_HOUR, minute=0, second=0, microsecond=0)
    if now < poll_start:
        wait_seconds = int((poll_start - now).total_seconds())
        print(f"  … waiting until 12:00 to start checking ({wait_seconds}s)")
        time.sleep(wait_seconds)

    while True:
        now = datetime.now(timezone("Europe/Athens"))
        if now.hour >= POLL_END_HOUR:
            print(f"  ✗ Timed out waiting for '{workflow_name}' — reached {POLL_END_HOUR}:00")
            return False

        runs = list(w.jobs.list_runs(job_id=job.job_id, completed_only=True, limit=5))
        for run in runs:
            if (run.end_time and run.end_time >= today_start_ms
                    and run.state.result_state and run.state.result_state.value == "SUCCESS"):
                print(f"  ✓ External workflow '{workflow_name}' completed successfully today.")
                return True

        print(f"  … still waiting (now: {now.strftime('%H:%M')}, checking every {POLL_INTERVAL_SEC // 60} min)")
        time.sleep(POLL_INTERVAL_SEC)


def trigger_report(report):
    """Trigger a report job (non-blocking). Returns (report_name, job, run) or raises."""
    report_name = report["report_name"]
    job_name = report["job_name"]
    skip_email = skip_email_override or report.get("skip_email", False)

    # Build job parameters
    job_params = {"run_date": run_date}
    if skip_email:
        job_params["skip_email"] = "true"

    print(f"\n▶ Triggering: {report_name} (job: {job_name})")

    job = next(w.jobs.list(name=job_name), None)
    if job is None:
        raise Exception(f"Job '{job_name}' not found in workspace")

    run = w.jobs.run_now(job_id=job.job_id, job_parameters=job_params)
    return run


def wait_for_run(report, run_response):
    """Wait for a triggered run to complete. Updates registry and returns status."""
    report_name = report["report_name"]
    start_time = datetime.now(timezone("Europe/Athens"))

    try:
        run = w.jobs.get_run(run_id=run_response.run_id)
        # Poll until terminal state
        while run.state.life_cycle_state.value in ("PENDING", "RUNNING", "QUEUED"):
            time.sleep(30)
            run = w.jobs.get_run(run_id=run_response.run_id)

        duration_sec = int((datetime.now(timezone("Europe/Athens")) - start_time).total_seconds())
        status = "SUCCESS" if run.state.result_state and run.state.result_state.value == "SUCCESS" else "FAILED"
        error_msg = ""
        if status == "FAILED":
            error_msg = run.state.state_message or "Unknown error"

        print(f"  {'✓' if status == 'SUCCESS' else '✗'} {report_name}: {status} ({duration_sec}s)")

    except Exception as e:
        duration_sec = int((datetime.now(timezone("Europe/Athens")) - start_time).total_seconds())
        status = "FAILED"
        error_msg = str(e)
        print(f"  ✗ {report_name}: FAILED → {e}")

    # Update registry (concurrency-safe via in-process lock)
    update_report_status(report_name, status, duration_sec)

    completed[report_name] = status
    results.append((report_name, status, duration_sec, error_msg))
    return status


# ── Execute: parallel with smart dependency handling ──
from concurrent.futures import ThreadPoolExecutor, as_completed

# Split eligible reports into groups:
# 1. no_ext_dep: no depends_on_workflow → trigger immediately
# 2. has_ext_dep: has depends_on_workflow → wait for external workflow first, then trigger
# 3. has_report_dep: has depends_on_report → run after their dependency completes
no_ext_dep = [r for r in eligible if not r.get("depends_on_workflow") and not r.get("depends_on_report")]
has_ext_dep = [r for r in eligible if r.get("depends_on_workflow") and not r.get("depends_on_report")]
has_report_dep = [r for r in eligible if r.get("depends_on_report")]

print(f"\n{'─' * 40}")
print(f"Immediate (no external dep): {[r['report_name'] for r in no_ext_dep]}")
print(f"Waiting for external workflow: {[r['report_name'] for r in has_ext_dep]}")
print(f"Depends on other report: {[r['report_name'] for r in has_report_dep]}")


def _trigger_and_wait(report):
    """Trigger a report and wait for completion. Used by thread pool."""
    try:
        run_response = trigger_report(report)
        return wait_for_run(report, run_response)
    except Exception as e:
        report_name = report["report_name"]
        print(f"  ✗ {report_name}: FAILED → {e}")
        completed[report_name] = "FAILED"
        results.append((report_name, "FAILED", 0, str(e)))
        return "FAILED"


def _is_deferred_eom(report):
    """True for EOM reports whose area gate was deferred until the workflow completes."""
    run_days = report.get("run_days")
    return bool(run_days) and run_days.strip().lower() == "eom"


def _recheck_eom_gate_after_workflow(report):
    """
    Re-evaluate the deferred EOM area gate against FRESH area state (the dependency
    workflow has just advanced v_area_latest_status). Returns True to proceed, False to
    skip. Marks the report SKIPPED in the registry if the gate is not satisfied.
    """
    ok, reason = check_eom_area_gate(report.get("run_after_area"))
    if ok:
        print(f"  ✓ {report['report_name']}: EOM area gate satisfied after workflow ({reason})")
        return True

    report_name = report["report_name"]
    print(f"  ⏭ {report_name}: EOM area gate not satisfied after workflow ({reason})")
    completed[report_name] = "SKIPPED"
    results.append((report_name, "SKIPPED", 0, reason))
    update_report_status(report_name, "SKIPPED", 0)
    return False


def _wait_ext_then_trigger(report):
    """Wait for external workflow dependency, then trigger and wait for report."""
    wf = report["depends_on_workflow"]
    if not wait_for_workflow_dependency(wf):
        report_name = report["report_name"]
        print(f"  ✗ {report_name}: external dependency '{wf}' timed out")
        completed[report_name] = "SKIPPED"
        results.append((report_name, "SKIPPED", 0, f"external dependency '{wf}' timed out"))
        update_report_status(report_name, "SKIPPED", 0)
        return "SKIPPED"

    # Deferred EOM area gate: the workflow has now advanced v_area_latest_status, so
    # re-check the month-close condition against fresh state before triggering.
    if _is_deferred_eom(report) and not _recheck_eom_gate_after_workflow(report):
        return "SKIPPED"

    return _trigger_and_wait(report)


# Launch all groups in parallel using a thread pool
all_tasks = []
with ThreadPoolExecutor(max_workers=max(len(no_ext_dep) + len(has_ext_dep), 1)) as executor:
    # Group 1: trigger immediately (no external dep)
    for report in no_ext_dep:
        all_tasks.append(executor.submit(_trigger_and_wait, report))

    # Group 2: wait for external workflow, then trigger
    for report in has_ext_dep:
        all_tasks.append(executor.submit(_wait_ext_then_trigger, report))

    # Wait for all parallel tasks to finish
    for future in as_completed(all_tasks):
        future.result()

# Group 3: dependent reports (sequential, after their dependency completed)
print(f"\n{'─' * 40}")
if has_report_dep:
    print(f"Running {len(has_report_dep)} dependent report(s) sequentially...")

for report in has_report_dep:
    dep = report.get("depends_on_report")
    if dep and completed.get(dep) == "FAILED":
        print(f"  ⏭ {report['report_name']}: skipped (dependency '{dep}' failed)")
        completed[report["report_name"]] = "SKIPPED"
        results.append((report["report_name"], "SKIPPED", 0, f"dependency '{dep}' failed"))
        update_report_status(report["report_name"], "SKIPPED", 0)
        continue

    if dep and dep not in completed:
        print(f"  ⚠ {report['report_name']}: dependency '{dep}' not in completed set, skipping")
        completed[report["report_name"]] = "SKIPPED"
        results.append((report["report_name"], "SKIPPED", 0, f"dependency '{dep}' not found"))
        update_report_status(report["report_name"], "SKIPPED", 0)
        continue

    # Check external workflow dep for dependent reports too
    wf_dep = report.get("depends_on_workflow")
    if wf_dep and not wait_for_workflow_dependency(wf_dep):
        print(f"  ✗ {report['report_name']}: external dependency '{wf_dep}' timed out")
        completed[report["report_name"]] = "SKIPPED"
        results.append((report["report_name"], "SKIPPED", 0, f"external dependency '{wf_dep}' timed out"))
        update_report_status(report["report_name"], "SKIPPED", 0)
        continue

    # Deferred EOM area gate: if this report's area gate was deferred (it has a workflow
    # dep that advances v_area_latest_status), re-check against fresh state before trigger.
    if wf_dep and _is_deferred_eom(report) and not _recheck_eom_gate_after_workflow(report):
        continue

    _trigger_and_wait(report)

# COMMAND ----------

# ── Summary ──
print(f"\n{'═' * 60}")
print(f"MainReport Orchestrator — Summary")
print(f"{'═' * 60}")
failed = [r for r in results if r[1] == "FAILED"]
skipped_results = [r for r in results if r[1] == "SKIPPED"]
success = [r for r in results if r[1] == "SUCCESS"]

print(f"  SUCCESS: {len(success)} | FAILED: {len(failed)} | SKIPPED: {len(skipped_results)}")
for name, status, duration, error in results:
    icon = "✓" if status == "SUCCESS" else "✗" if status == "FAILED" else "⏭"
    line = f"  {icon} {name}: {status} ({duration}s)"
    if error:
        line += f" — {error}"
    print(line)

if failed:
    raise Exception(f"{len(failed)} report(s) failed: {[r[0] for r in failed]}")