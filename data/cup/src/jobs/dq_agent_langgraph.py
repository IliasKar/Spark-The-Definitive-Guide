# Databricks notebook source

# MAGIC %md
# MAGIC # Data Quality Agent — LangGraph Implementation
# MAGIC
# MAGIC This notebook runs after the curated ETL completes (triggered by Airflow after 21:00).
# MAGIC It validates the freshly loaded BusinessDate slice using the LangGraph-based DQ agent.
# MAGIC
# MAGIC **Architecture:**
# MAGIC - Rule-based DQ checks (null rates, duplicates, referential integrity)
# MAGIC - LangGraph StateGraph routes findings by severity:
# MAGIC   - **HIGH/CRITICAL** → Agentic ReAct loop (tool-calling with `run_sql`, `get_table_schema`, `query_dq_history`)
# MAGIC   - **MEDIUM** → Pre-defined investigative queries + single LLM reasoning pass
# MAGIC   - **LOW** → Persisted to Delta only (no LLM, no alert)
# MAGIC - Results persisted to `primaryactive.DataQualityResults`
# MAGIC - Failures sent to MS Teams "Airflow Failures/Updates" channel
# MAGIC
# MAGIC **Dependencies (install on cluster or via %pip):**
# MAGIC ```
# MAGIC langgraph>=1.0.0
# MAGIC langchain-core>=1.0.0
# MAGIC databricks-langchain>=0.4.0
# MAGIC ```
# MAGIC
# MAGIC > **Why the pin below is explicit:** `langgraph` is published together with the
# MAGIC > `langgraph-prebuilt` and `langgraph-checkpoint` sub-packages. Databricks clusters
# MAGIC > often ship these at mismatched versions, which surfaces as
# MAGIC > `cannot import name 'ExecutionInfo' from 'langgraph.runtime'` when
# MAGIC > `langgraph.prebuilt` (ToolNode) expects a newer runtime API than the installed
# MAGIC > `langgraph` core provides. Installing `langgraph` fresh and restarting Python
# MAGIC > fixes it.

# COMMAND ----------

# Install langgraph (1.x) plus the LangChain/Databricks deps this notebook uses.
#
# IMPORTANT: do NOT pin langgraph-prebuilt / langgraph-checkpoint by hand. `langgraph`
# is released together with those sub-packages and its own metadata pins the exact
# compatible ranges (e.g. langgraph 1.x -> langgraph-prebuilt >=1.1,<1.2 and
# langgraph-checkpoint >=4.1,<5). Hand-pinning them to the wrong ranges is what left
# the mismatched combo in place and kept the `ExecutionInfo` import broken. Installing
# just `langgraph` lets pip pull the matching sub-packages automatically.
#
# `--upgrade` is REQUIRED: the cluster pre-installs an older langgraph under
# .../cluster_libraries/.../langgraph/runtime.py (that copy lacks `ExecutionInfo`).
# Without --upgrade, pip sees langgraph as "already satisfied" and never replaces it.
#
# NOTE on `openai<3.0.0`: this notebook uses ChatDatabricks (databricks-langchain),
# NOT OpenAI. But the cluster pre-installs `langchain-openai`, which requires
# `openai>=1.109.1,<3.0.0`. Including it in the SAME command constrains the resolver
# so --upgrade doesn't pull openai 3.x and print a dependency-conflict error.
#
# `--force-reinstall` is needed so the fresh langgraph (and its matched
# langgraph-prebuilt / langgraph-checkpoint) land in the notebook-scoped env and
# shadow the mismatched cluster_libraries copy. Pinning `langgraph>=1.2,<2.0`
# selects the line whose core exports `ExecutionInfo` (validated in QA).
%pip install --upgrade --force-reinstall \
    "langgraph>=1.2,<2.0" \
    "langchain-core>=1.0,<2.0" \
    "databricks-langchain>=0.4.0" \
    "openai>=1.109.1,<3.0.0"

# COMMAND ----------

# Restart Python so the freshly installed langgraph packages replace the cluster's
# pre-imported copy. This is MANDATORY — without it, Python keeps using the old
# langgraph from cluster_libraries and the ExecutionInfo import error persists even
# after a successful %pip install above.
dbutils.library.restartPython()

# COMMAND ----------

import sys
import os
import importlib
import boto3
from pyspark.sql import SparkSession
from databricks_langchain import ChatDatabricks
from langgraph.graph.state import CompiledStateGraph

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# Helper imports — all from dq_agent_langgraph package
langgraph_path = os.path.abspath("../helpers/curated_helpers")
if langgraph_path not in sys.path:
    sys.path.append(langgraph_path)

print(f"[DQ Agent] LangGraph path: {langgraph_path}")

# Drop any import caches so the freshly installed langgraph packages (post
# restartPython) and the dq_agent_langgraph helpers are picked up cleanly.
importlib.invalidate_caches()

from dq_agent_langgraph.data_quality_checks import (
    DQResult,
    check_null_rates_bronze,
    check_null_rates_silver,
    check_amount_null_floor,
    check_duplicates,
    check_referential_integrity,
    discover_primaryactive_checks,
    get_latest_business_date,
)
from dq_agent_langgraph.graph_builder import create_dq_agent
from dq_agent_langgraph.teams_alerting import send_teams_alert
from dq_agent_langgraph.email_alerting import send_email_alert

# ---------------------------------------------------------------------------
# MLflow observability
# ---------------------------------------------------------------------------
# Autolog captures the full LangGraph execution (severity routing, every LLM
# call, each ReAct tool call, token usage, latency) as a nested trace. Traces
# are logged to the experiment set below and browsable in the MLflow UI, giving
# production observability for both scheduled (post-ETL) and on-demand runs.
import mlflow

mlflow.langchain.autolog()

# COMMAND ----------

# ---------------------------------------------------------------------------
# Parameters (passed by Airflow / Databricks job)
# ---------------------------------------------------------------------------
dbutils.widgets.text('run_date', 'None', '01-Run Date')
dbutils.widgets.text('env_prefix', 'None', '02-Environment')
dbutils.widgets.text('model_endpoint', 'databricks-meta-llama-3-3-70b-instruct', '03-Model Endpoint')

run_date = dbutils.widgets.get('run_date')
env_prefix = dbutils.widgets.get('env_prefix')
model_endpoint = dbutils.widgets.get('model_endpoint')

# Resolve env_prefix from AWS SSM if not provided
if env_prefix in ('None', '', None):
    from botocore.exceptions import NoCredentialsError

    secrets_scope = "aws-databricks"
    aws_keys = dbutils.secrets.list(scope=secrets_scope)
    key_names = [key.key for key in aws_keys] if aws_keys else []
    aws_key = key_names[0] if key_names else None
    aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=aws_key)

    ssm_client = boto3.client(
        'ssm',
        region_name='eu-central-1',
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret_key,
    )
    response = ssm_client.get_parameter(Name='/cfn/bucket_prefix', WithDecryption=False)
    env_prefix = response['Parameter']['Value']
else:
    secrets_scope = "aws-databricks"
    aws_keys = dbutils.secrets.list(scope=secrets_scope)
    key_names = [key.key for key in aws_keys] if aws_keys else []
    aws_key = key_names[0] if key_names else None
    aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=aws_key)

    ssm_client = boto3.client(
        'ssm',
        region_name='eu-central-1',
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret_key,
    )

# Build catalog paths
catalog = f"`{env_prefix}-cdh`"
primaryactive = f"{catalog}.`primaryactive`"
landed = f"{catalog}.`landed`"

# We only validate the latest loaded slice. If no run_date was passed, resolve it
# to the MAX(BusinessDate) of a core table (exposure) so the run always targets
# the freshest curated data.
if run_date in ('None', '', None):
    run_date = get_latest_business_date(spark, f"{primaryactive}.exposure")
    print(f"[DQ Agent - LangGraph] run_date not provided — using latest BusinessDate: {run_date}")

print(f"[DQ Agent - LangGraph] Environment: {env_prefix} | BusinessDate: {run_date}")
print(f"[DQ Agent - LangGraph] Model: {model_endpoint}")
print(f"[DQ Agent - LangGraph] Primary Active: {primaryactive}")
print(f"[DQ Agent - LangGraph] Landed: {landed}")

# ---------------------------------------------------------------------------
# MLflow experiment + Unity Catalog trace location
# ---------------------------------------------------------------------------
# Traces are stored as governed Delta tables (OpenTelemetry format) in the UC
# schema `{env}-cdh`.dqagenttrace, instead of the capped (100k) experiment
# backend. The schema + grants are provisioned by the deployment notebook
# src/scripts_transfer_hive_to_uc/deploy_dqagenttrace_schema.py and MUST exist
# before this runs — binding does NOT create the schema.
#
# IMPORTANT: a UC trace location can only be bound at experiment CREATION time
# and is immutable afterwards. We therefore use a dedicated experiment name
# (…/dq_agent_uc); the legacy …/dq_agent experiment stays on the default backend
# and cannot be re-bound. `set_experiment` is an upsert: once the UC-bound
# experiment exists, re-runs reuse it (trace_location is optional on reruns).
#
# A SQL warehouse is required to write and view UC traces; its id is passed via
# MLFLOW_TRACING_SQL_WAREHOUSE_ID (prod default below, overridable by widget).
from mlflow.entities.trace_location import UnityCatalog

dbutils.widgets.text('sql_warehouse_id', '818510884d309018', '04-SQL Warehouse ID (UC trace writes)')
dbutils.widgets.text('trace_schema', 'dqagenttrace', '05-UC trace schema (under {env}-cdh)')
dbutils.widgets.text('trace_table_prefix', 'dq_agent', '06-UC trace table prefix')

sql_warehouse_id = dbutils.widgets.get('sql_warehouse_id').strip()
trace_schema = dbutils.widgets.get('trace_schema').strip()
trace_table_prefix = dbutils.widgets.get('trace_table_prefix').strip()

mlflow.set_tracking_uri("databricks")
os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = sql_warehouse_id

mlflow_experiment = f"/Shared/dq_agent/{env_prefix}/dq_agent_uc"
mlflow.set_experiment(
    experiment_name=mlflow_experiment,
    trace_location=UnityCatalog(
        catalog_name=f"{env_prefix}-cdh",
        schema_name=trace_schema,
        table_prefix=trace_table_prefix,
    ),
)
print(f"[DQ Agent - LangGraph] MLflow experiment: {mlflow_experiment}")
print(f"[DQ Agent - LangGraph] UC trace location: `{env_prefix}-cdh`.{trace_schema} "
      f"(prefix={trace_table_prefix}, warehouse={sql_warehouse_id})")

# COMMAND ----------

# ===========================================================================
# DQ CHECK CONFIGURATION
# ===========================================================================

# 1. NULL RATE MONITORING — Bronze (Landed) Tables [MEDIUM]
BRONZE_NULL_CHECKS = [
    ("profits_loan_details", ["ACCOUNT_NUMBER", "agreement_number"], 0.0, "MEDIUM"),
    ("profits_loan_balance", ["ACCOUNT_NUMBER", "TOTAL_BALANCE"], 0.0, "MEDIUM"),
    ("profits_customer", ["CUST_ID"], 0.0, "MEDIUM"),
    ("profits_loan_relation", ["FK_ACCOUNT", "FK_CUSTOMER"], 0.0, "MEDIUM"),
    ("qca_dim_cases_static", ["CASE_ACCOUNT_FOR_SEARCH", "cust_code"], 0.0, "MEDIUM"),
    ("qca_fs_customers_snap", ["CUST_CODE"], 0.0, "MEDIUM"),
    ("arotron_eaiSIG_BE", ["eaiSIG_BE_CASE_S_ID", "eaiSIG_BE_CASE_OUTBAL"], 0.0, "MEDIUM"),
    ("arotron_eaiSIG_BPR", ["eaiSIG_BPR_CUST_S_UNID"], 0.0, "MEDIUM"),
]

# 2. & 3. SILVER NULL + DUPLICATE CHECKS — discovered dynamically for ALL
# business tables in primaryactive. The ID (first column) drives the duplicate
# check and must not be null; monetary amount columns must not be null either.
# System/otel/trace/metrics tables are excluded by discover_primaryactive_checks.
PRIMARYACTIVE_CHECKS = discover_primaryactive_checks(spark, catalog)
print(f"[DQ Agent] Discovered {len(PRIMARYACTIVE_CHECKS)} primaryactive business tables to check.")

# 4. CROSS-SOURCE REFERENTIAL INTEGRITY [MEDIUM]
REFERENTIAL_INTEGRITY_CHECKS = [
    ("exposure", "party", "PartyKey", "PartyKey", "MEDIUM"),
    ("exposure", "agreement", "AgreementKey", "AgreementKey", "MEDIUM"),
    # collateral links to loans via AgreementKey — the curated collateral table
    # has no ExposureKey column (see collateral_qca / collateral_thesis notebooks).
    ("collateral", "agreement", "AgreementKey", "AgreementKey", "MEDIUM"),
]

# COMMAND ----------

# ===========================================================================
# EXECUTE ALL DQ CHECKS
# ===========================================================================

all_results: list[DQResult] = []

# --- 1. Bronze Null Checks ---
print("\n" + "=" * 60)
print("[DQ Agent] Running Bronze (Landed) Null Rate Checks...")
print("=" * 60)

for table_name, columns, threshold, sev in BRONZE_NULL_CHECKS:
    print(f"  Checking {landed}.{table_name} ...")
    results = check_null_rates_bronze(
        spark=spark,
        catalog_landed=landed,
        landed_table_name=table_name,
        run_date=run_date,
        critical_columns=columns,
        null_threshold=threshold,
        severity=sev,
    )
    all_results.extend(results)
    for r in results:
        symbol = "✓" if r.status == "PASS" else "✗"
        print(f"    {symbol} [{r.severity}] {r.details}")

# --- 2. & 3. Silver Null + Amount-floor + Duplicate Checks (ALL primaryactive business tables) ---
# Null (strict): ID + balance/principal/... must not be null.
# Amount-floor: *Amount*-named columns flagged MEDIUM when null_rate < 1%.
# Duplicate: on the first-column ID; SKIPPED for ext* tables.
print("\n" + "=" * 60)
print("[DQ Agent] Running Silver (primaryactive) Null + Amount-floor + Duplicate Checks...")
print("=" * 60)

for cfg in PRIMARYACTIVE_CHECKS:
    full_path = cfg["table_path"]
    print(f"  Checking {full_path} (id={cfg['id_column']}, "
          f"null_cols={cfg['null_columns']}, amount_cols={cfg.get('amount_columns', [])}, "
          f"skip_dup={cfg.get('skip_duplicate_check', False)}) ...")

    # Strict null checks: ID column + non-Amount amount-like columns (balance,
    # principal, ...) must not be null (threshold 0.0).
    null_results = check_null_rates_silver(
        spark=spark,
        curated_table_path=full_path,
        run_date=run_date,
        critical_columns=cfg["null_columns"],
        null_threshold=0.0,
        source_system=None,
        severity="MEDIUM",
    )
    all_results.extend(null_results)
    for r in null_results:
        symbol = "✓" if r.status == "PASS" else "✗"
        print(f"    {symbol} [{r.severity}] {r.details}")

    # Amount columns (name contains 'Amount'): inverted floor rule — a null rate
    # BELOW 1% (including zero nulls) is a possible problem flagged at MEDIUM;
    # a null rate >= 1% is expected (PASS).
    amount_results = check_amount_null_floor(
        spark=spark,
        curated_table_path=full_path,
        run_date=run_date,
        amount_columns=cfg.get("amount_columns", []),
        source_system=None,
    )
    all_results.extend(amount_results)
    for r in amount_results:
        symbol = "✓" if r.status == "PASS" else "✗"
        print(f"    {symbol} [{r.severity}] {r.details}")

    # Duplicate check on the unique first-column ID — SKIPPED for ext* tables
    # (external feeds whose grain is not a unique single-column key).
    if cfg.get("skip_duplicate_check", False):
        print(f"    ⤼ duplicate check skipped for {cfg['table']} (ext* table).")
    else:
        dup_results = check_duplicates(
            spark=spark,
            curated_table_path=full_path,
            run_date=run_date,
            key_columns=[cfg["id_column"]],
            source_system=None,
            severity="HIGH",
        )
        all_results.extend(dup_results)
        for r in dup_results:
            symbol = "✓" if r.status == "PASS" else "✗"
            print(f"    {symbol} [{r.severity}] {r.details}")

# --- 4. Referential Integrity ---
print("\n" + "=" * 60)
print("[DQ Agent] Running Referential Integrity Checks...")
print("=" * 60)

for src_table, tgt_table, src_col, tgt_col, sev in REFERENTIAL_INTEGRITY_CHECKS:
    src_path = f"{primaryactive}.{src_table}"
    tgt_path = f"{primaryactive}.{tgt_table}"
    print(f"  Checking {src_path}.{src_col} → {tgt_path}.{tgt_col} ...")
    results = check_referential_integrity(
        spark=spark,
        source_table_path=src_path,
        target_table_path=tgt_path,
        source_key_column=src_col,
        target_key_column=tgt_col,
        run_date=run_date,
        severity=sev,
    )
    all_results.extend(results)
    for r in results:
        symbol = "✓" if r.status == "PASS" else "✗"
        print(f"    {symbol} [{r.severity}] {r.details}")

# COMMAND ----------

# ===========================================================================
# LANGGRAPH AGENT — AI-POWERED ROOT-CAUSE ANALYSIS
# ===========================================================================

failures = [r for r in all_results if r.status == "FAIL"]
passes = [r for r in all_results if r.status == "PASS"]

# Group failures by severity
high_failures = [f for f in failures if f.severity in ("CRITICAL", "HIGH")]
medium_failures = [f for f in failures if f.severity == "MEDIUM"]
low_failures = [f for f in failures if f.severity == "LOW"]

print("\n" + "=" * 60)
print(f"[DQ Agent] SUMMARY — BusinessDate: {run_date}")
print("=" * 60)
print(f"  Total checks:      {len(all_results)}")
print(f"  Passed:            {len(passes)}")
print(f"  Failed:            {len(failures)}")
print(f"    🟠 HIGH/CRIT:    {len(high_failures)}")
print(f"    🟡 MEDIUM:       {len(medium_failures)}")
print(f"    ⚪ LOW:          {len(low_failures)}")
print("=" * 60)

# --- Run LangGraph Agent ---
agent_result = None
if failures:
    print("\n[DQ Agent] 🤖 Running LangGraph agent for root-cause analysis...")
    print(f"[DQ Agent]    Model: {model_endpoint}")

    try:
        # Create the LangGraph agent
        dq_agent = create_dq_agent(
            spark=spark,
            catalog=catalog,
            run_date=run_date,
            env_prefix=env_prefix,
            model_endpoint=model_endpoint,
        )

        # Convert DQResult objects to DQFinding dicts for the graph state
        findings_for_agent = [
            {
                "check_name": r.check_name,
                "table_name": r.table_name,
                "status": r.status,
                "severity": r.severity,
                "details": r.details,
                "business_date": r.business_date,
                "source_system": r.source_system,
                "column_name": r.column_name,
                "metric_value": r.metric_value,
                "threshold": r.threshold,
            }
            for r in all_results
        ]

        # Invoke the LangGraph agent inside an MLflow run so the whole
        # execution is captured as one named, tagged trace (browsable in the
        # MLflow experiment). autolog() records the nested LLM/tool spans.
        with mlflow.start_run(run_name=f"dq_agent_{run_date}"):
            mlflow.set_tags({
                "run_date": run_date,
                "env_prefix": env_prefix,
                "model_endpoint": model_endpoint,
                "total_failures": len(failures),
                "high_critical_failures": len(high_failures),
                "medium_failures": len(medium_failures),
            })

            agent_result = dq_agent.invoke({
                "findings": findings_for_agent,
                "run_date": run_date,
                "catalog": catalog,
                "env_prefix": env_prefix,
            })

            # Log the resolved severity tier as a metric-friendly param
            mlflow.set_tag("severity_tier", agent_result.get("severity_tier", "N/A"))

        # Print results
        print(f"\n[DQ Agent] ✅ LangGraph agent completed successfully")
        print(f"[DQ Agent]    Severity tier: {agent_result.get('severity_tier', 'N/A')}")
        print(f"\n[DQ Agent] AI Summary: {agent_result.get('summary', 'N/A')}")

        if agent_result.get("root_causes"):
            print("[DQ Agent] Root Causes:")
            for cause in agent_result["root_causes"]:
                print(f"    • {cause}")

        if agent_result.get("differing_columns"):
            print(f"[DQ Agent] Differing columns (duplicate driver): "
                  f"{', '.join(agent_result['differing_columns'])}")

        if agent_result.get("landed_table_origin"):
            print(f"[DQ Agent] Origin: {agent_result['landed_table_origin']} "
                  f"(owner: {agent_result.get('issue_owner', 'unknown')})")
        else:
            print(f"[DQ Agent] Owner: {agent_result.get('issue_owner', 'unknown')}")

        if agent_result.get("troubleshooting_sql"):
            print("[DQ Agent] Troubleshooting SQL:")
            print(f"    {agent_result['troubleshooting_sql']}")

        if agent_result.get("recommendations"):
            print("[DQ Agent] Recommendations:")
            for rec in agent_result["recommendations"]:
                print(f"    • {rec}")

        # Per-finding breakdown — one entry per failing finding
        if agent_result.get("findings_analysis"):
            print("\n[DQ Agent] Per-finding analysis:")
            for fa in agent_result["findings_analysis"]:
                print(
                    f"  [{fa.get('finding_index', '?')}] "
                    f"{fa.get('check_name', '?')} | {fa.get('table_name', '?')} "
                    f"| {fa.get('severity', '?')} | owner={fa.get('issue_owner', '?')}"
                )
                if fa.get("root_cause"):
                    print(f"       root cause: {fa['root_cause']}")
                if fa.get("differing_columns"):
                    print(f"       differing columns: {', '.join(fa['differing_columns'])}")
                if fa.get("landed_table_origin"):
                    print(f"       landed origin: {fa['landed_table_origin']}")
                if fa.get("troubleshooting_sql"):
                    print(f"       SQL: {fa['troubleshooting_sql']}")
                if fa.get("recommendation"):
                    print(f"       fix: {fa['recommendation']}")

        if agent_result.get("severity_overrides"):
            print("[DQ Agent] Severity Overrides:")
            for idx, new_sev in agent_result["severity_overrides"].items():
                print(f"    • Finding [{idx}] → {new_sev}")

    except Exception as e:
        import traceback
        print(f"[DQ Agent] ⚠️ LangGraph agent failed: {str(e)}")
        traceback.print_exc()
        print("[DQ Agent] Proceeding with rule-based findings only.")

# --- Send Email Alert ---
# NOTE: MS Teams webhook/Workflow posting is blocked at the tenant level
# (Office 365 connector retired; Power Automate chat trigger returns 401).
# As a first step we deliver one consolidated email with all failures + the
# agent's analysis. The whole alerting step is wrapped so an email failure
# never aborts the job (persistence of DQ results still runs).
ALERT_RECIPIENTS_TO = ["dna-de@fshellas.onmicrosoft.com"]
ALERT_RECIPIENTS_CC = []

if failures:
    print("\n[DQ Agent] Sending alert email...")
    try:
        smtp_scope = "email-smtp"
        smtp_settings = {
            "ip": dbutils.secrets.get(scope=smtp_scope, key="ip"),
            "port": dbutils.secrets.get(scope=smtp_scope, key="port"),
            "from": dbutils.secrets.get(scope=smtp_scope, key="from"),
            "login": dbutils.secrets.get(scope=smtp_scope, key="login"),
            "password": dbutils.secrets.get(scope=smtp_scope, key="password"),
        }
        alert_sent = send_email_alert(
            failures=[
                {
                    "check_name": f.check_name,
                    "table_name": f.table_name,
                    "status": f.status,
                    "severity": f.severity,
                    "details": f.details,
                    "business_date": f.business_date,
                    "source_system": f.source_system,
                    "column_name": f.column_name,
                    "metric_value": f.metric_value,
                    "threshold": f.threshold,
                }
                for f in failures
            ],
            run_date=run_date,
            environment=env_prefix,
            smtp_settings=smtp_settings,
            recipients_to=ALERT_RECIPIENTS_TO,
            recipients_cc=ALERT_RECIPIENTS_CC,
            agent_state=agent_result,
        )
        if not alert_sent:
            print("[DQ Agent] WARNING: Failed to send alert email.")
    except Exception as e:
        print(f"[DQ Agent] WARNING: Alert email step failed: {e}")
        print("[DQ Agent] Proceeding — alerting failure is non-fatal.")
else:
    print("\n[DQ Agent] All checks passed. No alert sent.")

# COMMAND ----------

# ===========================================================================
# PERSIST DQ RESULTS TO DELTA TABLE — Historical Tracking
# ===========================================================================

from pyspark.sql import Row
import pyspark.sql.functions as F
from datetime import datetime

dq_table_path = f"{catalog}.`primaryactive`.DataQualityResults"

# Create table if not exists
dq_schema = """
    check_name STRING,
    table_name STRING,
    status STRING,
    severity STRING,
    details STRING,
    business_date DATE,
    source_system STRING,
    column_name STRING,
    metric_value DOUBLE,
    threshold DOUBLE,
    check_timestamp TIMESTAMP,
    ai_summary STRING,
    ai_root_causes STRING,
    ai_recommendations STRING,
    ai_landed_origin STRING,
    ai_differing_columns STRING,
    ai_issue_owner STRING,
    ai_troubleshooting_sql STRING,
    ai_findings_analysis STRING,
    BusinessDate DATE
"""

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {dq_table_path} (
        {dq_schema}
    )
    USING DELTA
    CLUSTER BY (BusinessDate)
    TBLPROPERTIES (
        'delta.logRetentionDuration' = 'interval 5 weeks',
        'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7',
        'delta.columnMapping.mode' = 'name',
        'delta.dataSkippingStatsColumns' = 'BusinessDate'
    )
""")

# Prepare AI analysis text
ai_summary_text = agent_result.get("summary", "") if agent_result else ""
ai_root_causes_text = "; ".join(agent_result.get("root_causes", [])) if agent_result else ""
ai_recommendations_text = "; ".join(agent_result.get("recommendations", [])) if agent_result else ""
ai_landed_origin = agent_result.get("landed_table_origin", "") if agent_result else ""
ai_differing_columns = "; ".join(agent_result.get("differing_columns", [])) if agent_result else ""
ai_issue_owner = agent_result.get("issue_owner", "") if agent_result else ""
ai_troubleshooting_sql = agent_result.get("troubleshooting_sql", "") if agent_result else ""

import json as _json
ai_findings_analysis = (
    _json.dumps(agent_result.get("findings_analysis", []), default=str)
    if agent_result and agent_result.get("findings_analysis")
    else ""
)

# Build rows
rows = [
    Row(
        check_name=r.check_name,
        table_name=r.table_name,
        status=r.status,
        severity=r.severity,
        details=r.details,
        business_date=datetime.strptime(r.business_date, "%Y-%m-%d").date(),
        source_system=r.source_system or "",
        column_name=r.column_name or "",
        metric_value=float(r.metric_value) if r.metric_value is not None else None,
        threshold=float(r.threshold) if r.threshold is not None else None,
        check_timestamp=datetime.strptime(r.timestamp, "%Y-%m-%d %H:%M:%S"),
        ai_summary=ai_summary_text if r.status == "FAIL" else "",
        ai_root_causes=ai_root_causes_text if r.status == "FAIL" else "",
        ai_recommendations=ai_recommendations_text if r.status == "FAIL" else "",
        ai_landed_origin=ai_landed_origin if r.status == "FAIL" else "",
        ai_differing_columns=ai_differing_columns if r.status == "FAIL" else "",
        ai_issue_owner=ai_issue_owner if r.status == "FAIL" else "",
        ai_troubleshooting_sql=ai_troubleshooting_sql if r.status == "FAIL" else "",
        ai_findings_analysis=ai_findings_analysis if r.status == "FAIL" else "",
        BusinessDate=datetime.strptime(r.business_date, "%Y-%m-%d").date(),
    )
    for r in all_results
]

if rows:
    dq_df = spark.createDataFrame(rows)
    dq_df.write.format("delta").clusterBy("BusinessDate").mode("append").saveAsTable(dq_table_path)
    print(f"[DQ Agent] {len(rows)} results persisted to {dq_table_path}")
else:
    print("[DQ Agent] No results to persist.")

# COMMAND ----------

# ===========================================================================
# VISUALIZE THE LANGGRAPH (optional — for debugging/documentation)
# ===========================================================================
# Uncomment to display the graph structure in the notebook output:

# from IPython.display import Image, display
# dq_agent = create_dq_agent(spark, catalog, run_date, env_prefix, model_endpoint)
# display(Image(dq_agent.get_graph().draw_mermaid_png()))
