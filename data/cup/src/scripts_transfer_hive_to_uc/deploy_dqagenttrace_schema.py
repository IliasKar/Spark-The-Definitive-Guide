# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment — `dqagenttrace` Unity Catalog schema (DQ Agent trace location)
# MAGIC
# MAGIC One-off / idempotent deployment script that provisions the Unity Catalog
# MAGIC schema used as the **MLflow trace location** for the Data Quality Agent
# MAGIC (`src/jobs/dq_agent_langgraph.py`).
# MAGIC
# MAGIC In MLflow 3, an experiment can be bound to a UC schema so traces are stored
# MAGIC as governed Delta tables (OpenTelemetry format) instead of the capped
# MAGIC experiment backend. The binding call (`mlflow.set_experiment(...,
# MAGIC trace_location=UnityCatalog(...))`) does **not** create the catalog/schema —
# MAGIC they must exist first. This notebook creates the schema and the required
# MAGIC grants so the DQ agent job can create and write the trace tables.
# MAGIC
# MAGIC **What MLflow creates on first run** (NOT created here — MLflow owns them):
# MAGIC `{table_prefix}_otel_spans`, `{table_prefix}_otel_logs`,
# MAGIC `{table_prefix}_otel_metrics`, `{table_prefix}_otel_annotations`.
# MAGIC
# MAGIC **Key constraints (MLflow 3 UC tracing):**
# MAGIC - MLflow Python **>= 3.14** is required by the DQ agent job (DBR 18 ships MLflow 3.x).
# MAGIC - A **SQL warehouse** is required to write/read UC traces.
# MAGIC - `ALL_PRIVILEGES` is **NOT** sufficient for the trace tables — `MODIFY` and
# MAGIC   `SELECT` must be granted **explicitly**.
# MAGIC - An experiment can only be bound to a UC trace location **at creation time**,
# MAGIC   and the binding is immutable. Existing experiments cannot be re-bound.
# MAGIC
# MAGIC Docs: Databricks "Store OpenTelemetry traces in Unity Catalog" (MLflow 3).

# COMMAND ----------

# DBTITLE 1,Parameters
# Prod defaults are pre-filled. Override via the widgets for QA/dev dry-runs.
dbutils.widgets.text("catalog",       "p-cdh",          "01-Catalog (e.g. p-cdh / qa-cdh)")
dbutils.widgets.text("schema",        "dqagenttrace",   "02-Schema to create")
dbutils.widgets.text("job_principal", "Service_Principal", "03-DQ agent job run-as principal (SP app-id / user / group)")
dbutils.widgets.text("reader_group",  "",               "04-Optional reader group for traces (SELECT only)")
dbutils.widgets.text("table_prefix",  "dq_agent",       "05-MLflow trace table_prefix")
dbutils.widgets.text("apply_grants",  "true",           "06-Apply grants (true/false)")

catalog       = dbutils.widgets.get("catalog").strip()
schema        = dbutils.widgets.get("schema").strip()
job_principal = dbutils.widgets.get("job_principal").strip()
reader_group  = dbutils.widgets.get("reader_group").strip()
table_prefix  = dbutils.widgets.get("table_prefix").strip()
apply_grants  = dbutils.widgets.get("apply_grants").strip().lower() == "true"

if not catalog or not schema:
    raise ValueError("Both 'catalog' and 'schema' are required.")

cat_q    = f"`{catalog}`"
schema_q = f"`{catalog}`.`{schema}`"

print(f"[deploy] Target schema : {schema_q}")
print(f"[deploy] Table prefix  : {table_prefix}")
print(f"[deploy] Job principal : {job_principal or '(not set — grants to it skipped)'}")
print(f"[deploy] Reader group  : {reader_group or '(none)'}")
print(f"[deploy] Apply grants  : {apply_grants}")

# COMMAND ----------

# DBTITLE 1,Create schema (idempotent)
# Binding an experiment to a UC trace location does NOT create the schema — it
# must exist first. Requires USE CATALOG on the catalog and CREATE SCHEMA on it.
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {schema_q}
    COMMENT 'MLflow 3 trace location (OTel Delta tables) for the Data Quality Agent — src/jobs/dq_agent_langgraph.py'
""")
print(f"[deploy] Schema ensured: {schema_q}")

display(spark.sql(f"DESCRIBE SCHEMA EXTENDED {schema_q}"))

# COMMAND ----------

# DBTITLE 1,Grant schema-level privileges to the job principal
# The job run-as principal needs USE CATALOG + USE SCHEMA to reach the schema,
# and CREATE TABLE so MLflow can create the four _otel_* trace tables on first run.
if apply_grants and job_principal:
    spark.sql(f"GRANT USE CATALOG ON CATALOG {cat_q} TO `{job_principal}`")
    spark.sql(f"GRANT USE SCHEMA  ON SCHEMA  {schema_q} TO `{job_principal}`")
    spark.sql(f"GRANT CREATE TABLE ON SCHEMA {schema_q} TO `{job_principal}`")
    print(f"[deploy] Granted USE CATALOG / USE SCHEMA / CREATE TABLE to {job_principal}")
else:
    print("[deploy] Skipped schema-level grants (apply_grants is false or job_principal empty).")

# COMMAND ----------

# DBTITLE 1,Reader group (optional) — USE access on catalog/schema
if apply_grants and reader_group:
    spark.sql(f"GRANT USE CATALOG ON CATALOG {cat_q} TO `{reader_group}`")
    spark.sql(f"GRANT USE SCHEMA  ON SCHEMA  {schema_q} TO `{reader_group}`")
    print(f"[deploy] Granted USE CATALOG / USE SCHEMA to reader group {reader_group}")
else:
    print("[deploy] No reader group grants applied.")

# COMMAND ----------

# DBTITLE 1,Grant table-level MODIFY + SELECT on the four _otel_* trace tables
# IMPORTANT: ALL_PRIVILEGES is NOT sufficient for UC trace tables — MODIFY and
# SELECT must be granted EXPLICITLY (per Databricks MLflow 3 docs).
#
# These tables are created by MLflow on the FIRST agent run, not by this script.
# Re-run this notebook AFTER the first DQ agent run so the grants attach to the
# now-existing tables. Rows for tables that don't exist yet are reported and skipped.
_otel_tables = [
    f"{table_prefix}_otel_spans",
    f"{table_prefix}_otel_logs",
    f"{table_prefix}_otel_metrics",
    f"{table_prefix}_otel_annotations",
]

def _table_exists(tbl: str) -> bool:
    return spark.catalog.tableExists(f"{catalog}.{schema}.{tbl}")

if apply_grants and job_principal:
    for tbl in _otel_tables:
        fq = f"{schema_q}.`{tbl}`"
        if _table_exists(tbl):
            spark.sql(f"GRANT MODIFY ON TABLE {fq} TO `{job_principal}`")
            spark.sql(f"GRANT SELECT ON TABLE {fq} TO `{job_principal}`")
            print(f"[deploy] Granted MODIFY+SELECT on {fq} to {job_principal}")
            if reader_group:
                spark.sql(f"GRANT SELECT ON TABLE {fq} TO `{reader_group}`")
                print(f"[deploy] Granted SELECT on {fq} to {reader_group}")
        else:
            print(f"[deploy] NOTE: {fq} does not exist yet "
                  f"(MLflow creates it on first agent run) — re-run this cell afterwards.")
else:
    print("[deploy] Skipped table-level grants (apply_grants is false or job_principal empty).")

# COMMAND ----------

# DBTITLE 1,Verify
print("[deploy] Schema present:")
display(spark.sql(
    f"SELECT '{catalog}' AS catalog, '{schema}' AS schema, "
    f"(SELECT COUNT(*) FROM {cat_q}.information_schema.schemata "
    f" WHERE schema_name = '{schema}') AS schema_exists"
))

print("[deploy] Tables currently in schema (empty until first agent run):")
display(spark.sql(f"SHOW TABLES IN {schema_q}"))

print("[deploy] Current grants on schema:")
display(spark.sql(f"SHOW GRANTS ON SCHEMA {schema_q}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Follow-up: wire the DQ agent to this schema
# MAGIC
# MAGIC The deployment above only provisions the schema + grants. The DQ agent
# MAGIC notebook (`src/jobs/dq_agent_langgraph.py`) must bind its experiment to this
# MAGIC UC trace location. Because the binding is immutable and set at experiment
# MAGIC **creation** time, use a NEW experiment name for the UC-backed traces (an
# MAGIC existing experiment created on the default backend cannot be re-bound).
# MAGIC
# MAGIC ```python
# MAGIC import os, mlflow
# MAGIC from mlflow.entities.trace_location import UnityCatalog
# MAGIC
# MAGIC mlflow.set_tracking_uri("databricks")
# MAGIC # Prod SQL warehouse (write + UI read of UC traces):
# MAGIC os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = "818510884d309018"
# MAGIC
# MAGIC mlflow.set_experiment(
# MAGIC     experiment_name=f"/Shared/dq_agent/{env_prefix}/dq_agent_uc",
# MAGIC     trace_location=UnityCatalog(
# MAGIC         catalog_name=f"{env_prefix}-cdh",
# MAGIC         schema_name="dqagenttrace",
# MAGIC         table_prefix="dq_agent",
# MAGIC     ),
# MAGIC )
# MAGIC ```
