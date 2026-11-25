# Databricks notebook source
# MAGIC %md
# MAGIC # Daily PROD → QA table refresh — DEEP CLONE (post-migration)
# MAGIC
# MAGIC Variant of the daily refresh that uses **DEEP CLONE** instead of shallow.
# MAGIC Deep clone copies the data files into QA storage, so QA is fully isolated
# MAGIC from production/DR storage: no cross-region reads, no egress on every QA
# MAGIC read, no failover fragility, and VACUUM on production is not blocked.
# MAGIC
# MAGIC Deep clone is **incremental by design** — running it daily copies only the
# MAGIC files that changed since last time (source/target transaction-log diff).
# MAGIC
# MAGIC This notebook:
# MAGIC
# MAGIC 1. Discovers PROD tables in the source schemas (no hardcoded list).
# MAGIC 2. **Skip-if-unchanged:** before cloning, compares the PROD table's current
# MAGIC    Delta version against the `source_version` of the last successful run in
# MAGIC    the audit table. If unchanged, the table is skipped (`SKIPPED_UNCHANGED`)
# MAGIC    — no clone cost at all.
# MAGIC 3. Otherwise refresh/create with `CREATE OR REPLACE TABLE ... DEEP CLONE`.
# MAGIC    If QA still has a VIEW there (unexpected drift), it is NOT touched —
# MAGIC    flagged `NEEDS_ATTENTION`.
# MAGIC 4. If a clone fails specifically due to a column-mapping-mode change
# MAGIC    (`DELTA_UNSUPPORTED_COLUMN_MAPPING_MODE_CHANGE`), it falls back to
# MAGIC    `DROP TABLE` + re-clone (a full copy — unavoidable for that case) and
# MAGIC    marks the row `SUCCESS_AFTER_DROP`. Any other error stays `FAILED`
# MAGIC    without dropping anything.
# MAGIC 5. Writes one audit row per table immediately (not batched).
# MAGIC 6. Continues past individual failures; summarizes at the end.
# MAGIC 7. Sanity-checks that `qa-cdh.primary.*` / `qa-cdh.derivative.*` still compile.
# MAGIC 8. Syncs PROD view definitions into QA (`primary` / `derivative`),
# MAGIC    using ALTER VIEW for existing views (preserves owner/grants). PROD is read-only.
# MAGIC 9. Exits once at the very end.
# MAGIC
# MAGIC **First run = full copy** (slow, uses QA storage). Subsequent runs are fast
# MAGIC (only deltas, plus unchanged tables skipped outright). Ideally run the first
# MAGIC full copy while production is in its primary region.

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("source_catalog", "p-cdh", "Source catalog")
dbutils.widgets.text("target_catalog", "qa-cdh", "Target catalog")
dbutils.widgets.text(
    "schemas",
    "primaryactive,derivativeactive,reference,derivativearchive,primaryarchive,derivativeinternal",
    "Schemas (comma separated)",
)
dbutils.widgets.text("audit_table", "qa-cdh.ops.daily_refresh_audit_deepclone", "Audit table (3-part name)")
dbutils.widgets.text("parallelism", "4", "Parallel workers")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Dry run")
dbutils.widgets.dropdown("skip_unchanged", "true", ["true", "false"], "Skip tables whose source version is unchanged")
dbutils.widgets.text("expected_table_count", "265", "Expected table count (0 = skip check)")
dbutils.widgets.text("only_tables", "", "Optional filter: schema.table,schema.table")

SRC_CATALOG = dbutils.widgets.get("source_catalog").strip()
TGT_CATALOG = dbutils.widgets.get("target_catalog").strip()
SCHEMAS = [s.strip() for s in dbutils.widgets.get("schemas").split(",") if s.strip()]
AUDIT_TABLE = dbutils.widgets.get("audit_table").strip()
PARALLELISM = max(1, int(dbutils.widgets.get("parallelism")))
DRY_RUN = dbutils.widgets.get("dry_run") == "true"
SKIP_UNCHANGED = dbutils.widgets.get("skip_unchanged") == "true"
EXPECTED_COUNT = int(dbutils.widgets.get("expected_table_count"))
ONLY_TABLES = {t.strip().lower() for t in dbutils.widgets.get("only_tables").split(",") if t.strip()}

CLONEABLE_SOURCE_TYPES = {"MANAGED", "EXTERNAL"}
TABLE_TYPES = {"MANAGED", "EXTERNAL", "MANAGED_SHALLOW_CLONE"}
VIEW_TYPES = {"VIEW", "MATERIALIZED_VIEW"}

print(f"source         : {SRC_CATALOG}")
print(f"target         : {TGT_CATALOG}")
print(f"schemas        : {SCHEMAS}")
print(f"dry_run        : {DRY_RUN}")
print(f"skip_unchanged : {SKIP_UNCHANGED}")
print(f"parallelism    : {PARALLELISM}")

# COMMAND ----------

# MAGIC %md ## 2. Helpers

# COMMAND ----------

import time
import uuid
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, LongType, DoubleType, TimestampType
)

RUN_ID = str(uuid.uuid4())
RUN_STARTED_AT = datetime.utcnow()


def qi(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def fq(*parts: str) -> str:
    return ".".join(qi(p) for p in parts)


def split3(name: str):
    parts = [p.strip().strip("`") for p in name.split(".")]
    if len(parts) != 3:
        raise ValueError(f"Expected a 3-part name, got: {name}")
    return parts


def current_delta_version(table_fq: str):
    """Latest committed Delta version of a table (max version in its history)."""
    try:
        row = spark.sql(f"DESCRIBE HISTORY {table_fq} LIMIT 1").collect()[0]
        return int(row["version"])
    except Exception:
        return None

# COMMAND ----------

# MAGIC %md ## 3. Discover PROD tables + current QA state

# COMMAND ----------

schema_list = ", ".join("'" + s.replace("'", "''") + "'" for s in SCHEMAS)

src_rows = spark.sql(f"""
    SELECT table_schema, table_name, table_type
    FROM {qi(SRC_CATALOG)}.information_schema.tables
    WHERE table_schema IN ({schema_list})
""").collect()

source_tables = []
skipped_sources = []
for r in src_rows:
    key = f"{r.table_schema}.{r.table_name}".lower()
    if ONLY_TABLES and key not in ONLY_TABLES:
        continue
    if (r.table_type or "").upper() in CLONEABLE_SOURCE_TYPES:
        source_tables.append((r.table_schema, r.table_name))
    else:
        skipped_sources.append((r.table_schema, r.table_name, r.table_type))

source_tables.sort()
print(f"Discovered {len(source_tables)} cloneable PROD tables.")
if skipped_sources:
    print(f"Skipped {len(skipped_sources)} non-cloneable source objects:")
    for sc, tb, tt in skipped_sources:
        print(f"  {sc}.{tb} [{tt}]")

if EXPECTED_COUNT and not ONLY_TABLES and len(source_tables) != EXPECTED_COUNT:
    print(f"\n*** NOTE: expected {EXPECTED_COUNT}, discovered {len(source_tables)}. "
          f"PROD table list changed since last check — not necessarily a problem, "
          f"just worth knowing. ***")

tgt_rows = spark.sql(f"""
    SELECT table_schema, table_name, table_type
    FROM {qi(TGT_CATALOG)}.information_schema.tables
    WHERE table_schema IN ({schema_list})
""").collect()
target_types = {(r.table_schema.lower(), r.table_name.lower()): (r.table_type or "").upper() for r in tgt_rows}

to_refresh, to_create, needs_attention = [], [], []
for sc, tb in source_tables:
    existing = target_types.get((sc.lower(), tb.lower()))
    if existing in TABLE_TYPES:
        to_refresh.append((sc, tb))
    elif existing is None:
        to_create.append((sc, tb))
    elif existing in VIEW_TYPES:
        needs_attention.append((sc, tb, existing))
    else:
        needs_attention.append((sc, tb, existing))

print(f"\nTo refresh (already tables): {len(to_refresh)}")
print(f"To create (new in PROD, missing in QA): {len(to_create)}")
print(f"Needs attention (unexpected type, NOT touched): {len(needs_attention)}")
for sc, tb, t in needs_attention:
    print(f"  {sc}.{tb} is currently {t} — skipped, review manually")

# COMMAND ----------

# MAGIC %md ## 4. Audit table + last-known source versions
# MAGIC
# MAGIC The audit table doubles as the skip-if-unchanged state: for each table we
# MAGIC read the `source_version` of its most recent successful run and compare it
# MAGIC to the PROD table's current Delta version.

# COMMAND ----------

audit_cat, audit_schema, audit_name = split3(AUDIT_TABLE)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq(audit_cat, audit_schema)}")
AUDIT_FQ = fq(audit_cat, audit_schema, audit_name)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_FQ} (
    run_id            STRING,
    run_started_at    TIMESTAMP,
    table_schema      STRING,
    table_name        STRING,
    action            STRING,
    status            STRING,
    source_version    LONG,
    duration_seconds  DOUBLE,
    dry_run           BOOLEAN,
    error_message     STRING,
    finished_at       TIMESTAMP
) USING DELTA
""")
print(f"audit table: {AUDIT_FQ}")
print(f"run_id: {RUN_ID}")

# Last successfully-synced source_version per table (only real clone successes
# count as a baseline — skips do not advance it on their own, but they also do
# not need to since they carry the same version forward).
LAST_SYNCED_VERSION = {}
if SKIP_UNCHANGED:
    try:
        rows = spark.sql(f"""
            WITH ok AS (
                SELECT table_schema, table_name, source_version, finished_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY table_schema, table_name
                           ORDER BY finished_at DESC
                       ) rn
                FROM {AUDIT_FQ}
                WHERE status IN ('SUCCESS', 'SUCCESS_AFTER_DROP', 'SKIPPED_UNCHANGED')
                  AND dry_run = false
                  AND source_version IS NOT NULL
            )
            SELECT table_schema, table_name, source_version FROM ok WHERE rn = 1
        """).collect()
        LAST_SYNCED_VERSION = {(r.table_schema, r.table_name): r.source_version for r in rows}
        print(f"Loaded last-synced versions for {len(LAST_SYNCED_VERSION)} tables.")
    except Exception as e:
        print(f"(could not load prior versions, will clone everything: {e})")

# COMMAND ----------

# MAGIC %md ## 5. Per-table refresh (DEEP CLONE, skip-if-unchanged, immediate audit)

# COMMAND ----------

AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("run_started_at", TimestampType()),
    StructField("table_schema", StringType()),
    StructField("table_name", StringType()),
    StructField("action", StringType()),
    StructField("status", StringType()),
    StructField("source_version", LongType()),
    StructField("duration_seconds", DoubleType()),
    StructField("dry_run", BooleanType()),
    StructField("error_message", StringType()),
    StructField("finished_at", TimestampType()),
])

# Errors that CREATE OR REPLACE cannot bridge in-place but DROP + re-clone can
# (the fresh table inherits the source table's properties / column-mapping mode).
# NOTE: with DEEP CLONE this fallback re-copies the whole table — rare (only when
# the source changes column-mapping mode), so the one-off full copy is acceptable.
DROP_RETRY_MARKERS = (
    "DELTA_UNSUPPORTED_COLUMN_MAPPING_MODE_CHANGE",
    "DELTA_UNSUPPORTED_COLUMN_MAPPING",
    "column mapping mode",
    "Changing column mapping",
)


def write_audit_row(rec: dict):
    spark.createDataFrame([Row(**rec)], schema=AUDIT_SCHEMA).write.mode("append").saveAsTable(AUDIT_FQ)


def clone_source_version(target_fq: str):
    """The source version recorded by the clone operation on the target."""
    try:
        row = spark.sql(f"DESCRIBE HISTORY {target_fq} LIMIT 1").collect()[0]
        v = (row["operationParameters"] or {}).get("sourceVersion")
        return int(v) if v is not None else None
    except Exception:
        return None


def process_table(schema: str, table: str, action: str) -> dict:
    started = time.time()
    src = fq(SRC_CATALOG, schema, table)
    tgt = fq(TGT_CATALOG, schema, table)

    rec = {
        "run_id": RUN_ID, "run_started_at": RUN_STARTED_AT,
        "table_schema": schema, "table_name": table, "action": action,
        "status": "UNKNOWN", "source_version": None, "duration_seconds": None,
        "dry_run": DRY_RUN, "error_message": None, "finished_at": None,
    }

    def finish(status, error=None, version=None):
        rec["status"] = status
        rec["error_message"] = error[:8000] if error else None
        rec["source_version"] = version
        rec["duration_seconds"] = round(time.time() - started, 3)
        rec["finished_at"] = datetime.utcnow()
        write_audit_row(rec)
        return rec

    if DRY_RUN:
        return finish("DRY_RUN")

    # --- Skip-if-unchanged: compare PROD current version to last synced version.
    src_version = current_delta_version(src)
    if (
        SKIP_UNCHANGED
        and action == "REFRESH"                       # only skip existing QA tables
        and src_version is not None
        and LAST_SYNCED_VERSION.get((schema, table)) == src_version
    ):
        # Source unchanged since last successful sync — nothing to copy.
        return finish("SKIPPED_UNCHANGED", version=src_version)

    def _clone():
        spark.sql(f"CREATE OR REPLACE TABLE {tgt} DEEP CLONE {src}")

    try:
        _clone()
        return finish("SUCCESS", version=clone_source_version(tgt))
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        # Targeted fallback: only for known column-mapping incompatibilities.
        if any(marker in msg for marker in DROP_RETRY_MARKERS):
            try:
                # tgt is always the QA target (fq(TGT_CATALOG, ...)); PROD is never dropped.
                spark.sql(f"DROP TABLE IF EXISTS {tgt}")
                _clone()
                return finish("SUCCESS_AFTER_DROP", version=clone_source_version(tgt))
            except Exception as e2:
                return finish(
                    "FAILED",
                    error=f"drop+retry failed: {type(e2).__name__}: {e2}\n{traceback.format_exc()}",
                )
        # Any other error: do NOT drop, just record it.
        return finish("FAILED", error=f"{msg}\n{traceback.format_exc()}")

# COMMAND ----------

# MAGIC %md ## 6. Run

# COMMAND ----------

wall_start = time.time()
results = []
work_items = [(sc, tb, "REFRESH") for sc, tb in to_refresh] + [(sc, tb, "CREATE") for sc, tb in to_create]

if not work_items:
    print("Nothing to refresh or create.")
else:
    with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
        futures = [pool.submit(process_table, sc, tb, action) for sc, tb, action in work_items]
        for i, f in enumerate(futures, start=1):
            rec = f.result()
            results.append(rec)
            marker = {
                "SUCCESS": "ok   ",
                "SUCCESS_AFTER_DROP": "ok*  ",
                "SKIPPED_UNCHANGED": "skip ",
                "FAILED": "FAIL ",
                "DRY_RUN": "dry  ",
            }.get(rec["status"], "---- ")
            print(f"[{i:>3}/{len(futures)}] {marker} {rec['action']:<7} "
                  f"{rec['table_schema']}.{rec['table_name']} v={rec['source_version']} "
                  f"{rec['duration_seconds']}s")

wall_seconds = round(time.time() - wall_start, 2)
print(f"\nWall clock: {wall_seconds}s. Audit rows already written incrementally to {AUDIT_FQ}.")

# COMMAND ----------

# MAGIC %md ## 7. Summary

# COMMAND ----------

from collections import Counter

counts = Counter(r["status"] for r in results)
failed = [r for r in results if r["status"] == "FAILED"]
dropped = [r for r in results if r["status"] == "SUCCESS_AFTER_DROP"]
skipped_unchanged = [r for r in results if r["status"] == "SKIPPED_UNCHANGED"]

print("=" * 70)
print(f"RUN {RUN_ID}  dry_run={DRY_RUN}  duration={wall_seconds}s  (DEEP CLONE)")
print(f"Refreshed (existing tables) : {len(to_refresh)}")
print(f"Created (new in PROD)       : {len(to_create)}")
print(f"Skipped (source unchanged)  : {len(skipped_unchanged)}")
print(f"Needs attention (skipped)   : {len(needs_attention)}")
for status, n in sorted(counts.items()):
    print(f"  {status:<20} {n}")
print("=" * 70)

if dropped:
    print("\nRecreated via DROP + deep clone (source changed column-mapping mode):")
    for r in dropped:
        print(f"  {r['table_schema']}.{r['table_name']}")

if needs_attention:
    print("\nSkipped — unexpected type in QA, review manually (not auto-touched):")
    for sc, tb, t in needs_attention:
        print(f"  {sc}.{tb} is {t}")

if failed:
    print("\nFAILURES:")
    for r in failed:
        first_line = (r["error_message"] or "").split("\n")[0]
        print(f"  {r['table_schema']}.{r['table_name']}: {first_line}")

# COMMAND ----------

# MAGIC %md ## 8. Sanity check — do the logical `primary` / `derivative` views still compile?

# COMMAND ----------

DEPENDENT_SCHEMAS = ["primary", "derivative"]
broken_dependent = []

if DRY_RUN:
    print("Dry run — skipping dependent view check.")
else:
    dep_rows = spark.sql(f"""
        SELECT table_schema, table_name
        FROM {qi(TGT_CATALOG)}.information_schema.tables
        WHERE table_schema IN ({", ".join("'" + s + "'" for s in DEPENDENT_SCHEMAS)})
          AND table_type = 'VIEW'
    """).collect()

    for r in dep_rows:
        v = fq(TGT_CATALOG, r.table_schema, r.table_name)
        try:
            spark.sql(f"SELECT * FROM {v} LIMIT 0").collect()
        except Exception as e:
            broken_dependent.append((r.table_schema, r.table_name, str(e).split("\n")[0]))

    print(f"Checked {len(dep_rows)} views in {DEPENDENT_SCHEMAS}.")
    if broken_dependent:
        print(f"\n*** {len(broken_dependent)} dependent views no longer resolve: ***")
        for sc, tb, err in broken_dependent:
            print(f"  {sc}.{tb}: {err}")
    else:
        print("All dependent views still resolve.")

# COMMAND ----------

# MAGIC %md ## 9. Sync view definitions: PROD (p-cdh) → QA (qa-cdh)
# MAGIC
# MAGIC Driven by PROD. For each PROD view in `primary` / `derivative`: read
# MAGIC (read-only) its definition, rewrite `p-cdh` → `qa-cdh`, and apply to QA
# MAGIC with ALTER VIEW (existing → preserves owner/grants) or CREATE VIEW (new).
# MAGIC
# MAGIC *** SAFETY: PROD (p-cdh) is only read. No writes to p-cdh. ***
# MAGIC
# MAGIC Uses the notebook-level `DRY_RUN` (widget) so a dry run writes nothing.

# COMMAND ----------

import re
from pyspark.sql import functions as F, types as T

PROD_CATALOG = SRC_CATALOG   # p-cdh
QA_CATALOG = TGT_CATALOG     # qa-cdh
VIEW_SYNC_SCHEMAS = ["primary", "derivative"]


# ----------------------------------------------------------------------
# HARD SAFETY GUARD: every write statement must target the QA target and
# never p-cdh.
# ----------------------------------------------------------------------
def assert_qa_only_write(sql_text: str, qa_target_fq: str):
    head = sql_text.lstrip()[:80].lower()
    is_write = head.startswith(("create", "alter", "drop", "replace", "insert",
                                "update", "delete", "truncate", "merge"))
    if not is_write:
        return
    if qa_target_fq not in sql_text:
        raise RuntimeError(f"BLOCKED: write χωρίς το QA target {qa_target_fq}")
    if re.search(r"(?i)`?p-cdh`?\s*\.", sql_text[:120]):
        raise RuntimeError("BLOCKED: write statement φαίνεται να στοχεύει p-cdh")


def to_qa(text: str) -> str:
    """Αντικαθιστά όλα τα p-cdh references (case-insensitive) με qa-cdh."""
    if text is None:
        return None
    text = re.sub(r"`[pP]-cdh`", "`qa-cdh`", text)   # `p-cdh` / `P-cdh`
    text = re.sub(r"(?i)\bp-cdh\b", "qa-cdh", text)  # χωρίς backticks
    text = re.sub(r"`QA-cdh`", "`qa-cdh`", text)     # κεφαλαία QA-cdh
    text = re.sub(r"(?i)\bqa-cdh\b", "qa-cdh", text)
    return text


view_schema_in = ", ".join(f"'{s}'" for s in VIEW_SYNC_SCHEMAS)

# 1. Driver = ALL PROD views of the schemas (read-only)
prod_views = spark.sql(f"""
    SELECT v.table_schema, v.table_name, v.view_definition
    FROM system.information_schema.views v
    JOIN system.information_schema.tables t
      ON  t.table_catalog = v.table_catalog
      AND t.table_schema  = v.table_schema
      AND t.table_name    = v.table_name
    WHERE v.table_catalog = '{PROD_CATALOG}'
      AND v.table_schema IN ({view_schema_in})
      AND t.table_type = 'VIEW'
    ORDER BY v.table_schema, v.table_name
""").collect()

print(f"Βρέθηκαν {len(prod_views)} PROD views στα schemas {VIEW_SYNC_SCHEMAS}.\n")

# 2. Which QA views already exist (CREATE vs UPDATE in the report)
qa_existing = {
    (r["table_schema"], r["table_name"])
    for r in spark.sql(f"""
        SELECT table_schema, table_name
        FROM system.information_schema.tables
        WHERE table_catalog = '{QA_CATALOG}'
          AND table_schema IN ({view_schema_in})
          AND table_type = 'VIEW'
    """).collect()
}
print(f"Υπάρχουν ήδη {len(qa_existing)} QA views στα ίδια schemas.\n")

# 3. For each PROD view -> rewrite -> ALTER VIEW (existing, preserves owner/grants)
#    or CREATE VIEW (new); fall back to CREATE OR REPLACE only if ALTER fails.
view_status_records = []

for row in prod_views:
    sch = row["table_schema"]
    name = row["table_name"]
    qa_view_fq = f"`{QA_CATALOG}`.`{sch}`.`{name}`"
    prod_view_fq = f"`{PROD_CATALOG}`.`{sch}`.`{name}`"

    exists = (sch, name) in qa_existing
    action = "UPDATE" if exists else "CREATE"
    prod_body = row["view_definition"]

    try:
        # Fallback: if the definition is missing from information_schema,
        # pull it from SHOW CREATE (PROD, read-only).
        if not prod_body or not str(prod_body).strip():
            ddl = spark.sql(f"SHOW CREATE TABLE {prod_view_fq}").collect()[0][0]  # READ-ONLY
            ddl = ddl.rstrip().rstrip(";")
            m = re.search(r"(?is)\bAS\b\s*(.*)\Z", ddl)
            if not m:
                raise ValueError("Could not parse SELECT body from PROD SHOW CREATE TABLE")
            prod_body = m.group(1).strip()

        prod_body = prod_body.rstrip().rstrip(";")

        # references p-cdh -> qa-cdh
        qa_body = to_qa(prod_body)
        if re.search(r"(?i)`?p-cdh`?", qa_body):
            raise RuntimeError("BLOCKED: εναπόμεινε p-cdh reference μετά τη μετατροπή")

        # Existing QA view -> ALTER VIEW ... AS (preserves owner & grants).
        # New view -> CREATE VIEW. If ALTER fails (e.g. column set changed),
        # fall back to CREATE OR REPLACE so the sync still succeeds.
        if exists:
            primary_sql = f"ALTER VIEW {qa_view_fq} AS {qa_body}"
        else:
            primary_sql = f"CREATE VIEW {qa_view_fq} AS {qa_body}"
        fallback_sql = f"CREATE OR REPLACE VIEW {qa_view_fq} AS {qa_body}"

        # SAFETY — both candidate statements must target QA only, never p-cdh
        assert_qa_only_write(primary_sql, qa_view_fq)
        assert_qa_only_write(fallback_sql, qa_view_fq)

        if DRY_RUN:
            print(f"[DRY_RUN][{action}] {qa_view_fq}\n{primary_sql}\n")
            view_status_records.append((sch, name, action, "WOULD_APPLY", None))
        else:
            try:
                spark.sql(primary_sql)
                applied_status = "SUCCESS"
            except Exception as e_primary:
                spark.sql(fallback_sql)
                applied_status = "SUCCESS_VIA_REPLACE"
                print(f"  note: {sch}.{name} fell back to CREATE OR REPLACE "
                      f"({type(e_primary).__name__})")
            spark.sql(f"REFRESH TABLE {qa_view_fq}")
            view_status_records.append((sch, name, action, applied_status, None))
            print(f"OK [{action}]  {qa_view_fq}  ({applied_status})")

    except Exception as e:
        view_status_records.append((sch, name, action, "FAILED", str(e)[:2000]))
        print(f"FAILED [{action}]  {qa_view_fq}\n         {repr(e)}")

# 4. Report
view_status_schema = T.StructType([
    T.StructField("schema", T.StringType()),
    T.StructField("view_name", T.StringType()),
    T.StructField("action", T.StringType()),       # CREATE / UPDATE
    T.StructField("sync_status", T.StringType()),   # SUCCESS / SUCCESS_VIA_REPLACE / FAILED / WOULD_APPLY
    T.StructField("error", T.StringType()),
])

view_status_df = spark.createDataFrame(view_status_records, schema=view_status_schema)

OK_STATUSES = ["SUCCESS", "SUCCESS_VIA_REPLACE", "WOULD_APPLY"]
view_total = view_status_df.count()
view_ok = view_status_df.filter(F.col("sync_status").isin(OK_STATUSES)).count()
view_failed = view_status_df.filter(F.col("sync_status") == "FAILED").count()
view_created = view_status_df.filter((F.col("action") == "CREATE") &
                                     F.col("sync_status").isin(OK_STATUSES)).count()
view_updated = view_status_df.filter((F.col("action") == "UPDATE") &
                                     F.col("sync_status").isin(OK_STATUSES)).count()

print(f"\n==== SUMMARY (PROD -> QA view sync) ====")
print(f"Mode      : {'DRY_RUN' if DRY_RUN else 'APPLY'}")
print(f"Total PROD: {view_total}")
print(f"OK        : {view_ok}  (created: {view_created}, updated: {view_updated})")
print(f"Failed    : {view_failed}")

display(view_status_df.orderBy("sync_status", "action", "schema", "view_name"))

# COMMAND ----------

# MAGIC %md ## 10. Exit value (for job alerting)
# MAGIC
# MAGIC Clean exit on purpose — the job stays green even when individual tables or
# MAGIC views fail (alerting is handled at the job level off this JSON).

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps({
    "run_id": RUN_ID,
    "clone_type": "DEEP",
    "dry_run": DRY_RUN,
    "refreshed": len(to_refresh),
    "created": len(to_create),
    "skipped_unchanged": len(skipped_unchanged),
    "needs_attention": len(needs_attention),
    "succeeded": counts.get("SUCCESS", 0) + counts.get("SUCCESS_AFTER_DROP", 0),
    "succeeded_after_drop": counts.get("SUCCESS_AFTER_DROP", 0),
    "failed": counts.get("FAILED", 0),
    "failed_tables": [f"{r['table_schema']}.{r['table_name']}" for r in failed],
    "dependent_views_broken": len(broken_dependent),
    "views_synced_ok": view_ok,
    "views_synced_failed": view_failed,
    "duration_seconds": wall_seconds,
}))
