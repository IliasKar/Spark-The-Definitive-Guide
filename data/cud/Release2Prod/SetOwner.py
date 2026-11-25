# Databricks notebook source
# MAGIC %md
# MAGIC # Ownership check & fix — p-cdh & p-cdh-delivery
# MAGIC
# MAGIC Ensures every object (catalogs, schemas, tables, views, materialized views)
# MAGIC in both catalogs is owned by the Service_Principal. Idempotent — safe to
# MAGIC run repeatedly (as part of a recurring job). Functions are explicitly
# MAGIC excluded (handled separately elsewhere).
# MAGIC
# MAGIC `dry_run=true` shows what would change without altering anything.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalogs", "p-cdh,p-cdh-delivery", "Catalogs (comma separated)")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run")

SERVICE_PRINCIPAL_APP_ID = "00f3ea27-8236-4f6a-8f9c-177668555720"
CATALOGS = [c.strip() for c in dbutils.widgets.get("catalogs").split(",") if c.strip()]
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

EXCLUDED_SCHEMAS = {"information_schema"}
# Per-catalog schema exclusions (catalog -> set of schema names to skip entirely)
EXCLUDED_SCHEMAS_BY_CATALOG = {
    "p-cdh": {"admin", "billing"},
}

print(f"catalogs: {CATALOGS}")
print(f"dry_run : {DRY_RUN}")

# COMMAND ----------

# MAGIC %md ## Helpers

# COMMAND ----------

def qi(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"

def fq(*parts: str) -> str:
    return ".".join(qi(p) for p in parts)

SP = f"`{SERVICE_PRINCIPAL_APP_ID}`"

results = {"catalog": [], "schema": [], "table": []}

def apply_owner(level, fqn, ddl_kind, current_owner, dry_run):
    """ddl_kind: 'CATALOG' | 'SCHEMA' | 'TABLE' | 'VIEW'. Returns status string."""
    if current_owner == SERVICE_PRINCIPAL_APP_ID:
        return "ALREADY_OK"
    if dry_run:
        return "WOULD_FIX"
    try:
        spark.sql(f"ALTER {ddl_kind} {fqn} OWNER TO {SP}")
        return "FIXED"
    except Exception as e:
        return f"ERROR: {type(e).__name__}: {e}"

# COMMAND ----------

# MAGIC %md ## 1. Catalogs

# COMMAND ----------

for cat in CATALOGS:
    fqn = qi(cat)
    try:
        rows = spark.sql(f"DESCRIBE CATALOG EXTENDED {fqn}").collect()
        owner = None
        for r in rows:
            if r[0] and r[0].strip().lower() == "owner":
                owner = r[1].strip()
                break
    except Exception as e:
        owner = None
        print(f"{cat}: could not read owner: {e}")

    status = apply_owner("catalog", fqn, "CATALOG", owner, DRY_RUN)
    results["catalog"].append((cat, owner, status))
    print(f"[catalog] {cat}: owner={owner}  -> {status}")

# COMMAND ----------

# MAGIC %md ## 2. Schemas

# COMMAND ----------

for cat in CATALOGS:
    schemas = spark.sql(f"""
        SELECT schema_name, schema_owner
        FROM {qi(cat)}.information_schema.schemata
    """).collect()

    excluded_here = EXCLUDED_SCHEMAS | EXCLUDED_SCHEMAS_BY_CATALOG.get(cat, set())

    for r in schemas:
        sc = r.schema_name
        if sc in excluded_here:
            continue
        owner = r.schema_owner
        fqn = fq(cat, sc)
        status = apply_owner("schema", fqn, "SCHEMA", owner, DRY_RUN)
        results["schema"].append((f"{cat}.{sc}", owner, status))
        if status not in ("ALREADY_OK",):
            print(f"[schema] {cat}.{sc}: owner={owner}  -> {status}")

print(f"\nSchemas checked: {len(results['schema'])}")

# COMMAND ----------

# MAGIC %md ## 3. Tables / Views / Materialized Views

# COMMAND ----------

VIEW_KIND_TYPES = {"VIEW"}
# Materialized views sometimes require ALTER VIEW, sometimes ALTER TABLE depending
# on runtime version — we try VIEW first and fall back to TABLE on failure.
MV_TYPES = {"MATERIALIZED_VIEW"}

def apply_owner_table_like(fqn, table_type, current_owner, dry_run):
    if current_owner == SERVICE_PRINCIPAL_APP_ID:
        return "ALREADY_OK"
    if dry_run:
        return "WOULD_FIX"

    primary_kind = "VIEW" if table_type in VIEW_KIND_TYPES | MV_TYPES else "TABLE"
    fallback_kind = "TABLE" if primary_kind == "VIEW" else "VIEW"

    try:
        spark.sql(f"ALTER {primary_kind} {fqn} OWNER TO {SP}")
        return "FIXED"
    except Exception as e1:
        try:
            spark.sql(f"ALTER {fallback_kind} {fqn} OWNER TO {SP}")
            return "FIXED (fallback)"
        except Exception as e2:
            return f"ERROR: primary({primary_kind}): {e1} | fallback({fallback_kind}): {e2}"


for cat in CATALOGS:
    tables = spark.sql(f"""
        SELECT table_schema, table_name, table_type, table_owner
        FROM {qi(cat)}.information_schema.tables
        WHERE table_schema NOT IN ({",".join("'" + s + "'" for s in EXCLUDED_SCHEMAS)})
    """).collect()

    for r in tables:
        fqn = fq(cat, r.table_schema, r.table_name)
        status = apply_owner_table_like(fqn, r.table_type, r.table_owner, DRY_RUN)
        results["table"].append((f"{cat}.{r.table_schema}.{r.table_name}", r.table_type, r.table_owner, status))
        if status not in ("ALREADY_OK",):
            print(f"[{r.table_type}] {cat}.{r.table_schema}.{r.table_name}: owner={r.table_owner} -> {status}")

print(f"\nTables/views/MVs checked: {len(results['table'])}")

# COMMAND ----------

# MAGIC %md ## 4. Summary

# COMMAND ----------

from collections import Counter

for level in ["catalog", "schema", "table"]:
    items = results[level]
    statuses = [i[-1] for i in items]
    # normalize error messages to a single bucket for counting
    normalized = [s if not s.startswith("ERROR") else "ERROR" for s in statuses]
    counts = Counter(normalized)
    print(f"--- {level} ({len(items)} total) ---")
    for k, v in counts.items():
        print(f"  {k}: {v}")

errors = [i for i in results["catalog"] + results["schema"] + results["table"] if str(i[-1]).startswith("ERROR")]
if errors:
    print(f"\n{len(errors)} errors — review:")
    for e in errors:
        print(f"  {e}")

if DRY_RUN:
    print("\ndry_run=true — nothing was changed. Set dry_run='false' to apply.")
