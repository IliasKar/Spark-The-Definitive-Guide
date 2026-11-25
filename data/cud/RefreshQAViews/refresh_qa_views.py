# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 1: Change View Owners

# COMMAND ----------

CATALOG = "qa-cdh"
SCHEMAS = (
    'derivativearchive',
    'primaryarchive',
    'derivativeactive',
    'primaryactive',
    'derivative',
    'primary',
    'reference'
)
SP_APP_ID = "00f3ea27-8236-4f6a-8f9c-177668555720"

schemas_sql_list = ",".join(f"'{s}'" for s in SCHEMAS)

print("=" * 60)
print("Owner change σε VIEWS")
print("=" * 60)

views_df = spark.sql(f"""
    SELECT table_catalog, table_schema, table_name
    FROM system.information_schema.views
    WHERE table_catalog = '{CATALOG}'
      AND table_schema IN ({schemas_sql_list})
""")

view_owner_failed = []
view_owner_ok = 0

for r in views_df.toLocalIterator():
    fq = f"`{r['table_catalog']}`.`{r['table_schema']}`.`{r['table_name']}`"
    try:
        spark.sql(f"ALTER VIEW {fq} SET OWNER TO `{SP_APP_ID}`")
        view_owner_ok += 1
    except Exception as e:
        view_owner_failed.append((fq, str(e)))
        print(f"   FAIL owner (view): {fq} -> {str(e)[:150]}")

print(f"\nViews owner OK: {view_owner_ok} | FAILED: {len(view_owner_failed)}")

if view_owner_failed:
    print(f"\nViews που ΔΕΝ άλλαξαν owner ({len(view_owner_failed)}):")
    for fq, err in view_owner_failed:
        print(f"  - {fq}")

# Αν θες το Job να ΣΤΑΜΑΤΗΣΕΙ σε αποτυχίες, ξεσχολίασε την επόμενη γραμμή:
# if view_owner_failed:
#     dbutils.notebook.exit("FAILED")

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 2: Change Function Owners

# COMMAND ----------

CATALOG = "qa-cdh"
SCHEMAS = (
    'derivativearchive',
    'primaryarchive',
    'derivativeactive',
    'primaryactive',
    'derivative',
    'primary',
    'reference'
)
SP_APP_ID = "00f3ea27-8236-4f6a-8f9c-177668555720"

schemas_sql_list = ",".join(f"'{s}'" for s in SCHEMAS)

print("=" * 60)
print("Owner change σε FUNCTIONS")
print("=" * 60)

functions_df = spark.sql(f"""
    SELECT routine_catalog, routine_schema, routine_name
    FROM system.information_schema.routines
    WHERE routine_catalog = '{CATALOG}'
      AND routine_schema IN ({schemas_sql_list})
""")

func_owner_failed = []
func_owner_ok = 0

for r in functions_df.toLocalIterator():
    fq = f"`{r['routine_catalog']}`.`{r['routine_schema']}`.`{r['routine_name']}`"
    try:
        spark.sql(f"ALTER FUNCTION {fq} OWNER TO `{SP_APP_ID}`")
        func_owner_ok += 1
    except Exception as e:
        func_owner_failed.append((fq, str(e)))
        print(f"   FAIL owner (function): {fq} -> {str(e)[:150]}")

print(f"\nFunctions owner OK: {func_owner_ok} | FAILED: {len(func_owner_failed)}")

if func_owner_failed:
    print(f"\nFunctions που ΔΕΝ άλλαξαν owner ({len(func_owner_failed)}):")
    for fq, err in func_owner_failed:
        print(f"  - {fq}")

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 3: Refresh Views

# COMMAND ----------

import re

views = spark.sql("""
    SELECT t.table_catalog, t.table_schema, t.table_name, v.view_definition
    FROM system.information_schema.tables t
    JOIN system.information_schema.views v
      ON t.table_catalog = v.table_catalog
     AND t.table_schema  = v.table_schema
     AND t.table_name    = v.table_name
    WHERE t.table_catalog = 'qa-cdh'
      AND t.table_schema IN (
          'derivativearchive','primaryarchive','derivativeactive',
          'primaryactive','derivative','primary','reference'
      )
      AND t.table_type = 'VIEW'
""")

for r in views.toLocalIterator():
    cat, sch, name = r['table_catalog'], r['table_schema'], r['table_name']
    fq = f"`{cat}`.`{sch}`.`{name}`"
    sql_body = r['view_definition']
    try:
        if not sql_body or not str(sql_body).strip():
            ddl = spark.sql(f"SHOW CREATE TABLE {fq}").collect()[0][0]
            ddl = ddl.rstrip().rstrip(";")
            m = re.search(r"(?is)\sAS\s(.*)\Z", ddl)
            if not m:
                raise ValueError("Could not parse SELECT body")
            sql_body = m.group(1).strip()
        sql_body = sql_body.rstrip().rstrip(";")
        spark.sql(f"ALTER VIEW {fq} AS {sql_body}")
        spark.sql(f"REFRESH TABLE {fq}")
        print(f"Recompiled view: {fq}")
    except Exception as e:
        print(f"   Failed to recompile {fq}, reason: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 4: Update Functions (Schema: primary)

# COMMAND ----------

from pyspark.sql import functions as F, types as T
import re

CATALOG = "qa-cdh"
SCHEMA = "primary"

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE `{CATALOG}`.`{SCHEMA}`")


def table_exists(ident: str) -> bool:
    """Check if a table/view exists."""
    try:
        return spark._jsparkSession.catalog().tableExists(ident)
    except Exception:
        return False


def parse_desc_rows(rows):
    """Parse DESCRIBE FUNCTION EXTENDED output into a structured dict."""
    lines = []
    for r in rows:
        cand = next((str(v) for v in r if v is not None and str(v).strip() != ""), None)
        if cand:
            lines.append(cand)

    text = "\n".join(lines)
    language = comment = security = None
    null_handling = None
    return_columns, return_type = [], None
    body_sql_raw = None

    # language / comment / security
    for label, var in [
        ("Language", "language"),
        ("Comment", "comment"),
        ("Security", "security")
    ]:
        m = re.search(fr"(?im)^\s*{label}:\s*(.+)$", text)
        if m:
            locals()[var] = m.group(1).strip()

    # null handling
    m = re.search(r"(?im)^\s*(Returns null on null input|Called on null input).*", text)
    if m:
        null_handling = m.group(0).strip()

    # return type / columns
    returns_idx = None
    for i, ln in enumerate(lines):
        if re.search(r"(?i)^\s*Returns?:", ln):
            returns_idx = i
            break

    if returns_idx is not None:
        same = lines[returns_idx]
        # inline TABLE<...> format
        mtab = re.search(r"(?is)TABLE\s*<\s*(.*?)\s*>", same)
        if mtab:
            inside = mtab.group(1)
            for part in [x.strip() for x in inside.split(",") if x.strip()]:
                mt = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+(.+)$", part)
                if mt:
                    return_columns.append({"name": mt.group(1), "type": mt.group(2).strip()})
            return_type = f"TABLE<{inside}>"
        else:
            # multi-line column list after "Returns:"
            j = returns_idx + 1
            while j < len(lines):
                ln = lines[j].strip()
                if not ln or re.match(
                    r"(?i)^(Language|Owner|Type|Definition|SQL body|Body|Source|Comment|Security):",
                    ln,
                ):
                    break
                mt = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+(.+)$", ln)
                if mt:
                    return_columns.append({"name": mt.group(1), "type": mt.group(2).strip()})
                    j += 1
                    continue
                break
            if return_columns:
                inside = ", ".join([f"{c['name']} {c['type']}" for c in return_columns])
                return_type = f"TABLE<{inside}>"
            else:
                m2 = re.search(r"(?im)^\s*Returns?:\s*(.+)$", text)
                if m2:
                    return_type = m2.group(1).strip()

    # raw SQL body
    m = re.search(r"(?im)^\s*(Definition|SQL body|Body|Source):\s*(.*)$", text)
    if m:
        endpos = m.end(0)
        inline = (m.group(2) or "").rstrip()
        rest = text[endpos:]
        body_sql_raw = (
            inline + ("\n" if rest and not rest.startswith("\n") else "") + rest
        ).strip()

    return {
        "language_raw": language,
        "return_columns": return_columns,
        "return_type_raw": return_type,
        "body_sql_raw": body_sql_raw,
        "comment": comment,
        "security": security,
        "null_handling": null_handling,
    }


# ----------------------------------------------------------------------
# Read all functions for the given schema
# ----------------------------------------------------------------------
routines_ident = f"`{CATALOG}`.`information_schema`.`routines`"

if table_exists(routines_ident):
    funcs = spark.sql(f"""
        SELECT specific_name
        FROM {routines_ident}
        WHERE specific_schema = '{SCHEMA}' AND routine_type = 'FUNCTION'
    """)
    func_names = [r.specific_name for r in funcs.collect()]
else:
    funcs = spark.sql(f"SHOW USER FUNCTIONS IN `{CATALOG}`.`{SCHEMA}`").collect()
    func_names = [row["function"].split(".")[-1] for row in funcs]

# ----------------------------------------------------------------------
# Parse DESCRIBE FUNCTION output (metadata, body, raw return info)
# ----------------------------------------------------------------------
rows = []
for name in func_names:
    try:
        desc = spark.sql(
            f"DESCRIBE FUNCTION EXTENDED `{CATALOG}`.`{SCHEMA}`.`{name}`"
        ).collect()
        meta = parse_desc_rows(desc)
        rows.append((
            CATALOG,
            SCHEMA,
            name,
            meta["return_columns"],
            meta["return_type_raw"],
            meta["language_raw"],
            meta["body_sql_raw"],
            meta["comment"],
            meta["security"],
            meta["null_handling"],
        ))
    except Exception:
        # ignore functions we can't describe
        pass

schema_desc = T.StructType([
    T.StructField("catalog", T.StringType()),
    T.StructField("schema", T.StringType()),
    T.StructField("function_name", T.StringType()),
    T.StructField("return_columns",
                  T.ArrayType(T.StructType([
                      T.StructField("name", T.StringType()),
                      T.StructField("type", T.StringType()),
                  ])),
                  ),
    T.StructField("return_type_raw", T.StringType()),
    T.StructField("language_raw", T.StringType()),
    T.StructField("body_sql_raw", T.StringType()),
    T.StructField("comment", T.StringType()),
    T.StructField("security", T.StringType()),
    T.StructField("null_handling", T.StringType()),
])

df = spark.createDataFrame(rows, schema=schema_desc)

# ----------------------------------------------------------------------
# Get full parameter info from INFORMATION_SCHEMA.PARAMETERS
#   (preserving original parameter order)
#   *** FIX: use FULL_DATA_TYPE to keep DECIMAL(19,2) etc. ***
# ----------------------------------------------------------------------
param_ident = f"`{CATALOG}`.`information_schema`.`parameters`"

params_df = spark.sql(f"""
    SELECT
        specific_name  AS function_name,
        ordinal_position,
        parameter_name AS name,
        COALESCE(full_data_type, data_type) AS type
    FROM {param_ident}
    WHERE specific_schema = '{SCHEMA}'
""")

# Preserve parameter order using ordinal_position
params_grouped = (
    params_df
    .groupBy("function_name")
    .agg(
        F.sort_array(
            F.collect_list(F.struct("ordinal_position", "name", "type"))
        ).alias("parameters_ord")
    )
    .withColumn(
        "parameters",
        F.expr("transform(parameters_ord, x -> named_struct('name', x.name, 'type', x.type))")
    )
    .drop("parameters_ord")
)

df = (
    df.join(params_grouped, "function_name", "left")
    .withColumn("language_value", F.upper(F.coalesce("language_raw", F.lit("SQL"))))
)

# Build "name type" parameter list for CREATE FUNCTION
df = df.withColumn(
    "param_list_sql",
    F.when(
        F.size("parameters") > 0,
        F.expr("array_join(transform(parameters, x -> concat(x.name, ' ', x.type)), ', ')"),
    ).otherwise(F.lit("")),
)

# Dummy argument values to call the function safely
df = df.withColumn(
    "param_call_args_sql",
    F.when(
        F.size("parameters") > 0,
        F.expr("""
            array_join(transform(parameters,
                x -> CASE
                    WHEN upper(x.type) LIKE 'DATE%' THEN concat("DATE '", '2000-01-01', "'")
                    WHEN upper(x.type) LIKE 'TIMESTAMP%' THEN concat("TIMESTAMP '", '2000-01-01 00:00:00', "'")
                    WHEN upper(x.type) LIKE 'STRING%' OR upper(x.type) LIKE 'VARCHAR%' THEN "'X'"
                    WHEN upper(x.type) LIKE 'INT%' OR upper(x.type) LIKE 'BIGINT%' OR upper(x.type) LIKE 'SMALLINT%' OR upper(x.type) LIKE 'TINYINT%' THEN '0'
                    WHEN upper(x.type) LIKE 'DECIMAL%' OR upper(x.type) LIKE 'NUMERIC%' OR upper(x.type) LIKE 'DOUBLE%' OR upper(x.type) LIKE 'FLOAT%' THEN '0.0'
                    WHEN upper(x.type) LIKE 'BOOLEAN%' THEN 'FALSE'
                    ELSE 'NULL'
                END
            ), ', ')
        """),
    ).otherwise(F.lit("")),
)

# ----------------------------------------------------------------------
# Collect grants per function
# ----------------------------------------------------------------------
grants_df = None
for name in func_names:
    try:
        g = (
            spark.sql(f"SHOW GRANTS ON FUNCTION `{CATALOG}`.`{SCHEMA}`.`{name}`")
            .withColumn("catalog", F.lit(CATALOG))
            .withColumn("schema", F.lit(SCHEMA))
            .withColumn("function_name", F.lit(name))
        )
        grants_df = g if grants_df is None else grants_df.unionByName(g, allowMissingColumns=True)
    except Exception:
        # no grants or not visible
        pass

if grants_df is None:
    grants_ready = spark.createDataFrame(
        [], "catalog STRING, schema STRING, function_name STRING, grant_sql ARRAY<STRING>"
    )
else:
    cols = {c.lower(): c for c in grants_df.columns}
    pcol = cols.get("principal") or cols.get("grantee")
    acol = cols.get("actiontype") or cols.get("privilege") or cols.get("action")

    if pcol and acol:
        fqn_for_grant = F.concat(
            F.lit("`"),
            F.col("catalog"),
            F.lit("`.`"),
            F.col("schema"),
            F.lit("`.`"),
            F.col("function_name"),
            F.lit("`"),
        )
        grants_ready = (
            grants_df.select(
                "catalog",
                "schema",
                "function_name",
                F.col(pcol).alias("grantee"),
                F.col(acol).alias("privilege"),
            )
            .where(F.col("grantee").isNotNull() & F.col("privilege").isNotNull())
            .withColumn(
                "grant_sql_single",
                F.concat(
                    F.lit("GRANT "),
                    F.col("privilege"),
                    F.lit(" ON FUNCTION "),
                    fqn_for_grant,
                    F.lit(" TO `"),
                    F.col("grantee"),
                    F.lit("`"),
                ),
            )
            .groupBy("catalog", "schema", "function_name")
            .agg(F.collect_list("grant_sql_single").alias("grant_sql"))
        )
    else:
        grants_ready = spark.createDataFrame(
            [], "catalog STRING, schema STRING, function_name STRING, grant_sql ARRAY<STRING>"
        )

# ----------------------------------------------------------------------
# Try to infer TABLE return schemas by actually running the functions
# ----------------------------------------------------------------------
infer_data = []
for r in df.collect():
    inferred_cols = []
    if (r.return_type_raw or "").upper().startswith("TABLE") and r.body_sql_raw:
        # Prepare the SQL body, replacing parameter names with dummy values
        sql_body = r.body_sql_raw
        params = r.parameters or []
        dummy_args = (r.param_call_args_sql or "").split(", ")
        param_map = {p['name']: v for p, v in zip(params, dummy_args)}

        # Replace parameter references in the SQL body with dummy values
        for pname, pval in param_map.items():
            # Replace only whole word matches (avoid partial replacements)
            sql_body = re.sub(rf"\b{re.escape(pname)}\b", pval, sql_body)

        # Wrap in SELECT * FROM (...) LIMIT 0 if not already a SELECT
        if not sql_body.strip().upper().startswith("SELECT"):
            sql_body = f"SELECT * FROM ({sql_body})"
        q = f"{sql_body} LIMIT 0"

        try:
            sdf = spark.sql(q)
            inferred_cols = [
                {"name": f.name, "type": f.dataType.simpleString().upper()}
                for f in sdf.schema.fields
            ]
        except Exception:
            inferred_cols = []

    infer_data.append((r.catalog, r.schema, r.function_name, inferred_cols))

infer_df = spark.createDataFrame(
    infer_data,
    """catalog STRING,
       schema STRING,
       function_name STRING,
       inferred_return_columns ARRAY<STRUCT<name:STRING,type:STRING>>""",
).withColumn(
    "inferred_return_type_full",
    F.when(
        F.size("inferred_return_columns") > 0,
        F.concat(
            F.lit("TABLE ("),
            F.expr(
                "array_join(transform(inferred_return_columns, x -> concat(x.name,' ',x.type)), ', ')"
            ),
            F.lit(")"),
        ),
    ),
)

# ----------------------------------------------------------------------
# Build final definition + grants per function
# ----------------------------------------------------------------------
res = (
    df.alias("d")
    .join(grants_ready.alias("g"), ["catalog", "schema", "function_name"], "left")
    .join(infer_df.alias("i"), ["catalog", "schema", "function_name"], "left")
    .withColumn(
        "effective_return_type",
        F.coalesce(
            F.col("i.inferred_return_type_full"),
            # handle old TABLE<...> syntax
            F.regexp_replace(
                F.regexp_replace(F.col("d.return_type_raw"), "<", "("),
                ">",
                ")",
            ),
        ),
    )
    .withColumn(
        "create_function_sql",
        F.concat(
            F.lit("CREATE OR REPLACE FUNCTION `"),
            F.col("catalog"),
            F.lit("`.`"),
            F.col("schema"),
            F.lit("`.`"),
            F.col("function_name"),
            F.lit("`("),
            F.col("param_list_sql"),
            F.lit(") RETURNS "),
            F.col("effective_return_type"),
            F.lit(" LANGUAGE "),
            F.col("language_value"),
            F.lit(" RETURN "),
            F.col("body_sql_raw"),
            F.lit(";"),
        ),
    )
    .withColumn(
        "grant_sql",
        F.coalesce(
            F.col("g.grant_sql"),
            F.array().cast(T.ArrayType(T.StringType())),
        ),
    )
    .select(
        "catalog",
        "schema",
        "function_name",
        "param_list_sql",
        "parameters",
        "param_call_args_sql",
        "effective_return_type",
        "inferred_return_columns",
        "body_sql_raw",
        "language_value",
        "comment",
        "security",
        "null_handling",
        "grant_sql",
        "create_function_sql",
    )
)

# ----------------------------------------------------------------------
# Apply CREATE FUNCTION + GRANTS and capture status
# ----------------------------------------------------------------------
rows_to_apply = res.select(
    "catalog", "schema", "function_name", "create_function_sql", "grant_sql"
).collect()

status_records = []
for r in rows_to_apply:
    fn_fqn = f"`{r.catalog}`.`{r.schema}`.`{r.function_name}`"
    create_status = "SKIPPED"
    create_error = None
    grant_status = "SKIPPED"
    grant_error = None

    # create / replace function
    create_sql = (r.create_function_sql or "").strip()
    if create_sql:
        create_sql_clean = create_sql.rstrip().rstrip(";")
        try:
            spark.sql(create_sql_clean)
            create_status = "SUCCESS"
        except Exception as e:
            create_status = "FAILED"
            create_error = str(e)

    # only run grants if create succeeded
    grants = r.grant_sql or []
    if grants:
        if create_status != "SUCCESS":
            grant_status = "SKIPPED"
        else:
            all_ok = True
            errors = []
            for g in grants:
                if not g:
                    continue
                grant_sql_clean = g.rstrip().rstrip(";")
                try:
                    spark.sql(grant_sql_clean)
                except Exception as e:
                    all_ok = False
                    errors.append(str(e))
            grant_status = "SUCCESS" if all_ok else "FAILED"
            if errors:
                grant_error = " | ".join(errors)[:4000]

    status_records.append((
        r.catalog,
        r.schema,
        r.function_name,
        create_status,
        create_error,
        grant_status,
        grant_error,
    ))

status_schema = T.StructType([
    T.StructField("catalog", T.StringType()),
    T.StructField("schema", T.StringType()),
    T.StructField("function_name", T.StringType()),
    T.StructField("create_status", T.StringType()),
    T.StructField("create_error", T.StringType()),
    T.StructField("grant_status", T.StringType()),
    T.StructField("grant_error", T.StringType()),
])

status_df = spark.createDataFrame(status_records, schema=status_schema)

# final result with statuses
res_with_status = res.join(status_df, ["catalog", "schema", "function_name"], "left")
display(res_with_status)


# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 5: Update Functions (Schema: derivative)

# COMMAND ----------

from pyspark.sql import functions as F, types as T
import re

CATALOG = "qa-cdh"
SCHEMA = "derivative"

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE `{CATALOG}`.`{SCHEMA}`")


def table_exists(ident: str) -> bool:
    """Check if a table/view exists."""
    try:
        return spark._jsparkSession.catalog().tableExists(ident)
    except Exception:
        return False


def parse_desc_rows(rows):
    """Parse DESCRIBE FUNCTION EXTENDED output into a structured dict."""
    lines = []
    for r in rows:
        cand = next((str(v) for v in r if v is not None and str(v).strip() != ""), None)
        if cand:
            lines.append(cand)

    text = "\n".join(lines)
    language = comment = security = None
    null_handling = None
    return_columns, return_type = [], None
    body_sql_raw = None

    # language / comment / security
    for label, var in [
        ("Language", "language"),
        ("Comment", "comment"),
        ("Security", "security")
    ]:
        m = re.search(fr"(?im)^\s*{label}:\s*(.+)$", text)
        if m:
            locals()[var] = m.group(1).strip()

    # null handling
    m = re.search(r"(?im)^\s*(Returns null on null input|Called on null input).*", text)
    if m:
        null_handling = m.group(0).strip()

    # return type / columns
    returns_idx = None
    for i, ln in enumerate(lines):
        if re.search(r"(?i)^\s*Returns?:", ln):
            returns_idx = i
            break

    if returns_idx is not None:
        same = lines[returns_idx]
        # inline TABLE<...> format
        mtab = re.search(r"(?is)TABLE\s*<\s*(.*?)\s*>", same)
        if mtab:
            inside = mtab.group(1)
            for part in [x.strip() for x in inside.split(",") if x.strip()]:
                mt = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+(.+)$", part)
                if mt:
                    return_columns.append({"name": mt.group(1), "type": mt.group(2).strip()})
            return_type = f"TABLE<{inside}>"
        else:
            # multi-line column list after "Returns:"
            j = returns_idx + 1
            while j < len(lines):
                ln = lines[j].strip()
                if not ln or re.match(
                    r"(?i)^(Language|Owner|Type|Definition|SQL body|Body|Source|Comment|Security):",
                    ln,
                ):
                    break
                mt = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+(.+)$", ln)
                if mt:
                    return_columns.append({"name": mt.group(1), "type": mt.group(2).strip()})
                    j += 1
                    continue
                break
            if return_columns:
                inside = ", ".join([f"{c['name']} {c['type']}" for c in return_columns])
                return_type = f"TABLE<{inside}>"
            else:
                m2 = re.search(r"(?im)^\s*Returns?:\s*(.+)$", text)
                if m2:
                    return_type = m2.group(1).strip()

    # raw SQL body
    m = re.search(r"(?im)^\s*(Definition|SQL body|Body|Source):\s*(.*)$", text)
    if m:
        endpos = m.end(0)
        inline = (m.group(2) or "").rstrip()
        rest = text[endpos:]
        body_sql_raw = (
            inline + ("\n" if rest and not rest.startswith("\n") else "") + rest
        ).strip()

    return {
        "language_raw": language,
        "return_columns": return_columns,
        "return_type_raw": return_type,
        "body_sql_raw": body_sql_raw,
        "comment": comment,
        "security": security,
        "null_handling": null_handling,
    }


# ----------------------------------------------------------------------
# Read all functions for the given schema
# ----------------------------------------------------------------------
routines_ident = f"`{CATALOG}`.`information_schema`.`routines`"

if table_exists(routines_ident):
    funcs = spark.sql(f"""
        SELECT specific_name
        FROM {routines_ident}
        WHERE specific_schema = '{SCHEMA}' AND routine_type = 'FUNCTION'
    """)
    func_names = [r.specific_name for r in funcs.collect()]
else:
    funcs = spark.sql(f"SHOW USER FUNCTIONS IN `{CATALOG}`.`{SCHEMA}`").collect()
    func_names = [row["function"].split(".")[-1] for row in funcs]

# ----------------------------------------------------------------------
# Parse DESCRIBE FUNCTION output (metadata, body, raw return info)
# ----------------------------------------------------------------------
rows = []
for name in func_names:
    try:
        desc = spark.sql(
            f"DESCRIBE FUNCTION EXTENDED `{CATALOG}`.`{SCHEMA}`.`{name}`"
        ).collect()
        meta = parse_desc_rows(desc)
        rows.append((
            CATALOG,
            SCHEMA,
            name,
            meta["return_columns"],
            meta["return_type_raw"],
            meta["language_raw"],
            meta["body_sql_raw"],
            meta["comment"],
            meta["security"],
            meta["null_handling"],
        ))
    except Exception:
        # ignore functions we can't describe
        pass

schema_desc = T.StructType([
    T.StructField("catalog", T.StringType()),
    T.StructField("schema", T.StringType()),
    T.StructField("function_name", T.StringType()),
    T.StructField("return_columns",
                  T.ArrayType(T.StructType([
                      T.StructField("name", T.StringType()),
                      T.StructField("type", T.StringType()),
                  ])),
                  ),
    T.StructField("return_type_raw", T.StringType()),
    T.StructField("language_raw", T.StringType()),
    T.StructField("body_sql_raw", T.StringType()),
    T.StructField("comment", T.StringType()),
    T.StructField("security", T.StringType()),
    T.StructField("null_handling", T.StringType()),
])

df = spark.createDataFrame(rows, schema=schema_desc)

# ----------------------------------------------------------------------
# Get full parameter info from INFORMATION_SCHEMA.PARAMETERS
#   (preserving original parameter order)
#   *** FIX: use FULL_DATA_TYPE to keep DECIMAL(19,2) etc. ***
# ----------------------------------------------------------------------
param_ident = f"`{CATALOG}`.`information_schema`.`parameters`"

params_df = spark.sql(f"""
    SELECT
        specific_name  AS function_name,
        ordinal_position,
        parameter_name AS name,
        COALESCE(full_data_type, data_type) AS type
    FROM {param_ident}
    WHERE specific_schema = '{SCHEMA}'
""")

# Preserve parameter order using ordinal_position
params_grouped = (
    params_df
    .groupBy("function_name")
    .agg(
        F.sort_array(
            F.collect_list(F.struct("ordinal_position", "name", "type"))
        ).alias("parameters_ord")
    )
    .withColumn(
        "parameters",
        F.expr("transform(parameters_ord, x -> named_struct('name', x.name, 'type', x.type))")
    )
    .drop("parameters_ord")
)

df = (
    df.join(params_grouped, "function_name", "left")
    .withColumn("language_value", F.upper(F.coalesce("language_raw", F.lit("SQL"))))
)

# Build "name type" parameter list for CREATE FUNCTION
df = df.withColumn(
    "param_list_sql",
    F.when(
        F.size("parameters") > 0,
        F.expr("array_join(transform(parameters, x -> concat(x.name, ' ', x.type)), ', ')"),
    ).otherwise(F.lit("")),
)

# Dummy argument values to call the function safely
df = df.withColumn(
    "param_call_args_sql",
    F.when(
        F.size("parameters") > 0,
        F.expr("""
            array_join(transform(parameters,
                x -> CASE
                    WHEN upper(x.type) LIKE 'DATE%' THEN concat("DATE '", '2000-01-01', "'")
                    WHEN upper(x.type) LIKE 'TIMESTAMP%' THEN concat("TIMESTAMP '", '2000-01-01 00:00:00', "'")
                    WHEN upper(x.type) LIKE 'STRING%' OR upper(x.type) LIKE 'VARCHAR%' THEN "'X'"
                    WHEN upper(x.type) LIKE 'INT%' OR upper(x.type) LIKE 'BIGINT%' OR upper(x.type) LIKE 'SMALLINT%' OR upper(x.type) LIKE 'TINYINT%' THEN '0'
                    WHEN upper(x.type) LIKE 'DECIMAL%' OR upper(x.type) LIKE 'NUMERIC%' OR upper(x.type) LIKE 'DOUBLE%' OR upper(x.type) LIKE 'FLOAT%' THEN '0.0'
                    WHEN upper(x.type) LIKE 'BOOLEAN%' THEN 'FALSE'
                    ELSE 'NULL'
                END
            ), ', ')
        """),
    ).otherwise(F.lit("")),
)

# ----------------------------------------------------------------------
# Collect grants per function
# ----------------------------------------------------------------------
grants_df = None
for name in func_names:
    try:
        g = (
            spark.sql(f"SHOW GRANTS ON FUNCTION `{CATALOG}`.`{SCHEMA}`.`{name}`")
            .withColumn("catalog", F.lit(CATALOG))
            .withColumn("schema", F.lit(SCHEMA))
            .withColumn("function_name", F.lit(name))
        )
        grants_df = g if grants_df is None else grants_df.unionByName(g, allowMissingColumns=True)
    except Exception:
        # no grants or not visible
        pass

if grants_df is None:
    grants_ready = spark.createDataFrame(
        [], "catalog STRING, schema STRING, function_name STRING, grant_sql ARRAY<STRING>"
    )
else:
    cols = {c.lower(): c for c in grants_df.columns}
    pcol = cols.get("principal") or cols.get("grantee")
    acol = cols.get("actiontype") or cols.get("privilege") or cols.get("action")

    if pcol and acol:
        fqn_for_grant = F.concat(
            F.lit("`"),
            F.col("catalog"),
            F.lit("`.`"),
            F.col("schema"),
            F.lit("`.`"),
            F.col("function_name"),
            F.lit("`"),
        )
        grants_ready = (
            grants_df.select(
                "catalog",
                "schema",
                "function_name",
                F.col(pcol).alias("grantee"),
                F.col(acol).alias("privilege"),
            )
            .where(F.col("grantee").isNotNull() & F.col("privilege").isNotNull())
            .withColumn(
                "grant_sql_single",
                F.concat(
                    F.lit("GRANT "),
                    F.col("privilege"),
                    F.lit(" ON FUNCTION "),
                    fqn_for_grant,
                    F.lit(" TO `"),
                    F.col("grantee"),
                    F.lit("`"),
                ),
            )
            .groupBy("catalog", "schema", "function_name")
            .agg(F.collect_list("grant_sql_single").alias("grant_sql"))
        )
    else:
        grants_ready = spark.createDataFrame(
            [], "catalog STRING, schema STRING, function_name STRING, grant_sql ARRAY<STRING>"
        )

# ----------------------------------------------------------------------
# Try to infer TABLE return schemas by actually running the functions
# ----------------------------------------------------------------------
infer_data = []
for r in df.collect():
    inferred_cols = []
    if (r.return_type_raw or "").upper().startswith("TABLE") and r.body_sql_raw:
        # Prepare the SQL body, replacing parameter names with dummy values
        sql_body = r.body_sql_raw
        params = r.parameters or []
        dummy_args = (r.param_call_args_sql or "").split(", ")
        param_map = {p['name']: v for p, v in zip(params, dummy_args)}

        # Replace parameter references in the SQL body with dummy values
        for pname, pval in param_map.items():
            # Replace only whole word matches (avoid partial replacements)
            sql_body = re.sub(rf"\b{re.escape(pname)}\b", pval, sql_body)

        # Wrap in SELECT * FROM (...) LIMIT 0 if not already a SELECT
        if not sql_body.strip().upper().startswith("SELECT"):
            sql_body = f"SELECT * FROM ({sql_body})"
        q = f"{sql_body} LIMIT 0"

        try:
            sdf = spark.sql(q)
            inferred_cols = [
                {"name": f.name, "type": f.dataType.simpleString().upper()}
                for f in sdf.schema.fields
            ]
        except Exception:
            inferred_cols = []

    infer_data.append((r.catalog, r.schema, r.function_name, inferred_cols))

infer_df = spark.createDataFrame(
    infer_data,
    """catalog STRING,
       schema STRING,
       function_name STRING,
       inferred_return_columns ARRAY<STRUCT<name:STRING,type:STRING>>""",
).withColumn(
    "inferred_return_type_full",
    F.when(
        F.size("inferred_return_columns") > 0,
        F.concat(
            F.lit("TABLE ("),
            F.expr(
                "array_join(transform(inferred_return_columns, x -> concat(x.name,' ',x.type)), ', ')"
            ),
            F.lit(")"),
        ),
    ),
)

# ----------------------------------------------------------------------
# Build final definition + grants per function
# ----------------------------------------------------------------------
res = (
    df.alias("d")
    .join(grants_ready.alias("g"), ["catalog", "schema", "function_name"], "left")
    .join(infer_df.alias("i"), ["catalog", "schema", "function_name"], "left")
    .withColumn(
        "effective_return_type",
        F.coalesce(
            F.col("i.inferred_return_type_full"),
            # handle old TABLE<...> syntax
            F.regexp_replace(
                F.regexp_replace(F.col("d.return_type_raw"), "<", "("),
                ">",
                ")",
            ),
        ),
    )
    .withColumn(
        "create_function_sql",
        F.concat(
            F.lit("CREATE OR REPLACE FUNCTION `"),
            F.col("catalog"),
            F.lit("`.`"),
            F.col("schema"),
            F.lit("`.`"),
            F.col("function_name"),
            F.lit("`("),
            F.col("param_list_sql"),
            F.lit(") RETURNS "),
            F.col("effective_return_type"),
            F.lit(" LANGUAGE "),
            F.col("language_value"),
            F.lit(" RETURN "),
            F.col("body_sql_raw"),
            F.lit(";"),
        ),
    )
    .withColumn(
        "grant_sql",
        F.coalesce(
            F.col("g.grant_sql"),
            F.array().cast(T.ArrayType(T.StringType())),
        ),
    )
    .select(
        "catalog",
        "schema",
        "function_name",
        "param_list_sql",
        "parameters",
        "param_call_args_sql",
        "effective_return_type",
        "inferred_return_columns",
        "body_sql_raw",
        "language_value",
        "comment",
        "security",
        "null_handling",
        "grant_sql",
        "create_function_sql",
    )
)

# ----------------------------------------------------------------------
# Apply CREATE FUNCTION + GRANTS and capture status
# ----------------------------------------------------------------------
rows_to_apply = res.select(
    "catalog", "schema", "function_name", "create_function_sql", "grant_sql"
).collect()

status_records = []
for r in rows_to_apply:
    fn_fqn = f"`{r.catalog}`.`{r.schema}`.`{r.function_name}`"
    create_status = "SKIPPED"
    create_error = None
    grant_status = "SKIPPED"
    grant_error = None

    # create / replace function
    create_sql = (r.create_function_sql or "").strip()
    if create_sql:
        create_sql_clean = create_sql.rstrip().rstrip(";")
        try:
            spark.sql(create_sql_clean)
            create_status = "SUCCESS"
        except Exception as e:
            create_status = "FAILED"
            create_error = str(e)

    # only run grants if create succeeded
    grants = r.grant_sql or []
    if grants:
        if create_status != "SUCCESS":
            grant_status = "SKIPPED"
        else:
            all_ok = True
            errors = []
            for g in grants:
                if not g:
                    continue
                grant_sql_clean = g.rstrip().rstrip(";")
                try:
                    spark.sql(grant_sql_clean)
                except Exception as e:
                    all_ok = False
                    errors.append(str(e))
            grant_status = "SUCCESS" if all_ok else "FAILED"
            if errors:
                grant_error = " | ".join(errors)[:4000]

    status_records.append((
        r.catalog,
        r.schema,
        r.function_name,
        create_status,
        create_error,
        grant_status,
        grant_error,
    ))

status_schema = T.StructType([
    T.StructField("catalog", T.StringType()),
    T.StructField("schema", T.StringType()),
    T.StructField("function_name", T.StringType()),
    T.StructField("create_status", T.StringType()),
    T.StructField("create_error", T.StringType()),
    T.StructField("grant_status", T.StringType()),
    T.StructField("grant_error", T.StringType()),
])

status_df = spark.createDataFrame(status_records, schema=status_schema)

# final result with statuses
res_with_status = res.join(status_df, ["catalog", "schema", "function_name"], "left")
display(res_with_status)
