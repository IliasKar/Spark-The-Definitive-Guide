# Databricks notebook source
# MAGIC %md
# MAGIC #### OWNER CORRECTION -> SERVICE_PRINCIPAL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-check: function inventory + owner fix (Service_Principal)
# MAGIC Βρίσκει όλες τις functions στα p-cdh.derivative / p-cdh.primary
# MAGIC δείχνει ό,τι δεν έχει owner τον
# MAGIC Service_Principal, και το διορθώνει αυτόματα.

# COMMAND ----------

SERVICE_PRINCIPAL_APP_ID = "00f3ea27-8236-4f6a-8f9c-177668555720"
CATALOG = "p-cdh"
SCHEMAS = ["derivative", "primary"]

import re

def qi(name):
    return "`" + name.replace("`", "``") + "`"

def requote_fqn(raw_fqn: str) -> str:
    """SHOW USER FUNCTIONS returns unquoted dotted names (e.g. p-cdh.derivative.foo),
    which breaks on catalogs like p-cdh that contain a hyphen. Split into exactly
    3 parts and re-quote each one before using it in SQL."""
    parts = raw_fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected a 3-part function name, got: {raw_fqn}")
    return ".".join(qi(p.strip("`")) for p in parts)

def get_owner(fqn: str):
    """Parse the Owner: line out of DESCRIBE FUNCTION EXTENDED output."""
    rows = spark.sql(f"DESCRIBE FUNCTION EXTENDED {fqn}").collect()
    text = "\n".join(r[0] for r in rows)
    m = re.search(r"^Owner:\s*(.+)$", text, re.MULTILINE)
    return m.group(1).strip() if m else None

all_functions = []
for schema in SCHEMAS:
    rows = spark.sql(f"SHOW USER FUNCTIONS IN {qi(CATALOG)}.{qi(schema)}").collect()
    for r in rows:
        all_functions.append(r[0])  # fully qualified function name, unquoted from Spark

print(f"Found {len(all_functions)} functions across {SCHEMAS}:")
for f in all_functions:
    print(f"  {f}")

# COMMAND ----------

# MAGIC %md ### Check owner on each, fix if it isn't the Service_Principal

# COMMAND ----------

fixed, already_ok, errors = [], [], []

for raw_fqn in all_functions:
    try:
        fqn = requote_fqn(raw_fqn)
    except Exception as e:
        errors.append((raw_fqn, f"could not parse name: {e}"))
        continue

    try:
        owner = get_owner(fqn)
    except Exception as e:
        errors.append((raw_fqn, f"could not read owner: {e}"))
        continue

    if owner == SERVICE_PRINCIPAL_APP_ID:
        already_ok.append(raw_fqn)
        continue

    try:
        spark.sql(f"ALTER FUNCTION {fqn} OWNER TO `{SERVICE_PRINCIPAL_APP_ID}`")
        fixed.append((raw_fqn, owner))
    except Exception as e:
        errors.append((raw_fqn, f"owner was '{owner}', ALTER FUNCTION OWNER TO failed: {e}"))

print(f"Already correct owner: {len(already_ok)}")
print(f"Fixed (owner changed): {len(fixed)}")
for fqn, old_owner in fixed:
    print(f"  {fqn}  (was: {old_owner})")
if errors:
    print(f"\nErrors: {len(errors)}")
    for fqn, err in errors:
        print(f"  {fqn}: {err}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### FUNCTIONS UPDATE - PRIMARY

# COMMAND ----------

from pyspark.sql import functions as F, types as T
import re

CATALOG = "p-cdh"
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
    for label, var in [("Language", "language"),
                       ("Comment", "comment"),
                       ("Security", "security")]:
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
                    return_columns.append(
                        {"name": mt.group(1), "type": mt.group(2).strip()}
                    )
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
                    return_columns.append(
                        {"name": mt.group(1), "type": mt.group(2).strip()}
                    )
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
        rows.append(
            (
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
            )
        )
    except Exception:
        # ignore functions we can't describe
        pass

schema_desc = T.StructType(
    [
        T.StructField("catalog", T.StringType()),
        T.StructField("schema", T.StringType()),
        T.StructField("function_name", T.StringType()),
        T.StructField(
            "return_columns",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("name", T.StringType()),
                        T.StructField("type", T.StringType()),
                    ]
                )
            ),
        ),
        T.StructField("return_type_raw", T.StringType()),
        T.StructField("language_raw", T.StringType()),
        T.StructField("body_sql_raw", T.StringType()),
        T.StructField("comment", T.StringType()),
        T.StructField("security", T.StringType()),
        T.StructField("null_handling", T.StringType()),
    ]
)

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
            F.collect_list(
                F.struct("ordinal_position", "name", "type")
            )
        ).alias("parameters_ord")
    )
    .withColumn(
        "parameters",
        F.expr(
            "transform(parameters_ord, x -> named_struct('name', x.name, 'type', x.type))"
        )
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
        F.expr(
            "array_join(transform(parameters, x -> concat(x.name, ' ', x.type)), ', ')"
        ),
    ).otherwise(F.lit("")),
)

# Dummy argument values to call the function safely
df = df.withColumn(
    "param_call_args_sql",
    F.when(
        F.size("parameters") > 0,
        F.expr(
            """
          array_join(
            transform(
              parameters,
              x -> CASE
                     WHEN upper(x.type) LIKE 'DATE%' THEN concat("DATE '", '2000-01-01', "'")
                     WHEN upper(x.type) LIKE 'TIMESTAMP%' THEN concat("TIMESTAMP '", '2000-01-01 00:00:00', "'")
                     WHEN upper(x.type) LIKE 'STRING%' OR upper(x.type) LIKE 'VARCHAR%' THEN "'X'"
                     WHEN upper(x.type) LIKE 'INT%' OR upper(x.type) LIKE 'BIGINT%' 
                          OR upper(x.type) LIKE 'SMALLINT%' OR upper(x.type) LIKE 'TINYINT%' THEN '0'
                     WHEN upper(x.type) LIKE 'DECIMAL%' OR upper(x.type) LIKE 'NUMERIC%' 
                          OR upper(x.type) LIKE 'DOUBLE%' OR upper(x.type) LIKE 'FLOAT%' THEN '0.0'
                     WHEN upper(x.type) LIKE 'BOOLEAN%' THEN 'FALSE'
                     ELSE 'NULL'
                   END
            ),
            ', '
          )
        """
        ),
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
        grants_df = g if grants_df is None else grants_df.unionByName(
            g, allowMissingColumns=True
        )
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
    """
    catalog STRING,
    schema STRING,
    function_name STRING,
    inferred_return_columns ARRAY<STRUCT<name:STRING,type:STRING>>
    """,
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

    status_records.append(
        (
            r.catalog,
            r.schema,
            r.function_name,
            create_status,
            create_error,
            grant_status,
            grant_error,
        )
    )

status_schema = T.StructType(
    [
        T.StructField("catalog", T.StringType()),
        T.StructField("schema", T.StringType()),
        T.StructField("function_name", T.StringType()),
        T.StructField("create_status", T.StringType()),
        T.StructField("create_error", T.StringType()),
        T.StructField("grant_status", T.StringType()),
        T.StructField("grant_error", T.StringType()),
    ]
)

status_df = spark.createDataFrame(status_records, schema=status_schema)

# final result with statuses
res_with_status = res.join(status_df, ["catalog", "schema", "function_name"], "left")

display(res_with_status)

# COMMAND ----------

# MAGIC %md
# MAGIC #### FUNCTIONS UPDATE - DELIVERY

# COMMAND ----------

from pyspark.sql import functions as F, types as T
import re

CATALOG = "p-cdh"
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
    for label, var in [("Language", "language"),
                       ("Comment", "comment"),
                       ("Security", "security")]:
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
                    return_columns.append(
                        {"name": mt.group(1), "type": mt.group(2).strip()}
                    )
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
                    return_columns.append(
                        {"name": mt.group(1), "type": mt.group(2).strip()}
                    )
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
        rows.append(
            (
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
            )
        )
    except Exception:
        # ignore functions we can't describe
        pass

schema_desc = T.StructType(
    [
        T.StructField("catalog", T.StringType()),
        T.StructField("schema", T.StringType()),
        T.StructField("function_name", T.StringType()),
        T.StructField(
            "return_columns",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("name", T.StringType()),
                        T.StructField("type", T.StringType()),
                    ]
                )
            ),
        ),
        T.StructField("return_type_raw", T.StringType()),
        T.StructField("language_raw", T.StringType()),
        T.StructField("body_sql_raw", T.StringType()),
        T.StructField("comment", T.StringType()),
        T.StructField("security", T.StringType()),
        T.StructField("null_handling", T.StringType()),
    ]
)

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
            F.collect_list(
                F.struct("ordinal_position", "name", "type")
            )
        ).alias("parameters_ord")
    )
    .withColumn(
        "parameters",
        F.expr(
            "transform(parameters_ord, x -> named_struct('name', x.name, 'type', x.type))"
        )
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
        F.expr(
            "array_join(transform(parameters, x -> concat(x.name, ' ', x.type)), ', ')"
        ),
    ).otherwise(F.lit("")),
)

# Dummy argument values to call the function safely
df = df.withColumn(
    "param_call_args_sql",
    F.when(
        F.size("parameters") > 0,
        F.expr(
            """
          array_join(
            transform(
              parameters,
              x -> CASE
                     WHEN upper(x.type) LIKE 'DATE%' THEN concat("DATE '", '2000-01-01', "'")
                     WHEN upper(x.type) LIKE 'TIMESTAMP%' THEN concat("TIMESTAMP '", '2000-01-01 00:00:00', "'")
                     WHEN upper(x.type) LIKE 'STRING%' OR upper(x.type) LIKE 'VARCHAR%' THEN "'X'"
                     WHEN upper(x.type) LIKE 'INT%' OR upper(x.type) LIKE 'BIGINT%' 
                          OR upper(x.type) LIKE 'SMALLINT%' OR upper(x.type) LIKE 'TINYINT%' THEN '0'
                     WHEN upper(x.type) LIKE 'DECIMAL%' OR upper(x.type) LIKE 'NUMERIC%' 
                          OR upper(x.type) LIKE 'DOUBLE%' OR upper(x.type) LIKE 'FLOAT%' THEN '0.0'
                     WHEN upper(x.type) LIKE 'BOOLEAN%' THEN 'FALSE'
                     ELSE 'NULL'
                   END
            ),
            ', '
          )
        """
        ),
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
        grants_df = g if grants_df is None else grants_df.unionByName(
            g, allowMissingColumns=True
        )
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
    """
    catalog STRING,
    schema STRING,
    function_name STRING,
    inferred_return_columns ARRAY<STRUCT<name:STRING,type:STRING>>
    """,
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

    status_records.append(
        (
            r.catalog,
            r.schema,
            r.function_name,
            create_status,
            create_error,
            grant_status,
            grant_error,
        )
    )

status_schema = T.StructType(
    [
        T.StructField("catalog", T.StringType()),
        T.StructField("schema", T.StringType()),
        T.StructField("function_name", T.StringType()),
        T.StructField("create_status", T.StringType()),
        T.StructField("create_error", T.StringType()),
        T.StructField("grant_status", T.StringType()),
        T.StructField("grant_error", T.StringType()),
    ]
)

status_df = spark.createDataFrame(status_records, schema=status_schema)

# final result with statuses
res_with_status = res.join(status_df, ["catalog", "schema", "function_name"], "left")

display(res_with_status)
