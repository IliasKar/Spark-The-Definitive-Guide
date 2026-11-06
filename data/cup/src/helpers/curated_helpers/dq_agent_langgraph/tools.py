"""
DQ Agent — LangGraph Tools
============================
Tools exposed to the LLM agent for investigating data quality issues.
Each tool is a standard LangChain tool (decorated with @tool) so LangGraph
can bind them to the model automatically.

These tools execute read-only queries against Databricks Unity Catalog
via the active SparkSession.
"""

from typing import Optional

from langchain_core.tools import tool
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Module-level SparkSession holder
# We inject this at agent construction time so tools can use it.
# ---------------------------------------------------------------------------

_spark: Optional[SparkSession] = None
_catalog: str = ""
_run_date: str = ""


def configure_tools(spark: SparkSession, catalog: str, run_date: str) -> None:
    """
    Initialize the tool context. Must be called before the graph is invoked.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session (from Databricks notebook).
    catalog : str
        Catalog path e.g. "`p-cdh`"
    run_date : str
        The BusinessDate being analyzed (YYYY-MM-DD).
    """
    global _spark, _catalog, _run_date
    _spark = spark
    _catalog = catalog
    _run_date = run_date


# ---------------------------------------------------------------------------
# Safety validation
# ---------------------------------------------------------------------------

_FORBIDDEN_KEYWORDS = [
    "DROP", "DELETE", "INSERT", "UPDATE", "MERGE",
    "CREATE", "ALTER", "TRUNCATE", "GRANT", "REVOKE",
]


def _validate_sql(sql: str) -> Optional[str]:
    """Return an error message if SQL is not read-only, else None."""
    sql_upper = sql.strip().upper()
    # Must start with allowed statements
    if not (
        sql_upper.startswith("SELECT")
        or sql_upper.startswith("SHOW")
        or sql_upper.startswith("DESCRIBE")
    ):
        return "ERROR: Only SELECT, SHOW, and DESCRIBE statements are allowed."

    # Block forbidden keywords (ignoring comments)
    sql_no_comments = sql_upper.split("--")[0]
    for keyword in _FORBIDDEN_KEYWORDS:
        if keyword in sql_no_comments:
            return f"ERROR: {keyword} statements are not allowed. Read-only queries only."

    return None


# ---------------------------------------------------------------------------
# Tool Definitions
# ---------------------------------------------------------------------------

@tool
def run_sql(sql: str, reasoning: str = "") -> str:
    """
    Execute a read-only SQL query against Databricks Unity Catalog.

    Use this to investigate data quality issues by querying landed (bronze)
    or primaryactive (silver) tables. Only SELECT/SHOW/DESCRIBE are allowed.
    Results are limited to 50 rows.

    Args:
        sql: The SQL SELECT query to execute. Use catalog format: `{env}-cdh`.`schema`.table_name
        reasoning: Brief explanation of why this query helps investigate the issue.
    """
    if _spark is None:
        return "ERROR: SparkSession not configured. Call configure_tools() first."

    error = _validate_sql(sql)
    if error:
        return error

    try:
        df = _spark.sql(sql)
        rows = df.limit(50).collect()
        columns = df.columns

        if not rows:
            return f"Query returned 0 rows. Columns: {columns}"

        result_lines = [f"Columns: {columns}", f"Rows returned: {len(rows)}", "---"]
        for row in rows[:20]:
            result_lines.append(str(row.asDict()))

        if len(rows) > 20:
            result_lines.append(f"... ({len(rows)} total rows, showing first 20)")

        return "\n".join(result_lines)
    except Exception as e:
        return f"ERROR executing SQL: {str(e)}"


@tool
def get_table_schema(table_path: str) -> str:
    """
    Get the column names and data types for a Unity Catalog table.

    Use this to understand table structure before writing queries.

    Args:
        table_path: Full table path, e.g. `p-cdh`.`primaryactive`.exposure
    """
    if _spark is None:
        return "ERROR: SparkSession not configured. Call configure_tools() first."

    try:
        df = _spark.read.table(table_path)
        schema_info = [(f.name, str(f.dataType)) for f in df.schema.fields]
        return (
            f"Table: {table_path}\n"
            f"Columns ({len(schema_info)}):\n"
            + "\n".join([f"  - {name}: {dtype}" for name, dtype in schema_info])
        )
    except Exception as e:
        return f"ERROR reading schema: {str(e)}"


@tool
def query_dq_history(
    table_name_filter: str = "",
    check_name_filter: str = "",
    days_back: int = 14,
) -> str:
    """
    Query the DataQualityResults table for historical DQ check results.

    Use this to determine if a failure is recurring or new. Always call
    this early in your investigation.

    Args:
        table_name_filter: Filter by table name (partial match). E.g. 'exposure'
        check_name_filter: Filter by check type. E.g. 'duplicate_detection'
        days_back: Number of days to look back. Default 14.
    """
    if _spark is None:
        return "ERROR: SparkSession not configured. Call configure_tools() first."

    dq_table = f"{_catalog}.`primaryactive`.DataQualityResults"

    where_clauses = [
        f"business_date >= DATE_SUB('{_run_date}', {days_back})",
        f"business_date < '{_run_date}'",
        "status = 'FAIL'",
    ]
    if table_name_filter:
        where_clauses.append(f"table_name LIKE '%{table_name_filter}%'")
    if check_name_filter:
        where_clauses.append(f"check_name = '{check_name_filter}'")

    query = f"""
        SELECT business_date, check_name, table_name, severity, details, metric_value
        FROM {dq_table}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY business_date DESC
        LIMIT 30
    """
    try:
        rows = _spark.sql(query).collect()
        if not rows:
            return "No historical failures found matching the filter."
        result_lines = [f"Historical failures ({len(rows)} found):"]
        for row in rows:
            result_lines.append(
                f"  [{row['business_date']}] {row['check_name']} | "
                f"{row['table_name']} | {row['severity']} | "
                f"metric={row['metric_value']} | {str(row['details'])[:80]}"
            )
        return "\n".join(result_lines)
    except Exception as e:
        return f"DataQualityResults table not available yet: {str(e)}"


# ---------------------------------------------------------------------------
# Export all tools as a list (used by the graph builder)
# ---------------------------------------------------------------------------

ALL_TOOLS = [run_sql, get_table_schema, query_dq_history]
