"""
pipeline_log_helpers.py
Helper functions for writing task-level tracking to pipeline.task_runs.

APPEND-ONLY EVENT STREAM
------------------------
task_runs is a pure event stream. Each row is ONE event with ONE timestamp
(event_timestamp). A task produces at least two rows:
  - log_task_start()  -> INSERT event_type='start'
  - log_task_end()    -> INSERT event_type='completed' | 'failed'

Both writes are plain INSERTs (blind appends). Delta does NOT raise
ConcurrentAppendException between concurrent blind appends, so this model
is safe under heavy parallelism — no retries, no in-place UPDATE, nothing
ever gets stuck as 'running' because of a lost update race.

start_timestamp / end_timestamp / duration are NOT stored on task_runs — they
are DERIVED at read time by the view pipeline.v_task_latest_status, which pairs
the latest terminal event (completed/failed) with the most recent 'start' event
that precedes it (the last run cycle).

area_runs (written by _check_area_completion) DOES store start/end/duration,
because there it is a one-off aggregate (MIN start / MAX end across the area's
tasks) computed once when the area completes.
"""

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Auto-detect context
# ---------------------------------------------------------------------------

def _get_caller_notebook():
    """Returns just the notebook name (not the full internal path)."""
    try:
        from databricks.sdk.runtime import dbutils
        full_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return full_path.rsplit("/", 1)[-1] if "/" in full_path else full_path
    except Exception:
        return None


def _get_databricks_run_id():
    """Returns the current Databricks job run ID.

    On shared/UC clusters, currentRunId() and tags() are often blocked
    (Py4JSecurityException). idInJob() and jobId() remain whitelisted and
    return the task run id, so we prefer those.
    """
    def _unwrap_option(v):
        if v is None:
            return None
        s = str(v)
        if s in ("None", ""):
            return None
        if s.startswith("Some(") and s.endswith(")"):
            return s[5:-1]
        return s

    try:
        from databricks.sdk.runtime import dbutils
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

        # 1. idInJob() — the task run id (whitelisted on shared clusters)
        try:
            val = _unwrap_option(ctx.idInJob())
            if val:
                return val
        except Exception:
            pass

        # 2. currentRunId() — works on non-shared clusters
        try:
            rid = ctx.currentRunId()
            try:
                val = _unwrap_option(rid.get().toString())
            except Exception:
                val = _unwrap_option(rid)
            if val:
                return val
        except Exception:
            pass

        # 3. Tag fallbacks (may be blocked on shared clusters)
        for tag in ("taskRunId", "runId", "jobRunId", "multitaskParentRunId"):
            try:
                run_id = ctx.tags().get(tag).getOrElse(None)
                if run_id:
                    return run_id
            except Exception:
                pass

        return None
    except Exception:
        return None


def _get_databricks_job_name():
    """Returns the current Databricks job name."""
    try:
        from databricks.sdk.runtime import dbutils
        tags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
        return tags.get("jobName").getOrElse(None)
    except Exception:
        return None


def _get_env_prefix_from_spark(spark: SparkSession):
    """Resolve env_prefix from SSM (cached per session)."""
    try:
        import boto3
        ssm_client = boto3.client("ssm", region_name="eu-central-1")
        return ssm_client.get_parameter(Name="/cfn/bucket_prefix")["Parameter"]["Value"]
    except Exception as e:
        print(f"[WARNING] Could not resolve env_prefix from SSM: {e}")
        return None


def _sql_str(v):
    """Render a Python value as a SQL string literal or NULL (escapes single quotes)."""
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def log_task_start(
    spark: SparkSession,
    pipeline_area: str,
    business_date: str,
    task_name: str,
    schema_name: str = None,
    group_name: str = None,
    triggered_by: str = None,
    databricks_run_id: str = None,
    env_prefix: str = None,
):
    """
    Append a 'start' event into pipeline.task_runs (plain INSERT, concurrency-safe).

    Args:
        spark: SparkSession
        pipeline_area: 'staging' | 'landing' | 'landed' | 'curated'
        business_date: business date being processed (YYYY-MM-DD)
        task_name: what is being processed (e.g., 'profits_loan')
        schema_name: source schema (e.g., 'profits')
        group_name: group within schema
        triggered_by: notebook name (auto-detected if None)
        databricks_run_id: job run ID (auto-detected if None)
        env_prefix: environment prefix (auto-detected if None)
    """
    if env_prefix is None:
        env_prefix = _get_env_prefix_from_spark(spark)
    if env_prefix is None:
        print("[WARNING] env_prefix not available — skipping task log write")
        return

    if triggered_by is None:
        triggered_by = _get_caller_notebook()
    if databricks_run_id is None:
        databricks_run_id = _get_databricks_run_id()
    job_name = _get_databricks_job_name()

    table = f"`{env_prefix}-cdh`.pipeline.task_runs"

    spark.sql(f"""
        INSERT INTO {table} (
            pipeline_area, business_date, schema_name, group_name,
            task_name, event_type, databricks_run_id, job_name,
            error_message, triggered_by, event_timestamp
        ) VALUES (
            '{pipeline_area}',
            CAST('{business_date}' AS DATE),
            {_sql_str(schema_name)},
            {_sql_str(group_name)},
            '{task_name}',
            'start',
            {_sql_str(databricks_run_id)},
            {_sql_str(job_name)},
            NULL,
            {_sql_str(triggered_by)},
            current_timestamp()
        )
    """)
    print(f"[task_runs] START: {pipeline_area}.{task_name} ({business_date})")


def log_task_end(
    spark: SparkSession,
    pipeline_area: str,
    business_date: str,
    task_name: str,
    status: str = "completed",
    error_message: str = None,
    env_prefix: str = None,
):
    """
    Append an end event ('completed' or 'failed') into pipeline.task_runs
    (plain INSERT, concurrency-safe).

    This is a pure event write — it does NOT read the start row and does NOT
    compute duration. start/end/duration are derived at read time by the view
    pipeline.v_task_latest_status.

    Args:
        spark: SparkSession
        pipeline_area: 'staging' | 'landing' | 'landed' | 'curated'
        business_date: business date being processed (YYYY-MM-DD)
        task_name: what was processed (must match the start call)
        status: 'completed' or 'failed' (mapped to event_type)
        error_message: error details if failed (optional)
        env_prefix: environment prefix (auto-detected if None)
    """
    if env_prefix is None:
        env_prefix = _get_env_prefix_from_spark(spark)
    if env_prefix is None:
        print("[WARNING] env_prefix not available — skipping task log write")
        return

    event_type = "failed" if str(status).lower() == "failed" else "completed"
    table = f"`{env_prefix}-cdh`.pipeline.task_runs"

    spark.sql(f"""
        INSERT INTO {table} (
            pipeline_area, business_date, schema_name, group_name,
            task_name, event_type, databricks_run_id, job_name,
            error_message, triggered_by, event_timestamp
        ) VALUES (
            '{pipeline_area}',
            CAST('{business_date}' AS DATE),
            NULL,
            NULL,
            '{task_name}',
            '{event_type}',
            {_sql_str(_get_databricks_run_id())},
            {_sql_str(_get_databricks_job_name())},
            {_sql_str(error_message)},
            {_sql_str(_get_caller_notebook())},
            current_timestamp()
        )
    """)
    print(f"[task_runs] {event_type.upper()}: {pipeline_area}.{task_name} ({business_date})")

    # Check if all expected tasks for this area are completed
    if event_type == "completed":
        _check_area_completion(spark, env_prefix, pipeline_area, business_date)


# ---------------------------------------------------------------------------
# Area completion check
# ---------------------------------------------------------------------------

def _check_area_completion(spark: SparkSession, env_prefix: str, pipeline_area: str, business_date: str):
    """
    Checks if all expected tasks for a pipeline_area + business_date have
    reached a terminal event. If all are 'completed' (none 'failed'/'running'),
    inserts a 'completed' record into area_runs with derived start/end/duration.

    Per-task start/end are derived from the event stream (last run cycle):
      - end   = timestamp of the latest terminal event (completed/failed)
      - start = timestamp of the latest 'start' event at or before that end
    Area start = MIN(task start), area end = MAX(task end).

    For staging/landing/landed:
      - expected_tasks has one row per table under 'stg-lng-lnd'
      - task_runs has separate events per area (staging, landing, landed)
      - Each area checks independently against the expected table list.

    For curated:
      - expected_tasks has one row per notebook under 'curated'.
    """
    try:
        expected_table = f"`{env_prefix}-cdh`.pipeline.expected_tasks"
        task_log_table = f"`{env_prefix}-cdh`.pipeline.task_runs"
        exec_log_table = f"`{env_prefix}-cdh`.pipeline.area_runs"

        # Map pipeline_area to the expected_tasks area
        if pipeline_area in ("staging", "landing", "landed"):
            expected_area = "stg-lng-lnd"
        else:
            expected_area = pipeline_area

        # Count expected tasks
        expected_count = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {expected_table}
            WHERE pipeline_area = '{expected_area}'
        """).first()["cnt"]

        if expected_count == 0:
            return

        # Latest event per task for THIS area, then tally terminal states.
        result = spark.sql(f"""
            WITH latest AS (
                SELECT task_name, event_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY task_name
                        ORDER BY event_timestamp DESC
                    ) AS rn
                FROM {task_log_table}
                WHERE pipeline_area = '{pipeline_area}'
                  AND business_date = CAST('{business_date}' AS DATE)
            )
            SELECT
                COUNT(DISTINCT CASE WHEN event_type IN ('completed', 'failed') THEN task_name END) AS terminal_count,
                SUM(CASE WHEN event_type = 'failed' THEN 1 ELSE 0 END) AS failed_count
            FROM latest
            WHERE rn = 1
        """).first()

        terminal_count = result["terminal_count"] or 0
        failed_count = result["failed_count"] or 0

        if terminal_count < expected_count:
            return
        if failed_count > 0:
            return

        # Check if already logged (avoid duplicates)
        already_logged = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {exec_log_table}
            WHERE pipeline_area = '{pipeline_area}'
              AND business_date = CAST('{business_date}' AS DATE)
              AND status = 'completed'
        """).first()["cnt"]

        if already_logged > 0:
            return

        # Derive per-task start/end from the event stream (last run cycle),
        # then aggregate to the area level (MIN start / MAX end).
        spark.sql(f"""
            INSERT INTO {exec_log_table} (
                pipeline_area, business_date, status,
                start_timestamp, end_timestamp, duration_seconds,
                triggered_by, databricks_run_id, error_message, logged_at
            )
            WITH ends AS (
                -- latest terminal event per task = end of the last cycle
                SELECT task_name, event_timestamp AS end_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY task_name ORDER BY event_timestamp DESC
                    ) AS rn
                FROM {task_log_table}
                WHERE pipeline_area = '{pipeline_area}'
                  AND business_date = CAST('{business_date}' AS DATE)
                  AND event_type IN ('completed', 'failed')
            ),
            task_cycle AS (
                SELECT e.task_name,
                       e.end_ts,
                       -- latest start at or before this end = start of the last cycle
                       (SELECT MAX(s.event_timestamp)
                        FROM {task_log_table} s
                        WHERE s.pipeline_area = '{pipeline_area}'
                          AND s.business_date = CAST('{business_date}' AS DATE)
                          AND s.task_name = e.task_name
                          AND s.event_type = 'start'
                          AND s.event_timestamp <= e.end_ts) AS start_ts
                FROM ends e
                WHERE e.rn = 1
            )
            SELECT
                '{pipeline_area}',
                CAST('{business_date}' AS DATE),
                'completed',
                MIN(start_ts),
                MAX(end_ts),
                CAST(unix_timestamp(MAX(end_ts)) - unix_timestamp(MIN(start_ts)) AS INT),
                'auto_completion_check',
                NULL,
                NULL,
                current_timestamp()
            FROM task_cycle
        """)
        print(f"[area_runs] AREA COMPLETED: {pipeline_area} ({business_date})")

    except Exception as e:
        print(f"[WARNING] Area completion check failed (non-blocking): {e}")
