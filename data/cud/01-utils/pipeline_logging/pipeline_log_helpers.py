"""
pipeline_log_helpers.py  (cud repo — 01-utils/pipeline_logging)
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

env_prefix is resolved by the caller (controller already calls
aws_utils.get_env_prefix()) and passed in. If not provided we try the
aws_utils / SSM fallback; if that also fails we skip logging (non-blocking).
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


def _get_env_prefix_fallback():
    """Resolve env_prefix via aws_utils (preferred) or direct SSM (last resort)."""
    try:
        import aws_utils
        return aws_utils.get_env_prefix()
    except Exception:
        pass
    try:
        import boto3
        ssm_client = boto3.client("ssm", region_name="eu-central-1")
        return ssm_client.get_parameter(Name="/cfn/bucket_prefix")["Parameter"]["Value"]
    except Exception as e:
        print(f"[WARNING] Could not resolve env_prefix: {e}")
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
        pipeline_area: e.g. 'derivative'
        business_date: business date being processed (YYYY-MM-DD)
        task_name: what is being processed (e.g., 'DExposure')
        schema_name / group_name: optional metadata (start event only)
        triggered_by: notebook name (auto-detected if None)
        databricks_run_id: job run ID (auto-detected if None)
        env_prefix: environment prefix (resolved via aws_utils if None)
    """
    if env_prefix is None:
        env_prefix = _get_env_prefix_fallback()
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
    """
    if env_prefix is None:
        env_prefix = _get_env_prefix_fallback()
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

    expected_tasks must have one row per table under pipeline_area='derivative'
    (populated by the redis extract_expected_tasks notebook).
    """
    try:
        expected_table = f"`{env_prefix}-cdh`.pipeline.expected_tasks"
        task_log_table = f"`{env_prefix}-cdh`.pipeline.task_runs"
        exec_log_table = f"`{env_prefix}-cdh`.pipeline.area_runs"

        expected_count = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {expected_table}
            WHERE pipeline_area = '{pipeline_area}'
        """).first()["cnt"]

        if expected_count == 0:
            return

        # Latest event per task, then tally terminal states.
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

        # Avoid duplicate area records
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


# ---------------------------------------------------------------------------
# Explicit area completion (job-driven, no expected_tasks dependency)
# ---------------------------------------------------------------------------

def _resolve_business_date_from_area(spark: SparkSession, env_prefix: str, source_area: str):
    """
    Resolve a business_date from pipeline.v_area_latest_status for a given
    source area. The view returns the latest completed run per pipeline_area,
    so this returns that area's latest business_date.

        SELECT business_date
        FROM `{env}-cdh`.pipeline.v_area_latest_status
        WHERE pipeline_area = '<source_area>'

    Returns the business_date as a string (YYYY-MM-DD) or None if no row.
    """
    view = f"`{env_prefix}-cdh`.pipeline.v_area_latest_status"
    row = spark.sql(f"""
        SELECT business_date
        FROM {view}
        WHERE pipeline_area = {_sql_str(source_area)}
        ORDER BY business_date DESC
        LIMIT 1
    """).first()
    if row is None or row["business_date"] is None:
        return None
    return str(row["business_date"])


def log_area_completed(
    spark: SparkSession,
    pipeline_area: str,
    business_date: str = None,
    env_prefix: str = None,
    skip_task_runs: bool = False,
    business_date_based_on_area: str = None,
):
    """
    Write a 'completed' record for a whole pipeline_area into pipeline.area_runs,
    driven by the JOB (a final task that depends on all leaf tasks), NOT by
    expected_tasks.

    Two modes:
      - skip_task_runs=False (default): derives business_date, start, end, duration
        from task_runs (the events the per-table controller already wrote).
        Intended for flows that write per-table events (derivative, stg-lng-lnd…).
      - skip_task_runs=True: writes a simple completed record with
        start=end=now, duration=0. Does NOT read task_runs at all.
        Intended for areas that have no per-table logging (reports, exports, adhoc).
        business_date MUST be resolvable in this mode — either supplied directly
        via `business_date`, or sourced from another area via
        `business_date_based_on_area`.

    business_date_based_on_area:
      When provided and no explicit `business_date` is given, the business_date
      is resolved from pipeline.v_area_latest_status for that source area:
          SELECT business_date FROM pipeline.v_area_latest_status
          WHERE pipeline_area = '<business_date_based_on_area>'
      Intended for reports whose own area has no task_runs but that align with
      another area's business_date (e.g. 'ptools_thirdparties').

    Idempotent: if a 'completed' record for this area + business_date already
    exists, nothing is written. Non-blocking: never raises.
    """
    if env_prefix is None:
        env_prefix = _get_env_prefix_fallback()
    if env_prefix is None:
        print("[WARNING] env_prefix not available — skipping area completion write")
        return

    try:
        task_log_table = f"`{env_prefix}-cdh`.pipeline.task_runs"
        exec_log_table = f"`{env_prefix}-cdh`.pipeline.area_runs"

        # --- Resolve business_date ---
        # skip_task_runs is the switch between the two modes:
        #   True  -> simple mode. business_date comes from the caller, either
        #            directly via `business_date` or via a lookup on
        #            `business_date_based_on_area`. task_runs is NEVER consulted.
        #   False -> derive mode. business_date is ALWAYS taken from task_runs.
        #            `business_date` and `business_date_based_on_area` are ignored.
        if skip_task_runs:
            # Simple mode: resolve business_date from the source area if no
            # explicit business_date was supplied.
            if business_date is None and business_date_based_on_area:
                business_date = _resolve_business_date_from_area(
                    spark, env_prefix, business_date_based_on_area
                )
                if business_date is None:
                    print(f"[WARNING] No v_area_latest_status row for source area "
                          f"'{business_date_based_on_area}' — skipping area completion")
                    return
                print(f"[area_runs] business_date {business_date} resolved from source area "
                      f"'{business_date_based_on_area}'")

            if business_date is None:
                print(f"[WARNING] skip_task_runs=True but no business_date supplied "
                      f"(and no business_date_based_on_area resolved) — skipping area completion")
                return
        else:
            # Derive mode: business_date must come from task_runs. Any supplied
            # business_date / business_date_based_on_area is ignored by design.
            if business_date is not None or business_date_based_on_area is not None:
                print(f"[INFO] skip_task_runs=False → ignoring supplied business_date / "
                      f"business_date_based_on_area; deriving from task_runs for area '{pipeline_area}'")
                business_date = None
            # Resolve from task_runs (option B)
            if business_date is None:
                row = spark.sql(f"""
                    SELECT MAX(business_date) AS bd
                    FROM {task_log_table}
                    WHERE pipeline_area = '{pipeline_area}'
                """).first()
                business_date = row["bd"] if row else None
                if business_date is None:
                    print(f"[WARNING] No task_runs rows for area '{pipeline_area}' — skipping area completion")
                    return
                business_date = str(business_date)

        # Idempotency: skip if already logged
        already_logged = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {exec_log_table}
            WHERE pipeline_area = '{pipeline_area}'
              AND business_date = CAST('{business_date}' AS DATE)
              AND status = 'completed'
        """).first()["cnt"]

        if already_logged > 0:
            print(f"[area_runs] Already logged: {pipeline_area} ({business_date}) — skipping")
            return

        # --- Write the completion record ---
        if skip_task_runs:
            # Simple mode: no task_runs dependency, just stamp completed now.
            spark.sql(f"""
                INSERT INTO {exec_log_table} (
                    pipeline_area, business_date, status,
                    start_timestamp, end_timestamp, duration_seconds,
                    triggered_by, databricks_run_id, error_message, logged_at
                ) VALUES (
                    '{pipeline_area}',
                    CAST('{business_date}' AS DATE),
                    'completed',
                    current_timestamp(),
                    current_timestamp(),
                    0,
                    'job_final_task',
                    {_sql_str(_get_databricks_run_id())},
                    NULL,
                    current_timestamp()
                )
            """)
        else:
            # Full mode: derive start/end from task_runs (last run cycle per task).
            spark.sql(f"""
                INSERT INTO {exec_log_table} (
                    pipeline_area, business_date, status,
                    start_timestamp, end_timestamp, duration_seconds,
                    triggered_by, databricks_run_id, error_message, logged_at
                )
                WITH ends AS (
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
                    'job_final_task',
                    {_sql_str(_get_databricks_run_id())},
                    NULL,
                    current_timestamp()
                FROM task_cycle
            """)

        print(f"[area_runs] AREA COMPLETED (job-driven): {pipeline_area} ({business_date})")

    except Exception as e:
        print(f"[WARNING] Area completion write failed (non-blocking): {e}")
