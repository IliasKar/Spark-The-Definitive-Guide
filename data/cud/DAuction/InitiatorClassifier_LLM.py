# Databricks notebook source
# DBTITLE 1,Initiator Classification — LLM-based
# MAGIC %md
# MAGIC ## Initiator Classification — LLM-based (replaces BERT + Excel)
# MAGIC
# MAGIC This notebook replaces the BERT classifier (`MLModel.py`) and Excel-based enrichment (`InitiatorMapper.py`).
# MAGIC
# MAGIC **3-step pipeline:**
# MAGIC 1. **Classification scope** — rows with null `OriginHastener` get `Initiator = 'Undefined'`; the rest go to step 2. Texts are normalised (upper-case, collapsed whitespace) for cache matching.
# MAGIC 2. **Cache lookup + `ai_classify`** — check previously classified texts in `{env_prefix}-ds.reference.initiator_classifier_cache`. Cache misses are sent to `ai_classify` on a SQL warehouse in batches. Results are persisted via MERGE (upsert). Auto-invalidates when labels or instructions change (SHA256 hash).
# MAGIC 3. **Enrichment** — resolve the predicted Initiator to 4 business columns (`IsHastenerServicer`, `Hastener`, `InitiatorCepalAB`, `PortfolioCategory`) from the `initiator_mapping` rows loaded in step 2.
# MAGIC
# MAGIC **Tables used:**
# MAGIC - `{env_prefix}-ds.reference.initiator_mapping` — label names, descriptions, and the 4 enrichment columns, loaded once at runtime and used by both steps 2 and 3
# MAGIC - `{env_prefix}-ds.reference.initiator_classifier_cache` — persistent LLM result cache (OriginHastener, Initiator, classified_date, labels_hash, instructions_hash)
# MAGIC - `{env_prefix}-ds.reference._temp_unmatched_classifier` — ephemeral temp table for batch ai_classify calls (dropped after use)
# MAGIC
# MAGIC **Cache behavior:**
# MAGIC - After first full run, subsequent runs require near-zero LLM calls (only truly new texts)
# MAGIC - Instructions change → full cache wipe; labels-only change → selective invalidation (fallback buckets only)
# MAGIC
# MAGIC **Prerequisites:** `final_df` (Spark DataFrame with `OriginHastener` column) and `env_prefix` must already exist.

# COMMAND ----------

# DBTITLE 1,File paths and row count checkpoint
import pandas as pd
import json

# --- Strip enrichment columns from any previous run, then materialize ---
# localCheckpoint: loses data when executors are evicted during the ~15 min LLM run.
# rdd.checkpoint / sparkContext: blocked on shared/USER_ISOLATION clusters (JVM_ATTRIBUTE_NOT_SUPPORTED).
# Fix: write final_df to a temp Delta table and read back.
# This fully breaks the lazy lineage and is fault-tolerant (data lives on S3).
_enrichment_cols = {'Initiator', 'IsHastenerServicer', 'Hastener', 'InitiatorCepalAB', 'PortfolioCategory'}
_cols = [c for c in final_df.columns if c not in _enrichment_cols]
_CP_TABLE = f"`{env_prefix}-ds`.`reference`.`_temp_daauction_df_checkpoint`"

final_df.select(*_cols).createOrReplaceTempView("_tmp_final_df_for_cp")
spark.sql(f"CREATE OR REPLACE TABLE {_CP_TABLE} AS SELECT * FROM _tmp_final_df_for_cp")
final_df = spark.table(_CP_TABLE)

# --- Checkpoint: save original row count for validation at the end ---
_original_row_count = final_df.count()
_original_columns = final_df.columns
print(f"Input final_df: {_original_row_count:,} rows, {len(_original_columns)} columns")

# COMMAND ----------

# DBTITLE 1,Step 1 — Classification scope and normalisation
# ---------------------------------------------------------------------------
# • OriginHastener IS NULL → Initiator = 'Undefined'
# • Otherwise              → Initiator = NULL (needs LLM, via ai_classify)
# ---------------------------------------------------------------------------
from pyspark.sql.functions import upper, trim, regexp_replace, col

final_df.createOrReplaceTempView("final_df_raw")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW final_with_scope AS
    SELECT
        f.*,
        CASE
            WHEN f.OriginHastener IS NULL THEN 'Undefined'
            ELSE NULL
        END AS Initiator
    FROM final_df_raw f
""")

# --- Normalized join key ---
# Collapses whitespace runs + case so pure formatting variants of the same
# OriginHastener text (extra/double spaces, different casing) share one
# cache entry and one LLM call instead of being treated as distinct texts.
# This is NEVER persisted to the cache table — only used in-memory for matching.
spark.table("final_with_scope").withColumn(
    "_norm_hastener",
    upper(trim(regexp_replace(col("OriginHastener"), r"\s+", " ")))
).createOrReplaceTempView("final_with_scope")

# --- Report match rates ---
_stats = spark.sql("""
    SELECT
        COUNT(*)                                          AS total,
        SUM(CASE WHEN OriginHastener IS NULL THEN 1 ELSE 0 END) AS null_hastener,
        SUM(CASE WHEN Initiator IS NULL THEN 1 ELSE 0 END)      AS need_llm
    FROM final_with_scope
""").collect()[0]

print(f"Step 1: Classification scope identified")
print(f"  Total rows:           {_stats['total']:,}")
print(f"  Null OriginHastener:  {_stats['null_hastener']:,} (set to 'Undefined')")
print(f"  Need LLM:             {_stats['need_llm']:,}")

# COMMAND ----------

# DBTITLE 1,Step 2 — Cache lookup + ai_classify (batched)
# ---------------------------------------------------------------------------
# Call ai_classify (v2) ONLY for rows where Initiator IS NULL.
# ai_classify v2 requires serverless SQL — not available on classic clusters.
# Strategy: write unmatched texts to a UC temp table, run ai_classify on a
# SQL warehouse via the Statement Execution REST API, fetch results back.
#
# Uses only `requests` (pre-installed) — no extra pip dependencies.
# Much cheaper than ai_query — uses Databricks' built-in classification model.
#
# BATCHING: processes in chunks of _BATCH_SIZE to stay within query timeouts
# and Foundation Model API rate limits. Safe for both bootstrap and production.
#
# CACHING: persists LLM results in a UC table so the same text is never
# classified twice. Auto-invalidates when labels/instructions change (via hash).
#
# MATCHING: both the cache lookup and the final result join use the
# normalized key (_norm_hastener, from the previous cell) instead of the raw
# OriginHastener string. This means formatting variants (extra spaces,
# different casing) of a text that's already in the cache are resolved from
# cache instead of triggering a new LLM call + a new cache row. Within a
# single run, unmatched texts are also deduplicated by _norm_hastener before
# being sent to ai_classify, so only one representative spelling per distinct
# text is ever classified or written to cache.
# ---------------------------------------------------------------------------
import json
import requests
import time
import math
import hashlib
import pandas as pd

# ▼▼▼ CONFIG ▼▼▼
_TEST_SAMPLE_SIZE = None         # set to integer (e.g. 1000) for test mode
_BATCH_SIZE = 500                # texts per ai_classify batch (~7 min each)
_MAX_WAIT_PER_BATCH = 900        # seconds to wait per batch (15 min safety)
# ▲▲▲ ------▲▲▲

# --- SQL warehouse connection config ---
_WAREHOUSE_ID = "818510884d309018"
_WORKSPACE_HOST = spark.conf.get("spark.databricks.workspaceUrl")
_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
_TEMP_TABLE = f"`{env_prefix}-ds`.`reference`.`_temp_unmatched_classifier`"
_CACHE_TABLE = f"`{env_prefix}-ds`.`reference`.`initiator_classifier_cache`"
_API_URL = f"https://{_WORKSPACE_HOST}/api/2.0/sql/statements"
_HEADERS = {"Authorization": f"Bearer {_TOKEN}", "Content-Type": "application/json"}

# --- Label descriptions + enrichment columns: loaded from UC mapping table ---
# `label`/`description` build the ai_classify label set below; the 4 Incoming*
# columns feed the enrichment in Step 3. DISTINCT collapses the table's
# one-row-per-portfolio duplicates down to one row per label, since none of
# these 6 columns vary across a label's duplicate rows.
_LABELS_TABLE = f"`{env_prefix}-ds`.`reference`.`initiator_mapping`"
_labels_rows = spark.sql(f"""
    SELECT DISTINCT
        label,
        description,
        IncomingIs_Hastener_Servicer,
        IncomingHastener,
        IncomingInitiator_Cepal_AB,
        IncomingPortfolio_category
    FROM {_LABELS_TABLE}
""").collect()
_label_descriptions = {row["label"]: row["description"] for row in _labels_rows}
print(f"Loaded {len(_label_descriptions)} label descriptions from {_LABELS_TABLE}")

# --- Build labels JSON and instructions for ai_classify v2 ---
_labels_json = json.dumps(dict(sorted(_label_descriptions.items())), ensure_ascii=False)
print(f"Labels JSON: {len(_labels_json):,} chars")

_instructions = """Greek legal auction initiator (επισπεύδων) classification.

PRIORITY RULES (apply in order):

1. SPV ALWAYS WINS over servicer and bank:
   - If ANY SPV name from the label list appears in the text, classify under that SPV.
   - This applies even if a bank (ALPHA, EUROBANK, ΠΕΙΡΑΙΩΣ, ΕΘΝΙΚΗ) or servicer is also mentioned.
   - Example: "CEPAL HELLAS ... GALAXY II ... ειδικής διαδόχου της ALPHA ΤΡΑΠΕΖΑΣ" → Galaxy II (NOT Alpha Bank)

2. Ειδική διάδοχος (special successor) means securitization:
   - Phrases like "ειδικής διαδόχου της [BANK]", "διάδοχος της [BANK]" mean the SPV acquired the bank's loans.
   - ALWAYS classify under the SPV, NEVER the bank.
   - The bank is just the original lender — ignore it for classification.

3. Unknown SPV + known servicer → use servicer Undefined variant:
   - If the text mentions a servicer (CEPAL, INTRUM, doValue) managing an SPV NOT in the label list:
     → Use "Cepal Undefined Initiator", "Intrum Undefined Initiator", or "doValue Undefined Initiator"
   - Do NOT use "Other" for financial entities — "Other" is only for non-financial entities.

4. Bank labels only when the bank is the SOLE entity (no SPV and no servicer present).

5. Servicer alone (no SPV mentioned) → servicer Undefined variant.

6. PILLAR FINANCE is managed by QQuant → classify as QQuant regardless of which servicer appears.

7. Private individuals (Greek names), small companies, numeric codes → Other.

8. Truly unidentifiable text → Undefined."""

# --- Compute hashes for cache invalidation ---
# instructions_hash: if instructions change, full cache wipe (rules affect all classifications)
# labels_hash: if only labels change, selective invalidation (re-classify fallback buckets only)
_instructions_hash = hashlib.sha256(_instructions.encode()).hexdigest()[:12]
_labels_hash = hashlib.sha256(_labels_json.encode()).hexdigest()[:12]
print(f"Instructions hash: {_instructions_hash}")
print(f"Labels hash: {_labels_hash}")

# Fallback buckets: cached entries that might reclassify when a new label is added
_FALLBACK_INITIATORS = {'Other', 'Undefined', 'Cepal Undefined Initiator', 'Intrum Undefined Initiator', 'doValue Undefined Initiator'}

# --- Cache table: check existence and selective/full invalidation ---
_cache_exists = spark.catalog.tableExists(_CACHE_TABLE)
if _cache_exists:
    # Migration: add instructions_hash column if missing (first run after upgrade)
    _cache_columns = [c.name for c in spark.table(_CACHE_TABLE).schema]
    if 'instructions_hash' not in _cache_columns:
        print("Migrating cache table: adding instructions_hash column...")
        spark.sql(f"ALTER TABLE {_CACHE_TABLE} ADD COLUMN instructions_hash STRING")
        spark.sql(f"UPDATE {_CACHE_TABLE} SET instructions_hash = 'migrated'")

    _stored = spark.sql(f"SELECT DISTINCT instructions_hash, labels_hash FROM {_CACHE_TABLE} LIMIT 1").collect()
    if _stored:
        _stored_instr_hash = _stored[0]["instructions_hash"]
        _stored_labels_hash = _stored[0]["labels_hash"]
    else:
        _stored_instr_hash = _instructions_hash
        _stored_labels_hash = _labels_hash

    if _stored_instr_hash != _instructions_hash:
        # Instructions changed — full wipe (rules affect all classifications)
        print(f"⚠️  Instructions changed (stored={_stored_instr_hash}, current={_instructions_hash}) — FULL cache invalidation")
        spark.sql(f"DELETE FROM {_CACHE_TABLE}")
        _cache_count = 0
    elif _stored_labels_hash != _labels_hash:
        # Only labels changed — selective invalidation
        # Re-classify fallback buckets (Other/Undefined/servicer-undefined)
        # These are the entries that might now match a newly added label
        _fallback_list = "', '".join(_FALLBACK_INITIATORS)
        _deleted = spark.sql(f"""
            DELETE FROM {_CACHE_TABLE}
            WHERE Initiator IN ('{_fallback_list}')
        """)
        _remaining = spark.sql(f"SELECT COUNT(*) AS n FROM {_CACHE_TABLE}").collect()[0]["n"]
        print(f"⚠️  Labels changed (stored={_stored_labels_hash}, current={_labels_hash}) — SELECTIVE cache invalidation")
        print(f"  Deleted fallback entries (Other/Undefined/servicer-undefined) from cache")
        print(f"  Remaining valid cache entries: {_remaining:,}")
        # Update hashes on remaining entries so next run sees current hashes
        spark.sql(f"""
            UPDATE {_CACHE_TABLE}
            SET labels_hash = '{_labels_hash}', instructions_hash = '{_instructions_hash}'
        """)
        _cache_count = _remaining
    else:
        _cache_count = spark.sql(f"SELECT COUNT(*) AS n FROM {_CACHE_TABLE}").collect()[0]["n"]
        print(f"Cache table exists with {_cache_count:,} entries (instructions={_instructions_hash}, labels={_labels_hash})")
else:
    print(f"Cache table does not exist — will be created after first classification")
    _cache_count = 0

# --- Helper: execute SQL on SQL warehouse via REST API ---
def _run_on_warehouse(sql_statement, warehouse_id=_WAREHOUSE_ID, poll_interval=10, max_wait=_MAX_WAIT_PER_BATCH):
    """Execute SQL on a SQL warehouse and return results as a pandas DataFrame."""
    payload = {
        "warehouse_id": warehouse_id,
        "statement": sql_statement,
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }
    resp = requests.post(_API_URL, headers=_HEADERS, json=payload)
    resp.raise_for_status()
    result = resp.json()

    # Poll if still running
    elapsed = 0
    while result.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if elapsed >= max_wait:
            raise TimeoutError(f"SQL warehouse query timed out after {max_wait}s")
        time.sleep(poll_interval)
        elapsed += poll_interval
        stmt_id = result["statement_id"]
        poll_resp = requests.get(f"{_API_URL}/{stmt_id}", headers=_HEADERS)
        poll_resp.raise_for_status()
        result = poll_resp.json()

    state = result.get("status", {}).get("state", "UNKNOWN")
    if state == "FAILED":
        error = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL warehouse query failed: {error}")
    if state != "SUCCEEDED":
        raise RuntimeError(f"Unexpected query state: {state}")

    # Parse results
    columns = [c["name"] for c in result["manifest"]["schema"]["columns"]]
    rows = result.get("result", {}).get("data_array", [])
    return pd.DataFrame(rows, columns=columns)


# --- Build reverse mapping: normalized/variant label → original label ---
# Fixes Greek→Latin char substitution and case drift from ai_classify.
# Built here (not inside batching block) so it's available for cache hits too.
_GREEK_LOOKALIKES = 'ΑΒΓΔΕΖΗΙΚΛΜΝΟΡΤΥΧ'
_LATIN_EQUIVALENTS = 'ABGDEZHIKLMNOPTYX'
_normalized_to_original = {}
for _label in _label_descriptions.keys():
    _norm = _label
    for _g, _l in zip(_GREEK_LOOKALIKES, _LATIN_EQUIVALENTS):
        _norm = _norm.replace(_g, _l)
    _normalized_to_original[_norm] = _label
    _normalized_to_original[_norm.upper()] = _label
    _normalized_to_original[_label.upper()] = _label
# Exact originals map to themselves (highest priority)
for _label in _label_descriptions.keys():
    _normalized_to_original[_label] = _label
# Handle 'nan' from pandas NaN→string conversion
_normalized_to_original['nan'] = 'Undefined'
print(f"Reverse map built: {len(_normalized_to_original)} entries")

# --- Classify unmatched rows (BATCHED) ---
_need_llm = _stats['need_llm']

if _need_llm > 0:
    # --- Cache lookup: resolve what we can from previous LLM runs ---
    # Matched on the normalized key so whitespace/case variants of a text
    # already in the cache are resolved without a new LLM call.
    if _cache_exists and _cache_count > 0:
        spark.table(_CACHE_TABLE).withColumn(
            "_norm_hastener",
            upper(trim(regexp_replace(col("OriginHastener"), r"\s+", " ")))
        ).createOrReplaceTempView("_cache_normalized")

        spark.sql("""
            CREATE OR REPLACE TEMP VIEW _cache_hits AS
            SELECT f._norm_hastener, MIN(c.Initiator) AS Cached_Initiator
            FROM final_with_scope f
            INNER JOIN _cache_normalized c
                ON f._norm_hastener = c._norm_hastener
            WHERE f.Initiator IS NULL
            GROUP BY f._norm_hastener
        """)
        _cache_hit_count = spark.sql("SELECT COUNT(*) AS n FROM _cache_hits").collect()[0]["n"]
        print(f"  Cache hits: {_cache_hit_count:,} unique texts resolved from cache")

        # Reverse-map cached labels: fix stale entries with Greek→Latin corruption
        _cache_hits_pdf = spark.sql("SELECT * FROM _cache_hits").toPandas()
        _cache_hits_pdf["Cached_Initiator"] = _cache_hits_pdf["Cached_Initiator"].map(
            lambda x: _normalized_to_original.get(x, x)
        )
        spark.createDataFrame(_cache_hits_pdf).createOrReplaceTempView("_cache_hits")
    else:
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW _cache_hits AS
            SELECT CAST(NULL AS STRING) AS OriginHastener, CAST(NULL AS STRING) AS _norm_hastener, CAST(NULL AS STRING) AS Cached_Initiator
            WHERE 1=0
        """)
        _cache_hit_count = 0

    # Build the unmatched unique view (excluding cache hits), deduplicated by
    # normalized text — one representative raw spelling per distinct text,
    # so formatting variants aren't sent to ai_classify or cached separately.
    _limit_clause = f"LIMIT {_TEST_SAMPLE_SIZE}" if _TEST_SAMPLE_SIZE else ""
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW _unmatched_unique AS
        SELECT f._norm_hastener, MIN(f.OriginHastener) AS OriginHastener
        FROM final_with_scope f
        LEFT JOIN _cache_hits ch ON f._norm_hastener = ch._norm_hastener
        WHERE f.Initiator IS NULL
          AND ch._norm_hastener IS NULL
        GROUP BY f._norm_hastener
        {_limit_clause}
    """)

    _unique_count = spark.sql("SELECT COUNT(*) AS n FROM _unmatched_unique").collect()[0]["n"]
    if _TEST_SAMPLE_SIZE:
        print(f"⚠️  TEST MODE: sampling {_unique_count} of unique texts")
    print(f"Classification: {_need_llm:,} total rows needing it, {_cache_hit_count} from cache, {_unique_count} truly new texts to classify")

    # --- Step A: Write unmatched texts to UC table with row numbers ---
    if _unique_count > 0:
        print(f"Writing {_unique_count} unmatched texts to {_TEMP_TABLE}...")
        spark.sql(f"""
            CREATE OR REPLACE TABLE {_TEMP_TABLE} AS
            SELECT
                OriginHastener,
                _norm_hastener,
                ROW_NUMBER() OVER (ORDER BY OriginHastener) AS _row_num
            FROM _unmatched_unique
        """)

        # --- Step B: Process in batches ---
        # Escape for SQL string literals (single quotes)
        _instructions_sql = _instructions.replace("'", "\\\'")

        # Greek-to-Latin lookalike normalization (fixes mixed-alphabet SPV names)
        # Applied to BOTH input text AND label descriptions for consistency

        # Normalize label descriptions: apply same TRANSLATE to label JSON
        _labels_json_normalized = _labels_json
        for _g, _l in zip(_GREEK_LOOKALIKES, _LATIN_EQUIVALENTS):
            _labels_json_normalized = _labels_json_normalized.replace(_g, _l)
        _labels_sql = _labels_json_normalized.replace("'", "\\\'")

        _num_batches = math.ceil(_unique_count / _BATCH_SIZE)
        print(f"\nProcessing {_unique_count} texts in {_num_batches} batch(es) of {_BATCH_SIZE}")
        print("=" * 60)

        _all_batch_results = []
        _failed_batches = []
        _total_elapsed = 0
        _MAX_RETRIES = 3
        _RETRY_BACKOFF = [30, 60, 120]  # seconds between retries

        for _batch_idx in range(_num_batches):
            _row_start = _batch_idx * _BATCH_SIZE + 1
            _row_end = min((_batch_idx + 1) * _BATCH_SIZE, _unique_count)
            _batch_n = _row_end - _row_start + 1

            _warehouse_query = f"""
            SELECT
                OriginHastener,
                _norm_hastener,
                variant_get(
                    ai_classify(
                        TRANSLATE(OriginHastener, '{_GREEK_LOOKALIKES}', '{_LATIN_EQUIVALENTS}'),
                        '{_labels_sql}',
                        MAP('instructions', '{_instructions_sql}')
                    ),
                    '$.response[0]',
                    'STRING'
                ) AS LLM_Initiator
            FROM {_TEMP_TABLE}
            WHERE _row_num BETWEEN {_row_start} AND {_row_end}"""

            _batch_success = False
            for _attempt in range(_MAX_RETRIES):
                try:
                    _t0 = time.time()
                    _batch_pdf = _run_on_warehouse(_warehouse_query)
                    _batch_elapsed = time.time() - _t0
                    _total_elapsed += _batch_elapsed
                    _all_batch_results.append(_batch_pdf)

                    _done = sum(len(r) for r in _all_batch_results)
                    _per_text = _batch_elapsed / max(len(_batch_pdf), 1)
                    _remaining = (_unique_count - _done) * _per_text
                    print(
                        f"  Batch {_batch_idx+1}/{_num_batches}: "
                        f"{len(_batch_pdf)} rows in {_batch_elapsed:.0f}s "
                        f"({_per_text:.2f}s/text) | "
                        f"Done: {_done}/{_unique_count} | "
                        f"ETA: {_remaining/60:.0f} min"
                    )
                    _batch_success = True
                    break
                except (TimeoutError, requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
                    _wait = _RETRY_BACKOFF[min(_attempt, len(_RETRY_BACKOFF)-1)]
                    print(f"  ⚠️ Batch {_batch_idx+1} attempt {_attempt+1}/{_MAX_RETRIES} failed (transient): {e}")
                    if _attempt < _MAX_RETRIES - 1:
                        print(f"    Retrying in {_wait}s...")
                        time.sleep(_wait)
                except Exception as e:
                    # Non-transient error — do not retry
                    print(f"  ❌ Batch {_batch_idx+1}/{_num_batches} FAILED (non-transient): {e}")
                    break

            if not _batch_success:
                print(f"  ❌ Batch {_batch_idx+1}/{_num_batches} FAILED after {_MAX_RETRIES} attempts (rows {_row_start}-{_row_end})")
                _failed_batches.append((_batch_idx, _row_start, _row_end))

            # --- Persist batch to cache immediately (interrupt-safe) ---
            if _batch_success:
                _batch_pdf["LLM_Initiator"] = _batch_pdf["LLM_Initiator"].map(
                    lambda x: _normalized_to_original.get(x, x)
                )
                _batch_spark = spark.createDataFrame(_batch_pdf)
                _batch_spark.createOrReplaceTempView("_batch_to_merge")
                if not _cache_exists:
                    spark.sql(f"""
                        CREATE TABLE {_CACHE_TABLE} AS
                        SELECT
                            OriginHastener,
                            LLM_Initiator AS Initiator,
                            CURRENT_DATE() AS classified_date,
                            '{_labels_hash}' AS labels_hash,
                            '{_instructions_hash}' AS instructions_hash
                        FROM _batch_to_merge
                    """)
                    _cache_exists = True  # subsequent batches use MERGE
                else:
                    spark.sql(f"""
                        MERGE INTO {_CACHE_TABLE} AS target
                        USING (
                            SELECT
                                OriginHastener,
                                LLM_Initiator AS Initiator,
                                CURRENT_DATE() AS classified_date,
                                '{_labels_hash}' AS labels_hash,
                                '{_instructions_hash}' AS instructions_hash
                            FROM _batch_to_merge
                        ) AS source
                        ON target.OriginHastener = source.OriginHastener
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                    """)
                _persisted = sum(len(r) for r in _all_batch_results)
                print(f"    💾 Batch {_batch_idx+1} persisted to cache ({len(_batch_pdf)} rows, {_persisted}/{_unique_count} total saved)")

            # --- Abort if >20% of batches have failed ---
            _batches_attempted = _batch_idx + 1
            if _batches_attempted >= 5 and len(_failed_batches) / _batches_attempted > 0.2:
                raise RuntimeError(
                    f"ABORTING: {len(_failed_batches)}/{_batches_attempted} batches failed "
                    f"(>{20}% failure rate). Check SQL warehouse health and retry."
                )

        print("=" * 60)

        if _failed_batches:
            print(f"⚠️  {len(_failed_batches)} batch(es) failed: {_failed_batches}")
            print(f"  Successfully classified {sum(len(r) for r in _all_batch_results)} of {_unique_count} texts")

        # --- Step C: Combine all batch results → Spark temp view ---
        # Cache was already persisted per-batch above — just build the
        # in-memory view for the downstream dataset join.
        if _all_batch_results:
            _result_pdf = pd.concat(_all_batch_results, ignore_index=True)
            # Reverse mapping already applied per-batch during cache persist
            print(f"Total classified: {len(_result_pdf)} rows in {_total_elapsed:.0f}s ({_total_elapsed/60:.1f} min)")

            _classified_spark = spark.createDataFrame(_result_pdf)
            _classified_spark.createOrReplaceTempView("llm_classified_validated")

            print(f"✅ All batches persisted to cache: {len(_result_pdf):,} entries across {_num_batches - len(_failed_batches)} batch(es)")

            # Show test results
            if _TEST_SAMPLE_SIZE:
                print("\n--- ai_classify v2 Test Results (sample) ---")
                display(spark.sql("SELECT OriginHastener, LLM_Initiator FROM llm_classified_validated ORDER BY LLM_Initiator"))
        else:
            # All batches failed — create empty view so downstream doesn't break
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW llm_classified_validated AS
                SELECT CAST(NULL AS STRING) AS OriginHastener, CAST(NULL AS STRING) AS _norm_hastener, CAST(NULL AS STRING) AS LLM_Initiator
                WHERE 1=0
            """)
            print("❌ All batches failed — no classifications available")

        # --- Step E: Cleanup temp table ---
        spark.sql(f"DROP TABLE IF EXISTS {_TEMP_TABLE}")
        print(f"Cleaned up {_TEMP_TABLE}")

    else:
        # All unmatched texts were resolved from cache — no LLM needed
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW llm_classified_validated AS
            SELECT CAST(NULL AS STRING) AS OriginHastener, CAST(NULL AS STRING) AS _norm_hastener, CAST(NULL AS STRING) AS LLM_Initiator
            WHERE 1=0
        """)
        print(f"✅ All {_cache_hit_count} unmatched texts resolved from cache — no LLM calls needed!")

    # --- Step D: Merge predictions back into the full dataset ---
    # Priority: pre-set 'Undefined' (null OriginHastener) > cache > new LLM > Undefined fallback
    # Joined on the normalized key so EVERY raw spelling variant in the real
    # data (not just the one representative sent to ai_classify) picks up
    # the resolved Initiator. _norm_hastener is dropped before this becomes
    # final_df so it never leaks into the business dataset schema.
    # Note: reverse mapping (normalized→original labels) is already applied
    # in Step C via pandas .map() before caching, so no SQL-level remap needed.
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW final_with_initiator AS
        SELECT
            f.* EXCEPT(Initiator, _norm_hastener),
            CASE
                WHEN f.Initiator IS NOT NULL THEN f.Initiator
                WHEN ch.Cached_Initiator IS NOT NULL THEN ch.Cached_Initiator
                WHEN llm.LLM_Initiator IS NOT NULL THEN llm.LLM_Initiator
                ELSE 'Undefined'
            END AS Initiator
        FROM final_with_scope f
        LEFT JOIN _cache_hits ch
            ON f._norm_hastener = ch._norm_hastener
        LEFT JOIN llm_classified_validated llm
            ON f._norm_hastener = llm._norm_hastener
    """)

    _llm_stats = spark.sql("""
        SELECT
            SUM(CASE WHEN LLM_Initiator != 'Undefined' THEN 1 ELSE 0 END) AS classified,
            SUM(CASE WHEN LLM_Initiator = 'Undefined' THEN 1 ELSE 0 END) AS undefined
        FROM llm_classified_validated
    """).collect()[0]
    print(f"\nStep 2 complete:")
    print(f"  From cache:     {_cache_hit_count}")
    print(f"  New LLM calls:  {_llm_stats['classified'] or 0} classified, {_llm_stats['undefined'] or 0} undefined")

else:
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW final_with_initiator AS
        SELECT * EXCEPT(_norm_hastener) FROM final_with_scope
    """)
    print(f"Step 2: No classification needed — all OriginHastener values were null")

# COMMAND ----------

# DBTITLE 1,Step 3 — Enrichment lookup (Initiator → 4 business columns)
# ---------------------------------------------------------------------------
# Attach IsHastenerServicer, Hastener, InitiatorCepalAB and PortfolioCategory
# from the initiator_mapping rows loaded in Step 2 (one row per label).
# Nulls in the mapping table become '' to keep the columns non-null.
# ---------------------------------------------------------------------------

_mapping_df = spark.createDataFrame(
    [
        (
            row["label"],
            row["IncomingIs_Hastener_Servicer"] or "",
            row["IncomingHastener"] or "",
            row["IncomingInitiator_Cepal_AB"] or "",
            row["IncomingPortfolio_category"] or "",
        )
        for row in _labels_rows
    ],
    "Initiator STRING, IsHastenerServicer STRING, Hastener STRING, "
    "InitiatorCepalAB STRING, PortfolioCategory STRING",
)

final_df = spark.table("final_with_initiator").join(
    _mapping_df, on="Initiator", how="left"
).select(
    *spark.table("final_with_initiator").columns,
    "IsHastenerServicer", "Hastener", "InitiatorCepalAB", "PortfolioCategory",
)

print(f"Step 3: Enrichment complete")
print(f"  Labels mapped:  {_mapping_df.count()}")
print(f"  Output rows:    {final_df.count():,}")
print(f"  Columns added:  Initiator, IsHastenerServicer, Hastener, InitiatorCepalAB, PortfolioCategory")

# COMMAND ----------

# DBTITLE 1,Validation
# ---------------------------------------------------------------------------
# Final validation: row count, schema check, sample output.
# ---------------------------------------------------------------------------

_final_count = final_df.count()
assert _final_count == _original_row_count, (
    f"ROW COUNT MISMATCH: expected {_original_row_count:,}, got {_final_count:,}"
)
print(f"✅ Row count verified: {_final_count:,} (matches input)")

# Check that all 5 new columns exist
_required_cols = ["Initiator", "IsHastenerServicer", "Hastener", "InitiatorCepalAB", "PortfolioCategory"]
_missing = [c for c in _required_cols if c not in final_df.columns]
assert not _missing, f"Missing columns: {_missing}"
print(f"✅ All 5 enrichment columns present")

if _TEST_SAMPLE_SIZE:
    _undef_count = final_df.filter("Initiator = 'Undefined'").count()
    print(f"⚠️  TEST MODE: {_undef_count:,} rows have Initiator='Undefined' (expected — only {_TEST_SAMPLE_SIZE} unique texts were classified)")

# Sample output
print("\n--- Sample (10 rows with LLM-classified Initiator) ---")
display(
    final_df.select(
        "OriginHastener", "Initiator", "IsHastenerServicer",
        "Hastener", "InitiatorCepalAB", "PortfolioCategory"
    ).filter("Initiator != 'Undefined'").limit(10)
)

# --- Fallback label counts (drift visibility) ---
print("\n--- Fallback & Top Initiator Counts ---")
_fallback_counts = final_df.filter(
    "Initiator IN ('Undefined', 'Other', 'Cepal Undefined Initiator', 'Intrum Undefined Initiator', 'doValue Undefined Initiator')"
).groupBy("Initiator").count().orderBy("count", ascending=False).collect()
for row in _fallback_counts:
    print(f"  {row['Initiator']}: {row['count']:,}")
print("\n--- Top 10 Initiators ---")
_top10 = final_df.groupBy("Initiator").count().orderBy("count", ascending=False).limit(10).collect()
for row in _top10:
    print(f"  {row['Initiator']}: {row['count']:,}")

# --- Cleanup Delta checkpoint table ---
try:
    spark.sql(f"DROP TABLE IF EXISTS {_CP_TABLE}")
    print(f"\n✅ Checkpoint table cleaned up: {_CP_TABLE}")
except Exception as _cp_err:
    print(f"\n⚠️  Could not clean checkpoint table {_CP_TABLE}: {_cp_err}")