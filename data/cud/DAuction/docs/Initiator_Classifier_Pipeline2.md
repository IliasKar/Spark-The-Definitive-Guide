# Initiator Classification Pipeline — Technical Documentation

## 1. Overview

The Initiator Classification pipeline determines which entity (SPV, bank, servicer) initiated each Greek legal auction, based on the free-text `OriginHastener` field. It runs daily as part of the `DAuction` derivative table build.

**What it replaced:** The old BERT-based classifier (`MLModel.py`) + Excel-based enrichment (`InitiatorMapper.py`). The BERT model required a dedicated ML cluster, manual retraining, and had degrading accuracy on new SPVs.

**What it uses now:** Databricks' built-in `ai_classify` SQL function (serverless, no model management) combined with an exact-match dictionary and a persistent cache layer.

**Output:** 5 columns added to `final_df`:
- `Initiator` — classified label (e.g., "Galaxy II", "Phoenix", "Alpha Bank")
- `IsHastenerServicer` — whether the hastener is the servicer
- `Hastener` — normalized hastener name
- `InitiatorCepalAB` — Cepal A/B classification
- `PortfolioCategory` — portfolio category

---

## 2. Architecture Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│  DAuction.ipynb                                                      │
│                                                                      │
│  primary.Auction + derivative.DAuctionRealEstate + newsphone_basic   │
│         │                                                            │
│         ▼                                                            │
│  final_df (with OriginHastener column)                               │
│         │                                                            │
│         ▼  %run ./InitiatorClassifier_LLM                            │
└─────────┼────────────────────────────────────────────────────────────┘
          │
┌─────────▼────────────────────────────────────────────────────────────┐
│  InitiatorClassifier_LLM.ipynb                                        │
│                                                                       │
│  Checkpoint: materialize final_df to temp Delta table                 │
│     │  (breaks lineage, survives executor evictions)                  │
│     ▼                                                                 │
│  Step 1: Exact-match dictionary (HastenerTrainData.xlsx)              │
│     │  ~74% matched                                                   │
│     ▼                                                                 │
│  Step 2a: Exact-match JOIN on final_df                                │
│     │  Assigns Initiator from dictionary or marks NULL for LLM        │
│     ▼                                                                 │
│  Step 2b: Cache lookup + ai_classify via SQL warehouse REST API       │
│     │  Cache resolves previously seen texts                           │
│     │  Only truly new texts → LLM (batched, 500/batch)                │
│     │  Results reverse-mapped and persisted to cache                  │
│     ▼                                                                 │
│  Step 3: Homoglyph normalization + Enrichment JOIN                    │
│     │  Normalize Initiator via _normalized_to_original UDF            │
│     │  LEFT JOIN Initiator_Mapping.xlsx → 4 business columns          │
│     ▼                                                                 │
│  Step 4: Validation (row count, schema, fallback counts)              │
│     │  Cleanup checkpoint table                                       │
│     ▼                                                                 │
│  Output: final_df with 5 enrichment columns                          │
└───────────────────────────────────────────────────────────────────────┘
```

---

## 3. Step-by-Step Pipeline Description

### Checkpoint — Materialize `final_df`

Before any classification, `final_df` is written to a temporary Delta table and read back:

```python
_CP_TABLE = f"`{env_prefix}-ds`.`reference`.`_temp_daauction_df_checkpoint`"
spark.sql(f"CREATE OR REPLACE TABLE {_CP_TABLE} AS SELECT * FROM _tmp_final_df_for_cp")
final_df = spark.table(_CP_TABLE)
```

**Why not `localCheckpoint()`?** On shared/USER_ISOLATION clusters (Spark Connect), `sparkContext`-based operations like `rdd.checkpoint` and `sc.broadcast` are blocked (`JVM_ATTRIBUTE_NOT_SUPPORTED`). Additionally, `localCheckpoint` loses data when executors are evicted during the ~15-minute LLM run. Writing to Delta on S3 is fault-tolerant and fully breaks the lazy lineage.

Any enrichment columns from a previous run (`Initiator`, `IsHastenerServicer`, `Hastener`, `InitiatorCepalAB`, `PortfolioCategory`) are stripped before checkpointing.

---

### Step 1 — Exact-Match Dictionary

**Source:** `HastenerTrainData.xlsx` (on Unity Catalog Volume `/Volumes/{env_prefix}-cdh/derivative/DAuction_ML/`)

**Logic:**
- Load rows where `Approved == "yes"`
- Strip whitespace from Initiator labels
- Deduplicate by `HastenerDescr` keeping highest `version` (most recent correction)
- Create temp view `verified_initiators`

**Result:** A lookup table of `HastenerDescr → Initiator` with ~N unique entries.

**Coverage:** ~74% of daily records resolved here with 100% accuracy.

---

### Step 2a — Exact-Match Join

LEFT JOIN `final_df.OriginHastener` against `verified_initiators.HastenerDescr`:

| Condition | Initiator value |
|-----------|----------------|
| OriginHastener IS NULL | `'Undefined'` |
| Match found in dictionary | Verified label |
| No match | `NULL` (needs LLM/cache) |

Reports match rates (total, exact-matched, null hastener, need LLM).

---

### Step 2b — Cache Lookup + ai_classify

This step handles rows where `Initiator IS NULL` after the exact-match join.

#### Label Loading

Labels are loaded from a UC table (not hardcoded):

```
`{env_prefix}-ds`.`reference`.`initiator_classifier_labels`
```

Each row has a `label` and `description`. Any labels present in `verified_initiators` but missing from the labels table are added with a self-description. The final label count is typically ~100+ classes.

#### Cache Invalidation

**Table:** `` `{env_prefix}-ds`.`reference`.`initiator_classifier_cache` ``

**Schema:**

| Column | Type | Description |
|--------|------|-------------|
| OriginHastener | STRING | Hastener text (primary key for MERGE) |
| Initiator | STRING | Previously classified label |
| classified_date | DATE | When classification was made |
| labels_hash | STRING | SHA256[:12] of labels JSON |
| instructions_hash | STRING | SHA256[:12] of instructions text |

**Dual-hash invalidation:**

| What changed | Action | Cost |
|---|---|---|
| Nothing | Cache used as-is | Zero LLM calls |
| Only labels (new SPV added) | Delete fallback bucket entries only | Re-classify small subset |
| Instructions changed | Full cache wipe | Re-classify everything |

**Fallback buckets** (deleted on selective invalidation): `Other`, `Undefined`, `Cepal Undefined Initiator`, `Intrum Undefined Initiator`, `doValue Undefined Initiator`.

**Rationale:** When a new SPV is added, only texts previously classified as "couldn't identify" might now match the new label. Texts already classified as a specific SPV won't change.

**Migration:** If `instructions_hash` column is missing (first run after upgrade), it's added via ALTER TABLE and set to `'migrated'`, triggering a one-time full wipe.

#### Greek Homoglyph Normalization (Reverse Map)

Before any LLM calls, a reverse mapping is built:

```python
_GREEK_LOOKALIKES = 'ΑΒΓΔΕΖΗΙΚΛΜΝΟΡΤΥΧ'  # 17 uppercase Greek letters
_LATIN_EQUIVALENTS = 'ABGDEZHIKLMNOPTYX'   # their Latin lookalikes
```

For each label, the code generates the Latin-transliterated form and maps it back:
- `GΑΙΑ SECURITISATION` → normalized `GAIA SECURITISATION` → maps back to `GΑΙΑ SECURITISATION`
- `ΑΑΔΕ` → normalized `AADE` → maps back to `ΑΑΔΕ`
- `ΘΕΑ ΑΡΤΕΜΙΣ` → normalized `ΘEA APTEMIΣ` → maps back to `ΘΕΑ ΑΡΤΕΜΙΣ`

Also includes `.upper()` variants and `'nan' → 'Undefined'`.

**Why this works for all labels:**
- **Pure Latin labels** (Galaxy II, Alpha Bank): no Greek chars, pass through unchanged
- **Mixed labels** (GΑΙΑ, GLΑDΕ, ΑΑΔΕ): only the lookalike uppercase chars get swapped, reverse map catches the all-Latin output
- **Fully Greek labels** (Πειραιώς, Συνεταιριστική...): letters with no Latin lookalike (Π, Σ, Θ, all lowercase) pass through unchanged in both directions — the partially-swapped form is still unique and reversible

#### Cache Lookup

INNER JOIN unmatched rows against cache. Cache hits have the `Cached_Initiator` reverse-mapped via `_normalized_to_original` to fix any stale entries with Greek→Latin corruption.

#### ai_classify via SQL Warehouse

**Why a SQL warehouse?** `ai_classify` requires serverless SQL, not available on classic/interactive clusters. Communication is via the Statement Execution REST API.

**Flow:**
1. Exclude cache hits from unmatched rows → `_unmatched_unique`
2. Write to temp UC table with row numbers: `` `{env_prefix}-ds`.`reference`.`_temp_unmatched_classifier` ``
3. Process in batches of `_BATCH_SIZE` (default 500)
4. Each batch runs `ai_classify` with TRANSLATE-normalized input and labels
5. Collect results, reverse-map labels via pandas `.map()`
6. MERGE results into cache table
7. Drop temp table

**ai_classify query:**
```sql
SELECT
    OriginHastener,
    variant_get(
        ai_classify(
            TRANSLATE(OriginHastener, 'ΑΒΓΔΕΖΗΙΚΛΜΝΟΡΤΥΧ', 'ABGDEZHIKLMNOPTYX'),
            '<normalized_labels_json>',
            MAP('instructions', '<classification_rules>')
        ),
        '$.response[0]',
        'STRING'
    ) AS LLM_Initiator
FROM {_TEMP_TABLE}
WHERE _row_num BETWEEN {start} AND {end}
```

Both the input text and label descriptions JSON are TRANSLATE-normalized with the same character map, so classification happens in a consistent "best-effort Latin" space.

**Retry logic:**
- Up to 3 attempts per batch with backoff (30s, 60s, 120s)
- Retries on: TimeoutError, HTTP errors, connection errors
- Non-transient errors fail immediately
- Pipeline aborts if >20% of batches fail (after at least 5 attempted)

#### Merge Back to Dataset

Priority: exact-match > cache > new LLM > `'Undefined'`

```sql
CASE
    WHEN f.Initiator IS NOT NULL THEN f.Initiator          -- exact-match
    WHEN ch.Cached_Initiator IS NOT NULL THEN ch.Cached_Initiator  -- cache
    WHEN llm.LLM_Initiator IS NOT NULL THEN llm.LLM_Initiator     -- new LLM
    ELSE 'Undefined'
END AS Initiator
```

---

### Step 3 — Homoglyph Normalization + Enrichment Join

**Normalization:** Before the enrichment JOIN, a PySpark UDF applies `_normalized_to_original` to the `Initiator` column. This ensures that any Latin-transliterated variant from the LLM or cache (e.g., `GAIA SECURITISATION`) is mapped back to the canonical form that matches the mapping file (e.g., `GΑΙΑ SECURITISATION`).

```python
_norm_map_snapshot = _normalized_to_original.copy()

@udf(returnType=StringType())
def _normalize_initiator(value):
    if value is None:
        return value
    return _norm_map_snapshot.get(value, value)
```

The dict is captured as a plain Python closure (~200 entries). `sc.broadcast()` is intentionally avoided because it's blocked on Spark Connect shared clusters.

**Enrichment JOIN:** LEFT JOIN normalized `Initiator` against `Initiator_Mapping.xlsx` (`IncomingInitiator` column) to produce 4 business columns. The mapping table is deduplicated on `IncomingInitiator` before join to prevent row multiplication. NULLs are coalesced to empty strings.

**Unmapped label warning:** If any Initiator value has no corresponding mapping entry, a warning lists the unmapped labels.

---

### Step 4 — Validation & Cleanup

- **Row count assertion:** Output must match input row count exactly
- **Schema check:** All 5 enrichment columns must exist
- **Test mode info:** If `_TEST_SAMPLE_SIZE` is set, reports how many rows have `'Undefined'`
- **Fallback counts:** Prints counts for Undefined, Other, and servicer-undefined labels
- **Top 10 Initiators:** Most frequent labels for sanity check
- **Checkpoint cleanup:** Drops the temp Delta table used for materialization

---

## 4. Classification Instructions

The `_instructions` text guides `ai_classify` with priority rules:

1. **SPV always wins** over servicer and bank
2. **Ειδική διάδοχος** (special successor) means securitization → classify under SPV, not bank
3. **Unknown SPV + known servicer** → use servicer Undefined variant (Cepal/Intrum/doValue)
4. **Bank labels** only when the bank is the sole entity
5. **Servicer alone** (no SPV) → servicer Undefined variant
6. **PILLAR FINANCE** → QQuant regardless of servicer
7. **Private individuals**, small companies, numeric codes → Other
8. **Truly unidentifiable** → Undefined

---

## 5. Tables & Files Reference

| Resource | Location | Purpose |
|----------|----------|---------|
| `HastenerTrainData.xlsx` | `/Volumes/{env_prefix}-cdh/derivative/DAuction_ML/` | Exact-match dictionary source |
| `Initiator_Mapping.xlsx` | `/Volumes/{env_prefix}-cdh/derivative/DAuction_ML/` | Enrichment lookup (Initiator → 4 columns) |
| `initiator_classifier_labels` | `` `{env_prefix}-ds`.`reference` `` | Label names + descriptions for ai_classify |
| `initiator_classifier_cache` | `` `{env_prefix}-ds`.`reference` `` | Persistent LLM result cache |
| `_temp_unmatched_classifier` | `` `{env_prefix}-ds`.`reference` `` | Ephemeral staging table for ai_classify batches |
| `_temp_daauction_df_checkpoint` | `` `{env_prefix}-ds`.`reference` `` | Lineage-breaking checkpoint (created & dropped per run) |

---

## 6. How to Add a New SPV/Label

1. **Add to the labels UC table** (`` `{env_prefix}-ds`.`reference`.`initiator_classifier_labels` ``):
   ```sql
   INSERT INTO `p-ds`.`reference`.`initiator_classifier_labels`
   VALUES ('NewSPV Cepal', 'SPV: NEWSPU SECURITISATION. Servicer: CEPAL HELLAS');
   ```

2. **Add to `Initiator_Mapping.xlsx`** — new row with `IncomingInitiator = "NewSPV Cepal"` and the 4 business column values.

3. **(Optional) Add to `HastenerTrainData.xlsx`** — if you have known exact-match texts for this SPV, add them with `Approved = "yes"` for immediate dictionary coverage.

**What happens on next run:**
- `labels_hash` changes → selective invalidation
- Fallback bucket entries deleted from cache
- Those texts get re-classified with the updated label list
- If any match the new SPV, they'll be correctly classified
- All other cached entries remain untouched

---

## 7. How to Modify Classification Rules

If you change the `_instructions` text:

- `instructions_hash` changes → **full cache wipe**
- All previously cached texts will be re-classified on the next run

**Recommendation:** Batch instruction changes together. Test with `_TEST_SAMPLE_SIZE = 50` first to verify the new rules produce expected results before setting to `None` for full production.

---

## 8. Configuration Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `_TEST_SAMPLE_SIZE` | `None` | Set to integer for test mode (limits unique texts classified) |
| `_BATCH_SIZE` | `500` | Texts per ai_classify batch |
| `_MAX_WAIT_PER_BATCH` | `900` (15 min) | Max seconds to wait for a single batch |
| `_MAX_RETRIES` | `3` | Retry attempts for transient failures |
| `_RETRY_BACKOFF` | `[30, 60, 120]` | Seconds between retries |
| `_WAREHOUSE_ID` | `818510884d309018` | SQL warehouse for ai_classify |
| `env_prefix` | `'p'` | Environment prefix (p = production) |

---

## 9. Troubleshooting

### SQL warehouse timeout
**Symptom:** `TimeoutError: SQL warehouse query timed out after 900s`
**Fix:** Reduce `_BATCH_SIZE` or increase `_MAX_WAIT_PER_BATCH`. Check warehouse status in Databricks UI.

### >20% batch failure abort
**Symptom:** `RuntimeError: ABORTING: X/Y batches failed (>20% failure rate)`
**Fix:** Check warehouse logs. Retry the notebook — transient issues often resolve. If persistent, check Foundation Model API quotas.

### Row count mismatch
**Symptom:** `AssertionError: ROW COUNT MISMATCH: expected X, got Y`
**Fix:** Check for duplicate entries in `HastenerTrainData.xlsx` or `Initiator_Mapping.xlsx`.

### Unmapped labels warning
**Symptom:** `⚠️ WARNING: N Initiator label(s) have NO mapping in Initiator_Mapping.xlsx`
**Fix:** Add the missing label to `Initiator_Mapping.xlsx` with appropriate business column values.

### Cache migration on first run
**Symptom:** `Migrating cache table: adding instructions_hash column...` followed by full cache wipe.
**Fix:** Expected one-time behavior. Subsequent runs will be fast.

### Orphaned temp table
**Symptom:** `_temp_unmatched_classifier` exists between runs.
**Fix:** No action needed — next run uses `CREATE OR REPLACE TABLE`.

### JVM_ATTRIBUTE_NOT_SUPPORTED
**Symptom:** Error referencing `sparkContext` or `sc.broadcast`.
**Fix:** This pipeline avoids `SparkContext` entirely. If you see this error, a code change inadvertently reintroduced `sc.broadcast()` or `rdd.checkpoint()`. Use plain Python closures and Delta table writes instead.

---

## 10. Cost & Performance

### Typical run times

| Scenario | Duration | LLM calls |
|----------|----------|-----------|
| Normal daily run (all cached) | ~2-3 min | 0 |
| Daily run with ~50 new texts | ~5-10 min | 1 batch |
| First run / full cache wipe | ~30-90 min | All batches |
| Selective invalidation (new SPV) | ~5-15 min | Fallback bucket only |

### How the cache minimizes cost
- After first full run, the cache holds all previously seen texts
- Daily runs typically see 0-50 new unique texts
- Dictionary handles ~74% of rows (zero LLM cost)
- Cache handles ~25% of rows (previously classified)
- LLM handles ~1% of rows (truly new texts)

Over time, the cache grows and LLM calls approach zero for steady-state operations.
