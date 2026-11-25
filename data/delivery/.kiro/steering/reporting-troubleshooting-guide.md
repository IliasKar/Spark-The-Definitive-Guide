---
inclusion: manual
---

# Reporting / Delivery Area — Troubleshooting Guide

A field guide for diagnosing failures in the delivery notebooks of
`gr.cepal.dna.de.delivery` (the `repoint/`, `platforms/`, `RCG/`, `LoanIT/`,
`DataTape/` report notebooks). These notebooks read curated/derivative cloud
tables plus external-file-backed catalogs and write one table per report that
is consumed by IT systems, regulators, or external companies — **accuracy is
critical**, so a report should fail loudly rather than deliver wrong data.

The methodology below is distilled from real incidents. The worked example is
the HAPS duplicate-ExposureKey incident (`repoint/HAPS/HAPS_AssignmentMapping`,
run_date 2026-10-05, branch `BAUDE-167_hardening`).

---

## 1. First Principles

- **Reproduce read-only.** Never write to prod (`p-cdh-delivery`, any `/Volumes/...`)
  while troubleshooting. All investigation is `SELECT`-only against `p-cdh`,
  `p-cdh-delivery`, and (when accessible) the external-file catalogs.
- **The failing table is usually already written.** Most notebooks `append`
  the result to the delivery table **before** the data-quality check runs. So
  the first and richest evidence is the delivery table itself for the failing
  `BusinessDate` — inspect it before re-running anything.
- **Trust the grain.** Every report has an intended grain (a key that should be
  unique per `BusinessDate`). Duplicates mean a join or a source violated that
  grain. Identify the grain first (it is the `key_columns` in the notebook's
  `check_table` call).
- **A clean factor is a structural signal.** Uniform ×2, ×3, ×4 duplication
  (not random) almost always means a join fan-out or a self-join squaring, not
  scattered data drift. Note the factor early — it tells you how many sources
  multiplied.

---

## 2. Tool / Catalog Access Map

| Catalog | SQL warehouse (MCP `databricks-prod`) | Prod cluster (notebook run) |
|---|---|---|
| `p-cdh` (primary, derivative, reference) | ✅ readable | ✅ |
| `p-cdh-delivery` (report outputs) | ✅ readable | ✅ |
| `p-reportinputfiles` (external report files) | ❌ no `USE CATALOG` | ✅ |
| `p-ptools` (assignments / offer_collection) | ❌ | ✅ |
| `p-ds` (DS-provided tables, e.g. payers flag) | varies | ✅ |

**Implication:** use the MCP SQL warehouse for anything in `p-cdh` /
`p-cdh-delivery` (fast, no cluster). For `*-reportinputfiles` / `*-ptools` /
`p-ds`, you must run read-only SQL from a notebook on a prod/dev cluster that
carries the instance profile. Plan the investigation so the cheap warehouse
queries narrow the problem before you need a cluster.

---

## 3. Duplicate-Key Failures (most common)

Symptom: `ValueError: Duplicate <key> entries found: [{'<key>': ..., 'count': N}, ...]`
raised by the notebook's `check_table(...)` data-quality cell.

### Step-by-step

1. **Measure the excess and the grain.**
   ```sql
   SELECT COUNT(*) AS total_rows,
          COUNT(DISTINCT <key>) AS distinct_keys,
          COUNT(*) - COUNT(DISTINCT <key>) AS excess
   FROM `p-cdh-delivery`.<schema>.<table>
   WHERE BusinessDate = '<run_date>';
   ```

2. **Find the duplication factor.**
   ```sql
   SELECT cnt AS copies_per_key, COUNT(*) AS num_keys
   FROM (SELECT <key>, COUNT(*) cnt FROM `p-cdh-delivery`.<schema>.<table>
         WHERE BusinessDate='<run_date>' GROUP BY <key> HAVING COUNT(*)>1)
   GROUP BY cnt ORDER BY cnt;
   ```
   A single uniform factor (e.g. every dup appears exactly 4×) → structural
   fan-out. Mixed factors → more than one culprit.

3. **Compare the duplicate rows column by column.** Pick one reported key and
   select all columns for its N rows.
   - **Rows identical** → an *upstream source has duplicate rows* (external
     file loaded twice / with dupes). The join just carried them through.
   - **Rows differ in some columns** → a *LEFT JOIN matched multiple distinct
     values* (the join key is not unique on the right side).

4. **Isolate the blast radius.** Group the excess by the natural segmenting
   column (PortfolioName, SourceSystem, etc.). Duplication concentrated in ONE
   segment is a strong lead — often a newly migrated portfolio or a single bad
   file.
   ```sql
   SELECT PortfolioName, COUNT(*)-COUNT(DISTINCT <key>) AS excess
   FROM `p-cdh-delivery`.<schema>.<table>
   WHERE BusinessDate='<run_date>' GROUP BY PortfolioName ORDER BY excess DESC;
   ```

5. **Walk the joins at the report grain.** Read the notebook and list every
   join that happens at (or collapses to) the grain. For each right-hand
   source, test uniqueness per its join key for the run_date. The first source
   that returns >1 row per key is the fan-out point.

6. **Confirm it is date-specific.** Compare the suspect source's per-key excess
   on the failing date vs the prior few dates. Excess appearing only on the
   failing date = a bad file for that date (transient). Excess on every date =
   a structural notebook/source problem.
   ```sql
   SELECT BusinessDate, COUNT(*)-COUNT(DISTINCT <joinkey>) AS excess
   FROM <source> WHERE BusinessDate BETWEEN '<prev>' AND '<run_date>' AND <filters>
   GROUP BY BusinessDate ORDER BY BusinessDate;
   ```

7. **Close the loop.** Prove the affected output keys map 1:1 to the suspect
   source's duplicated keys (join the dup-output-keys set to the dup-source-keys
   set and check the match is 100%). Do not stop at a plausible cause — confirm
   it explains *all* the excess.

### `RANK()` ties — the amplifier to watch for

A recurring HAPS-family pattern: a dedup implemented with `RANK()` /
`DENSE_RANK()` keeps **ties** (two rows sharing the ordered value both get rank
1). If such a ranked set is then **self-joined on `rank = 1`**, two tied rows
become 2×2 = 4. So one duplicate source row squares into ×4 at the output.

- Any `RANK()`/`DENSE_RANK()` used to "pick one row" is a latent bug. Prefer
  `ROW_NUMBER()` with a deterministic tiebreaker, which guarantees exactly one
  row.
- A self-join on `rankCol = 1` is doubly dangerous: harden it by deduplicating
  the ranked set to one row per partition before (or at) the join.

### Hardening pattern (defensive dedup)

Collapse a fan-out-prone source to one deterministic row per key with
`ROW_NUMBER()`, choosing a meaningful "best" row rather than an arbitrary one:

```sql
SELECT <cols>
FROM (
  SELECT <cols>,
         ROW_NUMBER() OVER (
           PARTITION BY <grain_key>
           ORDER BY CASE WHEN <preferred_non_null> IS NOT NULL THEN 0 ELSE 1 END,
                    <stable_tiebreakers...>
         ) AS rn
  FROM <source> WHERE <filters>
)
WHERE rn = 1
```

Validate the guard is a **no-op on clean data**: on good dates `kept_rows`
must equal `raw_rows`; on the bad date it collapses to `distinct_keys`. A guard
that changes clean dates has altered report semantics — fix it.

Apply the guard **at the source of the duplication** (so values stay correct)
AND optionally as a final one-row-per-grain safety net on the view that joins to
the report grain (so no future bad file can fan out through that path).

---

## 4. Empty / Row-Count Failures

Symptom: `The <table> dataset is empty for the date <run_date>` or counts far
below the expected baseline.

- **Date-type mismatch (silent zero rows).** Comparing a `date` column to a
  bare string can return 0 rows. In SQL ensure the literal is a date; in the
  PySpark DataFrame API use `lit(run_date).cast("date")` — bare string
  comparison silently yields nothing.
- **Reference-date resolution.** Many reports derive `ReferenceDateSoM`,
  `AssignmentCutOffDate`, `FirstAssignmentDate`, etc. from
  `reference.ref_calendar`. Resolve these first and sanity-check them — a wrong
  anchor date empties every downstream source. (HAPS 2026-10-05 →
  ReferenceDateSoM 2026-09-30, AssignmentCutOffDate 2026-10-05,
  FirstAssignmentDate 2026-10-09.)
- **Upstream not loaded yet.** The curated/derivative source may not have the
  run_date yet (ETL ordering). Check
  `SELECT MAX(BusinessDate) FROM <source>` before blaming the report.
- **Perimeter source choice.** For customer/exposure perimeter use
  `primary.exposure` (all relationship types); `derivative.dexposure` and
  `get_exposures_by_business_date()` return Primary Owner only.

---

## 5. Join / Key-Format Gotchas

- **ExposureKey prefix mismatch.** Different tables store ExposureKey with or
  without a 3-char portfolio prefix (`derivative.dexposure` /
  `primary.exposure` have it; `primary.interaction`,
  `primary.settlementexposure`, `landed.qca_*` are bare numbers;
  `primary.collection` is mixed). Strip with `substring(col, 4, 100)` — all
  prefixes are exactly 3 chars. **Do not** use `regexp_extract` inside `.ipynb`
  JSON — the pattern gets double-escaped and silently breaks.
- **Column ambiguity in exposure↔dexposure joins.** Many columns share names;
  alias dexposure columns (e.g. `dx_` prefix) before joining.
- **String vs int join keys.** Comparing a landed string column to an int
  column throws on cast; empty strings fail too. Use
  `try_cast(col as int)`.

---

## 6. New Portfolio Migration — Known Hazard

When a **new portfolio** is onboarded, its external files (assignment,
casecode, queues, offer_collection) may be **partially populated or contain
duplicate rows** for a while. This is the HAPS root cause: a new portfolio's
`primary.assignment` had full-row duplicates for one date, which the `RANK()`
self-join squared into ×4 duplicate ExposureKeys.

Guidance:
- Expect transient duplicate/empty source rows for a newly migrated portfolio;
  the external feed usually self-corrects within days.
- But **harden the notebook** so a bad feed can only degrade values, never
  break the grain — see the dedup pattern in §3. A report must survive an
  imperfect new-portfolio feed.
- When a report fails right after a portfolio goes live, check that portfolio's
  sources in isolation first (§3 step 4).

---

## 7. Evidence Checklist Before Proposing a Fix

- [ ] Excess row count and uniform duplication factor measured.
- [ ] Blast radius isolated (which segment / portfolio / source).
- [ ] Duplicate rows compared column-by-column (identical vs differing).
- [ ] Fan-out join/source identified and tested for >1 row per key.
- [ ] Date-specific vs structural confirmed (compare to prior dates).
- [ ] Affected output keys mapped 100% to the suspect source (loop closed).
- [ ] Proposed guard validated as a no-op on clean dates.
- [ ] No writes made to prod tables or volumes during investigation.

---

## 8. Worked Example — HAPS_AssignmentMapping (reference)

| Finding | Value |
|---|---|
| Report grain | `ExposureKey` per `BusinessDate` |
| Failing date | 2026-10-05 |
| Output | 232,355 rows / 218,876 distinct keys / 13,479 excess |
| Factor | uniform ×4 (4,493 keys × 4) |
| Blast radius | only "Athena Portfolio" (a newly migrated portfolio); all others clean |
| Dup rows | fully identical across all columns → upstream duplicate rows |
| Root source | `p-cdh.primary.assignment` (qca, Channel Assignment): 19,197 CaseCodes with exactly 2 identical rows on 2026-10-05; 0 excess on prior dates |
| Amplifier | `AllQueuesChannels` uses `RANK()`; the 2 tied rows both rank 1, and the `QueuesChannels` self-join on `RNKRefDateHistorical = 1` squares them → 2×2 = 4 |
| Loop closed | 4,493 / 4,493 affected output keys mapped to a duplicate-assignment CaseCode |
| Fix | dedup `AssignmentMtD` to one row per CaseCode+BusinessDate; add `ROW_NUMBER()` guard yielding one row per CaseCodeKey on `QueuesChannels` |
