# Review — hoist_collection_invoicing.ipynb export tail

**Verdict: APPROVED**

Scope: insertion of an export tail into the final code cell of
`HoistInvoicing/hoist_collection_invoicing.ipynb` (branch `DNA26-1716_platforms`,
working-tree change, no commit). Data logic must be byte-for-byte unchanged.

## Evidence

- **JSON/nbformat valid**: `json.load` succeeds; nbformat 4, 6 code cells. `source`
  remains list-of-strings style. Re-ran the narrow parse check — OK.
- **Diff scope**: `git diff --numstat` = `21 0` (21 insertions, 0 deletions), a single
  `@@` hunk, located entirely between `display(output_df)` and
  `collections_cal.unpersist()` in the final cell. No data pipeline cell altered;
  `display(output_df)` untouched.
- **No commit**: HEAD still at `22b3c5f` (prior commit); change is unstaged working-tree
  only.

## Blocking-criteria checks

1. Data logic unchanged — only the 21-line export tail added; collections/dexposure/
   settlement/assignment/output_df build intact. PASS.
2. `display(output_df)` present and unaltered. PASS.
3. `collections_cal.unpersist()` is the last executed statement. PASS.
4. Config path `config/live/hoist_collection_invoicing.json`; reads only
   `output_file_prefix` and `report_output` from `report_args`. PASS.
5. `output_dir = f"/Volumes/{env}-cdh-delivery/s3_external/reports/{report_output}{run_date.replace('-', '')}/"`
   + `os.makedirs(..., exist_ok=True)`. Matches required pattern exactly. PASS.
6. Import path `{_repo_root}/01-utils` via `sys.path.append`; `from export_utils import export_data`. PASS.
7. Call signature `export_data(output_df, output_file_prefix, run_date, output_dir, output_file_prefix)`
   cross-checked against `01-utils/export_utils.py`:
   `export_data(df, name, last_eom_business_date, output_dir, output_file_prefix)`.
   - `output_df` → df
   - `output_file_prefix` → name (intentional dual use, approved)
   - `run_date` (YYYY-MM-DD) → last_eom_business_date
   - `output_dir` → output_dir
   - `output_file_prefix` → output_file_prefix
   Filename resolves to `hoistinvoicing_hoistinvoicing_<date>...xlsx` as expected. PASS.
8. No new widgets; only pre-existing `business_date` widget. `last_eom_business_date`/
   `env_prefix`/`output_dir`/`output_file_prefix` are plain variables/args, not widgets. PASS.
9. Prerequisites available earlier in notebook: `import sys`, `env = aws_utils.get_env_prefix()`,
   `run_date` (string). PASS.
10. No git commit made. PASS.

## Notes (non-blocking)

- `report_output` in the config file is `"internalreports/hoistinvoicing/"` (trailing
  slash) vs. the original message's `"internalreports/hoistinvoicing"`. The notebook
  builds `output_dir` from the key as-is per the approved requirement; config content is
  out of scope for this notebook review. No correctness impact on the inserted tail.

No blocking findings.
