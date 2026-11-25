# Databricks notebook source
# MAGIC %md
# MAGIC ## Get the latest date 

# COMMAND ----------


from pyspark.sql.functions import trim,current_timestamp,from_utc_timestamp,sha2
from datetime import timedelta, datetime

table_name = dbutils.widgets.get("table_name")
run_date_input = dbutils.widgets.get("run_date")
unity = dbutils.widgets.get("unity")

# Import the module
import sys
sys.path.append("./aws_utils")
sys.path.append("./01-utils/pipeline_logging")

import aws_utils

# Get Environment Prefix
env = aws_utils.get_env_prefix()

# Pipeline task logging (append-only). Non-blocking: logging must never fail the job.
_log_enabled = False
try:
    from pipeline_log_helpers import log_task_start, log_task_end
    _log_enabled = True
except Exception as _log_err:
    print(f"[WARNING] pipeline_log_helpers import failed (logging disabled): {_log_err}")

result = spark.sql( f'''
    SELECT max(BusinessDate) run_date
        FROM `{env}-cdh`.primary.portfolio 
        '''
)
run_date = result.collect()[0]['run_date']
run_date = run_date.strftime('%Y-%m-%d')

if run_date_input != "-1":
    run_date = run_date_input
    
timeout = -1



# COMMAND ----------

# MAGIC %md
# MAGIC ### Dispatch table (single source of truth)

# COMMAND ----------

# Map each table_name to the notebook path(s) it runs. This single dict is the
# source of truth for BOTH dispatch AND pipeline_area — add a new table with ONE
# entry here and nothing else needs to change (no second list to remember).
# Value is one path (str) or several (list, run in order).
_NOTEBOOK_BY_TABLE = {
    # ── derivative ──────────────────────────────────────────────────────────
    "DAgreement":                          "DAgreement/DAgreement",
    "DCollateral":                         "DCollateral/DCollateral",
    "DCollection":                         "DCollection/DCollection",
    "DExposure":                           "DExposure/DExposure",
    "DEStatements":                        "DEStatements/DEStatements",
    "DEntityRelationships":                "DEntityRelationships/DEntityRelationships",
    "DParty":                              "dparty/DParty",
    "DPaymentSchedule":                    "DPaymentSchedule/DPaymentSchedule",
    "DRealEstate":                         "DRealEstate/DRealEstate",
    "DRestructuring":                      "DRestructuring/DRestructuring",
    # "DRestructuringWriteoff":            "DRestructuringWriteoff/DRestructuringWriteoff",
    "DSettlementExposure":                 "DSettlementExposure/DSettlementExposure",
    "DUnderLaw":                           "DUnderLaw/DUnderLaw",
    "ZeroingGBVExposure":                  "DExposure/ZeroingGBV",
    "DAuctionRealEstate":                  "DAuctionRealEstate/DAuctionRealEstate",
    "DDebtorUnified":                      "DebtorUnified/DDebtorUnified",
    "DAnalyticsMetrics":                   "danalyticsmetrics/DAnalyticsMetrics",
    "DAnalyticsMetrics_historical":        "danalyticsmetrics/DAnalyticsMetrics_historical",
    "DLegalDataCore":                      "DLegalDataCore/DLegalDataCore",
    "DLegalDataSecuritized":               "DLegalDataSecuritized/DLegalDataSecuritized",

    # ── ptools ──────────────────────────────────────────────────────────────
    "DPortfolioToolCore":                  "dporfoliotoolcore/DPortfolioToolCore",
    "DPortfolioToolSecuritized":           "dportfoliotoolsecuritized/DPortfolioToolSecuritized",
    "DPortfolioToolThirdParties":          ["dportfoliotoolthirdparties/DPortfolioToolThirdParties",
                                            "dportfoliotoolthirdparties/DPortfolioToolThirdPartiesMonthly"],

    # ── biteam ──────────────────────────────────────────────────────────────
    "DBPAmicableReport":                   "biteam/DBPAmicableReport",
    "DBPCepalRecoveriesAgreement":         "biteam/DBPCepalRecoveriesAgreement",
    "DBPCepalRecoveriesInitial":           "biteam/DBPCepalRecoveriesInitial",
    "DBPCepalRecoveriesParty":             "biteam/DBPCepalRecoveriesParty",
    "DBPCepalRecoveriesStrategy":          "biteam/DBPCepalRecoveriesStrategy",
    "DBPHAPS":                             "biteam/DBPHAPS",
    "DDPOFinalTable":                      "biteam/DDPOFinalTable",
    "DLegalProcessStatusAgreement":        "biteam/DLegalProcessStatusAgreement",
    "DLegalProcessStatusAgreementDebtor":  "biteam/DLegalProcessStatusAgreementDebtor",
    "DLegalProcessStatusDebtor":           "biteam/DLegalProcessStatusDebtor",
    "DLegalProcessStatusGroup":            "biteam/DLegalProcessStatusGroup",
    "DLegalProcessStatusPrimaryDebtor":    "biteam/DLegalProcessStatusPrimaryDebtor",
    "DLienAnalysisLTV":                    "biteam/DLienAnalysisLTV",
    "DLienAnalysisPropertyContract":       "biteam/DLienAnalysisPropertyContract",
    "DLienAnalysisRealEstateExposure":     "biteam/DLienAnalysisRealEstateExposure",
    "DPfFinalOutput":                      "biteam/DPfFinalOutput",
}

# pipeline_area is DERIVED from the notebook folder the table dispatches to, so a
# new table inherits its area automatically from the folder it lives in — no
# separate area list to maintain. Unlisted folders → derivative.
_AREA_BY_FOLDER = {
    "biteam":                      "biteam",
    "dporfoliotoolcore":           "ptools_core",
    "dportfoliotoolsecuritized":   "ptools_securitized",
    "dportfoliotoolthirdparties":  "ptools_thirdparties",
}


def _resolve_area(nb_paths):
    """Derive pipeline_area from the notebook folder (first path if a list)."""
    if not nb_paths:
        return "derivative"
    first_path = nb_paths[0] if isinstance(nb_paths, list) else nb_paths
    folder = first_path.split("/", 1)[0]
    return _AREA_BY_FOLDER.get(folder, "derivative")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Call the necessary notebook

# COMMAND ----------

# ---------------------------------------------------------------------------
# Data-driven dispatch with pipeline task logging.
# pipeline_area is resolved automatically from the dispatch folder (biteam /
# ptools_* / else derivative). One task_key == one controller run == one
# start/end event pair. Logging is fully non-blocking.
# ---------------------------------------------------------------------------
notebook_paths = _NOTEBOOK_BY_TABLE.get(table_name)
pipeline_area = _resolve_area(notebook_paths)

if notebook_paths is None:
    print('Not Available table')
else:
    if _log_enabled:
        try:
            log_task_start(spark, pipeline_area, run_date, table_name, env_prefix=env)
        except Exception as _e:
            print(f"[WARNING] Task log start failed (non-blocking): {_e}")

    try:
        for _nb in ([notebook_paths] if isinstance(notebook_paths, str) else notebook_paths):
            dbutils.notebook.run(_nb, -1, {"run_date": run_date, "unity": unity})

        if _log_enabled:
            try:
                log_task_end(spark, pipeline_area, run_date, table_name, status="completed", env_prefix=env)
            except Exception as _e:
                print(f"[WARNING] Task log end failed (non-blocking): {_e}")

    except Exception as _dispatch_err:
        if _log_enabled:
            try:
                log_task_end(spark, pipeline_area, run_date, table_name,
                             status="failed", error_message=str(_dispatch_err)[:500], env_prefix=env)
            except Exception:
                pass
        raise
