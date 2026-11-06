"""
Data Quality Agent - primaryactive (Silver Layer)
=================================================
Provides three core DQ checks that run against the freshly loaded
BusinessDate slice of any primaryactive table:

1. Null Rate Monitoring (bronze + silver)
2. Duplicate Detection
3. Cross-Source Referential Integrity

Results are collected into a list of DQResult objects. Any failures
are reported to MS Teams via an Incoming Webhook.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class DQResult:
    """Single data quality check result."""

    check_name: str
    table_name: str
    status: str  # "PASS" or "FAIL"
    severity: str  # "CRITICAL", "HIGH", "MEDIUM", "LOW"
    details: str
    business_date: str
    source_system: Optional[str] = None
    column_name: Optional[str] = None
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    timestamp: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


# ---------------------------------------------------------------------------
# 1. Null Rate Monitoring
# ---------------------------------------------------------------------------

def check_null_rates_bronze(
    spark: SparkSession,
    catalog_landed: str,
    landed_table_name: str,
    run_date: str,
    critical_columns: list[str],
    null_threshold: float = 0.0,
    severity: str = "MEDIUM",
) -> list[DQResult]:
    """
    Check for null values in critical columns of a landed (bronze) table
    for the given BusinessDate. If any critical column has a null rate
    above the threshold, it returns a FAIL result.

    Parameters
    ----------
    spark : SparkSession
    catalog_landed : str
        Full catalog path e.g. `p-cdh`.`landed`
    landed_table_name : str
        Table name in landed schema e.g. "profits_loan_details"
    run_date : str
        BusinessDate to check (YYYY-MM-DD)
    critical_columns : list[str]
        Columns that should not contain nulls
    null_threshold : float
        Acceptable null ratio (0.0 = no nulls allowed, 0.05 = up to 5%)
    severity : str
        Severity level for failures: CRITICAL, HIGH, MEDIUM, LOW

    Returns
    -------
    list[DQResult]
    """
    results: list[DQResult] = []

    try:
        df = spark.read.table(f"{catalog_landed}.{landed_table_name}").filter(
            f"BusinessDate = '{run_date}'"
        )
        total_count = df.count()

        if total_count == 0:
            results.append(
                DQResult(
                    check_name="null_rate_bronze",
                    table_name=f"{catalog_landed}.{landed_table_name}",
                    status="FAIL",
                    severity=severity,
                    details=f"No records found for BusinessDate={run_date}. Possible upstream extraction failure.",
                    business_date=run_date,
                    metric_value=0,
                )
            )
            return results

        for col_name in critical_columns:
            null_count = df.filter(F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")).count()
            null_rate = null_count / total_count

            status = "PASS" if null_rate <= null_threshold else "FAIL"
            results.append(
                DQResult(
                    check_name="null_rate_bronze",
                    table_name=f"{catalog_landed}.{landed_table_name}",
                    status=status,
                    severity=severity if status == "FAIL" else "LOW",
                    details=f"Column '{col_name}': {null_count}/{total_count} nulls ({null_rate:.2%})",
                    business_date=run_date,
                    column_name=col_name,
                    metric_value=null_rate,
                    threshold=null_threshold,
                )
            )
    except Exception as e:
        results.append(
            DQResult(
                check_name="null_rate_bronze",
                table_name=f"{catalog_landed}.{landed_table_name}",
                status="FAIL",
                severity=severity,
                details=f"Exception during check: {str(e)}",
                business_date=run_date,
            )
        )

    return results


def check_null_rates_silver(
    spark: SparkSession,
    curated_table_path: str,
    run_date: str,
    critical_columns: list[str],
    null_threshold: float = 0.0,
    source_system: Optional[str] = None,
    severity: str = "MEDIUM",
) -> list[DQResult]:
    """
    Check for null anomalies in critical columns of a primaryactive (silver)
    table for the given BusinessDate.

    Parameters
    ----------
    spark : SparkSession
    curated_table_path : str
        Full table path e.g. `p-cdh`.`primaryactive`.exposure
    run_date : str
        BusinessDate to check (YYYY-MM-DD)
    critical_columns : list[str]
        Columns that should not contain nulls
    null_threshold : float
        Acceptable null ratio (0.0 = no nulls allowed)
    source_system : str, optional
        If provided, filter also by SourceSystem
    severity : str
        Severity level for failures: CRITICAL, HIGH, MEDIUM, LOW

    Returns
    -------
    list[DQResult]
    """
    results: list[DQResult] = []

    try:
        df = spark.read.table(curated_table_path).filter(f"BusinessDate = '{run_date}'")
        if source_system:
            df = df.filter(f"SourceSystem = '{source_system}'")

        total_count = df.count()

        if total_count == 0:
            results.append(
                DQResult(
                    check_name="null_rate_silver",
                    table_name=curated_table_path,
                    status="FAIL",
                    severity=severity,
                    details=f"No records found for BusinessDate={run_date}"
                    + (f", SourceSystem={source_system}" if source_system else "")
                    + ". Curated load may have failed.",
                    business_date=run_date,
                    source_system=source_system,
                    metric_value=0,
                )
            )
            return results

        for col_name in critical_columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_rate = null_count / total_count

            status = "PASS" if null_rate <= null_threshold else "FAIL"
            results.append(
                DQResult(
                    check_name="null_rate_silver",
                    table_name=curated_table_path,
                    status=status,
                    severity=severity if status == "FAIL" else "LOW",
                    details=f"Column '{col_name}': {null_count}/{total_count} nulls ({null_rate:.2%})",
                    business_date=run_date,
                    source_system=source_system,
                    column_name=col_name,
                    metric_value=null_rate,
                    threshold=null_threshold,
                )
            )
    except Exception as e:
        results.append(
            DQResult(
                check_name="null_rate_silver",
                table_name=curated_table_path,
                status="FAIL",
                severity=severity,
                details=f"Exception during check: {str(e)}",
                business_date=run_date,
                source_system=source_system,
            )
        )

    return results


# ---------------------------------------------------------------------------
# 2. Duplicate Detection
# ---------------------------------------------------------------------------

def check_duplicates(
    spark: SparkSession,
    curated_table_path: str,
    run_date: str,
    key_columns: list[str],
    source_system: Optional[str] = None,
    severity: str = "HIGH",
) -> list[DQResult]:
    """
    Check for exact duplicate records in a primaryactive table based on
    the specified key columns for the given BusinessDate.

    Parameters
    ----------
    spark : SparkSession
    curated_table_path : str
        Full table path e.g. `p-cdh`.`primaryactive`.exposure
    run_date : str
        BusinessDate to check (YYYY-MM-DD)
    key_columns : list[str]
        Columns forming the unique key (e.g. ["ExposureId", "SourceSystem"])
    source_system : str, optional
        If provided, filter also by SourceSystem
    severity : str
        Severity level for failures: CRITICAL, HIGH, MEDIUM, LOW

    Returns
    -------
    list[DQResult]
    """
    results: list[DQResult] = []

    try:
        df = spark.read.table(curated_table_path).filter(f"BusinessDate = '{run_date}'")
        if source_system:
            df = df.filter(f"SourceSystem = '{source_system}'")

        total_count = df.count()

        if total_count == 0:
            results.append(
                DQResult(
                    check_name="duplicate_detection",
                    table_name=curated_table_path,
                    status="FAIL",
                    severity=severity,
                    details=f"No records found for BusinessDate={run_date}. Cannot check duplicates.",
                    business_date=run_date,
                    source_system=source_system,
                )
            )
            return results

        # Count duplicates by grouping on key columns
        duplicate_df = (
            df.groupBy(key_columns)
            .count()
            .filter("count > 1")
        )
        duplicate_groups = duplicate_df.count()
        duplicate_records = (
            duplicate_df.agg(F.sum("count").alias("total_dups")).first()["total_dups"]
            if duplicate_groups > 0
            else 0
        )

        if duplicate_groups > 0:
            # Get sample of duplicate keys for the alert message
            sample_keys = (
                duplicate_df.orderBy(F.desc("count"))
                .limit(5)
                .select(key_columns)
                .collect()
            )
            sample_str = "; ".join(
                [", ".join([f"{k}={row[k]}" for k in key_columns]) for row in sample_keys]
            )

            results.append(
                DQResult(
                    check_name="duplicate_detection",
                    table_name=curated_table_path,
                    status="FAIL",
                    severity=severity,
                    details=(
                        f"{duplicate_groups} duplicate key groups found "
                        f"({duplicate_records} total duplicate records out of {total_count}). "
                        f"Key columns: {key_columns}. "
                        f"Sample duplicates: [{sample_str}]"
                    ),
                    business_date=run_date,
                    source_system=source_system,
                    metric_value=duplicate_groups,
                )
            )
        else:
            results.append(
                DQResult(
                    check_name="duplicate_detection",
                    table_name=curated_table_path,
                    status="PASS",
                    severity="LOW",
                    details=f"No duplicates found in {total_count} records. Key columns: {key_columns}.",
                    business_date=run_date,
                    source_system=source_system,
                    metric_value=0,
                )
            )
    except Exception as e:
        results.append(
            DQResult(
                check_name="duplicate_detection",
                table_name=curated_table_path,
                status="FAIL",
                severity=severity,
                details=f"Exception during check: {str(e)}",
                business_date=run_date,
                source_system=source_system,
            )
        )

    return results


# ---------------------------------------------------------------------------
# 3. Cross-Source Referential Integrity
# ---------------------------------------------------------------------------

def check_referential_integrity(
    spark: SparkSession,
    source_table_path: str,
    target_table_path: str,
    source_key_column: str,
    target_key_column: str,
    run_date: str,
    source_system: Optional[str] = None,
    severity: str = "MEDIUM",
) -> list[DQResult]:
    """
    Check that every key value in the source table exists in the target table
    for the given BusinessDate. This validates cross-source referential integrity.

    Example: Every ExposureKey in primaryactive.exposure should have a corresponding
    PartyKey in primaryactive.party.

    Parameters
    ----------
    spark : SparkSession
    source_table_path : str
        Table containing the foreign key (e.g. primaryactive.exposure)
    target_table_path : str
        Table containing the referenced key (e.g. primaryactive.party)
    source_key_column : str
        Column in source table to check (e.g. "PartyKey")
    target_key_column : str
        Column in target table that should contain the value (e.g. "PartyKey")
    run_date : str
        BusinessDate to check (YYYY-MM-DD)
    source_system : str, optional
        If provided, filter source table by SourceSystem
    severity : str
        Severity level for failures: CRITICAL, HIGH, MEDIUM, LOW

    Returns
    -------
    list[DQResult]
    """
    results: list[DQResult] = []

    try:
        source_df = spark.read.table(source_table_path).filter(f"BusinessDate = '{run_date}'")
        target_df = spark.read.table(target_table_path).filter(f"BusinessDate = '{run_date}'")

        if source_system:
            source_df = source_df.filter(f"SourceSystem = '{source_system}'")

        # Get distinct keys from source (excluding nulls)
        source_keys = (
            source_df.select(F.col(source_key_column).alias("key"))
            .filter(F.col("key").isNotNull())
            .distinct()
        )
        source_key_count = source_keys.count()

        # Get distinct keys from target
        target_keys = (
            target_df.select(F.col(target_key_column).alias("key"))
            .filter(F.col("key").isNotNull())
            .distinct()
        )

        # Find orphan keys (in source but not in target)
        orphan_keys = source_keys.join(target_keys, on="key", how="left_anti")
        orphan_count = orphan_keys.count()

        if orphan_count > 0:
            orphan_rate = orphan_count / source_key_count if source_key_count > 0 else 0
            # Sample of orphan keys
            sample_orphans = [
                row["key"] for row in orphan_keys.limit(10).collect()
            ]

            results.append(
                DQResult(
                    check_name="referential_integrity",
                    table_name=source_table_path,
                    status="FAIL",
                    severity=severity,
                    details=(
                        f"{orphan_count}/{source_key_count} keys in "
                        f"{source_table_path}.{source_key_column} "
                        f"have no match in {target_table_path}.{target_key_column} "
                        f"({orphan_rate:.2%} orphan rate). "
                        f"Sample orphan keys: {sample_orphans[:5]}"
                    ),
                    business_date=run_date,
                    source_system=source_system,
                    column_name=source_key_column,
                    metric_value=orphan_rate,
                )
            )
        else:
            results.append(
                DQResult(
                    check_name="referential_integrity",
                    table_name=source_table_path,
                    status="PASS",
                    severity="LOW",
                    details=(
                        f"All {source_key_count} keys in "
                        f"{source_table_path}.{source_key_column} "
                        f"found in {target_table_path}.{target_key_column}."
                    ),
                    business_date=run_date,
                    source_system=source_system,
                    column_name=source_key_column,
                    metric_value=0.0,
                )
            )
    except Exception as e:
        results.append(
            DQResult(
                check_name="referential_integrity",
                table_name=source_table_path,
                status="FAIL",
                severity=severity,
                details=f"Exception during check: {str(e)}",
                business_date=run_date,
                source_system=source_system,
            )
        )

    return results


# ---------------------------------------------------------------------------
# 4. Schema-driven discovery of primaryactive tables and their check columns
# ---------------------------------------------------------------------------

import re

# System / observability / ML-monitoring tables that live in the primaryactive
# schema but are NOT curated business data. These must be skipped by the DQ
# checks (they have no business ID / BusinessDate contract).
_NON_BUSINESS_TABLE_PATTERNS = [
    re.compile(r"_otel_", re.IGNORECASE),
    re.compile(r"_trace_", re.IGNORECASE),
    re.compile(r"^\d+_", re.IGNORECASE),          # e.g. 732286313479313_otel_spans
    re.compile(r"drift_metrics", re.IGNORECASE),
    re.compile(r"profile_metrics", re.IGNORECASE),
    re.compile(r"^dataqualityresults$", re.IGNORECASE),  # the DQ output table itself
]

# Column-name patterns that indicate a monetary amount which should never be
# null. Matched case-insensitively against the column name. Kept conservative
# so non-amount numerics (rates, counts, days, flags, keys) are NOT included.
_AMOUNT_NAME_PATTERNS = [
    re.compile(r"amount", re.IGNORECASE),
    re.compile(r"balance", re.IGNORECASE),
    re.compile(r"\bgbv\b", re.IGNORECASE),
    re.compile(r"principal", re.IGNORECASE),
    re.compile(r"interest", re.IGNORECASE),
    re.compile(r"payment", re.IGNORECASE),
    re.compile(r"\bexpense", re.IGNORECASE),
    re.compile(r"commission", re.IGNORECASE),
    re.compile(r"\baccrued", re.IGNORECASE),
    re.compile(r"\bvalue$", re.IGNORECASE),
]

# Numeric Spark types that an amount column is allowed to be.
_NUMERIC_TYPE_TOKENS = ("Decimal", "Double", "Float", "Long", "Integer")


# Table-name prefixes in primaryactive that must be EXCLUDED from the duplicate
# check (but still get null checks). `ext*` tables (e.g. extkarma, extdod_dpd_fcy)
# are external-file feeds whose grain is not a unique single-column key, so the
# first-column duplicate check produces false positives.
_SKIP_DUPLICATE_PREFIXES = ("ext",)

# Amount columns tolerate a HIGHER null rate as normal. A null rate BELOW this
# floor (including zero nulls) is treated as a possible problem and raised at
# MEDIUM severity; a null rate at/above the floor is considered expected (PASS).
_AMOUNT_NULL_FLOOR = 0.01  # 1%


def _is_business_table(table_name: str) -> bool:
    """True if the table is a curated business table (not an otel/trace/metrics artifact)."""
    return not any(p.search(table_name) for p in _NON_BUSINESS_TABLE_PATTERNS)


def _skip_duplicate_check(table_name: str) -> bool:
    """True if this table must be excluded from the duplicate check (e.g. ext* feeds)."""
    return table_name.lower().startswith(_SKIP_DUPLICATE_PREFIXES)


def _looks_like_amount(col_name: str, dtype: str) -> bool:
    """True if the column is a numeric monetary amount by name + type."""
    if not any(tok in dtype for tok in _NUMERIC_TYPE_TOKENS):
        return False
    return any(p.search(col_name) for p in _AMOUNT_NAME_PATTERNS)


def _is_amount_named(col_name: str, dtype: str) -> bool:
    """True if the column is a numeric column whose NAME contains 'Amount'.

    These get the special floor rule (MEDIUM when null_rate < 1%). The broader
    `_looks_like_amount` set (balance/principal/interest/...) keeps the strict
    zero-null rule.
    """
    if not any(tok in dtype for tok in _NUMERIC_TYPE_TOKENS):
        return False
    return "amount" in col_name.lower()


# ---------------------------------------------------------------------------
# Amount null-floor check (special rule for *Amount* columns)
# ---------------------------------------------------------------------------

def check_amount_null_floor(
    spark: SparkSession,
    curated_table_path: str,
    run_date: str,
    amount_columns: list[str],
    null_floor: float = _AMOUNT_NULL_FLOOR,
    source_system: Optional[str] = None,
) -> list[DQResult]:
    """
    Null check for monetary *Amount* columns using an inverted rule:

    - null_rate <  null_floor (default 1%)  -> FAIL at MEDIUM
      (a fully- or almost-fully-populated amount column is itself suspicious)
    - null_rate >= null_floor               -> PASS
      (a materially-null amount column is considered expected/optional)

    This is the opposite of the strict ID/null check, where any null fails.
    """
    results: list[DQResult] = []
    if not amount_columns:
        return results

    try:
        df = spark.read.table(curated_table_path).filter(f"BusinessDate = '{run_date}'")
        if source_system:
            df = df.filter(f"SourceSystem = '{source_system}'")

        total_count = df.count()
        if total_count == 0:
            # Emptiness is already reported by the strict null/duplicate checks;
            # don't double-report here.
            return results

        for col_name in amount_columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_rate = null_count / total_count

            # FAIL only when nulls are strictly below the floor.
            is_problem = null_rate < null_floor
            results.append(
                DQResult(
                    check_name="amount_null_floor",
                    table_name=curated_table_path,
                    status="FAIL" if is_problem else "PASS",
                    severity="MEDIUM" if is_problem else "LOW",
                    details=(
                        f"Amount column '{col_name}': {null_count}/{total_count} nulls "
                        f"({null_rate:.2%}). Expected null rate >= {null_floor:.0%}; "
                        f"a lower rate is a possible problem."
                    ),
                    business_date=run_date,
                    source_system=source_system,
                    column_name=col_name,
                    metric_value=null_rate,
                    threshold=null_floor,
                )
            )
    except Exception as e:
        results.append(
            DQResult(
                check_name="amount_null_floor",
                table_name=curated_table_path,
                status="FAIL",
                severity="MEDIUM",
                details=f"Exception during amount null-floor check: {str(e)}",
                business_date=run_date,
                source_system=source_system,
            )
        )

    return results


def get_latest_business_date(spark: SparkSession, table_path: str) -> Optional[str]:
    """Return the MAX(BusinessDate) of a table as 'YYYY-MM-DD', or None if empty/missing."""
    try:
        row = spark.sql(f"SELECT MAX(BusinessDate) AS d FROM {table_path}").first()
        if row and row["d"] is not None:
            return row["d"].strftime("%Y-%m-%d") if hasattr(row["d"], "strftime") else str(row["d"])
    except Exception:
        return None
    return None


def discover_primaryactive_checks(
    spark: SparkSession,
    catalog: str,
    include_amount_null_checks: bool = True,
) -> list[dict]:
    """
    Discover every curated business table in `{catalog}.primaryactive` and build
    a per-table DQ check config, derived from each table's live schema.

    For each business table (system/otel/trace/metrics tables are skipped):
      - id_column: the FIRST column (the unique composite-key identifier)
      - null_columns: the id_column plus amount-like columns (balance, principal,
        interest, ...) that follow the STRICT zero-null rule
      - amount_columns: columns whose NAME contains 'Amount' — these follow the
        special floor rule (MEDIUM when null_rate < 1%), handled separately by
        check_amount_null_floor, and are NOT included in null_columns
      - skip_duplicate_check: True for ext* tables (external feeds whose grain is
        not a unique single-column key); they still get null checks
    Only tables that actually expose a `BusinessDate` column are included.

    Returns a list of dicts:
      {"table": "<name>", "table_path": "`{catalog}`.`primaryactive`.<name>",
       "id_column": "<col>", "null_columns": [...], "amount_columns": [...],
       "skip_duplicate_check": bool}
    """
    primaryactive = f"{catalog}.`primaryactive`"
    configs: list[dict] = []

    tables = spark.sql(f"SHOW TABLES IN {primaryactive}").collect()
    for t in tables:
        table_name = t["tableName"]
        if not _is_business_table(table_name):
            continue

        table_path = f"{primaryactive}.{table_name}"
        try:
            fields = spark.read.table(table_path).schema.fields
        except Exception:
            # Unreadable (permissions / transient) — skip rather than fail the run.
            continue

        col_names = [f.name for f in fields]
        if "BusinessDate" not in col_names:
            # Not a per-BusinessDate curated table (e.g. a static reference); skip.
            continue

        id_column = fields[0].name  # convention: first column is the unique key

        # Columns whose NAME contains 'Amount' use the special floor rule and are
        # checked separately — keep them OUT of the strict null_columns list.
        amount_columns: list[str] = []
        null_columns = [id_column]
        if include_amount_null_checks:
            for f in fields:
                if f.name == id_column:
                    continue
                dtype = str(f.dataType)
                if _is_amount_named(f.name, dtype):
                    amount_columns.append(f.name)
                elif _looks_like_amount(f.name, dtype):
                    # Other amount-like columns (balance/principal/...) keep the
                    # strict zero-null rule.
                    null_columns.append(f.name)

        configs.append({
            "table": table_name,
            "table_path": table_path,
            "id_column": id_column,
            "null_columns": null_columns,
            "amount_columns": amount_columns,
            "skip_duplicate_check": _skip_duplicate_check(table_name),
        })

    return configs
