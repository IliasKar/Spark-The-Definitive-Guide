"""
DQ Agent — LangGraph Evaluation Framework
===========================================
Runs the LangGraph agent against synthetic scenarios with mocked tools,
enabling model comparison and quality scoring.

Usage from a Databricks notebook:
    from dq_agent_langgraph.evals import run_eval_suite, compare_models
    results = run_eval_suite(spark, catalog="`d-cdh`", model="databricks-meta-llama-3-3-70b-instruct")
"""

import json
import time
from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import patch

from pyspark.sql import SparkSession

from .graph_builder import create_dq_agent


# ---------------------------------------------------------------------------
# Eval Data Structures
# ---------------------------------------------------------------------------

@dataclass
class EvalScenario:
    """A synthetic DQ failure scenario for evaluation."""

    id: str
    name: str
    description: str
    severity: str
    findings: list[dict]
    mock_tool_responses: dict  # tool_name -> list of responses (in order)
    expected_root_causes: list[str]
    expected_recommendations: list[str]
    expected_severity_override: Optional[dict] = None


@dataclass
class EvalResult:
    """Result of running one eval scenario."""

    scenario_id: str
    scenario_name: str
    model: str
    passed: bool
    score: float
    root_cause_hits: list[str]
    root_cause_misses: list[str]
    recommendation_hits: list[str]
    recommendation_misses: list[str]
    tool_calls_made: int
    latency_seconds: float
    raw_analysis: str
    details: str


# ---------------------------------------------------------------------------
# Eval Scenarios
# ---------------------------------------------------------------------------

EVAL_SCENARIOS = [
    EvalScenario(
        id="dup_exposure_profits_join",
        name="Duplicate Exposure records caused by faulty join in curated notebook",
        description=(
            "The exposure_profits curated notebook produces duplicates because "
            "a LEFT JOIN with profits_loan_relation (which has multiple rows per account) "
            "is missing a .distinct() call."
        ),
        severity="HIGH",
        findings=[
            {
                "check_name": "duplicate_detection",
                "table_name": "`p-cdh`.`primaryactive`.exposure",
                "status": "FAIL",
                "severity": "HIGH",
                "details": (
                    "847 duplicate key groups found (1694 total duplicate records "
                    "out of 245000). Key columns: ['ExposureId']. "
                    "Sample duplicates: [ExposureId=a3f2b8c...; ExposureId=7d1e4f9...]"
                ),
                "business_date": "2026-06-15",
                "source_system": None,
                "column_name": None,
                "metric_value": 847,
                "threshold": None,
            }
        ],
        mock_tool_responses={
            "run_sql": [
                "Columns: ['SourceSystem', 'dup_count']\nRows returned: 3\n---\n"
                "{'SourceSystem': 'profits', 'dup_count': 847}\n"
                "{'SourceSystem': 'qca', 'dup_count': 0}\n"
                "{'SourceSystem': 'arotron', 'dup_count': 0}",
                "Columns: ['ACCOUNT_NUMBER', 'cnt']\nRows returned: 5\n---\n"
                "{'ACCOUNT_NUMBER': '301234567', 'cnt': 1}\n"
                "{'ACCOUNT_NUMBER': '301234568', 'cnt': 1}\n"
                "{'ACCOUNT_NUMBER': '301234569', 'cnt': 1}\n"
                "{'ACCOUNT_NUMBER': '301234570', 'cnt': 1}\n"
                "{'ACCOUNT_NUMBER': '301234571', 'cnt': 1}",
                "Columns: ['FK_ACCOUNT', 'relation_count']\nRows returned: 5\n---\n"
                "{'FK_ACCOUNT': '301234567', 'relation_count': 2}\n"
                "{'FK_ACCOUNT': '301234568', 'relation_count': 3}\n"
                "{'FK_ACCOUNT': '301234569', 'relation_count': 2}\n"
                "{'FK_ACCOUNT': '301234570', 'relation_count': 2}\n"
                "{'FK_ACCOUNT': '301234571', 'relation_count': 4}",
                "Columns: ['BusinessDate', 'cnt']\nRows returned: 2\n---\n"
                "{'BusinessDate': '2026-06-14', 'cnt': 244153}\n"
                "{'BusinessDate': '2026-06-15', 'cnt': 245847}",
            ],
            "get_table_schema": [
                "Table: `p-cdh`.`primaryactive`.exposure\nColumns (45):\n"
                "  - ExposureId: StringType\n  - SourceSystem: StringType\n"
                "  - ExposureKey: StringType\n  - PartyKey: StringType\n"
                "  - AgreementKey: StringType\n  - PortfolioName: StringType\n"
                "  - BusinessDate: DateType\n  - IngestionTimestamp: TimestampType"
            ],
            "query_dq_history": [
                "No historical failures found matching the filter."
            ],
        },
        expected_root_causes=[
            "profits", "profits_loan_relation", "join", "relation", "distinct", "landed",
        ],
        expected_recommendations=[
            "distinct", "deduplic", "profits_loan_relation",
        ],
    ),
    EvalScenario(
        id="dup_exposure_landed_source_dups",
        name="Duplicate Exposure records from QCA source delivering duplicates in landed",
        description=(
            "qca_dim_cases_static landed table contains duplicate CASE_ACCOUNT_FOR_SEARCH "
            "rows. The source system exported duplicates. Agent should trace to landed."
        ),
        severity="HIGH",
        findings=[
            {
                "check_name": "duplicate_detection",
                "table_name": "`p-cdh`.`primaryactive`.exposure",
                "status": "FAIL",
                "severity": "HIGH",
                "details": (
                    "12450 duplicate key groups found (24900 total duplicate records "
                    "out of 269900). Key columns: ['ExposureId']."
                ),
                "business_date": "2026-06-15",
                "source_system": None,
                "column_name": None,
                "metric_value": 12450,
                "threshold": None,
            }
        ],
        mock_tool_responses={
            "run_sql": [
                "Columns: ['SourceSystem', 'dup_count']\nRows returned: 3\n---\n"
                "{'SourceSystem': 'profits', 'dup_count': 0}\n"
                "{'SourceSystem': 'qca', 'dup_count': 12450}\n"
                "{'SourceSystem': 'arotron', 'dup_count': 0}",
                "Columns: ['CASE_ACCOUNT_FOR_SEARCH', 'cnt']\nRows returned: 5\n---\n"
                "{'CASE_ACCOUNT_FOR_SEARCH': 'AB000000001234', 'cnt': 2}\n"
                "{'CASE_ACCOUNT_FOR_SEARCH': 'AB000000001235', 'cnt': 2}\n"
                "{'CASE_ACCOUNT_FOR_SEARCH': 'AB000000001236', 'cnt': 2}\n"
                "{'CASE_ACCOUNT_FOR_SEARCH': 'AB000000001237', 'cnt': 2}\n"
                "{'CASE_ACCOUNT_FOR_SEARCH': 'AB000000001238', 'cnt': 2}",
                "Columns: ['total_rows', 'distinct_keys']\nRows returned: 1\n---\n"
                "{'total_rows': 24900, 'distinct_keys': 12450}",
                "Columns: ['BusinessDate', 'total_rows', 'distinct_CASE_ACCOUNT']\nRows returned: 2\n---\n"
                "{'BusinessDate': '2026-06-14', 'total_rows': 12430, 'distinct_CASE_ACCOUNT': 12430}\n"
                "{'BusinessDate': '2026-06-15', 'total_rows': 24900, 'distinct_CASE_ACCOUNT': 12450}",
            ],
            "get_table_schema": [
                "Table: `p-cdh`.`landed`.qca_dim_cases_static\nColumns (30):\n"
                "  - CASE_ACCOUNT_FOR_SEARCH: StringType\n  - cust_code: StringType\n"
                "  - CASE_CODE: IntegerType\n  - BusinessDate: DateType"
            ],
            "query_dq_history": [
                "No historical failures found matching the filter."
            ],
        },
        expected_root_causes=[
            "qca", "qca_dim_cases_static", "CASE_ACCOUNT_FOR_SEARCH", "landed", "source", "duplicate",
        ],
        expected_recommendations=[
            "source system", "qca", "deduplic", "isolat", "rerun", "ticket",
        ],
    ),
    EvalScenario(
        id="dup_party_recurring",
        name="Recurring duplicate Party records from upstream source",
        description=(
            "profits_customer delivers duplicate CUST_ID rows (active + closed status). "
            "Recurring issue — agent should identify it as upstream and recurring."
        ),
        severity="HIGH",
        findings=[
            {
                "check_name": "duplicate_detection",
                "table_name": "`p-cdh`.`primaryactive`.party",
                "status": "FAIL",
                "severity": "HIGH",
                "details": (
                    "2103 duplicate key groups found (4206 total duplicate records "
                    "out of 198000). Key columns: ['PartyId']."
                ),
                "business_date": "2026-06-15",
                "source_system": None,
                "column_name": None,
                "metric_value": 2103,
                "threshold": None,
            }
        ],
        mock_tool_responses={
            "run_sql": [
                "Columns: ['SourceSystem', 'dup_count']\nRows returned: 3\n---\n"
                "{'SourceSystem': 'profits', 'dup_count': 2103}\n"
                "{'SourceSystem': 'qca', 'dup_count': 0}\n"
                "{'SourceSystem': 'arotron', 'dup_count': 0}",
                "Columns: ['CUST_ID', 'cnt']\nRows returned: 5\n---\n"
                "{'CUST_ID': '90001234', 'cnt': 2}\n"
                "{'CUST_ID': '90001235', 'cnt': 2}\n"
                "{'CUST_ID': '90001236', 'cnt': 2}\n"
                "{'CUST_ID': '90001237', 'cnt': 2}\n"
                "{'CUST_ID': '90001238', 'cnt': 2}",
                "Columns: ['CUST_STATUS', 'cnt']\nRows returned: 2\n---\n"
                "{'CUST_STATUS': '1', 'cnt': 2103}\n"
                "{'CUST_STATUS': '4', 'cnt': 2103}",
            ],
            "get_table_schema": [
                "Table: `p-cdh`.`landed`.profits_customer\nColumns (25):\n"
                "  - CUST_ID: StringType\n  - CUST_STATUS: StringType\n"
                "  - CUST_NAME: StringType\n  - BusinessDate: DateType"
            ],
            "query_dq_history": [
                "Historical failures (5 found):\n"
                "  [2026-06-14] duplicate_detection | `p-cdh`.`primaryactive`.party | HIGH | metric=2098\n"
                "  [2026-06-13] duplicate_detection | `p-cdh`.`primaryactive`.party | HIGH | metric=2095\n"
                "  [2026-06-12] duplicate_detection | `p-cdh`.`primaryactive`.party | HIGH | metric=2101\n"
                "  [2026-06-11] duplicate_detection | `p-cdh`.`primaryactive`.party | HIGH | metric=2099\n"
                "  [2026-06-10] duplicate_detection | `p-cdh`.`primaryactive`.party | HIGH | metric=2097"
            ],
        },
        expected_root_causes=[
            "profits_customer", "upstream", "source", "recurring", "landed", "CUST_ID",
        ],
        expected_recommendations=[
            "deduplic", "source system", "profits_customer", "ticket", "rerun", "admin",
        ],
    ),
]


# ---------------------------------------------------------------------------
# Mock Tool Executor for Evals
# ---------------------------------------------------------------------------

class MockToolExecutor:
    """Replaces real tool execution with pre-defined responses during evals."""

    def __init__(self, scenario: EvalScenario):
        self.scenario = scenario
        self.call_counters: dict[str, int] = {}

    def mock_invoke(self, tool_input: str, tool_name: str) -> str:
        """Return mock response for the given tool call."""
        if tool_name not in self.call_counters:
            self.call_counters[tool_name] = 0

        responses = self.scenario.mock_tool_responses.get(tool_name, [])
        idx = self.call_counters[tool_name]
        self.call_counters[tool_name] += 1

        if idx < len(responses):
            return responses[idx]
        return f"No more mock responses for {tool_name} (call #{idx + 1})"

    @property
    def total_calls(self) -> int:
        return sum(self.call_counters.values())


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _score_analysis(
    analysis_text: str,
    expected_causes: list[str],
    expected_recs: list[str],
) -> tuple[float, list[str], list[str], list[str], list[str]]:
    """Score the agent output against expected keywords."""
    text_lower = analysis_text.lower()

    cause_hits = [kw for kw in expected_causes if kw.lower() in text_lower]
    cause_misses = [kw for kw in expected_causes if kw.lower() not in text_lower]

    rec_hits = [kw for kw in expected_recs if kw.lower() in text_lower]
    rec_misses = [kw for kw in expected_recs if kw.lower() not in text_lower]

    total_expected = len(expected_causes) + len(expected_recs)
    total_hits = len(cause_hits) + len(rec_hits)
    score = total_hits / total_expected if total_expected > 0 else 1.0

    return score, cause_hits, cause_misses, rec_hits, rec_misses


# ---------------------------------------------------------------------------
# Eval Runner
# ---------------------------------------------------------------------------

def run_single_eval(
    spark: SparkSession,
    scenario: EvalScenario,
    catalog: str = "`p-cdh`",
    model: str = "databricks-meta-llama-3-3-70b-instruct",
) -> EvalResult:
    """
    Run a single eval scenario against the LangGraph DQ agent.
    Uses mock tool responses instead of real Spark queries.
    """
    from langchain_core.messages import ToolMessage
    from .tools import run_sql, get_table_schema, query_dq_history

    mock_executor = MockToolExecutor(scenario)
    run_date = scenario.findings[0].get("business_date", "2026-06-15")

    start_time = time.time()

    # Create the agent
    agent = create_dq_agent(
        spark=spark,
        catalog=catalog,
        run_date=run_date,
        env_prefix="p",
        model_endpoint=model,
    )

    # Patch tool invocations to use mock responses
    original_run_sql = run_sql.invoke
    original_get_schema = get_table_schema.invoke
    original_query_history = query_dq_history.invoke

    def mock_run_sql(input_data, *args, **kwargs):
        return mock_executor.mock_invoke(str(input_data), "run_sql")

    def mock_get_schema(input_data, *args, **kwargs):
        return mock_executor.mock_invoke(str(input_data), "get_table_schema")

    def mock_query_history(input_data, *args, **kwargs):
        return mock_executor.mock_invoke(str(input_data), "query_dq_history")

    try:
        with patch.object(run_sql, 'invoke', mock_run_sql), \
             patch.object(get_table_schema, 'invoke', mock_get_schema), \
             patch.object(query_dq_history, 'invoke', mock_query_history):

            result = agent.invoke({
                "findings": scenario.findings,
                "run_date": run_date,
                "catalog": catalog,
                "env_prefix": "p",
            })
    except Exception as e:
        return EvalResult(
            scenario_id=scenario.id,
            scenario_name=scenario.name,
            model=model,
            passed=False,
            score=0.0,
            root_cause_hits=[],
            root_cause_misses=scenario.expected_root_causes,
            recommendation_hits=[],
            recommendation_misses=scenario.expected_recommendations,
            tool_calls_made=mock_executor.total_calls,
            latency_seconds=time.time() - start_time,
            raw_analysis="",
            details=f"Agent error: {str(e)}",
        )

    latency = time.time() - start_time

    # Combine all text for scoring
    analysis_text = " ".join([
        result.get("summary", ""),
        " ".join(result.get("root_causes", [])),
        " ".join(result.get("recommendations", [])),
        result.get("landed_table_origin", ""),
        result.get("issue_owner", ""),
    ])

    score, c_hits, c_misses, r_hits, r_misses = _score_analysis(
        analysis_text,
        scenario.expected_root_causes,
        scenario.expected_recommendations,
    )

    return EvalResult(
        scenario_id=scenario.id,
        scenario_name=scenario.name,
        model=model,
        passed=score >= 0.5,
        score=score,
        root_cause_hits=c_hits,
        root_cause_misses=c_misses,
        recommendation_hits=r_hits,
        recommendation_misses=r_misses,
        tool_calls_made=mock_executor.total_calls,
        latency_seconds=latency,
        raw_analysis=analysis_text,
        details=f"Completed. Tier: {result.get('severity_tier', 'N/A')}",
    )


def run_eval_suite(
    spark: SparkSession,
    catalog: str = "`d-cdh`",
    model: str = "databricks-meta-llama-3-3-70b-instruct",
    scenarios: Optional[list[EvalScenario]] = None,
) -> list[EvalResult]:
    """
    Run all eval scenarios against a specific model.

    Parameters
    ----------
    spark : SparkSession
    catalog : str
        Catalog path for table references in the scenario.
    model : str
        Databricks FMAPI model endpoint to test.
    scenarios : list[EvalScenario], optional
        Custom scenarios. Defaults to EVAL_SCENARIOS.

    Returns
    -------
    list[EvalResult]
    """
    if scenarios is None:
        scenarios = EVAL_SCENARIOS

    results = []
    print(f"\n{'=' * 70}")
    print(f"  DQ Agent (LangGraph) Eval Suite — Model: {model}")
    print(f"  Scenarios: {len(scenarios)}")
    print(f"{'=' * 70}\n")

    for i, scenario in enumerate(scenarios):
        print(f"  [{i + 1}/{len(scenarios)}] {scenario.name}...")
        result = run_single_eval(spark, scenario, catalog=catalog, model=model)
        results.append(result)

        status = "✓ PASS" if result.passed else "✗ FAIL"
        print(f"    {status} | Score: {result.score:.0%} | "
              f"Tools used: {result.tool_calls_made} | "
              f"Latency: {result.latency_seconds:.1f}s")
        if result.root_cause_misses:
            print(f"    Missing causes: {result.root_cause_misses}")
        if result.recommendation_misses:
            print(f"    Missing recs: {result.recommendation_misses}")
        print()

    passed = sum(1 for r in results if r.passed)
    avg_score = sum(r.score for r in results) / len(results) if results else 0
    avg_latency = sum(r.latency_seconds for r in results) / len(results) if results else 0

    print(f"{'=' * 70}")
    print(f"  RESULTS: {passed}/{len(results)} passed | "
          f"Avg score: {avg_score:.0%} | Avg latency: {avg_latency:.1f}s")
    print(f"{'=' * 70}\n")

    return results


def compare_models(
    spark: SparkSession,
    models: list[str],
    catalog: str = "`d-cdh`",
    scenarios: Optional[list[EvalScenario]] = None,
) -> dict[str, list[EvalResult]]:
    """
    Run the eval suite across multiple models for comparison.

    Parameters
    ----------
    spark : SparkSession
    models : list[str]
        List of model endpoint names to compare.
    catalog : str
    scenarios : list[EvalScenario], optional

    Returns
    -------
    dict[str, list[EvalResult]]
    """
    all_results = {}
    for model in models:
        all_results[model] = run_eval_suite(spark, catalog=catalog, model=model, scenarios=scenarios)

    print(f"\n{'=' * 70}")
    print("  MODEL COMPARISON (LangGraph)")
    print(f"{'=' * 70}")
    print(f"  {'Model':<45} {'Pass Rate':<12} {'Avg Score':<12} {'Avg Latency'}")
    print(f"  {'-' * 45} {'-' * 10} {'-' * 10} {'-' * 10}")

    for model, results in all_results.items():
        passed = sum(1 for r in results if r.passed)
        avg_score = sum(r.score for r in results) / len(results) if results else 0
        avg_latency = sum(r.latency_seconds for r in results) / len(results) if results else 0
        print(f"  {model:<45} {passed}/{len(results):<10} {avg_score:<10.0%} {avg_latency:.1f}s")

    print(f"{'=' * 70}\n")
    return all_results
