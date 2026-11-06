"""
DQ Agent — LangGraph State Definition
======================================
Defines the shared state that flows through the LangGraph nodes.
"""

from dataclasses import dataclass, field
from typing import Annotated, Optional
from typing_extensions import TypedDict

from langgraph.graph.message import add_messages


class DQFinding(TypedDict):
    """A single DQ check finding passed into the agent."""

    check_name: str
    table_name: str
    status: str  # "PASS" or "FAIL"
    severity: str  # "CRITICAL", "HIGH", "MEDIUM", "LOW"
    details: str
    business_date: str
    source_system: Optional[str]
    column_name: Optional[str]
    metric_value: Optional[float]
    threshold: Optional[float]


class AgentState(TypedDict):
    """
    LangGraph state for the DQ Agent.

    The `messages` field uses LangGraph's message accumulator so
    the ReAct tool-calling loop appends messages automatically.
    """

    # --- Input fields (set once at graph invocation) ---
    findings: list[DQFinding]
    run_date: str
    catalog: str  # e.g. "`p-cdh`"
    env_prefix: str  # e.g. "p", "qa", "d"

    # --- Routing ---
    severity_tier: str  # "high_critical", "medium", "low"

    # --- LLM conversation (accumulated via add_messages) ---
    messages: Annotated[list, add_messages]

    # --- Output fields (set by analysis nodes) ---
    summary: str
    root_causes: list[str]
    recommendations: list[str]
    landed_table_origin: str
    landed_columns_affected: list[str]
    differing_columns: list[str]  # for duplicates: primaryactive columns that differ within an ID group
    issue_owner: str  # "source_system" or "etl_logic"
    troubleshooting_sql: str  # example query the engineer can run to investigate
    findings_analysis: list[dict]  # per-finding analysis, one entry per failing finding
    severity_overrides: dict
    queries_executed: list[str]
