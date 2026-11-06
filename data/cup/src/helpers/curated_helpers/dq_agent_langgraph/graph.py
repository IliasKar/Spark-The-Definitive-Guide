"""
DQ Agent — LangGraph Graph Definition
========================================
Defines the StateGraph with nodes for:
  1. Severity routing
  2. Agentic analysis (HIGH/CRITICAL) — ReAct tool-calling loop
  3. Pre-defined analysis (MEDIUM) — single LLM pass
  4. Result merging & output formatting

The graph is compiled into a runnable that can be invoked from a
Databricks notebook or deployed via Databricks Agent Framework.
"""

import json
from typing import Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode

from .prompts import AGENTIC_SYSTEM_PROMPT, FORCED_FINAL_PROMPT, MEDIUM_SYSTEM_PROMPT
from .state import AgentState
from .tools import ALL_TOOLS


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_AGENT_STEPS = 10  # Max tool-use iterations for HIGH/CRITICAL
MAX_DETAILED_FINDINGS = 5  # Max findings given full per-finding analysis (rest are listed/summarized)


# ---------------------------------------------------------------------------
# Node: Severity Router
# ---------------------------------------------------------------------------

def route_by_severity(state: AgentState) -> AgentState:
    """
    Classify the findings into severity tiers and set the routing field.
    Also prepares the initial messages for the LLM.
    """
    findings = state["findings"]
    failures = [f for f in findings if f["status"] == "FAIL"]

    high_critical = [f for f in failures if f["severity"] in ("CRITICAL", "HIGH")]
    medium = [f for f in failures if f["severity"] == "MEDIUM"]

    # If ANY HIGH/CRITICAL findings exist, use the agentic (tool-using) path and
    # analyze ALL failing findings in it (HIGH/CRITICAL first, then MEDIUM) so no
    # finding is dropped. Only when there are no HIGH/CRITICAL do we use the
    # lighter MEDIUM reasoning path. LOW/none → no LLM.
    if high_critical:
        tier = "high_critical"
        active_findings = high_critical + medium  # cover everything
    elif medium:
        tier = "medium"
        active_findings = medium
    else:
        tier = "low"
        active_findings = []

    # Build the initial messages for the LLM conversation
    catalog = state["catalog"]
    run_date = state["run_date"]

    findings_text = []
    for i, f in enumerate(active_findings):
        findings_text.append(
            f"[{i}] CHECK: {f['check_name']} | TABLE: {f['table_name']} | "
            f"SEVERITY: {f['severity']} | COLUMN: {f.get('column_name') or 'N/A'} | "
            f"SOURCE_SYSTEM: {f.get('source_system') or 'all'} | "
            f"METRIC: {f.get('metric_value')} | THRESHOLD: {f.get('threshold')} | "
            f"DETAILS: {f['details']}"
        )

    if tier == "high_critical":
        n_high = len(high_critical)
        n_med = len(medium)
        # Per-finding detail is produced ONLY for up to MAX_DETAILED_FINDINGS of
        # the most severe HIGH/CRITICAL findings. The remaining findings are
        # covered by the top-level roll-up. This keeps the JSON within the
        # model's output token budget so the response is not truncated (which
        # previously broke JSON parsing).
        detailed_n = min(n_high, MAX_DETAILED_FINDINGS)
        system_prompt = AGENTIC_SYSTEM_PROMPT.format(catalog=catalog)
        user_content = (
            f"Investigate the data quality failures below for BusinessDate = {run_date} "
            f"({n_high} HIGH/CRITICAL, {n_med} MEDIUM).\n"
            f"Catalog: {catalog}\n\n"
            f"Findings (indexed):\n" + "\n".join(findings_text) + "\n\n"
            f"Investigate the most severe HIGH/CRITICAL findings deeply with the tools. "
            f"In your JSON, return a `findings_analysis` array with AT MOST {detailed_n} "
            f"entries — one for each of the {detailed_n} most severe/important findings "
            f"(do NOT create entries for every finding), each with its specific "
            f"root_cause, differing_columns, landed_table_origin, troubleshooting_sql, "
            f"issue_owner and recommendation. Summarize ALL remaining findings "
            f"(the other HIGH/CRITICAL and the MEDIUM ones) briefly in the top-level "
            f"`summary` and `recommendations` only. Keep every field concise. "
            f"When done, provide your final JSON analysis."
        )
    elif tier == "medium":
        system_prompt = MEDIUM_SYSTEM_PROMPT
        # Cap per-finding detail to the first 20 medium findings to avoid
        # truncation; the rest are covered by the roll-up summary.
        system_prompt = MEDIUM_SYSTEM_PROMPT
        user_content = (
            f"Analyze these {len(active_findings)} MEDIUM severity DQ findings for "
            f"BusinessDate = {run_date}. Group similar findings (same check/table pattern) "
            f"rather than repeating them:\n\n"
            f"Findings (indexed):\n" + "\n".join(findings_text) + "\n\n"
            f"In your JSON, include a `findings_analysis` array with one entry per DISTINCT "
            f"issue (group similar findings; at most 20 entries), plus the top-level "
            f"roll-up fields. Keep each field concise. Provide your JSON analysis."
        )
    else:
        system_prompt = ""
        user_content = ""

    messages = []
    if system_prompt:
        messages.append(SystemMessage(content=system_prompt))
    if user_content:
        messages.append(HumanMessage(content=user_content))

    return {
        "severity_tier": tier,
        "messages": messages,
        "summary": "",
        "root_causes": [],
        "recommendations": [],
        "landed_table_origin": "",
        "landed_columns_affected": [],
        "differing_columns": [],
        "issue_owner": "",
        "troubleshooting_sql": "",
        "findings_analysis": [],
        "severity_overrides": {},
        "queries_executed": [],
    }


# ---------------------------------------------------------------------------
# Node: Agentic LLM Call (with tools bound)
# ---------------------------------------------------------------------------

def call_agent_model(state: AgentState) -> AgentState:
    """
    Call the LLM with tools bound. The LLM will either:
    - Return tool_calls (→ route to tool_node)
    - Return a final text response (→ route to parse_response)
    """
    from .graph_builder import get_model

    model = get_model()
    model_with_tools = model.bind_tools(ALL_TOOLS)
    response = model_with_tools.invoke(state["messages"])
    return {"messages": [response]}


# ---------------------------------------------------------------------------
# Node: Force Final Answer (when max steps exhausted)
# ---------------------------------------------------------------------------

def force_final_answer(state: AgentState) -> AgentState:
    """
    When the agent has used all tool steps, send a final prompt
    forcing it to produce the JSON analysis without further tool use.
    """
    from .graph_builder import get_model

    env = state.get("env_prefix", "p")
    forced_prompt = FORCED_FINAL_PROMPT.format(env=env)

    model = get_model()  # No tools bound → forces text response
    messages = state["messages"] + [HumanMessage(content=forced_prompt)]
    response = model.invoke(messages)
    return {"messages": [HumanMessage(content=forced_prompt), response]}


# ---------------------------------------------------------------------------
# Node: Parse LLM Response into structured output
# ---------------------------------------------------------------------------

def _extract_json_object(content: str) -> dict:
    """
    Robustly extract a JSON object from an LLM response.

    Handles: raw JSON, ```json fenced blocks, fences without a closing
    fence, and leading/trailing prose. Uses brace-matching to locate the
    first complete top-level {...} object. Raises ValueError/JSONDecodeError
    if no complete object can be parsed (e.g. the response was truncated).
    """
    if not content:
        raise ValueError("empty content")

    text = content.strip()

    # Strip a leading ```json / ``` fence if present.
    if text.startswith("```"):
        text = text[3:]
        if text[:4].lower() == "json":
            text = text[4:]
        # Drop a trailing closing fence if present.
        if "```" in text:
            text = text[: text.rindex("```")]
        text = text.strip()

    # Fast path: the whole thing is already a JSON object.
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Brace-matching: find the first balanced {...} (ignoring braces in strings).
    start = text.find("{")
    if start == -1:
        raise ValueError("no JSON object found")

    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
        else:
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return json.loads(text[start : i + 1])

    # Unbalanced braces → response was truncated.
    raise ValueError("no complete JSON object (truncated response)")


def parse_response(state: AgentState) -> AgentState:
    """
    Extract the final JSON analysis from the last AI message
    and populate the output fields in the state.
    """
    # Find the last AI message
    last_ai_msg = None
    for msg in reversed(state["messages"]):
        if isinstance(msg, AIMessage) and msg.content and not msg.tool_calls:
            last_ai_msg = msg
            break

    if not last_ai_msg:
        return {
            "summary": "No analysis produced.",
            "root_causes": [],
            "recommendations": [],
            "landed_table_origin": "",
            "landed_columns_affected": [],
            "differing_columns": [],
            "issue_owner": "",
            "troubleshooting_sql": "",
            "findings_analysis": [],
            "severity_overrides": {},
        }

    content = last_ai_msg.content

    try:
        parsed = _extract_json_object(content)

        return {
            "summary": parsed.get("summary", content[:500]),
            "root_causes": parsed.get("root_causes", []),
            "recommendations": parsed.get("recommendations", []),
            "landed_table_origin": parsed.get("landed_table_origin", ""),
            "landed_columns_affected": parsed.get("landed_columns_affected", []),
            "differing_columns": parsed.get("differing_columns", []),
            "issue_owner": parsed.get("issue_owner", ""),
            "troubleshooting_sql": parsed.get("troubleshooting_sql", ""),
            "findings_analysis": parsed.get("findings_analysis", []),
            "severity_overrides": parsed.get("severity_overrides", {}),
        }
    except (json.JSONDecodeError, IndexError, KeyError, ValueError):
        # Could not parse structured JSON (e.g. truncated / malformed response).
        # Surface a clean message instead of dumping raw ```json text.
        preview = content.strip() if content else ""
        if preview.startswith("```"):
            preview = preview.lstrip("`")
            if preview[:4].lower() == "json":
                preview = preview[4:]
        return {
            "summary": (
                "Agent response could not be parsed as JSON (likely truncated). "
                "Raw preview: " + preview[:300]
            ) if preview else "No analysis produced.",
            "root_causes": [],
            "recommendations": [],
            "landed_table_origin": "",
            "landed_columns_affected": [],
            "differing_columns": [],
            "issue_owner": "",
            "troubleshooting_sql": "",
            "findings_analysis": [],
            "severity_overrides": {},
        }


# ---------------------------------------------------------------------------
# Node: Low severity (no LLM needed)
# ---------------------------------------------------------------------------

def handle_low_severity(state: AgentState) -> AgentState:
    """For LOW severity / all-passing results — skip LLM entirely."""
    return {
        "summary": "All data quality checks passed or only LOW severity issues detected. No investigation needed.",
        "root_causes": [],
        "recommendations": [],
        "landed_table_origin": "",
        "landed_columns_affected": [],
        "differing_columns": [],
        "issue_owner": "",
        "troubleshooting_sql": "",
        "findings_analysis": [],
        "severity_overrides": {},
    }


# ---------------------------------------------------------------------------
# Node: Medium severity — pre-defined queries + single LLM pass
# ---------------------------------------------------------------------------

def run_predefined_queries(state: AgentState) -> AgentState:
    """
    Execute pre-defined investigative queries for MEDIUM findings,
    append their results to the conversation, then let the LLM reason
    in a single pass (no tools).
    """
    from .tools import _spark, _catalog, _run_date

    findings = [f for f in state["findings"] if f["status"] == "FAIL" and f["severity"] == "MEDIUM"]
    catalog = state["catalog"]
    run_date = state["run_date"]

    investigations = []
    queries_executed = []

    for finding in findings:
        table_name = finding["table_name"]

        # Volume comparison (today vs yesterday)
        try:
            query = f"""
                SELECT BusinessDate, COUNT(*) as cnt
                FROM {table_name}
                WHERE BusinessDate IN ('{run_date}', DATE_SUB('{run_date}', 1))
                GROUP BY BusinessDate ORDER BY BusinessDate
            """
            queries_executed.append(query)
            rows = _spark.sql(query).collect()
            volume_info = {row["BusinessDate"].strftime("%Y-%m-%d"): row["cnt"] for row in rows}
            investigations.append({
                "type": "volume_comparison",
                "table": table_name,
                "volumes": volume_info,
            })
        except Exception as e:
            investigations.append({
                "type": "volume_comparison",
                "table": table_name,
                "error": str(e),
            })

        # Null distribution by SourceSystem
        if finding["check_name"] in ("null_rate_bronze", "null_rate_silver") and finding.get("column_name"):
            col_name = finding["column_name"]
            try:
                query = f"""
                    SELECT SourceSystem, COUNT(*) as total,
                           SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) as nulls
                    FROM {table_name}
                    WHERE BusinessDate = '{run_date}'
                    GROUP BY SourceSystem
                """
                queries_executed.append(query)
                rows = _spark.sql(query).collect()
                null_by_source = [
                    {"source": row["SourceSystem"], "total": row["total"], "nulls": row["nulls"]}
                    for row in rows
                ]
                investigations.append({
                    "type": "null_by_source_system",
                    "table": table_name,
                    "column": col_name,
                    "results": null_by_source,
                })
            except Exception:
                pass

    # Append investigation results to conversation for LLM
    investigation_msg = HumanMessage(
        content=(
            f"Here are pre-computed investigation results for the MEDIUM findings:\n\n"
            f"{json.dumps(investigations, indent=2, default=str)}\n\n"
            f"Analyze these results and provide your JSON analysis."
        )
    )

    return {
        "messages": [investigation_msg],
        "queries_executed": queries_executed,
    }


def call_medium_model(state: AgentState) -> AgentState:
    """Single LLM call for MEDIUM severity — no tools, just reasoning."""
    from .graph_builder import get_model

    model = get_model()  # No tools
    response = model.invoke(state["messages"])
    return {"messages": [response]}


# ---------------------------------------------------------------------------
# Conditional Edges
# ---------------------------------------------------------------------------

def after_severity_router(state: AgentState) -> Literal["agentic", "medium", "low"]:
    """Route to the appropriate analysis path based on severity tier."""
    tier = state["severity_tier"]
    if tier == "high_critical":
        return "agentic"
    elif tier == "medium":
        return "medium"
    else:
        return "low"


def should_continue_agentic(state: AgentState) -> Literal["tools", "parse", "force_final"]:
    """
    After the LLM responds, decide whether to:
    - Execute tool calls
    - Parse the final response
    - Force a final answer (max steps reached)
    """
    messages = state["messages"]

    # Find the last AI message
    last_ai = None
    for msg in reversed(messages):
        if isinstance(msg, AIMessage):
            last_ai = msg
            break

    if last_ai is None:
        return "parse"

    # If the LLM wants to use tools
    if last_ai.tool_calls:
        # Count how many tool rounds we've done (each tool round = 1 AI message with tool_calls)
        tool_rounds = sum(
            1 for m in messages
            if isinstance(m, AIMessage) and m.tool_calls
        )
        if tool_rounds >= MAX_AGENT_STEPS:
            return "force_final"
        return "tools"

    # No tool calls → final answer
    return "parse"


# ---------------------------------------------------------------------------
# Graph Construction
# ---------------------------------------------------------------------------

def build_graph() -> StateGraph:
    """
    Build the DQ Agent LangGraph StateGraph.

    Returns the compiled graph ready to be invoked.
    """
    # Create the tool node (LangGraph's prebuilt ToolNode handles tool execution)
    tool_node = ToolNode(ALL_TOOLS)

    # Build the graph
    graph = StateGraph(AgentState)

    # --- Add nodes ---
    graph.add_node("route_severity", route_by_severity)
    graph.add_node("call_agent_model", call_agent_model)
    graph.add_node("tools", tool_node)
    graph.add_node("force_final_answer", force_final_answer)
    graph.add_node("parse_response", parse_response)
    graph.add_node("handle_low", handle_low_severity)
    graph.add_node("run_predefined_queries", run_predefined_queries)
    graph.add_node("call_medium_model", call_medium_model)

    # --- Set entry point ---
    graph.set_entry_point("route_severity")

    # --- Conditional edge: severity routing ---
    graph.add_conditional_edges(
        "route_severity",
        after_severity_router,
        {
            "agentic": "call_agent_model",
            "medium": "run_predefined_queries",
            "low": "handle_low",
        },
    )

    # --- Agentic path ---
    graph.add_conditional_edges(
        "call_agent_model",
        should_continue_agentic,
        {
            "tools": "tools",
            "parse": "parse_response",
            "force_final": "force_final_answer",
        },
    )
    graph.add_edge("tools", "call_agent_model")  # After tool execution, call LLM again
    graph.add_edge("force_final_answer", "parse_response")

    # --- Medium path ---
    graph.add_edge("run_predefined_queries", "call_medium_model")
    graph.add_edge("call_medium_model", "parse_response")

    # --- Terminal nodes ---
    graph.add_edge("parse_response", END)
    graph.add_edge("handle_low", END)

    return graph
