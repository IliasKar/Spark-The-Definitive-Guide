"""
DQ Agent — Graph Builder & Model Configuration
================================================
Provides the factory function to create the compiled LangGraph agent
and manages the LLM model instance (ChatDatabricks from databricks-langchain).

Usage from a Databricks notebook:
    from dq_agent_langgraph.graph_builder import create_dq_agent
    agent = create_dq_agent(spark, catalog="`p-cdh`", run_date="2026-06-15")
    result = agent.invoke({"findings": [...], ...})
"""

from typing import Optional

from databricks_langchain import ChatDatabricks
from langgraph.graph.state import CompiledStateGraph
from pyspark.sql import SparkSession

from .tools import configure_tools

# ---------------------------------------------------------------------------
# Module-level model instance
# ---------------------------------------------------------------------------

_model: Optional[ChatDatabricks] = None
DEFAULT_MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"


def get_model() -> ChatDatabricks:
    """Get the configured ChatDatabricks model instance."""
    if _model is None:
        raise RuntimeError(
            "Model not configured. Call create_dq_agent() first."
        )
    return _model


def create_dq_agent(
    spark: SparkSession,
    catalog: str,
    run_date: str,
    env_prefix: str = "p",
    model_endpoint: str = DEFAULT_MODEL_ENDPOINT,
    temperature: float = 0.1,
    max_tokens: int = 8192,
) -> CompiledStateGraph:
    """
    Create and compile the DQ Agent LangGraph.

    This is the main entry point. Call this from a Databricks notebook,
    then invoke the returned graph with the DQ findings.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session from Databricks notebook.
    catalog : str
        Catalog path, e.g. "`p-cdh`"
    run_date : str
        The BusinessDate being analyzed (YYYY-MM-DD).
    env_prefix : str
        Environment prefix: "d", "qa", or "p".
    model_endpoint : str
        Databricks Foundation Model API endpoint name.
        Examples:
            "databricks-meta-llama-3-3-70b-instruct"
            "databricks-claude-sonnet-4"
            "databricks-meta-llama-3-1-405b-instruct"
    temperature : float
        LLM temperature (0.0-1.0). Lower = more deterministic.
    max_tokens : int
        Maximum tokens in LLM response.

    Returns
    -------
    CompiledStateGraph
        The compiled LangGraph agent, ready to be invoked.

    Example
    -------
    ```python
    from dq_agent_langgraph.graph_builder import create_dq_agent

    agent = create_dq_agent(
        spark=spark,
        catalog="`p-cdh`",
        run_date="2026-06-15",
        env_prefix="p",
        model_endpoint="databricks-meta-llama-3-3-70b-instruct",
    )

    result = agent.invoke({
        "findings": findings_list,
        "run_date": "2026-06-15",
        "catalog": "`p-cdh`",
        "env_prefix": "p",
    })

    print(result["summary"])
    print(result["root_causes"])
    print(result["recommendations"])
    ```
    """
    global _model

    # --- Configure the LLM ---
    _model = ChatDatabricks(
        endpoint=model_endpoint,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    # --- Configure the tools (inject SparkSession) ---
    configure_tools(spark=spark, catalog=catalog, run_date=run_date)

    # --- Build and compile the graph ---
    from .graph import build_graph

    graph = build_graph()
    compiled = graph.compile()

    return compiled
