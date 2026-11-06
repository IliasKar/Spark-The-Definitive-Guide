"""
DQ Agent — LangGraph Implementation for Databricks
====================================================
A LangGraph-based data quality agent that investigates DQ failures
in the primaryactive (silver) layer using a structured graph workflow.

This implementation replaces the hand-rolled ReAct loop with LangGraph's
StateGraph, making the agent deployable via Databricks Agent Framework.
"""
