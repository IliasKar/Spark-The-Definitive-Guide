"""
MS Teams Alerting Module
========================
Formats DQ findings and LangGraph agent analysis into Adaptive Cards
and posts them to MS Teams via a Power Automate Workflow HTTP trigger.

NOTE: The legacy Office 365 "Incoming Webhook" connector was retired by
Microsoft (final rollout ~May 2026). This module targets its replacement:
a Power Automate "Post to a channel when a webhook request is received"
workflow, which exposes an HTTPS URL and accepts the same Adaptive Card
payload used here (a `message` object with an `attachments` array of
`application/vnd.microsoft.card.adaptive`). The workflow URL is stored in
AWS SSM Parameter Store and retrieved at runtime.
"""

import json
from typing import Optional

import boto3
import requests


def get_teams_webhook_url(ssm_client) -> str:
    """Retrieve the MS Teams Power Automate Workflow URL from AWS SSM.

    Stored under /cfn/teams_webhook_url. WithDecryption=True works for both
    SecureString (recommended for prod) and plain String (fine for testing)
    parameters — SSM ignores the flag for non-encrypted values.
    """
    response = ssm_client.get_parameter(
        Name="/cfn/teams_webhook_url",
        WithDecryption=True,
    )
    return response["Parameter"]["Value"]


def format_dq_alert_card(
    failures: list,
    run_date: str,
    environment: str,
    agent_state: Optional[dict] = None,
) -> dict:
    """
    Build an Adaptive Card payload for MS Teams summarizing DQ failures.
    Optionally includes AI-generated analysis from the LangGraph agent state.

    Parameters
    ----------
    failures : list[dict]
        List of failed DQ finding dicts (DQFinding TypedDicts).
    run_date : str
        The BusinessDate that was checked.
    environment : str
        Environment prefix (d, qa, p).
    agent_state : dict, optional
        The final LangGraph agent state with summary, root_causes, recommendations.
    """
    env_label = {"d": "DEV", "qa": "QA", "p": "PROD"}.get(environment, environment.upper())

    severity_icons = {
        "CRITICAL": "🔴",
        "HIGH": "🟠",
        "MEDIUM": "🟡",
        "LOW": "⚪",
    }

    # Group by severity
    severity_counts = {}
    for f in failures:
        sev = f["severity"] if isinstance(f, dict) else f.severity
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    # Build facts for each failure
    failure_facts = []
    for f in failures[:20]:
        if isinstance(f, dict):
            sev = f["severity"]
            check = f["check_name"]
            table = f["table_name"]
            source = f.get("source_system") or ""
            details = f["details"]
        else:
            sev = f.severity
            check = f.check_name
            table = f.table_name
            source = f.source_system or ""
            details = f.details

        icon = severity_icons.get(sev, "⚪")
        source_info = f" [{source}]" if source else ""
        failure_facts.append({
            "type": "TextBlock",
            "text": f"{icon} **[{sev}] {check}** | {table}{source_info}",
            "wrap": True,
            "size": "Small",
            "weight": "Bolder",
            "color": "Attention" if sev in ("CRITICAL", "HIGH") else "Warning",
        })
        failure_facts.append({
            "type": "TextBlock",
            "text": details[:200],
            "wrap": True,
            "size": "Small",
            "spacing": "None",
        })
        failure_facts.append({"type": "TextBlock", "text": "---", "spacing": "Small"})

    if len(failures) > 20:
        failure_facts.append({
            "type": "TextBlock",
            "text": f"... and {len(failures) - 20} more failures.",
            "wrap": True,
            "size": "Small",
            "isSubtle": True,
        })

    severity_summary = ", ".join(
        [f"{severity_icons.get(s, '')} {s}: {c}" for s, c in sorted(severity_counts.items())]
    )

    # Timestamp
    from datetime import datetime
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": None,
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"🚨 Data Quality Alert — {env_label}",
                        "weight": "Bolder",
                        "size": "Large",
                        "color": "Attention",
                    },
                    {
                        "type": "FactSet",
                        "facts": [
                            {"title": "BusinessDate", "value": run_date},
                            {"title": "Environment", "value": env_label},
                            {"title": "Total Failures", "value": str(len(failures))},
                            {"title": "By Severity", "value": severity_summary},
                            {"title": "Timestamp", "value": ts},
                        ],
                    },
                ]
                + _build_ai_analysis_section(agent_state)
                + [{"type": "TextBlock", "text": "**Failure Details:**", "wrap": True, "spacing": "Medium"}]
                + failure_facts[:50],
            },
        }],
    }

    return card


def _build_ai_analysis_section(agent_state: Optional[dict]) -> list:
    """Build Adaptive Card elements for the AI analysis section."""
    if agent_state is None:
        return []

    summary = agent_state.get("summary", "")
    root_causes = agent_state.get("root_causes", [])
    recommendations = agent_state.get("recommendations", [])

    if not summary and not root_causes:
        return []

    elements = [
        {
            "type": "TextBlock",
            "text": "**🤖 AI Root-Cause Analysis (LangGraph):**",
            "wrap": True,
            "spacing": "Medium",
            "weight": "Bolder",
        },
        {
            "type": "TextBlock",
            "text": summary[:500],
            "wrap": True,
            "size": "Small",
            "spacing": "Small",
        },
    ]

    if root_causes:
        causes_text = "\n".join([f"• {c}" for c in root_causes[:5]])
        elements.append({
            "type": "TextBlock",
            "text": f"**Root Causes:**\n{causes_text}",
            "wrap": True,
            "size": "Small",
            "spacing": "Small",
        })

    if recommendations:
        recs_text = "\n".join([f"• {r}" for r in recommendations[:5]])
        elements.append({
            "type": "TextBlock",
            "text": f"**Recommendations:**\n{recs_text}",
            "wrap": True,
            "size": "Small",
            "spacing": "Small",
        })

    landed_origin = agent_state.get("landed_table_origin", "")
    issue_owner = agent_state.get("issue_owner", "")
    if landed_origin:
        elements.append({
            "type": "TextBlock",
            "text": f"**Origin:** {landed_origin} | **Owner:** {issue_owner}",
            "wrap": True,
            "size": "Small",
            "spacing": "Small",
            "isSubtle": True,
        })

    elements.append({"type": "TextBlock", "text": "---", "spacing": "Medium"})
    return elements


def send_teams_alert(
    failures: list,
    run_date: str,
    environment: str,
    ssm_client,
    webhook_url_override: Optional[str] = None,
    agent_state: Optional[dict] = None,
) -> bool:
    """
    Send a DQ alert to the "Airflow Failures/Updates" MS Teams channel.

    Only CRITICAL, HIGH, and MEDIUM severity failures are sent.

    Parameters
    ----------
    failures : list
        List of failed DQ findings (dicts or DQResult objects).
    run_date : str
        The BusinessDate that was checked.
    environment : str
        Environment prefix (d, qa, p).
    ssm_client : boto3 SSM client
    webhook_url_override : str, optional
        Skip SSM lookup and use this Workflow URL directly (e.g. from a
        Databricks secret). Useful for local/dev testing.
    agent_state : dict, optional
        Final LangGraph agent state for AI analysis section.

    Returns
    -------
    bool
        True if alert sent successfully.
    """
    # Filter alertable
    alertable = []
    for f in failures:
        sev = f["severity"] if isinstance(f, dict) else f.severity
        if sev in ("CRITICAL", "HIGH", "MEDIUM"):
            alertable.append(f)

    if not alertable:
        print("[DQ Agent] No alertable failures (CRITICAL/HIGH/MEDIUM). No Teams message sent.")
        return True

    # Get the Power Automate Workflow URL (override wins over SSM lookup)
    webhook_url = webhook_url_override or get_teams_webhook_url(ssm_client)

    # Build card
    card_payload = format_dq_alert_card(alertable, run_date, environment, agent_state)

    # Send
    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(card_payload),
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if response.status_code in (200, 202):
            print(f"[DQ Agent] Alert sent successfully to Teams. Status: {response.status_code}")
            return True
        else:
            print(f"[DQ Agent] Failed to send Teams alert. Status: {response.status_code}")
            return False
    except Exception as e:
        print(f"[DQ Agent] Exception sending Teams alert: {str(e)}")
        return False
