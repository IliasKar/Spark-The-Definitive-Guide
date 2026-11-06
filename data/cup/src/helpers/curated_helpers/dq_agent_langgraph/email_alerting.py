"""
DQ Agent — Email Alerting Module
================================
Sends DQ failure alerts (findings + AI root-cause analysis) by email via SMTP.

This is the delivery path used while MS Teams webhook/Workflow posting is
blocked at the tenant level. It is self-contained: it builds its own HTML
body (no Jinja template / Volume needed) and sends through the corporate SMTP
relay whose settings live in the Databricks secret scope `email-smtp`.

The module itself does NOT touch dbutils (so it stays importable/testable);
the caller resolves the SMTP settings from secrets and passes them in.
"""

import html
import smtplib
from email.message import EmailMessage
from typing import Optional

# Max findings rendered with full detail in the email body. The agent is also
# prompted to produce at most this many detailed entries (MAX_DETAILED_FINDINGS
# in graph.py); the remaining failing checks are always listed in the summary
# table below so nothing is lost.
MAX_DETAILED_IN_EMAIL = 5


def _esc(value) -> str:
    """HTML-escape a value for safe inclusion in the email body."""
    return html.escape(str(value)) if value is not None else ""


def build_email_html(
    failures: list,
    run_date: str,
    environment: str,
    agent_state: Optional[dict] = None,
) -> str:
    """
    Build an HTML email body summarizing the DQ failures and the agent's
    root-cause analysis.

    Parameters
    ----------
    failures : list[dict]
        Failing DQ finding dicts.
    run_date : str
        BusinessDate checked.
    environment : str
        Environment prefix (d, qa, p).
    agent_state : dict, optional
        Final LangGraph agent state (summary, root_causes, recommendations,
        differing_columns, troubleshooting_sql, findings_analysis, ...).
    """
    env_label = {"d": "DEV", "qa": "QA", "p": "PROD"}.get(environment, str(environment).upper())

    sev_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    sev_color = {
        "CRITICAL": "#b30000",
        "HIGH": "#e67300",
        "MEDIUM": "#b38f00",
        "LOW": "#666666",
    }

    # Counts by severity
    counts: dict = {}
    for f in failures:
        sev = f["severity"] if isinstance(f, dict) else f.severity
        counts[sev] = counts.get(sev, 0) + 1
    counts_str = ", ".join(
        f"{s}: {counts[s]}" for s in sorted(counts, key=lambda s: sev_order.get(s, 9))
    )

    parts = [
        "<html><body style=\"font-family:Segoe UI,Arial,sans-serif;color:#222;\">",
        f"<h2 style=\"color:#b30000;\">🚨 Data Quality Alert — {_esc(env_label)}</h2>",
        "<table style=\"border-collapse:collapse;\">",
        f"<tr><td><b>BusinessDate</b></td><td>{_esc(run_date)}</td></tr>",
        f"<tr><td><b>Environment</b></td><td>{_esc(env_label)}</td></tr>",
        f"<tr><td><b>Total failures</b></td><td>{len(failures)}</td></tr>",
        f"<tr><td><b>By severity</b></td><td>{_esc(counts_str)}</td></tr>",
        "</table>",
    ]

    # AI analysis section
    if agent_state:
        summary = agent_state.get("summary", "")
        if summary:
            parts.append("<h3>🤖 AI Root-Cause Analysis</h3>")
            parts.append(f"<p>{_esc(summary)}</p>")

        findings_analysis = agent_state.get("findings_analysis") or []
        if findings_analysis:
            detailed = findings_analysis[:MAX_DETAILED_IN_EMAIL]
            remaining = len(findings_analysis) - len(detailed)
            heading = f"Per-finding analysis (top {len(detailed)})"
            parts.append(f"<h3>{heading}</h3>")
            for fa in detailed:
                sev = fa.get("severity", "")
                color = sev_color.get(sev, "#222")
                parts.append(
                    f"<div style=\"margin-bottom:12px;padding:8px;"
                    f"border-left:4px solid {color};background:#f7f7f7;\">"
                )
                parts.append(
                    f"<b style=\"color:{color};\">[{_esc(sev)}]</b> "
                    f"{_esc(fa.get('check_name'))} | {_esc(fa.get('table_name'))} "
                    f"(owner: {_esc(fa.get('issue_owner'))})<br>"
                )
                if fa.get("root_cause"):
                    parts.append(f"<b>Root cause:</b> {_esc(fa.get('root_cause'))}<br>")
                if fa.get("differing_columns"):
                    parts.append(
                        f"<b>Differing columns:</b> "
                        f"{_esc(', '.join(fa.get('differing_columns')))}<br>"
                    )
                if fa.get("landed_table_origin"):
                    parts.append(
                        f"<b>Landed origin:</b> {_esc(fa.get('landed_table_origin'))}<br>"
                    )
                if fa.get("troubleshooting_sql"):
                    parts.append(
                        f"<b>Troubleshooting SQL:</b>"
                        f"<pre style=\"white-space:pre-wrap;background:#eee;padding:6px;\">"
                        f"{_esc(fa.get('troubleshooting_sql'))}</pre>"
                    )
                if fa.get("recommendation"):
                    parts.append(f"<b>Fix:</b> {_esc(fa.get('recommendation'))}<br>")
                parts.append("</div>")

            if remaining > 0:
                parts.append(
                    f"<p><i>+ {remaining} more analyzed finding(s) not shown in detail. "
                    f"All failing checks are listed in the table below.</i></p>"
                )

    # Full findings list (every failing check, so nothing is lost)
    parts.append("<h3>All failing checks</h3>")
    parts.append(
        "<table style=\"border-collapse:collapse;font-size:13px;\" border=\"1\" cellpadding=\"4\">"
    )
    parts.append(
        "<tr style=\"background:#333;color:#fff;\">"
        "<th>Severity</th><th>Check</th><th>Table</th><th>Details</th></tr>"
    )
    sorted_failures = sorted(
        failures,
        key=lambda f: sev_order.get(
            f["severity"] if isinstance(f, dict) else f.severity, 9
        ),
    )
    for f in sorted_failures:
        sev = f["severity"] if isinstance(f, dict) else f.severity
        check = f["check_name"] if isinstance(f, dict) else f.check_name
        table = f["table_name"] if isinstance(f, dict) else f.table_name
        details = f["details"] if isinstance(f, dict) else f.details
        color = sev_color.get(sev, "#222")
        parts.append(
            f"<tr><td style=\"color:{color};font-weight:bold;\">{_esc(sev)}</td>"
            f"<td>{_esc(check)}</td><td>{_esc(table)}</td>"
            f"<td>{_esc(str(details)[:300])}</td></tr>"
        )
    parts.append("</table>")
    parts.append("</body></html>")
    return "".join(parts)


def send_email_alert(
    failures: list,
    run_date: str,
    environment: str,
    smtp_settings: dict,
    recipients_to: list,
    recipients_cc: Optional[list] = None,
    agent_state: Optional[dict] = None,
) -> bool:
    """
    Send a DQ alert email via SMTP.

    Only CRITICAL, HIGH, and MEDIUM severity failures are included.

    Parameters
    ----------
    failures : list
        Failing DQ findings (dicts or DQResult objects).
    run_date : str
        BusinessDate checked.
    environment : str
        Environment prefix (d, qa, p).
    smtp_settings : dict
        {"ip", "port", "from", "login", "password"} — resolved by the caller
        from the `email-smtp` Databricks secret scope.
    recipients_to : list[str]
        Primary recipient email addresses.
    recipients_cc : list[str], optional
        CC recipients.
    agent_state : dict, optional
        Final LangGraph agent state for the analysis section.

    Returns
    -------
    bool
        True if the email was sent successfully.
    """
    # Filter alertable severities
    alertable = []
    for f in failures:
        sev = f["severity"] if isinstance(f, dict) else f.severity
        if sev in ("CRITICAL", "HIGH", "MEDIUM"):
            alertable.append(f)

    if not alertable:
        print("[DQ Agent] No alertable failures (CRITICAL/HIGH/MEDIUM). No email sent.")
        return True

    recipients_cc = recipients_cc or []
    body_html = build_email_html(alertable, run_date, environment, agent_state)

    env_label = {"d": "DEV", "qa": "QA", "p": "PROD"}.get(environment, str(environment).upper())
    msg = EmailMessage()
    msg["From"] = smtp_settings["from"]
    msg["To"] = ", ".join(recipients_to)
    if recipients_cc:
        msg["Cc"] = ", ".join(recipients_cc)
    msg["Subject"] = (
        f"[DQ Alert][{env_label}] {len(alertable)} data quality failures "
        f"for BusinessDate {run_date}"
    )
    msg.set_content(
        "This alert requires an HTML-capable email client.", subtype="plain"
    )
    msg.add_alternative(body_html, subtype="html")

    all_recipients = list(recipients_to) + list(recipients_cc)
    try:
        with smtplib.SMTP(smtp_settings["ip"], int(smtp_settings["port"])) as server:
            server.starttls()
            server.login(smtp_settings["login"], smtp_settings["password"])
            server.send_message(msg, to_addrs=all_recipients)
        print(f"[DQ Agent] Alert email sent to {', '.join(all_recipients)}.")
        return True
    except Exception as e:
        print(f"[DQ Agent] Exception sending alert email: {str(e)}")
        return False
