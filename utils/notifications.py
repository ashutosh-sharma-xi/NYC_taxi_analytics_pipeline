"""Slack notifications for the pipeline (via an Incoming Webhook + requests).

Two entry points the Airflow DAG uses:
  - send_success(...)           : posted by the final task on a clean run
  - airflow_failure_callback(ctx): wired as on_failure_callback, fires on ANY task
                                    failure with a direct link to that task's log

Config (environment variables — never hard-coded):
  SLACK_WEBHOOK_URL   Incoming Webhook URL. If unset, notifications are logged
                      and skipped (so local/dev runs don't fail for lack of Slack).
  ENVIRONMENT         Optional label shown in the message (e.g. 'prod', 'dev').

Create a webhook at https://api.slack.com/messaging/webhooks and store the URL
in Airflow's environment (or an Airflow Variable/Connection) as SLACK_WEBHOOK_URL.
"""
from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger(__name__)

WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
_TIMEOUT = 15


def _post(text: str, blocks: list) -> bool:
    """POST to Slack. No-op (logged) if no webhook is configured."""
    if not WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set — skipping Slack post: %s", text)
        return False
    resp = requests.post(WEBHOOK_URL, json={"text": text, "blocks": blocks}, timeout=_TIMEOUT)
    resp.raise_for_status()
    return True


def send_success(dag_id: str, run_date: str, metrics: dict | None = None,
                 run_url: str | None = None) -> bool:
    """Post a green success message with optional run metrics and a link."""
    header = f":white_check_mark: *{dag_id}* succeeded — `{run_date}`  _({ENVIRONMENT})_"
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": header}}]
    if metrics:
        body = "\n".join(f"• *{k}*: {v}" for k, v in metrics.items())
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": body}})
    if run_url:
        blocks.append({"type": "context",
                       "elements": [{"type": "mrkdwn", "text": f"<{run_url}|View run>"}]})
    return _post(header, blocks)


def send_failure(dag_id: str, task_id: str, run_date: str,
                 log_url: str | None = None, error: str | None = None) -> bool:
    """Post a red failure message with the error and a link to the task log."""
    header = f":red_circle: *{dag_id}* FAILED at `{task_id}` — `{run_date}`  _({ENVIRONMENT})_"
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": header}}]
    if error:
        snippet = str(error)[:600]
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": f"```{snippet}```"}})
    if log_url:
        blocks.append({"type": "context",
                       "elements": [{"type": "mrkdwn",
                                     "text": f":mag: <{log_url}|Open the failing task's log>"}]})
    return _post(header, blocks)


# --- Airflow adapters (take the task context) -------------------------------

def airflow_failure_callback(context) -> None:
    """Wire as default_args['on_failure_callback'] — fires on any task failure.

    Pulls the failing task, run date, the exception, and the task-log URL
    (the 'where is the error' link) straight from the Airflow context.
    """
    ti = context.get("task_instance")
    try:
        send_failure(
            dag_id=context["dag"].dag_id,
            task_id=getattr(ti, "task_id", "unknown"),
            run_date=str(context.get("ds", "")),
            log_url=getattr(ti, "log_url", None),
            error=context.get("exception"),
        )
    except Exception as exc:  # never let alerting break the DAG further
        log.error("Failed to send Slack failure alert: %s", exc)
