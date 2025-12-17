from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import httpx


@dataclass(frozen=True)
class NotifyConfig:
    endpoint: str = os.getenv("ARDOA_ENDPOINT", "http://host.docker.internal:8088/events/ingest")
    timeout_s: float = float(os.getenv("ARDOA_TIMEOUT_S", "60"))
    logs_base: str = os.getenv("ARDOA_LOGS_BASE", "/opt/airflow/logs")
    data_base: str = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")


def _log_uri(dag_id: str, run_id: str, task_id: str, try_number: int, logs_base: str) -> str:
    # Mirrors Airflow file task log layout in LocalExecutor.
    path = os.path.join(
        logs_base,
        f"dag_id={dag_id}",
        f"run_id={run_id}",
        f"task_id={task_id}",
        f"attempt={try_number}.log",
    )
    return f"file://{path}"


def notify_failure_to_ardoa(context: Dict[str, Any], *, config: Optional[NotifyConfig] = None) -> None:
    """
    Airflow `on_failure_callback` that notifies ARDOA.
    This is intentionally small and pipeline-agnostic.
    """
    cfg = config or NotifyConfig()
    dag = context.get("dag")
    ti = context.get("task_instance")
    if not dag or not ti:
        return

    dag_id = dag.dag_id
    run_id = ti.run_id
    task_id = ti.task_id
    try_number = int(getattr(ti, "try_number", 1) or 1)

    event = {
        "event_id": f"airflow:{dag_id}:{run_id}:{task_id}:{try_number}",
        "platform": "airflow",
        "pipeline_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "try_number": try_number,
        "detected_at": datetime.utcnow().isoformat(),
        "status": "failed",
        "log_uri": _log_uri(dag_id, run_id, task_id, try_number, cfg.logs_base),
        "artifact_uris": [
            f"file://{os.path.join(cfg.data_base, 'grocery_runs', run_id, 'raw', 'transactions.json')}",
            f"file://{os.path.join(cfg.data_base, 'grocery_runs', run_id, 'staged', 'transactions.ndjson')}",
            f"file://{os.path.join(cfg.data_base, 'grocery_runs', run_id, 'out', 'reconcile.json')}",
        ],
        "metadata": {"exception": str(context.get("exception") or "")},
    }

    with httpx.Client(timeout=cfg.timeout_s) as client:
        # If this fails, we don't want the callback itself to crash the task.
        try:
            client.post(cfg.endpoint, json=event)
        except Exception:
            return


