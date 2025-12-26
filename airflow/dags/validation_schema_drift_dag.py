from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_dir(run_id: str) -> str:
    return f"/opt/airflow/data/validation_schema_runs/{run_id}"


def _payload_path(run_id: str) -> str:
    return f"{_run_dir(run_id)}/producer/payload.json"


def choose_scenario(**context: Any) -> str:
    # Evolve "over time": default stays stable, but callers can opt into drift via dag_run.conf.
    return str((context["dag_run"].conf or {}).get("scenario", "schema_v1"))


def producer_write(**context: Any) -> str:
    """
    Producer writes a JSON payload that evolves by schema version.
    This simulates realistic schema drift / contract changes over time.
    """
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _payload_path(run_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if scenario == "schema_drift_v2":
        payload: Dict[str, Any] = {
            "schema_version": 2,
            "rows": [{"id": 1, "amount_cents": 12345, "currency": "USD"}],
        }
    else:
        payload = {"schema_version": 1, "rows": [{"id": 1, "amount": 123.45}]}

    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.replace(tmp, path)
    return path


def consumer_transform(**context: Any) -> str:
    """
    Consumer expects schema_version=1.
    Under drift, this fails deterministically and points to the producer boundary.
    """
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _payload_path(run_id)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    ver = int(data.get("schema_version", 0))
    rows = data.get("rows") or []
    
    if scenario == "schema_drift_v2":
        # Handle v2 schema: amount_cents and currency fields
        if ver != 2:
            raise ValueError(f"Data contract failure: expected schema_version=2 but got {ver}")
        if not rows or "amount_cents" not in rows[0]:
            raise ValueError("Data contract failure: expected rows[0].amount_cents for schema_version=2")
    else:
        # Handle v1 schema: amount field
        if ver != 1:
            raise ValueError(f"Data contract failure: expected schema_version=1 but got {ver}")
        if not rows or "amount" not in rows[0]:
            raise ValueError("Data contract failure: expected rows[0].amount for schema_version=1")
    
    return "ok"


default_args = {
    "owner": "validation",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=1),
}


with DAG(
    dag_id="validation_schema_drift_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    is_paused_upon_creation=False,
    catchup=False,
    default_args=default_args,
    description="Synthetic schema/contract drill: producer schema drift breaks consumer deterministically.",
) as dag:
    choose = PythonOperator(task_id="choose_scenario", python_callable=choose_scenario)
    prod = PythonOperator(task_id="producer_write", python_callable=producer_write)
    cons = PythonOperator(task_id="consumer_transform", python_callable=consumer_transform)

    choose >> prod >> cons


