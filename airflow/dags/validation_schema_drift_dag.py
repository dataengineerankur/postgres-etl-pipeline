from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from grocery_lib.notify_ardoa import notify_failure_to_ardoa


def _payload_path(run_id: str) -> str:
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    return os.path.join(base, run_id, "raw", "payload.json")


def choose_scenario(**context) -> str:
    scenario = context["dag_run"].conf.get("scenario", "ok")
    return scenario


def producer_emit(**context):
    run_id = context["run_id"]
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _payload_path(run_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if scenario == "schema_drift_v2":
        payload = {
            "schema_version": 2,
            "rows": [
                {"amount": 100, "currency": "USD", "new_field": "value"}
            ]
        }
    else:
        payload = {
            "schema_version": 1,
            "rows": [
                {"amount": 100, "currency": "USD"}
            ]
        }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return "ok"


def consumer_transform(**context):
    run_id = context["run_id"]
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _payload_path(run_id)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    ver = int(data.get("schema_version", 0))
    if scenario == "schema_drift_v2" and ver != 2:
        raise ValueError(f"Data contract failure: expected schema_version=2 but got {ver}")

    # v1 contract
    rows = data.get("rows") or []
    if not rows or "amount" not in rows[0]:
        raise ValueError("Data contract failure: expected rows[0].amount for schema_version=1")
    return "ok"


default_args = {
    "owner": "grocery",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "on_failure_callback": notify_failure_to_ardoa,
}

with DAG(
    dag_id="validation_schema_drift_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Test schema drift validation",
    tags=["grocery", "validation", "ardoa"],
) as dag:
    t_choose = PythonOperator(
        task_id="choose_scenario",
        python_callable=choose_scenario,
    )

    t_producer = PythonOperator(
        task_id="producer_emit",
        python_callable=producer_emit,
    )

    t_consumer = PythonOperator(
        task_id="consumer_transform",
        python_callable=consumer_transform,
    )

    t_choose >> t_producer >> t_consumer
