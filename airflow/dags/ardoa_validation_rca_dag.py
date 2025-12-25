from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from grocery_lib.io_utils import (
    atomic_write_text,
    intentionally_non_atomic_write_text,
)
from grocery_lib.notify_ardoa import notify_failure_to_ardoa


def _artifact_path(run_id: str) -> str:
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    return os.path.join(base, "validation_rca_runs", run_id, "payload.json")


def choose_scenario(**context: Any) -> str:
    dag_run = context.get("dag_run")
    if dag_run and hasattr(dag_run, "conf"):
        conf = dag_run.conf or {}
        scenario = conf.get("scenario", "ok")
    else:
        scenario = "ok"
    return scenario


def write_raw_payload(**context: Any) -> str:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)
    
    payload = {"status": "ok", "rows": [{"id": 1, "value": "test"}]}
    text = json.dumps(payload, indent=2)
    
    if scenario == "non_atomic_write":
        intentionally_non_atomic_write_text(path=path, text=text, pause_s=1.5)
    else:
        atomic_write_text(path=path, text=text)
    
    return path


def consumer_read(**context: Any) -> str:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)

    # Handle non-atomic write scenario with retry logic
    max_retries = 3
    retry_delay = 1.0
    
    for attempt in range(max_retries):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            break
        except json.JSONDecodeError as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            else:
                raise

    if scenario == "wrong_shape_200" and "rows" not in data:
        raise ValueError("Data contract failure: expected key `rows` in producer artifact")
    return "ok"


default_args = {
    "owner": "ardoa",
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    "on_failure_callback": notify_failure_to_ardoa,
}

with DAG(
    dag_id="validation_rca_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="ARDOA validation RCA test DAG for race condition scenarios",
    tags=["ardoa", "validation", "rca"],
) as dag:
    t_choose = PythonOperator(
        task_id="choose_scenario",
        python_callable=choose_scenario,
    )
    
    t_write = PythonOperator(
        task_id="write_raw_payload",
        python_callable=write_raw_payload,
    )
    
    t_read = PythonOperator(
        task_id="consumer_read",
        python_callable=consumer_read,
    )
    
    t_choose >> t_write >> t_read
