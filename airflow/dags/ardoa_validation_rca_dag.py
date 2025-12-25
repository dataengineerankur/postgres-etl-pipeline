from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator


def _artifact_path(run_id: str) -> str:
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    return os.path.join(base, "validation_runs", run_id, "payload.json")


def choose_scenario(**context: Any) -> str:
    conf = context.get("dag_run").conf or {}
    scenario = conf.get("scenario", "ok")
    return scenario


def write_raw_payload(**context: Any) -> None:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    test_data = {"status": "ok", "rows": [{"id": 1}, {"id": 2}]}
    
    if scenario == "wrong_shape_200":
        test_data = {"status": "ok", "data": [{"id": 1}]}
    
    text = json.dumps(test_data, indent=2)
    
    if scenario == "non_atomic_write":
        with open(path, "w", encoding="utf-8") as f:
            mid = max(1, len(text) // 2)
            f.write(text[:mid])
            f.flush()
            os.fsync(f.fileno())
            time.sleep(2.0)
            f.write(text[mid:])
            f.flush()
            os.fsync(f.fileno())
    else:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)


def consumer_read(**context: Any) -> str:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)

    max_retries = 3
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            if scenario == "wrong_shape_200" and "rows" not in data:
                raise ValueError("Data contract failure: expected key `rows` in producer artifact")
            return "ok"
        except json.JSONDecodeError as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise


default_args = {
    "owner": "validation",
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="validation_rca_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Validation DAG for testing RCA scenarios (race conditions, data shape issues)",
    tags=["validation", "ardoa"],
) as dag:
    t_choose = PythonOperator(task_id="choose_scenario", python_callable=choose_scenario)
    t_write = PythonOperator(task_id="write_raw_payload", python_callable=write_raw_payload)
    t_read = PythonOperator(task_id="consumer_read", python_callable=consumer_read)

    t_choose >> t_write >> t_read
