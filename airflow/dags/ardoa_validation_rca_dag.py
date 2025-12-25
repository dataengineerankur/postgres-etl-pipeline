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
    return os.path.join(base, "validation_rca", run_id, "payload.json")


def choose_scenario(**context: Any) -> str:
    scenario = context["dag_run"].conf.get("scenario", "ok")
    return scenario


def write_raw_payload(**context: Any) -> None:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    data = {"run_id": run_id, "scenario": scenario, "rows": [1, 2, 3]}
    
    if scenario == "non_atomic_write":
        with open(path, "w", encoding="utf-8") as f:
            json_str = json.dumps(data)
            for i in range(0, len(json_str), 10):
                f.write(json_str[i:i+10])
                f.flush()
                time.sleep(0.1)
    elif scenario == "wrong_shape_200":
        data = {"run_id": run_id, "scenario": scenario}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)


def consumer_read(**context: Any) -> str:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)

    max_retries = 5
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
            else:
                raise

    return "ok"


default_args = {
    "owner": "ardoa",
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="validation_rca_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="RCA validation DAG for testing race conditions",
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
