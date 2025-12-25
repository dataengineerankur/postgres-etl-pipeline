import json
import os
import time
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator


def _artifact_path(run_id: str) -> str:
    """Generate artifact path for a given run_id."""
    base_dir = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    artifacts_dir = os.path.join(base_dir, "artifacts")
    os.makedirs(artifacts_dir, exist_ok=True)
    return os.path.join(artifacts_dir, f"{run_id}.json")


def choose_scenario(**context: Any) -> str:
    """Choose the test scenario from DAG configuration."""
    scenario = context["dag_run"].conf.get("scenario", "happy_path")
    return scenario


def write_raw_payload(**context: Any) -> None:
    """Write a JSON payload to disk. May simulate non-atomic writes."""
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)

    payload = {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

    if scenario == "wrong_shape_200":
        payload = {"status": "ok", "data": [1, 2, 3]}
    elif scenario == "non_atomic_write":
        # Simulate non-atomic write by writing partial JSON
        with open(path, "w", encoding="utf-8") as f:
            f.write('{"status": "incomplete')
            f.flush()
            time.sleep(0.5)  # Simulate slow write
            f.write('", "timestamp": "')
            f.write(datetime.utcnow().isoformat())
            f.write('"}')
        return

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def consumer_read(**context: Any) -> str:
    """Read and validate the JSON artifact. Handles non-atomic writes with retry logic."""
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)

    # Add retry logic for non-atomic write scenario
    max_retries = 3
    retry_delay = 0.2  # seconds
    
    for attempt in range(max_retries):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            break  # Success, exit retry loop
        except json.JSONDecodeError as e:
            if attempt < max_retries - 1:
                # Wait and retry
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                # Final attempt failed, re-raise the exception
                raise

    if scenario == "wrong_shape_200" and "rows" not in data:
        raise ValueError("Data contract failure: expected key `rows` in producer artifact")
    return "ok"


default_args = {
    "owner": "ardoa",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    "validation_rca_dag",
    default_args=default_args,
    description="Validation DAG for testing RCA scenarios",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["validation", "rca", "testing"],
) as dag:

    choose_scenario_task = PythonOperator(
        task_id="choose_scenario",
        python_callable=choose_scenario,
    )

    write_task = PythonOperator(
        task_id="write_raw_payload",
        python_callable=write_raw_payload,
    )

    read_task = PythonOperator(
        task_id="consumer_read",
        python_callable=consumer_read,
    )

    choose_scenario_task >> write_task >> read_task
