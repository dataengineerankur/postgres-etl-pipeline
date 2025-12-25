"""
Validation DAG for testing ARDOA RCA scenarios.
Tests various failure modes and race conditions.
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator


def _artifact_path(run_id: str) -> Path:
    """Return the path to the artifact for the given run_id."""
    base_dir = Path("/tmp/validation_artifacts")
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{run_id}.json"


def choose_scenario(**context: Any) -> str:
    """Read scenario from DAG run conf."""
    scenario = context["dag_run"].conf.get("scenario", "default")
    return scenario


def write_raw_payload(**context: Any) -> None:
    """Producer: Write payload to shared artifact location."""
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)

    data = {"status": "ok", "rows": [1, 2, 3]}

    if scenario == "non_atomic_write":
        # Simulate non-atomic write by writing in chunks with delay
        with open(path, "w", encoding="utf-8") as f:
            json_str = json.dumps(data)
            mid = len(json_str) // 2
            f.write(json_str[:mid])
            f.flush()  # Ensure partial write is flushed
            time.sleep(2)  # Delay to allow consumer to read incomplete file
            f.write(json_str[mid:])
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)


def consumer_read(**context: Any) -> str:
    """Consumer: Read payload from shared artifact location."""
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)

    # For non_atomic_write scenario, add retry logic with exponential backoff
    max_retries = 3
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            break  # Success
        except json.JSONDecodeError as e:
            if scenario == "non_atomic_write" and attempt < max_retries - 1:
                # Retry with exponential backoff for non-atomic write scenario
                time.sleep(retry_delay * (2 ** attempt))
                continue
            else:
                # Re-raise on last attempt or if not the expected scenario
                raise

    if scenario == "wrong_shape_200" and "rows" not in data:
        raise ValueError("Data contract failure: expected key `rows` in producer artifact")
    return "ok"


default_args = {
    "owner": "validation",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    "validation_rca_dag",
    default_args=default_args,
    description="Validation DAG for ARDOA RCA testing",
    schedule_interval=None,
    catchup=False,
    tags=["validation", "ardoa"],
) as dag:
    choose_task = PythonOperator(
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

    choose_task >> write_task >> read_task
