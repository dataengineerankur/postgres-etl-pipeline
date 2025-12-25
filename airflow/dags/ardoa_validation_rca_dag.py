"""
Validation DAG for testing ARDOA RCA scenarios.
"""

import json
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator


def _artifact_path(run_id: str) -> str:
    """Generate the artifact path for a given run_id."""
    artifacts_dir = Path("/tmp/ardoa_validation_artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    return str(artifacts_dir / f"{run_id}.json")


def choose_scenario(**context: Any) -> str:
    """Choose a test scenario based on Airflow configuration."""
    scenario = context["dag_run"].conf.get("scenario", "happy_path")
    return scenario


def write_raw_payload(**context: Any) -> None:
    """Write a JSON payload to disk, simulating various failure modes."""
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)

    if scenario == "non_atomic_write":
        # Simulate non-atomic write: write incomplete JSON
        with open(path, "w", encoding="utf-8") as f:
            f.write('{"status": "incomplete...')
        time.sleep(0.1)  # Simulate slow write
        # In a real scenario, the write might complete later
        # but the consumer may read before it's done
    elif scenario == "wrong_shape_200":
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"status": "ok"}, f)  # Missing 'rows' key
    else:
        # happy_path or default
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"status": "ok", "rows": [{"id": 1}]}, f)


def consumer_read(**context: Any) -> str:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)

    # Handle non_atomic_write scenario: catch JSONDecodeError
    if scenario == "non_atomic_write":
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            # Expected for non_atomic_write scenario
            raise ValueError(
                f"Race condition detected: JSON parse failed (non_atomic_write scenario). "
                f"Error: {e}"
            )
    else:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if scenario == "wrong_shape_200" and "rows" not in data:
            raise ValueError("Data contract failure: expected key `rows` in producer artifact")
    
    return "ok"


default_args = {
    "owner": "ardoa",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "validation_rca_dag",
    default_args=default_args,
    description="Validation DAG for ARDOA RCA testing",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["validation", "ardoa"],
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
