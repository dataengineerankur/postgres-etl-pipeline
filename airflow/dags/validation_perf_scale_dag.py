from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_dir(run_id: str) -> str:
    return f"/opt/airflow/data/validation_scale_runs/{run_id}"


def _artifact_path(run_id: str) -> str:
    return f"{_run_dir(run_id)}/artifacts/large_payload.json"


def fanout_task(i: int):
    def _inner(**_: Any) -> str:
        # Very small work; the fanout itself simulates many tasks / many logs in real pipelines.
        return f"ok:{i}"

    return _inner


def generate_large_artifact(**context: Any) -> str:
    """
    Create a large-but-deterministic artifact file to stress log parsing / excerpting / context truncation.
    """
    run_id = context["dag_run"].run_id
    path = _artifact_path(run_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Large payload: 5k rows of repetitive data (kept moderate to avoid OOM on dev laptops).
    payload: Dict[str, Any] = {"schema_version": 1, "rows": [{"id": i, "value": "x" * 64} for i in range(5000)]}
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.replace(tmp, path)

    # Also emit a big log line (simulates noisy upstream systems).
    print("large_log_prefix:" + ("y" * 20000))
    return path


def aggregate_and_fail(**context: Any) -> None:
    """
    Deterministic failure that references the large artifact (forces the system to fetch + reason about it).
    """
    run_id = context["dag_run"].run_id
    path = _artifact_path(run_id)
    # Fail with an error that mentions the artifact path (so evidence packs capture it).
    raise RuntimeError(f"Performance/scale drill: processing budget exceeded while handling artifact={path}")


default_args = {
    "owner": "validation",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=1),
}


with DAG(
    dag_id="validation_perf_scale_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    is_paused_upon_creation=False,
    catchup=False,
    # Keep this drill from starving the rest of the system on a dev laptop.
    max_active_tasks=4,
    max_active_runs=1,
    default_args=default_args,
    description="Synthetic perf/scale drill: many tasks + large artifacts/logs; deterministic failure at aggregate step.",
) as dag:
    # Fanout -> join -> heavy artifact -> fail
    # Small fanout to simulate scale without saturating LocalExecutor.
    fanouts = [PythonOperator(task_id=f"fanout_{i}", python_callable=fanout_task(i)) for i in range(6)]
    gen = PythonOperator(task_id="generate_large_artifact", python_callable=generate_large_artifact)
    agg = PythonOperator(task_id="aggregate_and_fail", python_callable=aggregate_and_fail)

    for t in fanouts:
        t >> gen
    gen >> agg


