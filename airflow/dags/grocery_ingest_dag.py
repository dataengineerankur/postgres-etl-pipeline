from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict

import httpx
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from grocery_lib.failure import failure_plan
from grocery_lib.io_utils import (
    RunPaths,
    atomic_write_text,
    ensure_dirs,
    intentionally_non_atomic_write_text,
    resolve_data_run_id,
)
from grocery_lib.notify_ardoa import notify_failure_to_ardoa


def _run_id(context) -> str:
    return resolve_data_run_id(context)


def init_dirs(**context) -> Dict[str, Any]:
    run_id = _run_id(context)
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)
    ensure_dirs(paths)
    return {"run_id": run_id, "run_dir": paths.run_dir}


def fetch_transactions(*, scenario: str, **context) -> Dict[str, Any]:
    run_id = _run_id(context)
    plan = failure_plan(run_id=run_id, scenario=scenario)
    api_base = os.getenv("MOCK_POS_API_BASE_URL", "http://mock_pos_api:8099")
    url = f"{api_base}/transactions"

    with httpx.Client(timeout=10.0) as client:
        r = client.get(url, params={"run_id": run_id, "scenario": plan.scenario, "n": 40})
        if r.status_code >= 400:
            raise RuntimeError(f"mock_pos_api_error status={r.status_code} body={r.text}")
        # `malformed_json` returns a string (invalid JSON) instead of JSON object.
        return {"run_id": run_id, "scenario": plan.scenario, "raw_text": r.text}


def write_raw_file(*, scenario: str, **context) -> Dict[str, Any]:
    run_id = _run_id(context)
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)
    ensure_dirs(paths)
    raw_path = os.path.join(paths.raw_dir, "transactions.json")

    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="fetch_transactions") or {}
    raw_text = payload.get("raw_text") or ""

    # Inject partial write / race window.
    if scenario == "partial_write":
        intentionally_non_atomic_write_text(path=raw_path, text=raw_text, pause_s=1.5)
    else:
        atomic_write_text(path=raw_path, text=raw_text)

    return {"run_id": run_id, "raw_path": raw_path, "scenario": scenario}


default_args = {
    "owner": "grocery",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "on_failure_callback": notify_failure_to_ardoa,
}

with DAG(
    dag_id="grocery_ingest_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Ingest grocery store transactions from mock POS API -> raw file",
    tags=["grocery", "mvp", "ardoa"],
) as dag:
    scenario = "{{ dag_run.conf.get('scenario', 'ok') }}"

    t_init = PythonOperator(task_id="init_dirs", python_callable=init_dirs)
    t_fetch = PythonOperator(task_id="fetch_transactions", python_callable=fetch_transactions, op_kwargs={"scenario": scenario})
    t_write = PythonOperator(task_id="write_raw_file", python_callable=write_raw_file, op_kwargs={"scenario": scenario})

    t_trigger_validate = TriggerDagRunOperator(
        task_id="trigger_validate",
        trigger_dag_id="grocery_validate_dag",
        conf={
            "scenario": scenario,
            "run_id": "{{ run_id }}",
        },
        wait_for_completion=False,
    )

    t_init >> t_fetch >> t_write >> t_trigger_validate
