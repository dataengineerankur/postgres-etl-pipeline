from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from grocery_lib.io_utils import RunPaths, ensure_dirs, atomic_write_text
from grocery_lib.notify_ardoa import notify_failure_to_ardoa
from grocery_lib.pg import fetch_all


def reconcile(**context) -> Dict[str, Any]:
    run_id = context["run_id"]
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)
    ensure_dirs(paths)
    out_path = os.path.join(paths.out_dir, "reconcile.json")

    # Canary: compare inserted rows this run vs last-known-good watermark (simple heuristic).
    count_rows = fetch_all(
        "SELECT count(*) FROM grocery.stg_transactions WHERE run_id = :run_id",
        {"run_id": run_id},
    )[0][0]

    # Intentional “data mismatch” failure when count is unexpectedly low.
    if int(count_rows) < 10:
        raise RuntimeError(f"canary_failed: too_few_rows run_id={run_id} count={count_rows}")

    result = {"run_id": run_id, "ok": True, "stg_rows": int(count_rows)}
    atomic_write_text(path=out_path, text=json.dumps(result))
    return result


default_args = {
    "owner": "grocery",
    "retries": 0,
    "on_failure_callback": notify_failure_to_ardoa,
}

with DAG(
    dag_id="grocery_reconcile_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Canary reconciliation checks",
    tags=["grocery", "mvp", "ardoa"],
) as dag:
    t_reconcile = PythonOperator(task_id="reconcile", python_callable=reconcile)


