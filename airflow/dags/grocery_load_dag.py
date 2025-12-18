from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from grocery_lib.io_utils import RunPaths, read_json, ensure_dirs
from grocery_lib.notify_ardoa import notify_failure_to_ardoa
from grocery_lib.pg import upsert_stg_transactions


def load_to_postgres(**context) -> Dict[str, Any]:
    """Load enriched transaction data into Postgres.

    This function constructs the path to the ``enriched.json`` artifact using
    the same ``RunPaths`` logic as the upstream ``enrich_transactions`` task.
    By relying on ``RunPaths`` we guarantee that both DAGs resolve the file
    location identically, eliminating path mismatches that caused the previous
    ``FileNotFoundError``.
    """
    run_id = context["run_id"]
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)
    # Ensure the directory structure exists (defensive, no side‑effects)
    ensure_dirs(paths)

    enriched_path = os.path.join(paths.out_dir, "enriched.json")
    # The read_json helper will raise a clear FileNotFoundError if the file is missing.
    payload = read_json(path=enriched_path)
    enriched_rows = payload.get("enriched", [])

    # Insert enriched rows into the staging table.
    upsert_stg_transactions(enriched_rows)

    return {"run_id": run_id, "loaded_rows": len(enriched_rows)}


default_args = {
    "owner": "grocery",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "on_failure_callback": notify_failure_to_ardoa,
}

with DAG(
    dag_id="grocery_load_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Load enriched transactions into Postgres",
    tags=["grocery", "mvp", "ardoa"],
) as dag:
    t_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )
    t_trigger_reconcile = TriggerDagRunOperator(
        task_id="trigger_reconcile",
        trigger_dag_id="grocery_reconcile_dag",
        wait_for_completion=False,
    )
    t_load >> t_trigger_reconcile
