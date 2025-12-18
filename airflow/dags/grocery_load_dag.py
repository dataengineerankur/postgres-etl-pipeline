from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from grocery_lib.io_utils import RunPaths, read_json, ensure_dirs
from grocery_lib.notify_ardoa import notify_failure_to_ardoa
from grocery_lib.pg import upsert_stg_transactions


def load_to_postgres(**context) -> Dict[str, Any]:
    """Load enriched transaction data into Postgres.

    This function validates the presence of the enriched artifact before attempting
    to read it. If the file is missing (e.g., because the upstream enrichment task
    failed), a clear ``FileNotFoundError`` is raised so that the DAG fails with an
    informative message rather than a generic traceback.
    """
    run_id = context["run_id"]
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)

    enriched_path = os.path.join(paths.out_dir, "enriched.json")

    # Ensure the output directory exists (defensive, though it should already exist)
    ensure_dirs(paths.out_dir)

    if not os.path.isfile(enriched_path):
        raise FileNotFoundError(
            f"Enriched artifact not found at {enriched_path}. "
            "Upstream enrichment may have failed or the raw input was missing."
        )

    # Read the enriched payload
    payload = read_json(path=enriched_path)
    enriched: List[Dict[str, Any]] = payload.get("enriched", [])

    # Upsert the enriched rows into the staging table
    upsert_stg_transactions(enriched)

    return {"run_id": run_id, "rows": len(enriched)}


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
