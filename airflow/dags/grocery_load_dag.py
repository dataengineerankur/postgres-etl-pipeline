from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from grocery_lib.io_utils import RunPaths, read_json
from grocery_lib.notify_ardoa import notify_failure_to_ardoa
from grocery_lib.pg import upsert_stg_transactions


def load_to_postgres(*, scenario: str, **context) -> Dict[str, Any]:
    """Load enriched transaction data into Postgres.

    This task expects the upstream *enrich* task to have written an
    ``enriched.json`` file to the ``out`` directory for the current run.
    If the file is missing we raise a ``FileNotFoundError`` with a clear
    message, preserving the original failure semantics while providing
    better diagnostics.
    """
    run_id = context["run_id"]
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)

    enriched_path = os.path.join(paths.out_dir, "enriched.json")

    # Verify that the enriched artifact exists before attempting to read it.
    if not os.path.exists(enriched_path):
        raise FileNotFoundError(
            f"Enriched artifact not found at {enriched_path}. "
            "Upstream enrichment may have failed or the raw input was missing."
        )

    # Read the enriched payload. ``read_json`` will raise its own
    # ``FileNotFoundError`` if the file disappears between the check and
    # the read, which is acceptable – the task will still fail.
    payload = read_json(path=enriched_path)

    # ``payload`` is expected to contain a top‑level ``enriched`` list.
    # Guard against unexpected schema drift while preserving failure
    # semantics – we let any ``KeyError`` or ``TypeError`` propagate.
    enriched_records = payload["enriched"]

    # Insert records into the staging table.
    upsert_stg_transactions(enriched_records)

    return {
        "run_id": run_id,
        "rows_loaded": len(enriched_records),
        "scenario": scenario,
    }


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
    scenario = "{{ dag_run.conf.get('scenario', 'ok') }}"

    t_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        op_kwargs={"scenario": scenario},
    )
    t_trigger_reconcile = TriggerDagRunOperator(
        task_id="trigger_reconcile",
        trigger_dag_id="grocery_reconcile_dag",
        conf={"scenario": scenario},
        wait_for_completion=False,
    )
    t_load >> t_trigger_reconcile
