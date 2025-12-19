from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from grocery_lib.io_utils import RunPaths, read_json, resolve_data_run_id
from grocery_lib.notify_ardoa import notify_failure_to_ardoa
from grocery_lib.pg import upsert_stg_transactions


def _conf_run_id(context: Dict[str, Any]) -> Optional[str]:
    """Best-effort extraction of dag_run.conf['run_id'] for diagnostics."""
    dag_run = context.get("dag_run")
    if dag_run is None:
        return None
    conf = getattr(dag_run, "conf", None)
    if not conf:
        return None
    run_id = conf.get("run_id")
    if isinstance(run_id, str) and run_id.strip() == "":
        return None
    return run_id


def load_to_postgres(**context: Any) -> Dict[str, Any]:
    conf_run_id = _conf_run_id(context)
    run_id = conf_run_id or resolve_data_run_id(context)
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)

    enriched_path = os.path.join(paths.out_dir, "enriched.json")

    # Verify that the enriched artifact exists before attempting to read it.
    if not os.path.exists(enriched_path):
        # Provide additional diagnostic context while preserving failure semantics.
        raw_path = os.path.join(paths.raw_dir, "transactions.json")
        staged_path = os.path.join(paths.staged_dir, "transactions.ndjson")

        missing: List[str] = [
            p
            for p in (raw_path, staged_path, enriched_path)
            if not os.path.exists(p)
        ]

        extra = ""
        if len(missing) > 1:
            extra += " Missing upstream artifacts: " + ", ".join(missing) + "."

        if not conf_run_id:
            extra += (
                " This DAG run did not receive a 'run_id' in dag_run.conf."
                " If you ran grocery_load_dag manually, pass the upstream data run_id"
                " (from grocery_ingest_dag/grocery_enrich_dag) in the run configuration."
            )

        raise FileNotFoundError(
            f"Enriched artifact not found at {enriched_path}. "
            "Upstream enrichment may have failed or the raw input was missing."
            + extra
        )

    # Read the enriched payload. ``read_json`` will raise its own
    # ``FileNotFoundError`` if the file disappears between the check and
    # the read, which is acceptable – the task will still fail.
    payload = read_json(path=enriched_path)

    transactions = payload.get("transactions")
    if not isinstance(transactions, list):
        raise RuntimeError(
            "Invalid enriched payload: expected JSON object with key 'transactions' (array)."
        )

    upsert_stg_transactions(transactions)
    return {"run_id": run_id, "enriched_path": enriched_path, "rows": len(transactions)}


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

    t_load = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    t_trigger_reconcile = TriggerDagRunOperator(
        task_id="trigger_reconcile",
        trigger_dag_id="grocery_reconcile_dag",
        conf={
            "scenario": scenario,
            "run_id": "{{ (ti.xcom_pull(task_ids='load_to_postgres') or {}).get('run_id') or dag_run.conf.get('run_id') }}",
        },
        wait_for_completion=False,
    )

    t_load >> t_trigger_reconcile
