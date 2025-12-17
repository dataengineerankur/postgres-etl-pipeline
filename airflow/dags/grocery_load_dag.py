from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from grocery_lib.io_utils import RunPaths, ensure_dirs, read_json
from grocery_lib.notify_ardoa import notify_failure_to_ardoa
from grocery_lib.pg import upsert_stg_transactions


def load_to_postgres(*, scenario: str, **context) -> Dict[str, Any]:
    run_id = context["run_id"]
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)
    ensure_dirs(paths)

    # Race-condition: enrich writes out/enriched.json; this task reads it.
    # In real pipelines you'd use dataset triggers or explicit file existence checks.
    enriched_path = os.path.join(paths.out_dir, "enriched.json")
    payload = read_json(path=enriched_path)
    enriched: List[Dict[str, Any]] = payload["enriched"]

    rows: List[Dict[str, Any]] = []
    for t in enriched:
        rows.append(
            {
                "run_id": run_id,
                "event_time": t["event_time"],
                "txn_id": t["txn_id"],
                "store_id": t["store_id"],
                "sku": t["sku"],
                "quantity": int(t["quantity"]),
                "unit_price_cents": int(t.get("unit_price_cents") or 0),
                "tender_type": t["tender_type"],
                "customer_id": t.get("customer_id"),
                "raw_payload": json.dumps(t),
            }
        )

    inserted = upsert_stg_transactions(rows)
    return {"run_id": run_id, "inserted": inserted, "scenario": scenario}


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
    description="Load staged/enriched transactions into Postgres",
    tags=["grocery", "mvp", "ardoa"],
) as dag:
    scenario = "{{ dag_run.conf.get('scenario', 'ok') }}"

    t_load = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres, op_kwargs={"scenario": scenario})
    t_trigger_reconcile = TriggerDagRunOperator(
        task_id="trigger_reconcile",
        trigger_dag_id="grocery_reconcile_dag",
        conf={"scenario": scenario},
        wait_for_completion=False,
    )

    t_load >> t_trigger_reconcile


